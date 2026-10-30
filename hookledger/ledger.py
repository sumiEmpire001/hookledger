import hashlib
import hmac
import json
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from urllib.parse import urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler


class LeaseLost(RuntimeError):
    pass


def sign(secret, timestamp, body):
    return hmac.new(secret.encode(), str(timestamp).encode() + b"." + body, hashlib.sha256).hexdigest()


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value):
        raise ValueError("Identifier must be 1..128 letters, numbers, dots, underscores or hyphens")


def no_constant(_):
    raise ValueError("Nonfinite JSON values are unsupported")


class Ledger:
    def __init__(self, path, secret, clock=time.time):
        if not isinstance(secret, str) or len(secret) < 16:
            raise ValueError("An ingestion secret of at least 16 characters is required")
        if str(path) == ":memory:":
            raise ValueError("Use a filesystem database")
        self.path, self.secret, self.clock = str(path), secret, clock
        with self.db() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,kind TEXT NOT NULL,body BLOB NOT NULL,received REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS subscribers(id TEXT PRIMARY KEY,url TEXT NOT NULL,secret TEXT NOT NULL,enabled INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS deliveries(id INTEGER PRIMARY KEY,event_id TEXT NOT NULL REFERENCES events(id),
                subscriber_id TEXT NOT NULL REFERENCES subscribers(id),state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,replays INTEGER NOT NULL DEFAULT 0,
                available REAL NOT NULL,token TEXT,lease REAL,error TEXT,
                UNIQUE(event_id,subscriber_id));
            CREATE TABLE IF NOT EXISTS history(seq INTEGER PRIMARY KEY,delivery_id INTEGER NOT NULL,at REAL NOT NULL,event TEXT NOT NULL,detail TEXT);
            """)

    @contextmanager
    def db(self, write=False):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except BaseException:
            if write:
                db.rollback()
            raise
        finally:
            db.close()

    def record(self, db, delivery_id, event, detail=None):
        db.execute("INSERT INTO history(delivery_id,at,event,detail) VALUES(?,?,?,?)", (delivery_id, self.clock(), event, detail))

    def subscribe(self, name, url, secret):
        identifier(name)
        parsed = urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
            raise ValueError("Subscriber URL must be HTTP(S), without credentials or fragment")
        if not isinstance(secret, str) or len(secret) < 16:
            raise ValueError("Subscriber secret must have at least 16 characters")
        with self.db(True) as db:
            db.execute("INSERT INTO subscribers VALUES(?,?,?,1)", (name, url, secret))

    def enable(self, name, enabled):
        if type(enabled) is not bool:
            raise ValueError("enabled must be boolean")
        with self.db(True) as db:
            if db.execute("UPDATE subscribers SET enabled=? WHERE id=?", (int(enabled), name)).rowcount != 1:
                raise ValueError("Subscriber not found")

    def ingest(self, body, timestamp, signature):
        if not isinstance(body, bytes) or not 0 < len(body) <= 65536:
            raise ValueError("Body must be 1..65536 bytes")
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, str)) or not re.fullmatch(r"[0-9]{1,12}", str(timestamp)):
            raise ValueError("Invalid signed timestamp")
        if abs(self.clock() - int(timestamp)) > 300:
            raise ValueError("Signed event timestamp is outside the five-minute window")
        if not isinstance(signature, str) or not re.fullmatch(r"[0-9a-f]{64}", signature) or not hmac.compare_digest(sign(self.secret, timestamp, body), signature):
            raise ValueError("Invalid event signature")
        event = json.loads(body, parse_constant=no_constant)
        if not isinstance(event, dict) or not {"id", "type", "data"} <= set(event):
            raise ValueError("Event requires id, type and data")
        identifier(event["id"])
        identifier(event["type"])
        with self.db(True) as db:
            existing = db.execute("SELECT body FROM events WHERE id=?", (event["id"],)).fetchone()
            if existing:
                if bytes(existing["body"]) != body:
                    raise ValueError("Event ID already belongs to different signed bytes")
                return {"id": event["id"], "duplicate": True}
            db.execute("INSERT INTO events VALUES(?,?,?,?)", (event["id"], event["type"], body, self.clock()))
            for subscriber in db.execute("SELECT id FROM subscribers WHERE enabled=1").fetchall():
                db.execute("INSERT INTO deliveries(event_id,subscriber_id,state,available) VALUES(?,?,'queued',?)", (event["id"], subscriber["id"], self.clock()))
            return {"id": event["id"], "duplicate": False}

    def queue(self, event_id, subscriber_id):
        """Attach a new subscriber to an already ingested event; existing pairs are unchanged."""
        with self.db(True) as db:
            db.execute("INSERT OR IGNORE INTO deliveries(event_id,subscriber_id,state,available) VALUES(?,?,'queued',?)", (event_id, subscriber_id, self.clock()))

    def claim(self):
        with self.db(True) as db:
            now = self.clock()
            for expired in db.execute("SELECT * FROM deliveries WHERE state='running' AND lease<=?", (now,)).fetchall():
                state = "dead" if expired["attempts"] >= 3 else "queued"
                db.execute("UPDATE deliveries SET state=?,available=?,token=NULL,lease=NULL,error='lease expired' WHERE id=?", (state, now, expired["id"]))
                self.record(db, expired["id"], "lease_expired", state)
            row = db.execute("""SELECT d.*,s.url,s.secret,e.body FROM deliveries d
                JOIN subscribers s ON s.id=d.subscriber_id JOIN events e ON e.id=d.event_id
                WHERE d.state='queued' AND d.available<=? AND s.enabled=1 ORDER BY d.available,d.id LIMIT 1""", (now,)).fetchone()
            if not row:
                return None
            job = dict(row)
            job.update(token=uuid.uuid4().hex, lease=now + 60, attempts=row["attempts"] + 1)
            db.execute("UPDATE deliveries SET state='running',attempts=?,token=?,lease=? WHERE id=?", (job["attempts"], job["token"], job["lease"], job["id"]))
            self.record(db, job["id"], "claimed", job["url"])
            return job

    def finish(self, job, status=None, error=None):
        success = type(status) is int and 200 <= status < 300 and error is None
        with self.db(True) as db:
            row = db.execute("SELECT * FROM deliveries WHERE id=?", (job["id"],)).fetchone()
            if not row or row["state"] != "running" or row["token"] != job["token"] or row["lease"] <= self.clock():
                raise LeaseLost("Delivery lease no longer belongs to this worker")
            state = "done" if success else "dead" if row["attempts"] >= 3 else "queued"
            detail = None if success else str(error or f"HTTP {status}")[:1000]
            db.execute("UPDATE deliveries SET state=?,available=?,token=NULL,lease=NULL,error=? WHERE id=?", (state, self.clock() + min(60, 2 ** row["attempts"]), detail, job["id"]))
            self.record(db, job["id"], "delivered" if success else "failed", str(status) if success else detail)
            return state

    def deliver_one(self, sender=None):
        job = self.claim()
        if not job:
            return None
        timestamp = str(int(self.clock()))
        headers = {"Content-Type": "application/json", "X-HookLedger-Timestamp": timestamp,
                   "X-HookLedger-Signature": sign(job["secret"], timestamp, bytes(job["body"])),
                   "Idempotency-Key": job["event_id"] + ":" + job["subscriber_id"]}
        try:
            status = (sender or http_sender)(job["url"], bytes(job["body"]), headers)
        except Exception as exc:
            return self.finish(job, error=type(exc).__name__ + ": " + str(exc))
        return self.finish(job, status=status)

    def replay(self, delivery_id):
        with self.db(True) as db:
            row = db.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
            if not row or row["state"] not in ("done", "dead") or row["replays"] >= 3:
                raise ValueError("Only terminal deliveries with fewer than three replays can be replayed")
            db.execute("UPDATE deliveries SET state='queued',attempts=0,replays=replays+1,available=?,error=NULL WHERE id=?", (self.clock(), delivery_id))
            self.record(db, delivery_id, "replayed")

    def deliveries(self):
        with self.db() as db:
            return [dict(row) for row in db.execute("SELECT id,event_id,subscriber_id,state,attempts,replays,error FROM deliveries ORDER BY id")]

    def history(self, delivery_id):
        with self.db() as db:
            return [dict(row) for row in db.execute("SELECT * FROM history WHERE delivery_id=? ORDER BY seq", (delivery_id,))]


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_):
        return None


def http_sender(url, body, headers):
    with build_opener(NoRedirect).open(Request(url, data=body, headers=headers, method="POST"), timeout=10) as response:
        return response.status
