"""Provider-free demo. Synthetic secrets and events are only used in a temporary directory."""
import json
import tempfile
import time
from pathlib import Path
from hookledger import Ledger, sign

with tempfile.TemporaryDirectory() as directory:
    ledger = Ledger(Path(directory) / "demo.db", "synthetic-ingestion-secret")
    ledger.subscribe("demo", "http://127.0.0.1:9999/demo", "synthetic-subscriber-secret")
    body = json.dumps({"id": "demo-event", "type": "example.created", "data": {"synthetic": True}}).encode()
    timestamp = int(time.time())
    print(ledger.ingest(body, timestamp, sign(ledger.secret, timestamp, body)))
    print(ledger.ingest(body, timestamp, sign(ledger.secret, timestamp, body)))
    print("Offline adapter result:", ledger.deliver_one(lambda url, body, headers: 204))
    print(json.dumps(ledger.deliveries(), indent=2))
