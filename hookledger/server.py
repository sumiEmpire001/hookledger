import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def make_server(ledger, port=8080):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, value):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            if self.path != "/events":
                self.reply(404, {"error": "Route not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 65536 or self.headers.get("Transfer-Encoding"):
                    self.reply(413, {"error": "A 1..65536 byte body with Content-Length is required"})
                    return
                self.connection.settimeout(5)
                result = ledger.ingest(self.rfile.read(length), self.headers.get("X-HookLedger-Timestamp"), self.headers.get("X-HookLedger-Signature"))
                self.reply(200 if result["duplicate"] else 201, result)
            except (ValueError, UnicodeError) as exc:
                self.reply(400, {"error": str(exc)})
            except TimeoutError:
                self.reply(408, {"error": "Body timed out"})
            except Exception:
                self.reply(500, {"error": "Internal error"})
    return ThreadingHTTPServer(("127.0.0.1", port), Handler)
