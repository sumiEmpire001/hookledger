import argparse
import json
import os
from .ledger import Ledger, LeaseLost


def main():
    parser = argparse.ArgumentParser(description="Signed webhook inbox and bounded delivery ledger")
    parser.add_argument("--db", default="hooks.db")
    commands = parser.add_subparsers(dest="command", required=True)
    subscribe = commands.add_parser("subscribe")
    subscribe.add_argument("id")
    subscribe.add_argument("url")
    commands.add_parser("deliver-once")
    commands.add_parser("list")
    replay = commands.add_parser("replay")
    replay.add_argument("id", type=int)
    serve = commands.add_parser("serve")
    serve.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    try:
        ledger = Ledger(args.db, os.environ.get("HOOKLEDGER_INGEST_SECRET", ""))
        if args.command == "subscribe":
            ledger.subscribe(args.id, args.url, os.environ.get("HOOKLEDGER_SUBSCRIBER_SECRET", ""))
        elif args.command == "deliver-once":
            print(json.dumps({"state": ledger.deliver_one()}))
        elif args.command == "list":
            print(json.dumps(ledger.deliveries(), indent=2))
        elif args.command == "replay":
            ledger.replay(args.id)
        else:
            from .server import make_server
            server = make_server(ledger, args.port)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
    except (ValueError, OSError, LeaseLost) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
