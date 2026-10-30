# HookLedger

A signed webhook inbox and durable delivery ledger with subscriber controls, deduplication, expiring worker leases, bounded retries and auditable replay. Built for Lord_Sumi's portfolio.

## Test without installing anything on your phone

Open **Actions → Tests and demo → Run workflow → main → Run workflow**. Expand each job's test step and **Run sample scenario**. The demo uses synthetic events and an offline delivery adapter; it does not contact an external subscriber.

## Run locally

Python 3.10+; standard library only.

```bash
python -m unittest discover -s tests -v
python demo.py
```

Set `HOOKLEDGER_INGEST_SECRET` and `HOOKLEDGER_SUBSCRIBER_SECRET` to separate local secrets of at least 16 characters before configuring a real subscriber:

```bash
python -m hookledger --db hooks.db subscribe receiver http://127.0.0.1:9000/hooks
python -m hookledger --db hooks.db serve --port 8080
```

The inbox binds only to `127.0.0.1`. POST a UTF-8 JSON object with `id`, `type` and `data` to `/events`, with `X-HookLedger-Timestamp` and `X-HookLedger-Signature` headers. The signature is lowercase HMAC-SHA256 of `timestamp + '.' + raw_body`, using the ingestion secret. The timestamp must be within five minutes of the receiving clock. The body is capped at 64 KiB.

Run a delivery attempt separately:

```bash
python -m hookledger --db hooks.db deliver-once
python -m hookledger --db hooks.db list
python -m hookledger --db hooks.db replay DELIVERY_ID
```

Use a scheduler or worker loop of your own to keep dispatching; this version does not start a background daemon automatically.

## Guarantees and behavior

- An event ID identifies exact signed body bytes. Repeating the same ID/body does not create duplicate delivery jobs. Reusing the ID with different bytes fails.
- Ingestion queues enabled subscribers present at that moment. `queue(event_id, subscriber_id)` can attach a later subscriber without duplicating an existing pair.
- Subscriber registration is immutable in this version. `enable` pauses or resumes pending deliveries; it does not cancel a request already in flight.
- SQLite `BEGIN IMMEDIATE` serializes claims. A 60-second token-fenced lease prevents stale workers from committing another worker's result.
- Each delivery has three attempts, exponential retry delays and a terminal dead-letter state. Explicit replay is permitted only from a terminal state and is capped at three replays.
- Outbound requests carry their own subscriber-secret HMAC and an event/subscriber idempotency key. Redirects are not followed. HTTP 2xx is success; exceptions and other statuses are failures.
- Audit history records claims, outcomes, expired leases and replays. Inspection does not expose subscriber secrets or lease tokens.

Delivery is **at least once**. A recipient may process an event before a timeout or worker crash makes the outcome unknown. Recipients must use the idempotency key to avoid duplicate effects. Lease expiry fences ledger state, not an already-started external request.

## Verification

Local validation on 31 October 2026: **16 tests passed**. Tests cover signed HTTP ingestion, invalid and expired signatures, concurrent deduplication and claims, restart persistence, signed outbound adapter calls, failed subscribers, retry budgets, explicit replay limits and real HTTP redirect refusal. Offline-adapter success does not establish a live third-party integration; no external provider was contacted by the demo.

## Limits

The inbox uses Python's development HTTP server, without TLS, network rate limits or multi-tenant authorization. Subscriber configuration and replay are local administration operations. Registered outbound URLs are trusted administrator configuration, not values accepted from incoming events; network destination allowlisting is not implemented. Subscriber secrets and event bodies are stored in the local database without encryption. Use URLs without embedded secrets; claim history records the target URL. There is no key-rotation UI, retention cleanup, cross-host database support or automatic schema migration. Protect the database and keep this demonstrator local.

MIT license; see [LICENSE](LICENSE).
