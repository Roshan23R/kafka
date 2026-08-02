# 8. Change Data Capture (CDC)

**Sync database changes downstream, no dual writes, no drift.**

An application writes to Postgres using completely ordinary SQL — it
has no idea Kafka exists. **Debezium** (a Kafka Connect plugin) tails
Postgres's write-ahead log (WAL) — the internal log Postgres already
keeps for crash recovery — and turns every row-level change into a
Kafka event automatically. A consumer then syncs those events into a
cache (Redis here) and a simulated search index, without ever querying
Postgres directly.

This is the heaviest pattern in this repo — it needs a real database
and a Kafka Connect worker, not just Python producers/consumers — so
it lives behind a Docker Compose **profile** and won't start with the
other 7 patterns' broker.


## Run it

1. Start the CDC stack (Postgres + Kafka Connect), **in addition to**
   the base broker:
   ```bash
   docker compose --profile cdc up -d
   ```
   This starts everything from the root `docker-compose.yml` (kafka,
   kafka-ui, redis) **plus** `postgres` and `kafka-connect`, since
   they're tagged with `profiles: ["cdc"]`.

2. Wait ~20-30 seconds for Kafka Connect to fully start, then register
   the Debezium connector:
   ```bash
   cd 08-cdc
   python register_connector.py
   ```
   This POSTs `connector-config.json` to Kafka Connect's REST API
   (`localhost:8083`). It retries automatically if Connect isn't ready
   yet.

3. Watch the sync consumer:
   ```bash
   python cdc_consumer.py
   ```

4. In another terminal, make changes to Postgres like a normal app
   would:
   ```bash
   python modify_orders.py list
   python modify_orders.py insert "Dave" 99.99
   python modify_orders.py update 1 SHIPPED
   python modify_orders.py delete 2
   ```

## What to observe

- The moment `register_connector.py` runs, `cdc_consumer.py` should
  immediately show 3 `SNAPSHOT` events — Debezium's initial read of
  the 3 rows already in `orders` from `init.sql`, before it starts
  streaming live changes.
- Every `insert`/`update`/`delete` via `modify_orders.py` shows up in
  `cdc_consumer.py` within roughly a second — that's the WAL-tailing
  latency, not polling.
- Check Redis directly to see the synced cache:
  ```bash
  docker exec -it redis redis-cli
  > KEYS order:*
  > GET order:1
  ```
- Delete an order and confirm its Redis key disappears too — CDC
  propagates deletes, not just inserts/updates.
- **The core idea to take away**: the application (`modify_orders.py`)
  never writes to Kafka, Redis, or a search index directly. It writes
  to Postgres, once. Everything downstream stays in sync automatically
  — no risk of the app successfully updating the DB but forgetting to
  update the cache (the "dual writes" problem the infographic panel
  title refers to).

## Why `REPLICA IDENTITY FULL` (in `init.sql`)

By default, Postgres's replication log only includes the primary key
for `UPDATE`/`DELETE` events, not the full row. `REPLICA IDENTITY
FULL` tells Postgres to include the entire row in the WAL, so
Debezium's `"before"` field is actually populated with real data
(useful for audit trails, or diffing what changed) instead of just a
row ID.

## Troubleshooting

- **`register_connector.py` keeps retrying and never succeeds**:
  Kafka Connect takes longer to start than expected. Check
  `docker compose logs kafka-connect` for errors, and confirm
  `docker ps` shows the `kafka-connect` container as healthy/running.
- **Connector registers but `cdc_consumer.py` sees nothing**: check
  connector status directly —
  ```bash
  curl localhost:8083/connectors/orders-connector/status
  ```
  A `"state": "FAILED"` here usually means a Postgres permissions or
  `wal_level` issue — the `debezium/postgres` image is pre-configured
  for logical replication, so this is more likely to happen if you
  swap in a plain `postgres` image instead.
- **`modify_orders.py` fails to connect**: confirm Postgres is
  reachable on `localhost:5432` and that `psycopg2-binary` installed
  correctly (`pip install -r requirements.txt` from the repo root).
- **Topic name mismatch**: the resulting Kafka topic is
  `<topic.prefix>.<schema>.<table>` — with this config that's
  `cdc.public.orders`. If you rename anything in
  `connector-config.json`, update `TOPIC` in `cdc_consumer.py` to
  match.

## Cleanup

```bash
docker compose --profile cdc down -v
```
The `-v` also removes the Postgres data volume, so the next
`docker compose --profile cdc up -d` starts from a completely fresh
database (re-running `init.sql`).
