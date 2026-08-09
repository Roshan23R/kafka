# 8. Change Data Capture (CDC)

> **The app updated the database. Did it remember to update the cache too?**

Picture a growing e-commerce app. Orders live in Postgres. But reads
are slow if every page hit the database directly, so someone adds a
Redis cache. And search needs its own index. Now every place in the
code that writes an order has to remember to *also* write to Redis,
*and* update the search index — three writes, one logical change, and
they're not atomic. One day the app crashes between writing to
Postgres and writing to Redis. Now they disagree, silently, and nobody
notices until a customer sees stale data.

```text
The "dual writes" trap:

app code
   |
   +---> write to Postgres  ---- succeeds
   |
   +---> write to Redis     ---- app crashes HERE
   |
   +---> write to search    ---- never happens

Postgres says one thing. Redis says another. Nobody told search at all.
```

CDC exists specifically to make this trap impossible.

## 🎯 The setup

An application writes to Postgres using completely ordinary SQL — it
has no idea Kafka exists. **Debezium** (a Kafka Connect plugin) tails
Postgres's write-ahead log (WAL) — the internal log Postgres already
keeps for crash recovery — and turns every row-level change into a
Kafka event automatically. A consumer then syncs those events into a
cache (Redis here) and a simulated search index, without ever querying
Postgres directly.

```text
modify_orders.py                Debezium                cdc_consumer.py
(ordinary SQL,                  (tails Postgres's         |
 no Kafka awareness)            own WAL --                | syncs to:
     |                          the log Postgres           +--> Redis (cache)
     v                          ALREADY keeps for           +--> "search index"
  Postgres                      crash recovery)                  (simulated)
     |                               |
     +--------- WAL ---------------->+----> Kafka topic
                                            cdc.public.orders
```

Only **one** write happens, ever — to Postgres. Everything downstream
is a side effect of that single write, guaranteed to eventually catch
up, never silently drifting apart.

This is the heaviest pattern in this repo — it needs a real database
and a Kafka Connect worker, not just Python producers/consumers — so
it lives behind a Docker Compose **profile** and won't start with the
other 7 patterns' broker.

> **Note on this file**: everything here was written and syntax/logic
> checked, but not run end-to-end against a live Debezium connector
> (no Docker available in the environment this was built in). If
> something doesn't come up cleanly on the first try, see
> Troubleshooting below — CDC setups are notoriously finicky about
> startup timing and Postgres permissions.

## 💡 Why tail the WAL instead of just... producing to Kafka from the app?

The obvious-seeming alternative — "just have the app also produce a
Kafka message whenever it writes to Postgres" — is exactly the dual-
write trap again, just moved one layer over. The app could still crash
between the two writes.

```text
Naive approach (still broken):        CDC approach (actually safe):

app code                              app code
   |                                     |
   +--> write Postgres  (succeeds)       +--> write Postgres (ONLY write)
   |                                            |
   +--> produce to Kafka (app crashes           v
        HERE, message never sent)         Postgres's OWN WAL already
                                           has the change durably --
   Same problem, moved one layer.         Debezium reads THAT, not
                                           something the app has to
                                           remember to also do.
```

The key insight: Postgres was *already* going to write this change to
its WAL, durably, as part of committing the transaction — that's how
Postgres survives its own crashes. CDC just reads a log that already,
unavoidably, exists. There's no second write for the app to forget.

## 🧠 What Kafka Connect actually is

Every other pattern in this repo uses `confluent_kafka`'s Producer and
Consumer directly. This one doesn't — `register_connector.py` makes a
plain HTTP call instead:

```text
Every other pattern:              This pattern:

your_script.py                    register_connector.py
   |                                   |
   | Producer() / Consumer()          | plain HTTP POST
   v                                   v
Kafka                              Kafka Connect (a separate,
(directly)                          standing worker process)
                                        |
                                        v
                                    runs the Debezium PLUGIN,
                                    which does the WAL-tailing
                                    and produces to Kafka FOR you
```

Kafka Connect is a framework for running connector plugins — Debezium
is one such plugin. You never write producer code for the Postgres
side at all; you just configure *what* to watch (`connector-config.json`)
and Kafka Connect + Debezium do the producing.

## 🏗️ Where this shows up for real

- **Cache invalidation done right** — exactly this repo's example:
  keep Redis or Memcached in sync with a source-of-truth database
  automatically.
- **Search indexing** — Elasticsearch/OpenSearch indexes kept current
  without the app ever writing to them directly.
- **Data warehouse sync** — feeding a warehouse (Snowflake, BigQuery)
  from an operational database in near-real-time, without batch ETL
  jobs running once a night.
- **Microservices migrations** — extracting a service from a
  monolith's database gradually, by having the new service consume CDC
  events instead of querying the old database directly.

## ▶️ Run it

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

## 👀 What to observe

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

## 🚀 Try breaking it

**1. Stop `kafka-connect`, make a change, restart it**

```bash
docker stop kafka-connect
python modify_orders.py update 1 SHIPPED
docker start kafka-connect
```
Does the update eventually show up in `cdc_consumer.py` once Connect
comes back, or is it lost? What does that tell you about where "the
truth" lives when Connect is briefly down?

**2. Insert a row directly with `psql`, bypassing `modify_orders.py`**

```bash
docker exec -it postgres psql -U postgres -d shopdb \
  -c "INSERT INTO orders (customer_name, amount) VALUES ('Eve', 42.00);"
```
Does `cdc_consumer.py` pick it up exactly the same as an insert via
`modify_orders.py`? What does that prove about *where* CDC actually
hooks in — the application, or the database itself?

**3. Deregister the connector, then make more changes**

```bash
curl -X DELETE localhost:8083/connectors/orders-connector
python modify_orders.py insert "Frank" 15.00
```
Do those changes show up anywhere in Kafka? Then re-register the
connector — does it pick up the missed change, or only changes from
this point forward?

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

## 💬 Questions worth answering yourself

- If Kafka Connect crashes and misses a Postgres change entirely, is
  that change lost forever, or does Debezium eventually catch up? What
  does your answer depend on (hint: think about WAL retention).
- Why does `modify_orders.py` need `psycopg2` while every other write
  operation in this repo uses `confluent_kafka`'s `Producer` directly?
- If you added a second downstream consumer — say, one that syncs to
  Elasticsearch instead of Redis — would `modify_orders.py` or
  `connector-config.json` need to change at all?

This is the last pattern we've built out together so far. Every
pattern from here shares the same underlying shape you've now seen
repeated eight times: a producer, a topic, a consumer, and a very
specific problem each one is solving — buffering, fan-out, running
totals, filtering, windowing, alerting, source-of-truth state, and now
keeping two systems honest without a human in the loop.