# EventFlow — Order Service (Week 1)

Implements the outbox pattern from the Week 1 notes: `POST /orders` writes
the order and an outbox row in one Postgres transaction; a separate poller
process is the only thing that publishes to Kafka.

## Files

- `sql/schema.sql` — `orders` and `outbox` tables
- `src/db.js` — shared Postgres pool
- `src/kafkaClient.js` — shared kafkajs client
- `src/server.js` — the REST API (`POST /orders`) — never touches Kafka
- `src/poller.js` — reads unpublished outbox rows, publishes, marks sent
- `scripts/migrate.js` — applies `sql/schema.sql`

## Setup

Requires the Week 0 `docker-compose.yml` (Redpanda + Postgres) already running.

```
cp .env.example .env
npm install
npm run migrate
```

## Run (two terminals)

```
# Terminal A
npm run start:server

# Terminal B
npm run start:poller
```

Optionally, a third terminal consuming the topic so you can watch events land:

```
docker compose exec kafka rpk topic create order-events
docker compose exec kafka rpk topic consume order-events
```

## Test the happy path

```
curl -X POST localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"11111111-1111-1111-1111-111111111111","totalCents":2599}'
```

You should get a 201 with the order back, and within ~500ms see `OrderCreated`
appear in the consumer terminal.

## The real milestone test — crash mid-write

This is the point of the whole pattern, so don't skip it:

1. With the poller **stopped** (Ctrl+C in Terminal B), POST another order.
   It's now sitting in Postgres with `published_at IS NULL` — nothing has
   reached Kafka.
2. `kill -9` the server process too, simulating a hard crash right after
   the transaction committed.
3. Restart both `npm run start:server` and `npm run start:poller`.
4. Watch the consumer terminal: the event still lands. It was never lost —
   it was durably sitting in the `outbox` table the whole time, waiting for
   a poller to pick it up.

If step 4 doesn't happen, something's off — check the poller logs first;
`published_at IS NULL` rows not being picked up is almost always a query
or connection issue, not a "the pattern doesn't work" issue.
