// The outbox poller — the ONLY thing in this codebase that talks to Kafka
// for order events. Runs as its own process (npm run start:poller),
// separate from the HTTP server.
//
// Loop: find unpublished outbox rows -> publish each to Kafka -> mark it
// published. If this process dies mid-loop, nothing is lost: on restart
// it just finds the same unpublished rows again and retries. That does
// mean a message could be published twice (e.g. if it crashes after the
// Kafka send but before the UPDATE) — this is "at-least-once" delivery.
// Week 3 (idempotency) is what makes that safe on the consumer side.

require('dotenv').config();
const { pool } = require('./db');
const { kafka } = require('./kafkaClient');
const { startHealthServer } = require('./healthServer');

const POLL_INTERVAL_MS = 500;
const BATCH_SIZE = 100;
const TOPIC = 'order-events';

let stopped = false;
process.on('SIGINT', () => { stopped = true; });
process.on('SIGTERM', () => { stopped = true; });

async function pollOnce(producer) {
  const { rows } = await pool.query(
    `SELECT id, aggregate_id, event_type, payload
     FROM outbox
     WHERE published_at IS NULL
     ORDER BY created_at
     LIMIT $1`,
    [BATCH_SIZE]
  );

  for (const row of rows) {
    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: row.aggregate_id,
          value: JSON.stringify({ type: row.event_type, ...row.payload }),
        },
      ],
    });

    await pool.query('UPDATE outbox SET published_at = now() WHERE id = $1', [row.id]);

    console.log(`Published ${row.event_type} for order ${row.aggregate_id}`);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function run() {
  startHealthServer();

  const producer = kafka.producer();
  await producer.connect();
  console.log(`Poller connected to Kafka, polling outbox every ${POLL_INTERVAL_MS}ms`);

  while (!stopped) {
    try {
      await pollOnce(producer);
    } catch (err) {
      console.error('Poller tick failed (will retry next tick):', err);
    }
    await sleep(POLL_INTERVAL_MS);
  }

  console.log('Shutting down poller...');
  await producer.disconnect();
  process.exit(0);
}

run().catch((err) => {
  console.error('Poller crashed on startup:', err);
  process.exit(1);
});
