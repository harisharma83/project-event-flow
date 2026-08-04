// Payment Service — the other saga participant. Simulates a mock payment
// gateway with one deterministic rule, so you can trigger a decline on
// demand for the milestone demo: any charge over $1,000 is declined,
// standing in for a real authorization-limit decline.
//
// Week 3: charge() is now idempotent, same idempotency key as Inventory
// Service — order_id. This saga only ever intends to attempt one charge
// per order, so a ChargePayment command for an order_id already in the
// payments table is a redelivery, not a new charge to make.

require('dotenv').config();
const { pool } = require('./db');
const { kafka } = require('./kafkaClient');

const DECLINE_ABOVE_CENTS = 100000; // $1,000 — POST an order above this to force a decline

async function sendPaymentEvent(orderId, amountCents, status, producer) {
  const event = {
    type: status === 'SUCCEEDED' ? 'PaymentSucceeded' : 'PaymentFailed',
    orderId,
    amountCents,
  };
  if (status !== 'SUCCEEDED') event.reason = 'exceeds mock gateway authorization limit ($1,000)';

  await producer.send({
    topic: 'payment-events',
    messages: [{ key: orderId, value: JSON.stringify(event) }],
  });
}

async function charge(orderId, amountCents, producer) {
  // Fast path, same reasoning as Inventory Service: cheap check first,
  // avoids doing decision work for the common sequential-duplicate case.
  // Each branch below logs exactly once and returns — no shared fall-
  // through log at the bottom, which is what caused a Week 3 logging bug:
  // a duplicate branch that logged "duplicate" and then fell through to
  // an unconditional final log line, printing "charged" a second time for
  // every duplicate even though only one row was ever actually written.
  const existing = await pool.query(`SELECT status FROM payments WHERE order_id = $1`, [orderId]);

  if (existing.rows.length > 0) {
    const status = existing.rows[0].status;
    console.log(`[payment] duplicate ChargePayment for order ${orderId}, replaying ${status} without re-charging`);
    await sendPaymentEvent(orderId, amountCents, status, producer);
    return;
  }

  const succeeded = typeof amountCents === 'number' && amountCents <= DECLINE_ABOVE_CENTS;
  let status = succeeded ? 'SUCCEEDED' : 'FAILED';

  // The real guarantee: ON CONFLICT DO NOTHING against UNIQUE(order_id).
  // The SELECT above can't see a concurrent duplicate that hasn't
  // committed yet — this is what actually prevents two charge rows for
  // one order under real concurrency, not the check above it.
  const inserted = await pool.query(
    `INSERT INTO payments (order_id, amount_cents, status) VALUES ($1, $2, $3)
     ON CONFLICT (order_id) DO NOTHING
     RETURNING status`,
    [orderId, amountCents ?? 0, status]
  );

  if (inserted.rows.length === 0) {
    // Lost the race against a concurrent duplicate — fetch whatever the
    // winner actually recorded, so our reply reports the true outcome.
    const winner = await pool.query(`SELECT status FROM payments WHERE order_id = $1`, [orderId]);
    status = winner.rows[0].status;
    console.log(`[payment] lost race on concurrent duplicate for order ${orderId}, no double-charge`);
    await sendPaymentEvent(orderId, amountCents, status, producer);
    return;
  }

  console.log(`[payment] ${status === 'SUCCEEDED' ? 'charged' : 'DECLINED'} ${amountCents}c for order ${orderId}`);
  await sendPaymentEvent(orderId, amountCents, status, producer);
}

async function run() {
  const producer = kafka.producer();
  await producer.connect();

  const consumer = kafka.consumer({ groupId: 'payment-service' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'payment-commands', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const cmd = JSON.parse(message.value.toString());
      if (cmd.type === 'ChargePayment') {
        await charge(cmd.orderId, cmd.totalCents, producer);
      }
    },
  });

  console.log('Payment Service consuming payment-commands');
}

run().catch((err) => {
  console.error('Payment Service crashed:', err);
  process.exit(1);
});