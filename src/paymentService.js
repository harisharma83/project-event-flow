// Payment Service — the other saga participant. Simulates a mock payment
// gateway with two INDEPENDENT kinds of "no" it can give you:
//
//   1. A business decline — the gateway answered, and the answer was
//      no (charge above the mock $1,000 authorization limit). This is
//      a normal, expected outcome. It is NOT a failure. It does not
//      touch the circuit breaker and is never retried.
//
//   2. A gateway failure — the gateway didn't answer at all (timeout,
//      5xx, connection reset). THIS is what retries and the circuit
//      breaker exist for.
//
// Week 3: charge() is idempotent, keyed by order_id.
// Week 4: adds retry with backoff+jitter and a circuit breaker around
// the gateway call, plus a dead-letter queue (payment-commands-dlq) for
// anything that still can't get through after retries are exhausted, or
// that can never succeed no matter how many times it's retried (a
// message that isn't even valid JSON — a "poison message").

require('dotenv').config();
const { pool } = require('./db');
const { kafka } = require('./kafkaClient');
const { CircuitBreaker } = require('./circuitBreaker');
const { withRetry } = require('./retry');

const DECLINE_ABOVE_CENTS = 100000; // $1,000 — POST an order above this to force a business decline

// Controls the MOCK gateway's simulated reliability. Independent of
// DECLINE_ABOVE_CENTS above — this is about the gateway being reachable
// at all, not about whether it approves the charge.
//   off        (default) never fails — normal operation for every other week's testing
//   always     every call fails — reliably trips the breaker, for the "force it open" demo
//   recovering fails for the first RECOVER_AFTER_MS after this process started, then
//              behaves normally from then on — lets you watch the FULL
//              CLOSED -> OPEN -> HALF_OPEN -> CLOSED cycle in a single run, no restart needed
//   flaky      random FLAKY_FAILURE_RATE chance of failure on every call — the most
//              "realistic" mode, unpredictable like a real network
const GATEWAY_FAILURE_MODE = process.env.GATEWAY_FAILURE_MODE || 'off';
const RECOVER_AFTER_MS = 12000;
const FLAKY_FAILURE_RATE = 0.4;

// NOT set at module load. Kafka consumer group join (rebalance) can take
// 20+ seconds on its own — if this clock started at process boot, the
// "recovering" window could fully elapse before a single real message
// is ever consumed, and you'd see zero failures. Instead it starts on
// the first actual gateway call, so the 12s failure window always
// covers the start of real traffic, regardless of how long the group
// join took.
let recoveringWindowStartedAt = null;

// One breaker instance for the lifetime of this process, shared by every
// charge() call — that's what makes it track failures ACROSS orders, not
// per order.
const breaker = new CircuitBreaker({
  failureThreshold: 5,
  cooldownMs: 10000,
  onStateChange: (from, to) => console.log(`[circuit] ${from} -> ${to}`),
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Simulates the network hop to a real payment gateway. Either returns
// normally (the gateway answered — approved or declined, both are valid
// answers) or throws (it didn't answer at all). Only the throw is a
// "failure" in the retry/circuit-breaker sense.
async function callMockGateway(orderId, amountCents) {
  await sleep(50); // pretend network latency

  let shouldFail = false;
  if (GATEWAY_FAILURE_MODE === 'always') {
    shouldFail = true;
  } else if (GATEWAY_FAILURE_MODE === 'recovering') {
    if (recoveringWindowStartedAt === null) recoveringWindowStartedAt = Date.now();
    shouldFail = Date.now() - recoveringWindowStartedAt < RECOVER_AFTER_MS;
  } else if (GATEWAY_FAILURE_MODE === 'flaky') {
    shouldFail = Math.random() < FLAKY_FAILURE_RATE;
  }

  if (shouldFail) {
    throw new Error('mock gateway timeout');
  }

  return { approved: typeof amountCents === 'number' && amountCents <= DECLINE_ABOVE_CENTS };
}

async function sendPaymentEvent(orderId, amountCents, status, replayed, producer) {
  const event = {
    type: status === 'SUCCEEDED' ? 'PaymentSucceeded' : 'PaymentFailed',
    orderId,
    amountCents,
    replayed,
  };
  if (status !== 'SUCCEEDED') event.reason = 'exceeds mock gateway authorization limit ($1,000)';

  await producer.send({
    topic: 'payment-events',
    messages: [{ key: orderId, value: JSON.stringify(event) }],
  });
}

// Everything that couldn't be turned into a successful charge attempt —
// retries exhausted, or the message couldn't even be parsed — lands
// here instead of crashing this process or blocking every order behind
// it in the partition. Whoever owns this service can query this topic
// later and decide what to do (retry manually, refund, page someone).
async function sendToDlq(payload, error, producer) {
  await producer.send({
    topic: 'payment-commands-dlq',
    messages: [{
      key: payload.orderId || null,
      value: JSON.stringify({ ...payload, error: error.message, failedAt: new Date().toISOString() }),
    }],
  });
  console.log(`[payment] -> DLQ: order ${payload.orderId || '(unparseable message)'} — ${error.message}`);
}

async function charge(orderId, amountCents, producer) {
  // Fast path, same reasoning as Inventory Service: cheap check first,
  // avoids doing decision work for the common sequential-duplicate case.
  const existing = await pool.query(`SELECT status FROM payments WHERE order_id = $1`, [orderId]);

  if (existing.rows.length > 0) {
    const status = existing.rows[0].status;
    console.log(`[payment] duplicate ChargePayment for order ${orderId}, replaying ${status} without re-charging`);
    await sendPaymentEvent(orderId, amountCents, status, true, producer);
    return;
  }

  // New work. Reach the gateway through retry (backoff + jitter), and
  // every individual attempt goes through the circuit breaker. If the
  // breaker is already OPEN, breaker.fire() rejects instantly (err.circuitOpen),
  // and withRetry sees that flag and stops immediately instead of
  // burning through its retry budget against a dependency it already
  // knows is down.
  let gatewayResult;
  try {
    gatewayResult = await withRetry(
      () => breaker.fire(() => callMockGateway(orderId, amountCents)),
      { maxRetries: 3, baseMs: 500, label: `charge ${orderId}` }
    );
  } catch (err) {
    await sendToDlq({ type: 'ChargePayment', orderId, amountCents }, err, producer);
    return;
  }

  // The gateway answered. This is the business decision — nothing to do
  // with retries or the breaker.
  const status = gatewayResult.approved ? 'SUCCEEDED' : 'FAILED';

  // Same guarantee as before: ON CONFLICT DO NOTHING against UNIQUE(order_id)
  // is what actually prevents a double-charge under real concurrency.
  const inserted = await pool.query(
    `INSERT INTO payments (order_id, amount_cents, status) VALUES ($1, $2, $3)
     ON CONFLICT (order_id) DO NOTHING
     RETURNING status`,
    [orderId, amountCents ?? 0, status]
  );

  if (inserted.rows.length === 0) {
    const winner = await pool.query(`SELECT status FROM payments WHERE order_id = $1`, [orderId]);
    const winnerStatus = winner.rows[0].status;
    console.log(`[payment] lost race on concurrent duplicate for order ${orderId}, no double-charge`);
    await sendPaymentEvent(orderId, amountCents, winnerStatus, true, producer);
    return;
  }

  console.log(`[payment] ${status === 'SUCCEEDED' ? 'charged' : 'DECLINED'} ${amountCents}c for order ${orderId}`);
  await sendPaymentEvent(orderId, amountCents, status, false, producer);
}

async function run() {
  const producer = kafka.producer();
  await producer.connect();

  const consumer = kafka.consumer({ groupId: 'payment-service' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'payment-commands', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let cmd;
      try {
        cmd = JSON.parse(message.value.toString());
      } catch (err) {
        // Not valid JSON at all — a poison message. No amount of
        // retrying will ever fix malformed input, so there's no point
        // trying. Straight to the DLQ. Crucially: we catch this here
        // and do NOT re-throw, so kafkajs still commits this message's
        // offset and moves on to the next one — a poison message does
        // not get to block every real order behind it in the partition.
        console.error(`[payment] poison message: could not parse as JSON:`, err.message);
        await sendToDlq({ raw: message.value.toString() }, err, producer);
        return;
      }

      if (cmd.type === 'ChargePayment') {
        await charge(cmd.orderId, cmd.totalCents, producer);
      }
    },
  });

  console.log(`Payment Service consuming payment-commands (gateway failure mode: ${GATEWAY_FAILURE_MODE})`);
}

run().catch((err) => {
  console.error('Payment Service crashed:', err);
  process.exit(1);
});
