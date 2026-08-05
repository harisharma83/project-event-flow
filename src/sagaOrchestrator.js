// The Saga Orchestrator — a generic interpreter for saga/saga-definition.json.
//
// This file does not know what "inventory" or "payment" mean. It only
// knows: states, the command a state issues on entry, and a table mapping
// incoming event types to the next state. That's what makes it
// data-driven — to change the saga (add a step, change the order,
// reroute a failure), you edit the JSON, not this file.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { pool } = require('./db');
const { kafka } = require('./kafkaClient');
const { startHealthServer } = require('./healthServer');

const definition = JSON.parse(
  fs.readFileSync(path.join(__dirname, '..', 'saga', 'saga-definition.json'), 'utf8')
);

// Every topic that could produce an event this saga reacts to: the
// trigger topic, plus one "-events" topic per command topic referenced
// in the definition (by convention, "x-commands" replies on "x-events").
function topicsToSubscribe() {
  const topics = new Set([definition.trigger.topic]);
  for (const state of Object.values(definition.states)) {
    if (state.onEnter) topics.add(state.onEnter.topic.replace('-commands', '-events'));
  }
  return [...topics];
}

async function startSaga(orderId, context, producer) {
  const initial = definition.initialState;
  const { rowCount } = await pool.query(
    `INSERT INTO saga_state (order_id, current_state, context)
     VALUES ($1, $2, $3)
     ON CONFLICT (order_id) DO NOTHING`,
    [orderId, initial, JSON.stringify(context)]
  );
  if (rowCount === 0) {
    console.log(`[saga ${orderId}] already started, ignoring duplicate trigger`);
    return;
  }
  await enterState(orderId, initial, context, producer);
}

async function enterState(orderId, stateName, context, producer) {
  const state = definition.states[stateName];
  console.log(`[saga ${orderId}] -> ${stateName}${state.compensation ? ' (compensation)' : ''}`);

  await pool.query(
    `UPDATE saga_state SET current_state = $2, updated_at = now() WHERE order_id = $1`,
    [orderId, stateName]
  );

  if (state.onEnter) {
    await producer.send({
      topic: state.onEnter.topic,
      messages: [{
        key: orderId,
        value: JSON.stringify({ type: state.onEnter.type, orderId, ...context }),
      }],
    });
  }

  if (state.terminal) {
    console.log(`[saga ${orderId}] finished: ${state.outcome}`);
  }
}

async function handleEvent(orderId, eventType, producer) {
  const { rows } = await pool.query(
    `SELECT current_state, context FROM saga_state WHERE order_id = $1`,
    [orderId]
  );
  if (rows.length === 0) return; // not a saga this orchestrator is tracking

  const { current_state: currentState, context } = rows[0];
  const stateDef = definition.states[currentState];
  const nextStateName = stateDef.on && stateDef.on[eventType];

  if (!nextStateName) {
    // This event doesn't apply to the saga's current state — ignore it.
    // Usually means a stale or duplicate event arrived after the saga
    // already moved on (at-least-once delivery, same idea as Week 1's
    // poller). Silently dropping it here is a small preview of Week 3.
    console.log(`[saga ${orderId}] ignoring ${eventType} while in ${currentState}`);
    return;
  }

  await enterState(orderId, nextStateName, context, producer);
}

async function run() {
  startHealthServer();

  const producer = kafka.producer();
  await producer.connect();

  const consumer = kafka.consumer({ groupId: 'saga-orchestrator' });
  await consumer.connect();
  const topics = topicsToSubscribe();
  for (const topic of topics) await consumer.subscribe({ topic, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const payload = JSON.parse(message.value.toString());
      if (!payload.type) return;

      if (topic === definition.trigger.topic) {
        if (payload.type === definition.trigger.type) {
          const orderId = payload.id; // OrderCreated carries the order itself, keyed by id
          const context = { totalCents: payload.total_cents, customerId: payload.customer_id };
          await startSaga(orderId, context, producer);
        }
        return;
      }

      await handleEvent(payload.orderId, payload.type, producer);
    },
  });

  console.log('Saga orchestrator subscribed to:', topics.join(', '));
}

run().catch((err) => {
  console.error('Saga orchestrator crashed:', err);
  process.exit(1);
});
