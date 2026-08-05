// Read Model Service — the CQRS read side. Two jobs running in one
// process: a Kafka consumer that projects every order-lifecycle event
// into one denormalized row per order (order_status), and a tiny REST
// API that answers "what's this order's status" from that one table.
//
// Nothing here writes to orders / inventory_reservations / payments /
// saga_state — those stay exactly as every prior week left them, owned
// by their own services. This is a second, deliberately separate model
// that exists purely to make reads fast and simple.
//
// Two things make this file different from every consumer written so
// far in this project:
//
//   1. fromBeginning: true. Payment Service and Inventory Service each
//      keep their own durable state in their own table from day one, so
//      they only ever need NEW messages (fromBeginning: false) — replaying
//      history would just redo work they've already recorded. A read
//      model starting up for the first time has an EMPTY order_status
//      table and no other source of truth to lean on — it has to replay
//      the full history of every topic it subscribes to, or it would
//      only ever know about orders placed after it happened to start.
//
//   2. It subscribes to three topics at once (order-events,
//      inventory-events, payment-events) in one consumer group, which is
//      exactly what CQRS requires: the read side needs to see everything
//      the write side produced, across every service, to answer one
//      question about one order.
//
// Known simplification, same spirit as every other week's flagged gaps:
// this project's saga only ever moves forward in a well-defined order for
// one order_id, so this projector applies updates as they arrive without
// checking "is this event older than what I already recorded." A stricter
// version would carry a sequence number or event timestamp and refuse to
// let an out-of-order event regress the status. Out of scope here.

require('dotenv').config();
const express = require('express');
const { pool } = require('./db');
const { kafka } = require('./kafkaClient');

const PORT = process.env.READ_MODEL_PORT || 3001;

async function upsertOrderCreated(evt) {
  await pool.query(
    `INSERT INTO order_status (order_id, customer_id, total_cents, status, updated_at)
     VALUES ($1, $2, $3, 'PLACED', now())
     ON CONFLICT (order_id) DO UPDATE
       SET customer_id = EXCLUDED.customer_id,
           total_cents = EXCLUDED.total_cents,
           updated_at = now()`,
    [evt.id, evt.customer_id, evt.total_cents]
  );
}

async function setStatus(orderId, status) {
  // ON CONFLICT here too: an inventory/payment event can arrive for an
  // order_id this projector hasn't seen OrderCreated for yet, if this
  // service was started mid-stream and topics are being replayed
  // out of their original wall-clock order. INSERT ... DO UPDATE means
  // the row exists either way; a later OrderCreated replay just fills in
  // customer_id/total_cents on top of it.
  await pool.query(
    `INSERT INTO order_status (order_id, status, updated_at)
     VALUES ($1, $2, now())
     ON CONFLICT (order_id) DO UPDATE
       SET status = EXCLUDED.status,
           updated_at = now()`,
    [orderId, status]
  );
}

async function project(topic, evt) {
  switch (evt.type) {
    case 'OrderCreated':
      await upsertOrderCreated(evt);
      break;
    case 'InventoryReserved':
      await setStatus(evt.orderId, 'INVENTORY_RESERVED');
      break;
    case 'InventoryFailed':
      await setStatus(evt.orderId, 'FAILED_NO_STOCK');
      break;
    case 'PaymentSucceeded':
      await setStatus(evt.orderId, 'COMPLETED');
      break;
    case 'PaymentFailed':
      await setStatus(evt.orderId, 'PAYMENT_DECLINED');
      break;
    case 'InventoryReleased':
      // Only reached via the saga's compensation path (payment declined,
      // inventory being released back) — the read model's way of naming
      // that same terminal outcome the saga orchestrator itself reaches.
      await setStatus(evt.orderId, 'FAILED_PAYMENT_DECLINED');
      break;
    default:
      // Unrecognized event type — ignore rather than crash. New event
      // types can be added to the write side over time; an unknown one
      // just means this projector doesn't have an opinion about it yet.
      break;
  }
}

async function startProjector() {
  const consumer = kafka.consumer({ groupId: 'read-model-service' });
  await consumer.connect();
  await consumer.subscribe({ topics: ['order-events', 'inventory-events', 'payment-events'], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const evt = JSON.parse(message.value.toString());
      await project(topic, evt);
      console.log(`[read-model] projected ${evt.type} (order ${evt.orderId || evt.id}) from ${topic}`);
    },
  });

  console.log('Read Model Service projecting order-events, inventory-events, payment-events (fromBeginning: true)');
}

function startApi() {
  const app = express();

  app.get('/orders/:id/status', async (req, res) => {
    const { rows } = await pool.query(
      `SELECT order_id, customer_id, total_cents, status, updated_at FROM order_status WHERE order_id = $1`,
      [req.params.id]
    );
    if (rows.length === 0) {
      // Deliberately ambiguous: this could mean "no such order," or it
      // could mean "the order was just placed and the projector hasn't
      // consumed OrderCreated yet." That ambiguity IS eventual
      // consistency, made visible at the API boundary.
      return res.status(404).json({ error: 'no status recorded yet for this order id' });
    }
    return res.json(rows[0]);
  });

  app.get('/health', (_req, res) => res.json({ ok: true }));

  app.listen(PORT, () => {
    console.log(`Read Model Service API listening on :${PORT}`);
  });
}

startApi();
startProjector().catch((err) => {
  console.error('Read Model Service projector crashed:', err);
  process.exit(1);
});
