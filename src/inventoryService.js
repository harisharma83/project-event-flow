// Inventory Service — a saga PARTICIPANT, not the orchestrator.
// It only understands two commands: reserve, and release. It has no idea
// a saga or an orchestrator exists — it just reacts to inventory-commands
// and reports the outcome on inventory-events. That separation is
// deliberate: participants stay simple and local; the orchestrator
// (sagaOrchestrator.js) owns all the sequencing knowledge.
//
// Week 3: reserve() is now idempotent. order_id is the idempotency key —
// this saga only ever intends to reserve stock once per order, so a
// ReserveInventory command carrying an order_id we've already handled is
// the same logical request arriving again, not a new one. See sql/schema.sql
// for the UNIQUE(order_id) constraint this logic relies on.

require('dotenv').config();
const { pool } = require('./db');
const { kafka } = require('./kafkaClient');

const SKU = 'DEMO-SKU'; // simplification: every order reserves 1 unit of a single demo product

async function reserve(orderId, producer) {
  // Fast path: have we already handled this order_id? Cheap check, avoids
  // taking the FOR UPDATE lock below for the common sequential-duplicate
  // case. This is an optimization, not the guarantee — see the ON CONFLICT
  // further down for what actually makes this safe under real concurrency.
  const existing = await pool.query(
    `SELECT status FROM inventory_reservations WHERE order_id = $1`,
    [orderId]
  );
  if (existing.rows.length > 0) {
    console.log(`[inventory] duplicate ReserveInventory for order ${orderId}, replaying reply without re-reserving`);
    // Known edge case, out of scope for this lightweight version: if the
    // reservation was already RELEASED (compensation already ran) before
    // this duplicate arrived, replying InventoryReserved again is not
    // quite right. That requires reasoning about cross-topic redelivery
    // ordering, which this project doesn't attempt to solve.
    const type = existing.rows[0].status === 'RESERVED' ? 'InventoryReserved' : 'InventoryFailed';
    await producer.send({
      topic: 'inventory-events',
      messages: [{ key: orderId, value: JSON.stringify({ type, orderId }) }],
    });
    return;
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { rows } = await client.query(
      `SELECT available_qty FROM inventory WHERE sku = $1 FOR UPDATE`,
      [SKU]
    );
    const available = rows[0]?.available_qty ?? 0;

    if (available < 1) {
      await client.query('ROLLBACK');
      await producer.send({
        topic: 'inventory-events',
        messages: [{ key: orderId, value: JSON.stringify({ type: 'InventoryFailed', orderId, reason: 'out of stock' }) }],
      });
      console.log(`[inventory] FAILED reserve for order ${orderId}: out of stock`);
      return;
    }

    await client.query(`UPDATE inventory SET available_qty = available_qty - 1 WHERE sku = $1`, [SKU]);

    // The real guarantee: ON CONFLICT DO NOTHING against UNIQUE(order_id).
    // If two copies of this command are being processed concurrently (the
    // upfront SELECT above can't catch that — both could pass it before
    // either writes), only one INSERT can ever win here. The database
    // enforces it atomically; no amount of application-level checking
    // above this line could guarantee that on its own.
    const inserted = await client.query(
      `INSERT INTO inventory_reservations (order_id, sku, qty, status) VALUES ($1, $2, 1, 'RESERVED')
       ON CONFLICT (order_id) DO NOTHING
       RETURNING id`,
      [orderId, SKU]
    );

    if (inserted.rows.length === 0) {
      // Lost the race: some other concurrent delivery of this same command
      // already inserted the reservation between our SELECT and this
      // INSERT. Undo the decrement we just did — the winner's decrement
      // already accounts for this order's stock — and reply as normal.
      await client.query(`UPDATE inventory SET available_qty = available_qty + 1 WHERE sku = $1`, [SKU]);
      await client.query('ROLLBACK');
      console.log(`[inventory] lost race on concurrent duplicate for order ${orderId}, no double-decrement`);
      await producer.send({
        topic: 'inventory-events',
        messages: [{ key: orderId, value: JSON.stringify({ type: 'InventoryReserved', orderId }) }],
      });
      return;
    }

    await client.query('COMMIT');

    await producer.send({
      topic: 'inventory-events',
      messages: [{ key: orderId, value: JSON.stringify({ type: 'InventoryReserved', orderId }) }],
    });
    console.log(`[inventory] reserved 1x ${SKU} for order ${orderId}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[inventory] reserve failed:', err);
  } finally {
    client.release();
  }
}

// The compensating action. Note this is not a "rollback" in the database
// sense — the earlier reservation was already committed. This is a new,
// forward-moving transaction whose job is to undo it logically.
//
// Unlike reserve(), this function was ALREADY idempotent before Week 3,
// by accident rather than design: the WHERE status = 'RESERVED' clause
// means a second ReleaseInventory for the same order simply matches zero
// rows the second time (status is already 'RELEASED'), so rows.length is
// 0 and the credit-back is skipped — no double-credit possible. Worth
// noticing, because it shows idempotency sometimes falls out of a
// state-checked WHERE clause for free, the same way Week 2's saga
// orchestrator got some of its idempotency for free from its state
// machine shape.
async function release(orderId, producer) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { rows } = await client.query(
      `UPDATE inventory_reservations
       SET status = 'RELEASED', released_at = now()
       WHERE order_id = $1 AND status = 'RESERVED'
       RETURNING sku, qty`,
      [orderId]
    );

    if (rows.length > 0) {
      await client.query(
        `UPDATE inventory SET available_qty = available_qty + $2 WHERE sku = $1`,
        [rows[0].sku, rows[0].qty]
      );
    }

    await client.query('COMMIT');

    await producer.send({
      topic: 'inventory-events',
      messages: [{ key: orderId, value: JSON.stringify({ type: 'InventoryReleased', orderId }) }],
    });
    console.log(`[inventory] released reservation for order ${orderId}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[inventory] release failed:', err);
  } finally {
    client.release();
  }
}

async function run() {
  const producer = kafka.producer();
  await producer.connect();

  const consumer = kafka.consumer({ groupId: 'inventory-service' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'inventory-commands', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const cmd = JSON.parse(message.value.toString());
      if (cmd.type === 'ReserveInventory') await reserve(cmd.orderId, producer);
      else if (cmd.type === 'ReleaseInventory') await release(cmd.orderId, producer);
    },
  });

  console.log('Inventory Service consuming inventory-commands');
}

run().catch((err) => {
  console.error('Inventory Service crashed:', err);
  process.exit(1);
});
