// Order Service — the REST side of the outbox pattern.
//
// The important thing to notice: this file never imports kafkaClient.js.
// Creating an order only ever touches Postgres. Publishing to Kafka is
// entirely the poller's job (src/poller.js) — that separation is the
// whole point of the pattern. See Week 1 notes for why.

require('dotenv').config();
const express = require('express');
const { pool } = require('./db');

const app = express();
app.use(express.json());

app.post('/orders', async (req, res) => {
  const { customerId, totalCents } = req.body || {};

  if (!customerId || !Number.isInteger(totalCents)) {
    return res.status(400).json({
      error: 'customerId (string) and totalCents (integer) are required',
    });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Write the order.
    const orderResult = await client.query(
      `INSERT INTO orders (customer_id, total_cents)
       VALUES ($1, $2)
       RETURNING id, customer_id, status, total_cents, created_at`,
      [customerId, totalCents]
    );
    const order = orderResult.rows[0];

    // 2. Write the outbox row, in the SAME transaction as step 1.
    //    If either insert fails, both roll back together — that's the
    //    guarantee the outbox pattern relies on.
    await client.query(
      `INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ($1, $2, $3, $4)`,
      ['order', order.id, 'OrderCreated', JSON.stringify(order)]
    );

    await client.query('COMMIT');

    // At this point the order is durably saved and the event is durably
    // queued for publishing — even though Kafka hasn't been contacted yet.
    return res.status(201).json(order);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Failed to create order:', err);
    return res.status(500).json({ error: 'internal error' });
  } finally {
    client.release();
  }
});

app.get('/health', (_req, res) => res.json({ ok: true }));

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Order Service listening on :${port}`);
});
