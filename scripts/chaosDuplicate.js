// Week 3 milestone script.
//
// Fires the SAME ReserveInventory and ChargePayment commands 5 times
// each, concurrently, directly at Inventory Service and Payment Service.
// This stands in for what a real broker restart or a consumer
// crash-before-offset-commit can genuinely cause: Kafka redelivering a
// message that was already fully processed. If Week 3's dedup logic is
// correct, 10 messages in produces exactly 1 reservation and 1 payment
// row — proving no double-reservation and no double-charge.
//
// Run with Inventory Service and Payment Service already running
// (npm run start:inventory / npm run start:payment) — this script only
// publishes commands, it doesn't process them itself.

require('dotenv').config();
const crypto = require('crypto');
const { pool } = require('../src/db');
const { kafka } = require('../src/kafkaClient');

const REPLAYS = 5;
const orderId = process.argv[2] || crypto.randomUUID();
const totalCents = 2599;

async function main() {
  console.log(`Chaos-duplicate test for order ${orderId}`);
  console.log(`Sending ${REPLAYS}x ReserveInventory and ${REPLAYS}x ChargePayment, concurrently...`);

  const producer = kafka.producer();
  await producer.connect();

  const reserveSends = Array.from({ length: REPLAYS }, () =>
    producer.send({
      topic: 'inventory-commands',
      messages: [{ key: orderId, value: JSON.stringify({ type: 'ReserveInventory', orderId }) }],
    })
  );
  const chargeSends = Array.from({ length: REPLAYS }, () =>
    producer.send({
      topic: 'payment-commands',
      messages: [{ key: orderId, value: JSON.stringify({ type: 'ChargePayment', orderId, totalCents }) }],
    })
  );

  await Promise.all([...reserveSends, ...chargeSends]);
  await producer.disconnect();

  console.log('All 10 messages sent. Waiting 3s for both services to process them...');
  await new Promise((resolve) => setTimeout(resolve, 3000));

  const res = await pool.query(
    `SELECT count(*)::int AS c FROM inventory_reservations WHERE order_id = $1`,
    [orderId]
  );
  const pay = await pool.query(
    `SELECT count(*)::int AS c FROM payments WHERE order_id = $1`,
    [orderId]
  );
  const stock = await pool.query(`SELECT available_qty FROM inventory WHERE sku = 'DEMO-SKU'`);

  const reservationCount = res.rows[0].c;
  const paymentCount = pay.rows[0].c;

  console.log('');
  console.log(`inventory_reservations rows for order ${orderId}: ${reservationCount}`);
  console.log(`payments rows for order ${orderId}: ${paymentCount}`);
  console.log(`DEMO-SKU available_qty now: ${stock.rows[0].available_qty}`);
  console.log('');

  const pass = reservationCount === 1 && paymentCount === 1;
  if (pass) {
    console.log(`PASS: 5x redelivery of each command produced exactly 1 reservation and 1 charge for order ${orderId} — dedupe worked.`);
  } else {
    console.log(`FAIL: expected exactly 1 reservation and 1 payment row, got ${reservationCount} and ${paymentCount}.`);
  }

  await pool.end();
  process.exit(pass ? 0 : 1);
}

main().catch((err) => {
  console.error('chaosDuplicate crashed:', err);
  process.exit(1);
});
