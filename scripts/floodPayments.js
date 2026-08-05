// Sends COUNT distinct ChargePayment commands — a fresh random order_id
// every time — in quick succession.
//
// Why not reuse chaosDuplicate.js? That script sends 5 COPIES of the
// SAME order_id, which is exactly what Week 3's idempotency dedup is
// designed to catch: only the first copy ever reaches the real charge
// logic, the other 4 are skipped by the fast-path check before they'd
// ever touch the gateway. That's correct behavior, but it means
// chaosDuplicate.js can't generate enough real gateway calls to trip a
// circuit breaker. This script sends COUNT genuinely NEW orders instead,
// so every single one reaches callMockGateway().
//
// Usage: npm run flood:payments -- 10
// (defaults to 8 if no count is given)

require('dotenv').config();
const crypto = require('crypto');
const { kafka } = require('../src/kafkaClient');

const COUNT = Number(process.argv[2]) || 8;
const DELAY_MS = 300;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const producer = kafka.producer();
  await producer.connect();

  console.log(`Sending ${COUNT} distinct ChargePayment commands, ${DELAY_MS}ms apart...`);

  for (let i = 0; i < COUNT; i++) {
    const orderId = crypto.randomUUID();
    await producer.send({
      topic: 'payment-commands',
      messages: [{
        key: orderId,
        value: JSON.stringify({ type: 'ChargePayment', orderId, totalCents: 2599 }),
      }],
    });
    console.log(`  sent ChargePayment for ${orderId} (${i + 1}/${COUNT})`);
    await sleep(DELAY_MS);
  }

  await producer.disconnect();
  console.log('Done.');
}

main().catch((err) => {
  console.error('floodPayments crashed:', err);
  process.exit(1);
});
