// Publishes one deliberately malformed message straight to
// payment-commands — not valid JSON at all. No number of retries could
// ever turn this into a successful charge, so it demonstrates the
// "straight to DLQ, no retry attempted" path in paymentService.js's
// eachMessage handler, distinct from the retry/circuit-breaker path
// (which is for gateway failures on otherwise well-formed commands).
//
// Usage: npm run poison:payment

require('dotenv').config();
const { kafka } = require('../src/kafkaClient');

async function main() {
  const producer = kafka.producer();
  await producer.connect();

  await producer.send({
    topic: 'payment-commands',
    messages: [{ key: 'poison-test', value: 'this is not valid JSON {{{' }],
  });

  await producer.disconnect();
  console.log('Sent a malformed (non-JSON) message to payment-commands.');
}

main().catch((err) => {
  console.error('sendPoisonMessage crashed:', err);
  process.exit(1);
});
