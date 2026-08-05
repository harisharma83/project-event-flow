// Notification Service — the simplest consumer in this project, and
// deliberately so. It exists this week for one purpose: to be a service
// you can run 3 copies of at once and watch Kafka's consumer group
// coordinator divide order-events' partitions across them live. It does
// not send a real notification anywhere — it logs what it would send,
// which partition the message came from, and which of its own replicas
// (this process) received it.
//
// Nothing about this file changes when you run 3 of it. Same groupId,
// same code, same subscribe call — that's the point. Kafka's group
// coordinator is what decides which of the 3 running instances gets
// which partitions, and re-decides every time an instance joins or
// leaves. This file has no idea how many siblings it has.
//
// For the rebalance to be visible at all, order-events needs more than
// 1 partition — see this week's run instructions for the rpk command
// that adds partitions before starting this service.

require('dotenv').config();
const crypto = require('crypto');
const { kafka } = require('./kafkaClient');
const { startHealthServer } = require('./healthServer');

// A short random id, just so this replica's own log lines are
// distinguishable from another replica's in a shared terminal capture —
// in practice you'll be running each replica in its own terminal window,
// where this matters less, but it's a cheap way to make a screen
// recording or copy-pasted log unambiguous about which process said what.
const REPLICA_ID = crypto.randomBytes(3).toString('hex');

async function run() {
  startHealthServer();

  const consumer = kafka.consumer({ groupId: 'notification-service' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-events', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const evt = JSON.parse(message.value.toString());
      console.log(
        `[notification replica=${REPLICA_ID}] partition=${partition} would notify customer ${evt.customer_id || '?'} — order ${evt.id || evt.orderId}: ${evt.type}`
      );
    },
  });

  console.log(`Notification Service (replica=${REPLICA_ID}) consuming order-events`);
}

run().catch((err) => {
  console.error('Notification Service crashed:', err);
  process.exit(1);
});
