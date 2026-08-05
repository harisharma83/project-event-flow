// Measures eventual consistency in milliseconds instead of relying on
// human typing speed to catch it. Manually copy-pasting an order id into
// a second command takes seconds; the whole saga (order -> reserve ->
// charge -> read model projecting all of it) likely finishes in tens of
// milliseconds locally. That gap between "fast enough for a human to
// never see it lag" and "still not actually zero" is the whole point of
// this script.
//
// What it does:
//   1. POSTs one new order to Order Service.
//   2. Immediately starts polling Read Model Service's status endpoint
//      in a tight loop (every POLL_INTERVAL_MS), timestamping every
//      response, until it sees a terminal status or times out.
//   3. Prints every poll with its elapsed time, so you see the exact
//      sequence: probably a 404 or two, then PLACED, then
//      INVENTORY_RESERVED, then COMPLETED (or a failure status) — each
//      one stamped with how many milliseconds after the POST it showed up.
//
// Usage: npm run measure:lag
//        npm run measure:lag -- 150000   (force a decline by pricing over $1,000)

require('dotenv').config();

const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL || 'http://localhost:3000';
const READ_MODEL_URL = process.env.READ_MODEL_URL || 'http://localhost:3001';
const POLL_INTERVAL_MS = 15;
const TIMEOUT_MS = 5000;
const TERMINAL_STATUSES = new Set(['COMPLETED', 'FAILED_NO_STOCK', 'FAILED_PAYMENT_DECLINED']);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function placeOrder(totalCents) {
  const res = await fetch(`${ORDER_SERVICE_URL}/orders`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      customerId: '11111111-1111-1111-1111-111111111111',
      totalCents,
    }),
  });
  if (!res.ok) {
    throw new Error(`Order Service returned ${res.status}: ${await res.text()}`);
  }
  return res.json();
}

async function pollStatus(orderId) {
  const res = await fetch(`${READ_MODEL_URL}/orders/${orderId}/status`);
  if (res.status === 404) {
    return { found: false, status: null };
  }
  if (!res.ok) {
    throw new Error(`Read Model Service returned ${res.status}: ${await res.text()}`);
  }
  const body = await res.json();
  return { found: true, status: body.status };
}

async function main() {
  const totalCents = Number(process.argv[2]) || 2599;

  console.log(`Placing order for ${totalCents}c...`);
  const t0 = Date.now();
  const order = await placeOrder(totalCents);
  const placedAt = Date.now();
  console.log(`Order ${order.id} placed. Order Service round-trip: ${placedAt - t0}ms.`);
  console.log(`Now polling Read Model Service every ${POLL_INTERVAL_MS}ms...\n`);

  let lastStatus = null;
  let polls = 0;

  while (Date.now() - t0 < TIMEOUT_MS) {
    const elapsed = Date.now() - t0;
    const { found, status } = await pollStatus(order.id);
    polls += 1;

    // Only print a line when something actually changed — otherwise a
    // fast-converging order would print the same status dozens of times
    // before the loop notices TIMEOUT_MS has elapsed.
    if (status !== lastStatus) {
      console.log(`+${String(elapsed).padStart(5, ' ')}ms  poll #${polls}:  ${found ? status : 'no row yet (404)'}`);
      lastStatus = status;
    }

    if (found && TERMINAL_STATUSES.has(status)) {
      console.log(`\nConverged to a terminal status (${status}) after ${Date.now() - t0}ms and ${polls} polls.`);
      return;
    }

    await sleep(POLL_INTERVAL_MS);
  }

  console.log(`\nTimed out after ${TIMEOUT_MS}ms without reaching a terminal status.`);
  console.log('Check that Saga Orchestrator, Inventory Service, Payment Service, and Read Model Service are all running.');
}

main().catch((err) => {
  console.error('measureConsistencyLag crashed:', err);
  process.exit(1);
});
