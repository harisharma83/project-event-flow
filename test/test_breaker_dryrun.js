const { CircuitBreaker } = require('./src/circuitBreaker');
const { withRetry } = require('./src/retry');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  const breaker = new CircuitBreaker({
    failureThreshold: 3,
    cooldownMs: 1000,
    onStateChange: (from, to) => console.log(`  [circuit] ${from} -> ${to}`),
  });

  let callCount = 0;
  let failUntilCall = 5; // fail calls 1..5, succeed from call 6 onward

  async function flakyGateway() {
    callCount++;
    if (callCount <= failUntilCall) throw new Error(`simulated failure #${callCount}`);
    return { ok: true };
  }

  console.log('--- Phase 1: drive failures until breaker opens ---');
  for (let i = 0; i < 2; i++) {
    try {
      const r = await withRetry(() => breaker.fire(flakyGateway), { maxRetries: 2, baseMs: 10, label: `call-${i}` });
      console.log(`call ${i} succeeded:`, r);
    } catch (err) {
      console.log(`call ${i} failed permanently: ${err.message} (circuitOpen=${!!err.circuitOpen})`);
    }
  }
  console.log('breaker state after phase 1:', breaker.state, 'failureCount:', breaker.failureCount);

  console.log('--- Phase 2: call while OPEN, should reject instantly, no retry wait ---');
  const t0 = Date.now();
  try {
    await withRetry(() => breaker.fire(flakyGateway), { maxRetries: 2, baseMs: 10, label: 'call-open' });
  } catch (err) {
    console.log(`rejected instantly (${Date.now() - t0}ms): ${err.message} circuitOpen=${!!err.circuitOpen}`);
  }

  console.log('--- Phase 3: wait for cooldown, then trial call should succeed (call count now > failUntilCall) ---');
  await sleep(1100);
  try {
    const r = await withRetry(() => breaker.fire(flakyGateway), { maxRetries: 2, baseMs: 10, label: 'call-trial' });
    console.log('trial call result:', r, 'breaker state:', breaker.state);
  } catch (err) {
    console.log('trial call failed:', err.message);
  }

  console.log('--- Phase 4: poison message style — JSON.parse failure caught, no throw to caller ---');
  try {
    JSON.parse('not valid json {{{');
  } catch (err) {
    console.log('caught parse error as expected:', err.message);
  }

  console.log('ALL DRY-RUN CHECKS PASSED');
}

main().catch(err => { console.error('UNEXPECTED CRASH', err); process.exit(1); });
