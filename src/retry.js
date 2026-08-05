// Retry with exponential backoff and jitter. Generic — takes any async
// function and a label for logging, knows nothing about payments.
//
// Exponential backoff: each retry waits longer than the last
// (baseMs, 2*baseMs, 4*baseMs, ...) instead of hammering the dependency
// at a fixed interval.
//
// Jitter: a random amount is added on top of the computed backoff. If
// ten consumer instances all failed at the same instant and all retried
// on the exact same fixed schedule, they'd all hit the recovering
// service again at the exact same moment — a self-inflicted retry
// storm. Randomizing the wait spreads those retries out.
//
// If the wrapped function throws an error with `circuitOpen: true` (see
// circuitBreaker.js), this stops retrying immediately instead of
// waiting out a full backoff — the breaker has already made the "don't
// bother, it's known-down" decision, so retrying here would just be
// wasted time on top of the breaker's own cooldown.

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetry(fn, { maxRetries = 3, baseMs = 500, label = 'operation' } = {}) {
  let attempt = 0;

  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt += 1;

      if (err.circuitOpen || attempt > maxRetries) {
        throw err;
      }

      const backoff = baseMs * 2 ** (attempt - 1);
      const jitter = Math.random() * backoff * 0.5;
      const waitMs = Math.round(backoff + jitter);

      console.log(
        `[retry] ${label}: attempt ${attempt} failed (${err.message}), retrying in ${waitMs}ms`
      );
      await sleep(waitMs);
    }
  }
}

module.exports = { withRetry };
