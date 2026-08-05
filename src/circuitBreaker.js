// A hand-rolled circuit breaker. Same reasoning as the Week 2 saga
// interpreter: this is a well-understood, small piece of logic, and
// building it ourselves teaches the mechanics instead of hiding them
// behind a library. No new npm dependency.
//
// Three states, exactly the ones from this week's diagram:
//   CLOSED    - normal operation. Calls go through. Failures are counted.
//               Enough consecutive-ish failures -> trip to OPEN.
//   OPEN      - calls are rejected immediately, without even attempting
//               the wrapped function. This is the whole point: stop
//               hammering a dependency you already know is down. After
//               cooldownMs has passed, the NEXT call is let through as a
//               trial -> moves to HALF_OPEN.
//   HALF_OPEN - exactly one trial call decides the outcome. Success ->
//               back to CLOSED, counters reset. Failure -> back to OPEN,
//               cooldown timer restarts.
//
// Deliberately generic: fire() takes any async function and knows
// nothing about payments, gateways, or Kafka. paymentService.js is the
// only file that knows what's being protected.

class CircuitBreaker {
  constructor({ failureThreshold = 5, cooldownMs = 10000, onStateChange } = {}) {
    this.failureThreshold = failureThreshold;
    this.cooldownMs = cooldownMs;
    this.onStateChange = onStateChange || (() => {});

    this.state = 'CLOSED';
    this.failureCount = 0;
    this.openedAt = null;
  }

  _setState(next) {
    if (this.state !== next) {
      this.onStateChange(this.state, next);
      this.state = next;
    }
  }

  _recordSuccess() {
    this.failureCount = 0;
    this._setState('CLOSED');
  }

  _recordFailure() {
    this.failureCount += 1;

    if (this.state === 'HALF_OPEN') {
      // The trial call failed. The dependency is still down — back to
      // OPEN, and the cooldown clock restarts from now.
      this.openedAt = Date.now();
      this._setState('OPEN');
      return;
    }

    if (this.failureCount >= this.failureThreshold) {
      this.openedAt = Date.now();
      this._setState('OPEN');
    }
  }

  // Decides whether fire() is even allowed to attempt the call. This is
  // where OPEN -> HALF_OPEN happens, the moment the cooldown elapses.
  _canAttempt() {
    if (this.state === 'CLOSED') return true;

    if (this.state === 'OPEN') {
      const elapsed = Date.now() - this.openedAt;
      if (elapsed >= this.cooldownMs) {
        this._setState('HALF_OPEN');
        return true; // this call IS the trial
      }
      return false;
    }

    // HALF_OPEN: this project has one consumer processing messages one
    // at a time, so there's no real risk of multiple concurrent trials
    // racing each other. A production breaker guarding concurrent
    // callers would need a lock here; not needed for this shape.
    return true;
  }

  async fire(fn) {
    if (!this._canAttempt()) {
      const err = new Error(`Circuit breaker is OPEN — call rejected without attempting it`);
      err.circuitOpen = true;
      throw err;
    }

    try {
      const result = await fn();
      this._recordSuccess();
      return result;
    } catch (err) {
      this._recordFailure();
      throw err;
    }
  }
}

module.exports = { CircuitBreaker };
