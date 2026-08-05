// A minimal HTTP listener whose ONLY job is to give Docker something to
// healthcheck. Used by every service in this project that otherwise has
// no HTTP server at all — Poller, Inventory Service, Payment Service,
// Saga Orchestrator, Notification Service are all pure Kafka consumers /
// background loops with nothing to curl.
//
// Deliberately plain `http`, not Express — this isn't a real API, it's a
// few bytes of "yes, this process is alive" for `docker compose`'s
// healthcheck to poll. No new dependency.
//
// Honest limitation, same spirit as every other gap flagged in this
// project: this is LIVENESS only ("the process is running and answering
// HTTP"), not READINESS ("this consumer is actually connected to Kafka
// and caught up"). A stricter version would track the last successful
// message-processed timestamp and fail the check if it's gone stale.
// That's closer to what Week 8's consumer-lag metrics are for — this is
// the cheap version that unblocks Docker healthchecks today.

const http = require('http');

function startHealthServer(port = 8080) {
  const server = http.createServer((_req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
  });

  server.listen(port, () => {
    console.log(`[health] listening on :${port} (liveness only)`);
  });

  return server;
}

module.exports = { startHealthServer };
