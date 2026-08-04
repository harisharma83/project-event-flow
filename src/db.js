// Single shared Postgres connection pool, used by both the HTTP server
// and the poller. `pg` handles pooling/reconnect for us.
require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

pool.on('error', (err) => {
  // A background error on an idle client shouldn't crash the process,
  // but we do want to know about it.
  console.error('Unexpected Postgres pool error:', err);
});

module.exports = { pool };
