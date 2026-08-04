-- EventFlow Week 1: Order Service schema (Postgres)

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID NOT NULL,
  status TEXT NOT NULL DEFAULT 'CREATED',
  total_cents INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The outbox table. A row here is written in the SAME transaction as the
-- orders row it describes. A separate poller process (src/poller.js) is
-- the only thing that ever talks to Kafka.
CREATE TABLE IF NOT EXISTS outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type TEXT NOT NULL,      -- e.g. 'order'
  aggregate_id UUID NOT NULL,        -- the order id; used as the Kafka message key
  event_type TEXT NOT NULL,          -- e.g. 'OrderCreated'
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ           -- NULL until the poller publishes it
);

-- Speeds up "give me the unpublished rows" without scanning the whole table.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
  ON outbox (created_at)
  WHERE published_at IS NULL;

-- EventFlow Week 2: saga participants (Inventory, Payment) and the
-- orchestrator's own state table.

CREATE TABLE IF NOT EXISTS inventory (
  sku TEXT PRIMARY KEY,
  available_qty INTEGER NOT NULL
);

INSERT INTO inventory (sku, available_qty)
VALUES ('DEMO-SKU', 5)
ON CONFLICT (sku) DO NOTHING;

CREATE TABLE IF NOT EXISTS inventory_reservations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL,
  sku TEXT NOT NULL DEFAULT 'DEMO-SKU',
  qty INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'RESERVED',   -- RESERVED | RELEASED
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  released_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS payments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL,
  amount_cents INTEGER NOT NULL,
  status TEXT NOT NULL,                      -- SUCCEEDED | FAILED
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The orchestrator's own durable state: which state each in-flight saga
-- is in, plus whatever context (order total, customer) it needs to pass
-- along to each command it issues.
CREATE TABLE IF NOT EXISTS saga_state (
  order_id UUID PRIMARY KEY,
  current_state TEXT NOT NULL,
  context JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- EventFlow Week 3: idempotency. Each order should ever get exactly one
-- reservation and exactly one payment attempt via this saga, no matter
-- how many times Kafka redelivers the command that triggers it. These
-- constraints are what inventoryService.js and paymentService.js's
-- dedup logic actually relies on for correctness — the application-level
-- check-then-act is a fast path; this is the real guarantee underneath it.
--
-- Postgres has no "ADD CONSTRAINT IF NOT EXISTS", so this DO block is the
-- standard idiom for making an ALTER TABLE safe to run more than once:
-- try it, and if it already exists (duplicate_object), do nothing.
DO $$
BEGIN
  ALTER TABLE inventory_reservations ADD CONSTRAINT inventory_reservations_order_id_key UNIQUE (order_id);
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE payments ADD CONSTRAINT payments_order_id_key UNIQUE (order_id);
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;
