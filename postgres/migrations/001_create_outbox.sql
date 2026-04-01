CREATE TABLE IF NOT EXISTS messaging_outbox (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    routing_key TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    headers     JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messaging_outbox_created_at ON messaging_outbox (created_at, id);
