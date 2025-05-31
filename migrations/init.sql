CREATE TABLE IF NOT EXISTS scenarios (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL default 'inactive',
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type TEXT,
    aggregate_id TEXT,
    event_type TEXT,
    payload JSONB,
    created_at TIMESTAMP DEFAULT now(),
    processed_at TIMESTAMP NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE
);