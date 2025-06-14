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

CREATE TABLE IF NOT EXISTS images (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id TEXT NOT NULL DEFAULT 'unknown',
    class TEXT NOT NULL DEFAULT 'noclass',
    confidence NUMERIC(2,2) NOT NULL DEFAULT 0.0,
    created_at TIMESTAMP NOT NULL DEFAULT now()
)