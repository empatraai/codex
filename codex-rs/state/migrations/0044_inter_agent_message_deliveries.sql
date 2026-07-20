CREATE TABLE inter_agent_message_deliveries (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL REFERENCES inter_agent_messages(id) ON DELETE CASCADE,
    sender_thread_id TEXT NOT NULL,
    target_thread_id TEXT NOT NULL,
    topic TEXT,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    delivered_at INTEGER,
    acked_at INTEGER,
    expired_at INTEGER,
    dead_lettered_at INTEGER,
    cancelled_at INTEGER,
    last_error TEXT,
    rollout_pointer TEXT,
    rollout_hash TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_inter_agent_message_deliveries_message_status
    ON inter_agent_message_deliveries(message_id, status, updated_at DESC);
