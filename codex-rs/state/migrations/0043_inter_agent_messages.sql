CREATE TABLE inter_agent_messages (
    id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES swarm_runs(id) ON DELETE SET NULL,
    correlation_id TEXT,
    in_reply_to TEXT,
    direct INTEGER NOT NULL DEFAULT 0,
    topic TEXT,
    sender_thread_id TEXT NOT NULL,
    target_thread_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    ttl_seconds INTEGER,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    expires_at INTEGER,
    rollout_pointer TEXT,
    rollout_hash TEXT,
    last_error TEXT,
    metadata_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_inter_agent_messages_status_priority ON inter_agent_messages(status, priority DESC, created_at ASC);
CREATE INDEX idx_inter_agent_messages_correlation ON inter_agent_messages(correlation_id);
CREATE INDEX idx_inter_agent_messages_in_reply_to ON inter_agent_messages(in_reply_to);
