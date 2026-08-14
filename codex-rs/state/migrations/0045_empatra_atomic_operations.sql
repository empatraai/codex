CREATE TABLE empatra_atomic_operations (
    operation_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL,
    issued_at_ms INTEGER NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('reserved', 'accepted', 'cleanup_pending', 'completed', 'failed')),
    error TEXT,
    initial_events_json TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX idx_empatra_atomic_operations_terminal_age
ON empatra_atomic_operations(state, updated_at_ms);

CREATE TABLE empatra_atomic_publication_outbox (
    operation_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    initial_events_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    FOREIGN KEY(operation_id) REFERENCES empatra_atomic_operations(operation_id) ON DELETE CASCADE
);
