CREATE TABLE swarm_checkpoints (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES swarm_runs(id) ON DELETE CASCADE,
    task_id TEXT REFERENCES swarm_tasks(id) ON DELETE SET NULL,
    thread_id TEXT NOT NULL,
    checkpoint_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_swarm_checkpoints_run_created_at ON swarm_checkpoints(run_id, created_at DESC, id DESC);
