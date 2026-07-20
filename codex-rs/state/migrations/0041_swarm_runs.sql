CREATE TABLE swarm_runs (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    status TEXT NOT NULL,
    title TEXT,
    spec_json TEXT,
    model_candidate_json TEXT,
    fallback_reason TEXT,
    max_concurrency INTEGER NOT NULL DEFAULT 1,
    token_budget INTEGER,
    runtime_budget_seconds INTEGER,
    fail_fast INTEGER NOT NULL DEFAULT 0,
    cancel_policy TEXT,
    token_usage INTEGER NOT NULL DEFAULT 0,
    runtime_usage_seconds INTEGER NOT NULL DEFAULT 0,
    deadline_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    last_error TEXT,
    result_json TEXT,
    cancelled_at INTEGER
);

CREATE TABLE swarm_tasks (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES swarm_runs(id) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,
    assigned_thread_id TEXT,
    order_index INTEGER NOT NULL,
    depends_on_task_ids_json TEXT NOT NULL DEFAULT '[]',
    task_kind TEXT NOT NULL,
    agent_type TEXT,
    instructions TEXT,
    result_token TEXT NOT NULL,
    status TEXT NOT NULL,
    model_candidate_json TEXT,
    model_route_json TEXT,
    candidate_index INTEGER,
    fallback_reason TEXT,
    lease_owner TEXT,
    lease_until INTEGER,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    token_usage INTEGER NOT NULL DEFAULT 0,
    runtime_usage_seconds INTEGER NOT NULL DEFAULT 0,
    deadline_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    last_error TEXT,
    result_json TEXT
);

CREATE TABLE swarm_attempts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES swarm_runs(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES swarm_tasks(id) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,
    status TEXT NOT NULL,
    lease_owner TEXT,
    lease_until INTEGER,
    model_candidate_json TEXT,
    fallback_reason TEXT,
    token_usage INTEGER NOT NULL DEFAULT 0,
    runtime_usage_seconds INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    updated_at INTEGER NOT NULL,
    last_error TEXT,
    result_json TEXT
);

CREATE INDEX idx_swarm_runs_thread_status ON swarm_runs(thread_id, status, updated_at DESC);
CREATE INDEX idx_swarm_tasks_run_order ON swarm_tasks(run_id, order_index ASC, id ASC);
CREATE INDEX idx_swarm_tasks_run_status ON swarm_tasks(run_id, status, order_index ASC);
CREATE INDEX idx_swarm_attempts_task_created_at ON swarm_attempts(task_id, created_at DESC);
CREATE INDEX idx_swarm_attempts_run_status ON swarm_attempts(run_id, status, created_at DESC);
