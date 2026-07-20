use super::*;
use crate::model::InterAgentMessageDeliveryRow;
use crate::model::InterAgentMessageRow;
use crate::model::SwarmAttemptRow;
use crate::model::SwarmCheckpointRow;
use crate::model::SwarmRunRow;
use crate::model::SwarmTaskRow;
use anyhow::anyhow;
use chrono::Utc;
use serde_json::Value;
use sqlx::Row;

fn to_json<T: serde::Serialize>(value: &T) -> anyhow::Result<String> {
    Ok(serde_json::to_string(value)?)
}

fn to_opt_json(value: Option<&Value>) -> anyhow::Result<Option<String>> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(Into::into)
}

impl StateRuntime {
    /// Persists a validated run and its entire task DAG atomically, then makes the run runnable.
    /// A process crash can therefore never leave a recoverable run without its tasks.
    pub async fn create_runnable_swarm_run(
        &self,
        params: &SwarmRunCreateParams,
        tasks: &[SwarmTaskCreateParams],
    ) -> anyhow::Result<SwarmRun> {
        if tasks.is_empty() {
            return Err(anyhow!("a runnable swarm run requires at least one task"));
        }
        if tasks.iter().any(|task| task.run_id != params.id) {
            return Err(anyhow!("every swarm task must belong to the created run"));
        }

        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
INSERT INTO swarm_runs (
    id, thread_id, status, title, spec_json, model_candidate_json, fallback_reason,
    max_concurrency, token_budget, runtime_budget_seconds, fail_fast, cancel_policy,
    token_usage, runtime_usage_seconds, deadline_at, created_at, updated_at,
    started_at, completed_at, last_error, result_json, cancelled_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, NULL, NULL, NULL, NULL)
            "#,
        )
        .bind(params.id.as_str())
        .bind(params.thread_id.as_str())
        .bind(SwarmRunStatus::Running.as_str())
        .bind(params.title.as_deref())
        .bind(to_opt_json(params.spec_json.as_ref())?)
        .bind(to_opt_json(params.model_candidate_json.as_ref())?)
        .bind(params.fallback_reason.as_deref())
        .bind(params.max_concurrency)
        .bind(params.token_budget)
        .bind(params.runtime_budget_seconds)
        .bind(i64::from(params.fail_fast))
        .bind(params.cancel_policy.as_deref())
        .bind(params.deadline_at.map(|dt| dt.timestamp()))
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        for task in tasks {
            sqlx::query(
                r#"
INSERT INTO swarm_tasks (
    id, run_id, thread_id, assigned_thread_id, order_index, depends_on_task_ids_json,
    task_kind, agent_type, instructions, result_token, status, model_candidate_json, model_route_json,
    candidate_index, fallback_reason, lease_owner, lease_until, max_attempts,
    attempt_count, token_usage, runtime_usage_seconds, deadline_at, created_at,
    updated_at, started_at, completed_at, last_error, result_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)
                "#,
            )
            .bind(task.id.as_str())
            .bind(task.run_id.as_str())
            .bind(task.thread_id.as_str())
            .bind(task.assigned_thread_id.as_deref())
            .bind(task.order_index)
            .bind(to_json(&task.depends_on_task_ids)?)
            .bind(task.task_kind.as_str())
            .bind(task.agent_type.as_deref())
            .bind(task.instructions.as_deref())
            .bind(task.result_token.as_str())
            .bind(SwarmTaskStatus::Pending.as_str())
            .bind(to_opt_json(task.model_candidate_json.as_ref())?)
            .bind(to_opt_json(task.model_route_json.as_ref())?)
            .bind(task.candidate_index)
            .bind(task.fallback_reason.as_deref())
            .bind(None::<&str>)
            .bind(None::<i64>)
            .bind(task.max_attempts)
            .bind(0_i64)
            .bind(0_i64)
            .bind(0_i64)
            .bind(task.deadline_at.map(|dt| dt.timestamp()))
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        self.get_swarm_run(params.id.as_str())
            .await?
            .ok_or_else(|| anyhow!("failed to load created runnable swarm run"))
    }

    pub async fn create_swarm_run(
        &self,
        params: &SwarmRunCreateParams,
    ) -> anyhow::Result<SwarmRun> {
        let now = Utc::now().timestamp();
        sqlx::query(
            r#"
INSERT INTO swarm_runs (
    id, thread_id, status, title, spec_json, model_candidate_json, fallback_reason,
    max_concurrency, token_budget, runtime_budget_seconds, fail_fast, cancel_policy,
    token_usage, runtime_usage_seconds, deadline_at, created_at, updated_at,
    started_at, completed_at, last_error, result_json, cancelled_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, NULL, NULL, NULL, NULL, NULL)
            "#,
        )
        .bind(params.id.as_str())
        .bind(params.thread_id.as_str())
        .bind(SwarmRunStatus::Pending.as_str())
        .bind(params.title.as_deref())
        .bind(to_opt_json(params.spec_json.as_ref())?)
        .bind(to_opt_json(params.model_candidate_json.as_ref())?)
        .bind(params.fallback_reason.as_deref())
        .bind(params.max_concurrency)
        .bind(params.token_budget)
        .bind(params.runtime_budget_seconds)
        .bind(i64::from(params.fail_fast))
        .bind(params.cancel_policy.as_deref())
        .bind(params.deadline_at.map(|dt| dt.timestamp()))
        .bind(now)
        .bind(now)
        .execute(self.pool.as_ref())
        .await?;
        self.get_swarm_run(params.id.as_str())
            .await?
            .ok_or_else(|| anyhow!("failed to load created swarm run"))
    }

    pub async fn create_swarm_tasks(
        &self,
        tasks: &[SwarmTaskCreateParams],
    ) -> anyhow::Result<Vec<SwarmTask>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }
        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        for task in tasks {
            sqlx::query(
                r#"
INSERT INTO swarm_tasks (
    id, run_id, thread_id, assigned_thread_id, order_index, depends_on_task_ids_json,
    task_kind, agent_type, instructions, result_token, status, model_candidate_json, model_route_json,
    candidate_index, fallback_reason, lease_owner, lease_until, max_attempts,
    attempt_count, token_usage, runtime_usage_seconds, deadline_at, created_at,
    updated_at, started_at, completed_at, last_error, result_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)
                "#,
            )
            .bind(task.id.as_str())
            .bind(task.run_id.as_str())
            .bind(task.thread_id.as_str())
            .bind(task.assigned_thread_id.as_deref())
            .bind(task.order_index)
            .bind(to_json(&task.depends_on_task_ids)?)
            .bind(task.task_kind.as_str())
            .bind(task.agent_type.as_deref())
            .bind(task.instructions.as_deref())
            .bind(task.result_token.as_str())
            .bind(SwarmTaskStatus::Pending.as_str())
            .bind(to_opt_json(task.model_candidate_json.as_ref())?)
            .bind(to_opt_json(task.model_route_json.as_ref())?)
            .bind(task.candidate_index)
            .bind(task.fallback_reason.as_deref())
            .bind(None::<&str>)
            .bind(None::<i64>)
            .bind(task.max_attempts)
            .bind(0_i64)
            .bind(0_i64)
            .bind(0_i64)
            .bind(task.deadline_at.map(|dt| dt.timestamp()))
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        let mut out = Vec::with_capacity(tasks.len());
        for task in tasks {
            out.push(
                self.get_swarm_task(task.id.as_str())
                    .await?
                    .ok_or_else(|| anyhow!("failed to load created swarm task"))?,
            );
        }
        Ok(out)
    }

    pub async fn get_swarm_run(&self, run_id: &str) -> anyhow::Result<Option<SwarmRun>> {
        let row = sqlx::query_as::<_, SwarmRunRow>("SELECT * FROM swarm_runs WHERE id = ?")
            .bind(run_id)
            .fetch_optional(self.pool.as_ref())
            .await?;
        row.map(SwarmRun::try_from).transpose()
    }

    pub async fn list_active_swarm_runs(&self, thread_id: &str) -> anyhow::Result<Vec<SwarmRun>> {
        sqlx::query_as::<_, SwarmRunRow>(
            "SELECT * FROM swarm_runs WHERE thread_id = ? AND status IN (?, ?) ORDER BY created_at ASC, id ASC",
        )
        .bind(thread_id)
        .bind(SwarmRunStatus::Pending.as_str())
        .bind(SwarmRunStatus::Running.as_str())
        .fetch_all(self.pool.as_ref())
        .await?
        .into_iter()
        .map(SwarmRun::try_from)
        .collect()
    }

    pub async fn get_swarm_task(&self, task_id: &str) -> anyhow::Result<Option<SwarmTask>> {
        let row = sqlx::query_as::<_, SwarmTaskRow>("SELECT * FROM swarm_tasks WHERE id = ?")
            .bind(task_id)
            .fetch_optional(self.pool.as_ref())
            .await?;
        row.map(SwarmTask::try_from).transpose()
    }

    pub async fn get_swarm_attempt(
        &self,
        attempt_id: &str,
    ) -> anyhow::Result<Option<SwarmAttempt>> {
        let row = sqlx::query_as::<_, SwarmAttemptRow>("SELECT * FROM swarm_attempts WHERE id = ?")
            .bind(attempt_id)
            .fetch_optional(self.pool.as_ref())
            .await?;
        row.map(SwarmAttempt::try_from).transpose()
    }

    pub async fn get_swarm_run_progress(&self, run_id: &str) -> anyhow::Result<SwarmRunProgress> {
        let row = sqlx::query(
            r#"
SELECT
    COUNT(*) AS total_tasks,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_tasks,
    SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END) AS ready_tasks,
    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_tasks,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_tasks,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_tasks,
    SUM(CASE WHEN status = 'retryable' THEN 1 ELSE 0 END) AS retryable_tasks,
    SUM(CASE WHEN status = 'escalated' THEN 1 ELSE 0 END) AS escalated_tasks,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_tasks
FROM swarm_tasks
WHERE run_id = ?
            "#,
        )
        .bind(run_id)
        .fetch_one(self.pool.as_ref())
        .await?;
        Ok(SwarmRunProgress {
            total_tasks: row.try_get::<i64, _>("total_tasks")? as usize,
            pending_tasks: row.try_get::<Option<i64>, _>("pending_tasks")?.unwrap_or(0) as usize,
            ready_tasks: row.try_get::<Option<i64>, _>("ready_tasks")?.unwrap_or(0) as usize,
            running_tasks: row.try_get::<Option<i64>, _>("running_tasks")?.unwrap_or(0) as usize,
            completed_tasks: row
                .try_get::<Option<i64>, _>("completed_tasks")?
                .unwrap_or(0) as usize,
            failed_tasks: row.try_get::<Option<i64>, _>("failed_tasks")?.unwrap_or(0) as usize,
            retryable_tasks: row
                .try_get::<Option<i64>, _>("retryable_tasks")?
                .unwrap_or(0) as usize,
            escalated_tasks: row
                .try_get::<Option<i64>, _>("escalated_tasks")?
                .unwrap_or(0) as usize,
            cancelled_tasks: row
                .try_get::<Option<i64>, _>("cancelled_tasks")?
                .unwrap_or(0) as usize,
        })
    }

    pub async fn mark_swarm_run_running(&self, run_id: &str) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE swarm_runs SET status = ?, started_at = COALESCE(started_at, ?), completed_at = NULL, cancelled_at = NULL, updated_at = ?, last_error = NULL WHERE id = ? AND status IN (?, ?)",
        )
        .bind(SwarmRunStatus::Running.as_str())
        .bind(now)
        .bind(now)
        .bind(run_id)
        .bind(SwarmRunStatus::Pending.as_str())
        .bind(SwarmRunStatus::Running.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn mark_swarm_run_completed(
        &self,
        run_id: &str,
        result_json: Option<&Value>,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE swarm_runs SET status = ?, completed_at = COALESCE(completed_at, ?), updated_at = ?, last_error = NULL, result_json = COALESCE(result_json, ?) WHERE id = ? AND status IN (?, ?)",
        )
        .bind(SwarmRunStatus::Completed.as_str())
        .bind(now)
        .bind(now)
        .bind(to_opt_json(result_json)?)
        .bind(run_id)
        .bind(SwarmRunStatus::Running.as_str())
        .bind(SwarmRunStatus::Completed.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn mark_swarm_run_failed(&self, run_id: &str, error: &str) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
UPDATE swarm_tasks
SET status = ?, lease_owner = NULL, assigned_thread_id = NULL, lease_until = NULL,
    completed_at = COALESCE(completed_at, ?), updated_at = ?,
    last_error = COALESCE(last_error, ?)
WHERE run_id = ? AND status NOT IN (?, ?, ?)
            "#,
        )
        .bind(SwarmTaskStatus::Cancelled.as_str())
        .bind(now)
        .bind(now)
        .bind(error)
        .bind(run_id)
        .bind(SwarmTaskStatus::Completed.as_str())
        .bind(SwarmTaskStatus::Failed.as_str())
        .bind(SwarmTaskStatus::Cancelled.as_str())
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
UPDATE swarm_attempts
SET status = ?, lease_owner = NULL, lease_until = NULL,
    completed_at = COALESCE(completed_at, ?), updated_at = ?,
    last_error = COALESCE(last_error, ?)
WHERE run_id = ? AND status = ?
            "#,
        )
        .bind(SwarmAttemptStatus::Failed.as_str())
        .bind(now)
        .bind(now)
        .bind(error)
        .bind(run_id)
        .bind(SwarmAttemptStatus::Running.as_str())
        .execute(&mut *tx)
        .await?;
        let result = sqlx::query(
            "UPDATE swarm_runs SET status = ?, completed_at = COALESCE(completed_at, ?), updated_at = ?, last_error = ? WHERE id = ? AND status IN (?, ?)",
        )
        .bind(SwarmRunStatus::Failed.as_str())
        .bind(now)
        .bind(now)
        .bind(error)
        .bind(run_id)
        .bind(SwarmRunStatus::Running.as_str())
        .bind(SwarmRunStatus::Failed.as_str())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn complete_swarm_run_if_done(
        &self,
        run_id: &str,
        result_json: Option<&Value>,
    ) -> anyhow::Result<bool> {
        let progress = self.get_swarm_run_progress(run_id).await?;
        if progress.running_tasks > 0
            || progress.pending_tasks > 0
            || progress.ready_tasks > 0
            || progress.retryable_tasks > 0
            || progress.escalated_tasks > 0
        {
            return Ok(false);
        }
        if progress.failed_tasks > 0 || progress.cancelled_tasks > 0 {
            return Ok(false);
        }
        self.mark_swarm_run_completed(run_id, result_json).await
    }

    pub async fn create_swarm_checkpoint(
        &self,
        checkpoint_id: &str,
        run_id: &str,
        task_id: Option<&str>,
        thread_id: &str,
        checkpoint_type: &str,
        payload_json: &Value,
    ) -> anyhow::Result<SwarmCheckpoint> {
        let now = Utc::now().timestamp();
        sqlx::query(
            r#"
INSERT INTO swarm_checkpoints (
    id, run_id, task_id, thread_id, checkpoint_type, payload_json, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(checkpoint_id)
        .bind(run_id)
        .bind(task_id)
        .bind(thread_id)
        .bind(checkpoint_type)
        .bind(serde_json::to_string(payload_json)?)
        .bind(now)
        .execute(self.pool.as_ref())
        .await?;
        let row =
            sqlx::query_as::<_, SwarmCheckpointRow>("SELECT * FROM swarm_checkpoints WHERE id = ?")
                .bind(checkpoint_id)
                .fetch_optional(self.pool.as_ref())
                .await?;
        row.map(SwarmCheckpoint::try_from)
            .transpose()?
            .ok_or_else(|| anyhow!("failed to load created swarm checkpoint"))
    }

    pub async fn claim_ready_swarm_task(
        &self,
        run_id: &str,
        lease_owner: &str,
        lease_seconds: i64,
    ) -> anyhow::Result<Option<SwarmTask>> {
        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
SELECT task.id
FROM swarm_tasks AS task
WHERE task.run_id = ?
  AND task.status IN (?, ?, ?, ?)
  AND task.attempt_count < task.max_attempts
  AND (task.lease_until IS NULL OR task.lease_until <= ?)
  AND NOT EXISTS (
      SELECT 1
      FROM swarm_tasks AS dep
      WHERE dep.run_id = task.run_id
        AND EXISTS (
            SELECT 1
            FROM json_each(task.depends_on_task_ids_json)
            WHERE json_each.value = dep.id
        )
        AND dep.status <> ?
  )
  AND NOT EXISTS (
      SELECT 1
      FROM swarm_tasks AS dep
      WHERE dep.run_id = task.run_id
        AND EXISTS (
            SELECT 1
            FROM json_each(task.depends_on_task_ids_json)
            WHERE json_each.value = dep.id
        )
        AND dep.status IN (?, ?)
  )
ORDER BY task.order_index ASC, task.id ASC
LIMIT 1
            "#,
        )
        .bind(run_id)
        .bind(SwarmTaskStatus::Pending.as_str())
        .bind(SwarmTaskStatus::Ready.as_str())
        .bind(SwarmTaskStatus::Retryable.as_str())
        .bind(SwarmTaskStatus::Escalated.as_str())
        .bind(now)
        .bind(SwarmTaskStatus::Completed.as_str())
        .bind(SwarmTaskStatus::Failed.as_str())
        .bind(SwarmTaskStatus::Cancelled.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        let task_id: String = row.try_get("id")?;
        let lease_until = now + lease_seconds.max(1);
        let result = sqlx::query(
            r#"
UPDATE swarm_tasks
SET status = ?, lease_owner = ?, lease_until = ?,
    attempt_count = attempt_count + 1, started_at = COALESCE(started_at, ?),
    updated_at = ?, last_error = NULL
WHERE id = ? AND run_id = ? AND status IN (?, ?, ?, ?)
  AND attempt_count < max_attempts
  AND (lease_until IS NULL OR lease_until <= ?)
            "#,
        )
        .bind(SwarmTaskStatus::Running.as_str())
        .bind(lease_owner)
        .bind(lease_until)
        .bind(now)
        .bind(now)
        .bind(task_id.as_str())
        .bind(run_id)
        .bind(SwarmTaskStatus::Pending.as_str())
        .bind(SwarmTaskStatus::Ready.as_str())
        .bind(SwarmTaskStatus::Retryable.as_str())
        .bind(SwarmTaskStatus::Escalated.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }
        self.get_swarm_task(task_id.as_str()).await
    }

    pub async fn bind_swarm_task_thread(
        &self,
        task_id: &str,
        lease_owner: &str,
        assigned_thread_id: &str,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE swarm_tasks SET assigned_thread_id = ?, updated_at = ? WHERE id = ? AND lease_owner = ? AND status = ?",
        )
        .bind(assigned_thread_id)
        .bind(now)
        .bind(task_id)
        .bind(lease_owner)
        .bind(SwarmTaskStatus::Running.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn update_swarm_task_routing(
        &self,
        task_id: &str,
        lease_owner: &str,
        model_candidate_json: &Value,
        model_route_json: &Value,
        candidate_index: i64,
        fallback_reason: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE swarm_tasks
SET model_candidate_json = ?, model_route_json = ?, candidate_index = ?,
    fallback_reason = ?, updated_at = ?
WHERE id = ? AND lease_owner = ? AND status = ?
            "#,
        )
        .bind(to_opt_json(Some(model_candidate_json))?)
        .bind(to_opt_json(Some(model_route_json))?)
        .bind(candidate_index)
        .bind(fallback_reason)
        .bind(now)
        .bind(task_id)
        .bind(lease_owner)
        .bind(SwarmTaskStatus::Running.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn renew_swarm_task_lease(
        &self,
        task_id: &str,
        worker_id: &str,
        lease_seconds: i64,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let lease_until = now + lease_seconds.max(1);
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE swarm_tasks SET lease_until = ?, updated_at = ? WHERE id = ? AND lease_owner = ? AND status = ? AND lease_until IS NOT NULL AND lease_until > ?",
        )
        .bind(lease_until)
        .bind(now)
        .bind(task_id)
        .bind(worker_id)
        .bind(SwarmTaskStatus::Running.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() > 0 {
            sqlx::query(
                "UPDATE swarm_attempts SET lease_until = ?, updated_at = ? WHERE task_id = ? AND lease_owner = ? AND status = ?",
            )
            .bind(lease_until)
            .bind(now)
            .bind(task_id)
            .bind(worker_id)
            .bind(SwarmAttemptStatus::Running.as_str())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn release_swarm_task_lease(
        &self,
        task_id: &str,
        lease_owner: &str,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE swarm_tasks SET lease_owner = NULL, assigned_thread_id = NULL, lease_until = NULL, updated_at = ? WHERE id = ? AND lease_owner = ? AND status = ?",
        )
        .bind(now)
        .bind(task_id)
        .bind(lease_owner)
        .bind(SwarmTaskStatus::Running.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn record_swarm_attempt_start(
        &self,
        attempt_id: &str,
        run_id: &str,
        task_id: &str,
        thread_id: &str,
        lease_owner: Option<&str>,
        lease_seconds: Option<i64>,
        model_candidate_json: Option<&Value>,
        fallback_reason: Option<&str>,
    ) -> anyhow::Result<SwarmAttempt> {
        let now = Utc::now().timestamp();
        sqlx::query(
            r#"
INSERT INTO swarm_attempts (
    id, run_id, task_id, thread_id, status, lease_owner, lease_until,
    model_candidate_json, fallback_reason, token_usage, runtime_usage_seconds,
    created_at, started_at, completed_at, updated_at, last_error, result_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, NULL, ?, NULL, NULL)
            "#,
        )
        .bind(attempt_id)
        .bind(run_id)
        .bind(task_id)
        .bind(thread_id)
        .bind(SwarmAttemptStatus::Running.as_str())
        .bind(lease_owner)
        .bind(lease_seconds.map(|secs| now + secs.max(1)))
        .bind(to_opt_json(model_candidate_json)?)
        .bind(fallback_reason)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(self.pool.as_ref())
        .await?;
        self.get_swarm_attempt(attempt_id)
            .await?
            .ok_or_else(|| anyhow!("failed to load created swarm attempt"))
    }

    pub async fn bind_swarm_attempt_thread(
        &self,
        attempt_id: &str,
        lease_owner: &str,
        thread_id: &str,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE swarm_attempts SET thread_id = ?, updated_at = ? WHERE id = ? AND lease_owner = ? AND status = ?",
        )
        .bind(thread_id)
        .bind(now)
        .bind(attempt_id)
        .bind(lease_owner)
        .bind(SwarmAttemptStatus::Running.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn record_swarm_attempt_end(
        &self,
        attempt_id: &str,
        status: SwarmAttemptStatus,
        token_usage: i64,
        runtime_usage_seconds: i64,
        result_json: Option<&Value>,
        last_error: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        let attempt = sqlx::query(
            "SELECT run_id, task_id, started_at FROM swarm_attempts WHERE id = ? AND status = ?",
        )
        .bind(attempt_id)
        .bind(SwarmAttemptStatus::Running.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(attempt) = attempt else {
            tx.commit().await?;
            return Ok(false);
        };
        let run_id: String = attempt.try_get("run_id")?;
        let task_id: String = attempt.try_get("task_id")?;
        let started_at: Option<i64> = attempt.try_get("started_at")?;
        let effective_runtime = if runtime_usage_seconds > 0 {
            runtime_usage_seconds
        } else {
            started_at.map_or(0, |started| now.saturating_sub(started))
        };
        let result = sqlx::query(
            r#"
UPDATE swarm_attempts
SET status = ?, token_usage = token_usage + ?, runtime_usage_seconds = runtime_usage_seconds + ?,
    result_json = ?, last_error = ?, completed_at = COALESCE(completed_at, ?), updated_at = ?
WHERE id = ? AND status = ?
            "#,
        )
        .bind(status.as_str())
        .bind(token_usage)
        .bind(effective_runtime)
        .bind(to_opt_json(result_json)?)
        .bind(last_error)
        .bind(now)
        .bind(now)
        .bind(attempt_id)
        .bind(SwarmAttemptStatus::Running.as_str())
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(false);
        }
        sqlx::query(
            "UPDATE swarm_tasks SET token_usage = token_usage + ?, runtime_usage_seconds = runtime_usage_seconds + ?, updated_at = ? WHERE id = ?",
        )
        .bind(token_usage)
        .bind(effective_runtime)
        .bind(now)
        .bind(task_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE swarm_runs SET token_usage = token_usage + ?, runtime_usage_seconds = runtime_usage_seconds + ?, updated_at = ? WHERE id = ?",
        )
        .bind(token_usage)
        .bind(effective_runtime)
        .bind(now)
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Commits a worker report, its attempt accounting, and the resulting task transition as one
    /// transaction. Cancellation and lease-expiry races therefore cannot close an attempt while
    /// leaving its task in the running state.
    pub async fn finalize_swarm_worker_report(
        &self,
        attempt_id: &str,
        task_id: &str,
        disposition: SwarmTaskAttemptDisposition,
        token_usage: i64,
        runtime_usage_seconds: i64,
        result_json: Option<&Value>,
        last_error: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        let attempt = sqlx::query(
            r#"
SELECT attempt.run_id, attempt.started_at, task.attempt_count, task.max_attempts
FROM swarm_attempts AS attempt
JOIN swarm_tasks AS task ON task.id = attempt.task_id
JOIN swarm_runs AS run ON run.id = attempt.run_id
WHERE attempt.id = ? AND attempt.task_id = ? AND attempt.status = ?
  AND task.status = ? AND run.status = ?
            "#,
        )
        .bind(attempt_id)
        .bind(task_id)
        .bind(SwarmAttemptStatus::Running.as_str())
        .bind(SwarmTaskStatus::Running.as_str())
        .bind(SwarmRunStatus::Running.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(attempt) = attempt else {
            tx.rollback().await?;
            return Ok(false);
        };
        let run_id: String = attempt.try_get("run_id")?;
        let started_at: Option<i64> = attempt.try_get("started_at")?;
        let attempt_count: i64 = attempt.try_get("attempt_count")?;
        let max_attempts: i64 = attempt.try_get("max_attempts")?;
        if matches!(
            disposition,
            SwarmTaskAttemptDisposition::Retryable | SwarmTaskAttemptDisposition::Escalated
        ) && attempt_count >= max_attempts
        {
            tx.rollback().await?;
            return Ok(false);
        }

        let effective_runtime = if runtime_usage_seconds > 0 {
            runtime_usage_seconds
        } else {
            started_at.map_or(0, |started| now.saturating_sub(started))
        };
        let attempt_status = match disposition {
            SwarmTaskAttemptDisposition::Succeeded => SwarmAttemptStatus::Succeeded,
            SwarmTaskAttemptDisposition::Failed
            | SwarmTaskAttemptDisposition::Retryable
            | SwarmTaskAttemptDisposition::Escalated => SwarmAttemptStatus::Failed,
        };
        let task_status = match disposition {
            SwarmTaskAttemptDisposition::Succeeded => SwarmTaskStatus::Completed,
            SwarmTaskAttemptDisposition::Failed => SwarmTaskStatus::Failed,
            SwarmTaskAttemptDisposition::Retryable => SwarmTaskStatus::Retryable,
            SwarmTaskAttemptDisposition::Escalated => SwarmTaskStatus::Escalated,
        };
        let task_result_json = (disposition == SwarmTaskAttemptDisposition::Succeeded)
            .then(|| to_opt_json(result_json))
            .transpose()?
            .flatten();
        let task_last_error = if disposition == SwarmTaskAttemptDisposition::Succeeded {
            None
        } else {
            last_error
        };
        let completed_at = matches!(
            disposition,
            SwarmTaskAttemptDisposition::Succeeded | SwarmTaskAttemptDisposition::Failed
        )
        .then_some(now);
        let escalated = disposition == SwarmTaskAttemptDisposition::Escalated;

        let task_update = sqlx::query(
            r#"
UPDATE swarm_tasks
SET status = ?, token_usage = token_usage + ?, runtime_usage_seconds = runtime_usage_seconds + ?,
    result_json = ?, last_error = ?, completed_at = ?, updated_at = ?,
    lease_owner = NULL, lease_until = NULL, assigned_thread_id = NULL,
    candidate_index = CASE WHEN ? THEN COALESCE(candidate_index, 0) + 1 ELSE candidate_index END,
    fallback_reason = CASE WHEN ? THEN COALESCE(?, fallback_reason) ELSE fallback_reason END
WHERE id = ? AND status = ?
            "#,
        )
        .bind(task_status.as_str())
        .bind(token_usage)
        .bind(effective_runtime)
        .bind(task_result_json)
        .bind(task_last_error)
        .bind(completed_at)
        .bind(now)
        .bind(escalated)
        .bind(escalated)
        .bind(last_error)
        .bind(task_id)
        .bind(SwarmTaskStatus::Running.as_str())
        .execute(&mut *tx)
        .await?;
        if task_update.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(false);
        }

        let attempt_update = sqlx::query(
            r#"
UPDATE swarm_attempts
SET status = ?, lease_owner = NULL, lease_until = NULL,
    token_usage = token_usage + ?, runtime_usage_seconds = runtime_usage_seconds + ?,
    result_json = ?, last_error = ?, completed_at = COALESCE(completed_at, ?), updated_at = ?
WHERE id = ? AND task_id = ? AND status = ?
            "#,
        )
        .bind(attempt_status.as_str())
        .bind(token_usage)
        .bind(effective_runtime)
        .bind(to_opt_json(result_json)?)
        .bind(task_last_error)
        .bind(now)
        .bind(now)
        .bind(attempt_id)
        .bind(task_id)
        .bind(SwarmAttemptStatus::Running.as_str())
        .execute(&mut *tx)
        .await?;
        if attempt_update.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(false);
        }

        let run_update = sqlx::query(
            "UPDATE swarm_runs SET token_usage = token_usage + ?, runtime_usage_seconds = runtime_usage_seconds + ?, updated_at = ? WHERE id = ? AND status = ?",
        )
        .bind(token_usage)
        .bind(effective_runtime)
        .bind(now)
        .bind(run_id)
        .bind(SwarmRunStatus::Running.as_str())
        .execute(&mut *tx)
        .await?;
        if run_update.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(false);
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn finish_task(
        &self,
        task_id: &str,
        status: SwarmTaskStatus,
        result_json: Option<&Value>,
        last_error: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE swarm_tasks
SET status = ?, result_json = ?, last_error = ?, completed_at = COALESCE(completed_at, ?),
    updated_at = ?, lease_owner = NULL, lease_until = NULL
WHERE id = ? AND status = ?
            "#,
        )
        .bind(status.as_str())
        .bind(to_opt_json(result_json)?)
        .bind(last_error)
        .bind(now)
        .bind(now)
        .bind(task_id)
        .bind(SwarmTaskStatus::Running.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn complete_swarm_task(
        &self,
        task_id: &str,
        result_json: Option<&Value>,
    ) -> anyhow::Result<bool> {
        self.finish_task(task_id, SwarmTaskStatus::Completed, result_json, None)
            .await
    }

    pub async fn fail_swarm_task(&self, task_id: &str, error: &str) -> anyhow::Result<bool> {
        self.finish_task(task_id, SwarmTaskStatus::Failed, None, Some(error))
            .await
    }

    pub async fn retry_swarm_task(
        &self,
        task_id: &str,
        error: Option<&str>,
    ) -> anyhow::Result<bool> {
        self.reset_task_for_retry(task_id, SwarmTaskStatus::Retryable, error)
            .await
    }

    pub async fn escalate_swarm_task(
        &self,
        task_id: &str,
        error: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE swarm_tasks
SET status = ?, lease_owner = NULL, lease_until = NULL, assigned_thread_id = NULL,
    candidate_index = COALESCE(candidate_index, 0) + 1,
    fallback_reason = COALESCE(?, fallback_reason), updated_at = ?, last_error = ?
WHERE id = ? AND status = ?
            "#,
        )
        .bind(SwarmTaskStatus::Escalated.as_str())
        .bind(error)
        .bind(now)
        .bind(error)
        .bind(task_id)
        .bind(SwarmTaskStatus::Running.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn reset_task_for_retry(
        &self,
        task_id: &str,
        status: SwarmTaskStatus,
        last_error: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE swarm_tasks
SET status = ?, lease_owner = NULL, lease_until = NULL, assigned_thread_id = NULL,
    updated_at = ?, last_error = ?
WHERE id = ? AND status = ?
            "#,
        )
        .bind(status.as_str())
        .bind(now)
        .bind(last_error)
        .bind(task_id)
        .bind(SwarmTaskStatus::Running.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn expire_swarm_leases(&self) -> anyhow::Result<usize> {
        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
UPDATE swarm_attempts
SET status = ?, lease_owner = NULL, lease_until = NULL,
    completed_at = COALESCE(completed_at, ?), updated_at = ?,
    last_error = COALESCE(last_error, 'task lease expired')
WHERE status = ?
  AND task_id IN (
      SELECT id FROM swarm_tasks
      WHERE status = ? AND lease_until IS NOT NULL AND lease_until <= ?
  )
            "#,
        )
        .bind(SwarmAttemptStatus::Failed.as_str())
        .bind(now)
        .bind(now)
        .bind(SwarmAttemptStatus::Running.as_str())
        .bind(SwarmTaskStatus::Running.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let result = sqlx::query(
            r#"
UPDATE swarm_tasks
SET
    status = CASE WHEN attempt_count >= max_attempts THEN ? ELSE ? END,
    lease_owner = NULL,
    assigned_thread_id = NULL,
    lease_until = NULL,
    updated_at = ?,
    last_error = COALESCE(last_error, 'lease expired')
WHERE status = ? AND lease_until IS NOT NULL AND lease_until <= ?
            "#,
        )
        .bind(SwarmTaskStatus::Failed.as_str())
        .bind(SwarmTaskStatus::Retryable.as_str())
        .bind(now)
        .bind(SwarmTaskStatus::Running.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn cancel_swarm_run(
        &self,
        run_id: &str,
        reason: &str,
    ) -> anyhow::Result<SwarmRunCancellation> {
        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        let run_update = sqlx::query(
            r#"
UPDATE swarm_runs
SET status = ?, cancelled_at = ?, completed_at = COALESCE(completed_at, ?), updated_at = ?, last_error = ?
WHERE id = ? AND status IN (?, ?)
            "#,
        )
        .bind(SwarmRunStatus::Cancelled.as_str())
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(reason)
        .bind(run_id)
        .bind(SwarmRunStatus::Pending.as_str())
        .bind(SwarmRunStatus::Running.as_str())
        .execute(&mut *tx)
        .await?;
        if run_update.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(SwarmRunCancellation {
                cancelled: false,
                worker_thread_ids: Vec::new(),
            });
        }
        let worker_thread_ids = sqlx::query(
            "SELECT DISTINCT assigned_thread_id FROM swarm_tasks WHERE run_id = ? AND status = ? AND assigned_thread_id IS NOT NULL",
        )
        .bind(run_id)
        .bind(SwarmTaskStatus::Running.as_str())
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| row.try_get::<String, _>("assigned_thread_id"))
        .collect::<Result<Vec<_>, _>>()?;
        sqlx::query(
            r#"
UPDATE swarm_tasks
SET status = ?, lease_owner = NULL, assigned_thread_id = NULL, lease_until = NULL, completed_at = COALESCE(completed_at, ?), updated_at = ?, last_error = COALESCE(last_error, ?)
WHERE run_id = ? AND status NOT IN (?, ?, ?)
            "#,
        )
        .bind(SwarmTaskStatus::Cancelled.as_str())
        .bind(now)
        .bind(now)
        .bind(reason)
        .bind(run_id)
        .bind(SwarmTaskStatus::Completed.as_str())
        .bind(SwarmTaskStatus::Failed.as_str())
        .bind(SwarmTaskStatus::Cancelled.as_str())
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
UPDATE swarm_attempts
SET status = ?, lease_owner = NULL, lease_until = NULL, completed_at = COALESCE(completed_at, ?), updated_at = ?, last_error = COALESCE(last_error, ?)
WHERE run_id = ? AND status = ?
            "#,
        )
        .bind(SwarmAttemptStatus::Cancelled.as_str())
        .bind(now)
        .bind(now)
        .bind(reason)
        .bind(run_id)
        .bind(SwarmAttemptStatus::Running.as_str())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(SwarmRunCancellation {
            cancelled: true,
            worker_thread_ids,
        })
    }

    pub async fn load_swarm_recovery_data(
        &self,
        run_id: &str,
    ) -> anyhow::Result<(
        Option<SwarmRun>,
        Vec<SwarmTask>,
        Vec<SwarmCheckpoint>,
        Vec<InterAgentMessage>,
        Vec<InterAgentMessageDelivery>,
    )> {
        let run = self.get_swarm_run(run_id).await?;
        let tasks = sqlx::query_as::<_, SwarmTaskRow>(
            "SELECT * FROM swarm_tasks WHERE run_id = ? ORDER BY order_index ASC, id ASC",
        )
        .bind(run_id)
        .fetch_all(self.pool.as_ref())
        .await?
        .into_iter()
        .map(SwarmTask::try_from)
        .collect::<anyhow::Result<Vec<_>>>()?;
        let checkpoints = sqlx::query_as::<_, SwarmCheckpointRow>(
            "SELECT * FROM swarm_checkpoints WHERE run_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(run_id)
        .fetch_all(self.pool.as_ref())
        .await?
        .into_iter()
        .map(SwarmCheckpoint::try_from)
        .collect::<anyhow::Result<Vec<_>>>()?;
        let messages = sqlx::query_as::<_, InterAgentMessageRow>(
            "SELECT * FROM inter_agent_messages WHERE run_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(run_id)
        .fetch_all(self.pool.as_ref())
        .await?
        .into_iter()
        .map(InterAgentMessage::try_from)
        .collect::<anyhow::Result<Vec<_>>>()?;
        let deliveries = sqlx::query_as::<_, InterAgentMessageDeliveryRow>(
            r#"
SELECT d.*
FROM inter_agent_message_deliveries d
JOIN inter_agent_messages m ON m.id = d.message_id
WHERE m.run_id = ?
ORDER BY d.created_at ASC, d.id ASC
            "#,
        )
        .bind(run_id)
        .fetch_all(self.pool.as_ref())
        .await?
        .into_iter()
        .map(InterAgentMessageDelivery::try_from)
        .collect::<anyhow::Result<Vec<_>>>()?;
        Ok((run, tasks, checkpoints, messages, deliveries))
    }

    pub async fn enqueue_inter_agent_message(
        &self,
        params: &SwarmMessageCreateParams,
    ) -> anyhow::Result<InterAgentMessage> {
        let now = Utc::now().timestamp();
        let expires_at = params.ttl_seconds.map(|ttl| now + ttl.max(1));
        sqlx::query(
            r#"
INSERT INTO inter_agent_messages (
    id, run_id, correlation_id, in_reply_to, direct, topic, sender_thread_id,
    target_thread_id, priority, ttl_seconds, status, attempt_count, expires_at,
    rollout_pointer, rollout_hash, last_error, metadata_json, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, ?, ?, ?)
            "#,
        )
        .bind(params.id.as_str())
        .bind(params.run_id.as_deref())
        .bind(params.correlation_id.as_deref())
        .bind(params.in_reply_to.as_deref())
        .bind(i64::from(params.direct))
        .bind(params.topic.as_deref())
        .bind(params.sender_thread_id.as_str())
        .bind(params.target_thread_id.as_deref())
        .bind(params.priority)
        .bind(params.ttl_seconds)
        .bind(InterAgentMessageStatus::Queued.as_str())
        .bind(expires_at)
        .bind(params.rollout_pointer.as_deref())
        .bind(params.rollout_hash.as_deref())
        .bind(serde_json::to_string(&params.metadata_json)?)
        .bind(now)
        .bind(now)
        .execute(self.pool.as_ref())
        .await?;
        self.get_inter_agent_message(params.id.as_str())
            .await?
            .ok_or_else(|| anyhow!("failed to load created inter-agent message"))
    }

    /// Creates a direct message and its recipient delivery atomically. Message bodies remain in
    /// the rollout path; this transaction contains delivery metadata only.
    pub async fn enqueue_inter_agent_message_with_delivery(
        &self,
        params: &SwarmMessageCreateParams,
        delivery_id: &str,
    ) -> anyhow::Result<(InterAgentMessage, InterAgentMessageDelivery)> {
        let target_thread_id = params
            .target_thread_id
            .as_deref()
            .ok_or_else(|| anyhow!("a direct inter-agent delivery requires a target thread"))?;
        let now = Utc::now().timestamp();
        let expires_at = params.ttl_seconds.map(|ttl| now + ttl.max(1));
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
INSERT INTO inter_agent_messages (
    id, run_id, correlation_id, in_reply_to, direct, topic, sender_thread_id,
    target_thread_id, priority, ttl_seconds, status, attempt_count, expires_at,
    rollout_pointer, rollout_hash, last_error, metadata_json, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, ?, ?, ?)
            "#,
        )
        .bind(params.id.as_str())
        .bind(params.run_id.as_deref())
        .bind(params.correlation_id.as_deref())
        .bind(params.in_reply_to.as_deref())
        .bind(i64::from(params.direct))
        .bind(params.topic.as_deref())
        .bind(params.sender_thread_id.as_str())
        .bind(target_thread_id)
        .bind(params.priority)
        .bind(params.ttl_seconds)
        .bind(InterAgentMessageStatus::Queued.as_str())
        .bind(expires_at)
        .bind(params.rollout_pointer.as_deref())
        .bind(params.rollout_hash.as_deref())
        .bind(serde_json::to_string(&params.metadata_json)?)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
INSERT INTO inter_agent_message_deliveries (
    id, message_id, sender_thread_id, target_thread_id, topic, status,
    attempt_count, delivered_at, acked_at, expired_at, dead_lettered_at,
    cancelled_at, last_error, rollout_pointer, rollout_hash, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?)
            "#,
        )
        .bind(delivery_id)
        .bind(params.id.as_str())
        .bind(params.sender_thread_id.as_str())
        .bind(target_thread_id)
        .bind(params.topic.as_deref())
        .bind(InterAgentMessageStatus::Queued.as_str())
        .bind(params.rollout_pointer.as_deref())
        .bind(params.rollout_hash.as_deref())
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let message = self
            .get_inter_agent_message(params.id.as_str())
            .await?
            .ok_or_else(|| anyhow!("failed to load created inter-agent message"))?;
        let delivery = self
            .get_inter_agent_message_delivery(delivery_id)
            .await?
            .ok_or_else(|| anyhow!("failed to load created inter-agent delivery"))?;
        Ok((message, delivery))
    }

    pub async fn get_inter_agent_message(
        &self,
        message_id: &str,
    ) -> anyhow::Result<Option<InterAgentMessage>> {
        let row = sqlx::query_as::<_, InterAgentMessageRow>(
            "SELECT * FROM inter_agent_messages WHERE id = ?",
        )
        .bind(message_id)
        .fetch_optional(self.pool.as_ref())
        .await?;
        row.map(InterAgentMessage::try_from).transpose()
    }

    pub async fn get_inter_agent_message_delivery(
        &self,
        delivery_id: &str,
    ) -> anyhow::Result<Option<InterAgentMessageDelivery>> {
        let row = sqlx::query_as::<_, InterAgentMessageDeliveryRow>(
            "SELECT * FROM inter_agent_message_deliveries WHERE id = ?",
        )
        .bind(delivery_id)
        .fetch_optional(self.pool.as_ref())
        .await?;
        row.map(InterAgentMessageDelivery::try_from).transpose()
    }

    pub async fn transition_inter_agent_message(
        &self,
        message_id: &str,
        status: InterAgentMessageStatus,
        error: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE inter_agent_messages
SET status = ?,
    attempt_count = attempt_count + CASE
        WHEN ? = 'delivered' THEN 1
        WHEN ? = 'dead_lettered' AND status = 'queued' THEN 1
        ELSE 0
    END,
    last_error = ?, updated_at = ?
WHERE id = ? AND status NOT IN (?, ?, ?, ?)
            "#,
        )
        .bind(status.as_str())
        .bind(status.as_str())
        .bind(status.as_str())
        .bind(error)
        .bind(now)
        .bind(message_id)
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(InterAgentMessageStatus::DeadLettered.as_str())
        .bind(InterAgentMessageStatus::Cancelled.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn ack_inter_agent_message_by_id(&self, message_id: &str) -> anyhow::Result<bool> {
        self.transition_inter_agent_message(message_id, InterAgentMessageStatus::Acked, None)
            .await
    }

    pub async fn ack_inter_agent_message_deliveries_by_message_id(
        &self,
        message_id: &str,
    ) -> anyhow::Result<usize> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE inter_agent_message_deliveries
SET status = ?, acked_at = COALESCE(acked_at, ?), updated_at = ?, last_error = NULL
WHERE message_id = ?
  AND status NOT IN (?, ?, ?, ?)
            "#,
        )
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(now)
        .bind(now)
        .bind(message_id)
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(InterAgentMessageStatus::DeadLettered.as_str())
        .bind(InterAgentMessageStatus::Cancelled.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn ack_inter_agent_message_and_deliveries_by_message_id(
        &self,
        message_id: &str,
    ) -> anyhow::Result<(bool, usize)> {
        let mut tx = self.pool.begin().await?;
        let message_acked = sqlx::query(
            r#"
UPDATE inter_agent_messages
SET status = ?, updated_at = ?, last_error = NULL
WHERE id = ? AND status NOT IN (?, ?, ?, ?)
            "#,
        )
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(Utc::now().timestamp())
        .bind(message_id)
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(InterAgentMessageStatus::DeadLettered.as_str())
        .bind(InterAgentMessageStatus::Cancelled.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected()
            > 0;
        let delivery_acked = sqlx::query(
            r#"
UPDATE inter_agent_message_deliveries
SET status = ?, acked_at = COALESCE(acked_at, ?), updated_at = ?, last_error = NULL
WHERE message_id = ?
  AND status NOT IN (?, ?, ?, ?)
            "#,
        )
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(Utc::now().timestamp())
        .bind(Utc::now().timestamp())
        .bind(message_id)
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(InterAgentMessageStatus::DeadLettered.as_str())
        .bind(InterAgentMessageStatus::Cancelled.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected() as usize;
        tx.commit().await?;
        Ok((message_acked, delivery_acked))
    }

    /// Acknowledges delivery for one recipient and marks the shared message ACKed only after
    /// every recipient delivery has reached a terminal state. This preserves broadcast semantics:
    /// the first recipient cannot accidentally ACK another recipient's queued copy.
    pub async fn ack_inter_agent_message_for_target(
        &self,
        message_id: &str,
        target_thread_id: &str,
    ) -> anyhow::Result<(bool, usize)> {
        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        let deliveries_acked = sqlx::query(
            r#"
UPDATE inter_agent_message_deliveries
SET status = ?, acked_at = COALESCE(acked_at, ?), updated_at = ?, last_error = NULL
WHERE message_id = ? AND target_thread_id = ?
  AND status NOT IN (?, ?, ?, ?)
            "#,
        )
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(now)
        .bind(now)
        .bind(message_id)
        .bind(target_thread_id)
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(InterAgentMessageStatus::DeadLettered.as_str())
        .bind(InterAgentMessageStatus::Cancelled.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected() as usize;
        let nonterminal_deliveries: i64 = sqlx::query_scalar(
            r#"
SELECT COUNT(*) FROM inter_agent_message_deliveries
WHERE message_id = ? AND status IN (?, ?)
            "#,
        )
        .bind(message_id)
        .bind(InterAgentMessageStatus::Queued.as_str())
        .bind(InterAgentMessageStatus::Delivered.as_str())
        .fetch_one(&mut *tx)
        .await?;
        let message_acked = if nonterminal_deliveries == 0 {
            sqlx::query(
                r#"
UPDATE inter_agent_messages
SET status = ?, updated_at = ?, last_error = NULL
WHERE id = ? AND status NOT IN (?, ?, ?, ?)
                "#,
            )
            .bind(InterAgentMessageStatus::Acked.as_str())
            .bind(now)
            .bind(message_id)
            .bind(InterAgentMessageStatus::Acked.as_str())
            .bind(InterAgentMessageStatus::Expired.as_str())
            .bind(InterAgentMessageStatus::DeadLettered.as_str())
            .bind(InterAgentMessageStatus::Cancelled.as_str())
            .execute(&mut *tx)
            .await?
            .rows_affected()
                > 0
        } else {
            false
        };
        tx.commit().await?;
        Ok((message_acked, deliveries_acked))
    }

    pub async fn expire_inter_agent_message_by_id(&self, message_id: &str) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE inter_agent_messages
SET status = ?, updated_at = ?, last_error = COALESCE(last_error, 'expired')
WHERE id = ? AND status NOT IN (?, ?, ?, ?)
            "#,
        )
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(now)
        .bind(message_id)
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(InterAgentMessageStatus::DeadLettered.as_str())
        .bind(InterAgentMessageStatus::Cancelled.as_str())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn expire_inter_agent_message_and_deliveries_by_message_id(
        &self,
        message_id: &str,
    ) -> anyhow::Result<(bool, usize)> {
        let now = Utc::now().timestamp();
        let mut tx = self.pool.begin().await?;
        let message_expired = sqlx::query(
            r#"
UPDATE inter_agent_messages
SET status = ?, updated_at = ?, last_error = COALESCE(last_error, 'expired')
WHERE id = ? AND status NOT IN (?, ?, ?, ?)
            "#,
        )
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(now)
        .bind(message_id)
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(InterAgentMessageStatus::DeadLettered.as_str())
        .bind(InterAgentMessageStatus::Cancelled.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected()
            > 0;
        let delivery_expired = sqlx::query(
            r#"
UPDATE inter_agent_message_deliveries
SET status = ?, updated_at = ?, expired_at = COALESCE(expired_at, ?), last_error = COALESCE(last_error, 'expired')
WHERE message_id = ?
  AND status NOT IN (?, ?, ?, ?)
            "#,
        )
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(now)
        .bind(now)
        .bind(message_id)
        .bind(InterAgentMessageStatus::Acked.as_str())
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(InterAgentMessageStatus::DeadLettered.as_str())
        .bind(InterAgentMessageStatus::Cancelled.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected() as usize;
        tx.commit().await?;
        Ok((message_expired, delivery_expired))
    }

    pub async fn expire_inter_agent_messages(&self) -> anyhow::Result<usize> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE inter_agent_messages
SET status = ?, updated_at = ?, last_error = COALESCE(last_error, 'expired')
WHERE status IN (?, ?) AND expires_at IS NOT NULL AND expires_at <= ?
            "#,
        )
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(now)
        .bind(InterAgentMessageStatus::Queued.as_str())
        .bind(InterAgentMessageStatus::Delivered.as_str())
        .bind(now)
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn expire_inter_agent_message_deliveries(&self) -> anyhow::Result<usize> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE inter_agent_message_deliveries
SET status = ?, updated_at = ?, expired_at = COALESCE(expired_at, ?), last_error = COALESCE(last_error, 'expired')
WHERE status IN (?, ?)
  AND message_id IN (
      SELECT id
      FROM inter_agent_messages
      WHERE expires_at IS NOT NULL AND expires_at <= ?
  )
            "#,
        )
        .bind(InterAgentMessageStatus::Expired.as_str())
        .bind(now)
        .bind(now)
        .bind(InterAgentMessageStatus::Queued.as_str())
        .bind(InterAgentMessageStatus::Delivered.as_str())
        .bind(now)
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn create_inter_agent_message_delivery(
        &self,
        delivery_id: &str,
        message_id: &str,
        sender_thread_id: &str,
        target_thread_id: &str,
        topic: Option<&str>,
        rollout_pointer: Option<&str>,
        rollout_hash: Option<&str>,
    ) -> anyhow::Result<InterAgentMessageDelivery> {
        let now = Utc::now().timestamp();
        sqlx::query(
            r#"
INSERT INTO inter_agent_message_deliveries (
    id, message_id, sender_thread_id, target_thread_id, topic, status,
    attempt_count, delivered_at, acked_at, expired_at, dead_lettered_at,
    cancelled_at, last_error, rollout_pointer, rollout_hash, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?)
            "#,
        )
        .bind(delivery_id)
        .bind(message_id)
        .bind(sender_thread_id)
        .bind(target_thread_id)
        .bind(topic)
        .bind(InterAgentMessageStatus::Queued.as_str())
        .bind(rollout_pointer)
        .bind(rollout_hash)
        .bind(now)
        .bind(now)
        .execute(self.pool.as_ref())
        .await?;
        let row = sqlx::query_as::<_, InterAgentMessageDeliveryRow>(
            "SELECT * FROM inter_agent_message_deliveries WHERE id = ?",
        )
        .bind(delivery_id)
        .fetch_optional(self.pool.as_ref())
        .await?;
        row.map(InterAgentMessageDelivery::try_from)
            .transpose()?
            .ok_or_else(|| anyhow!("failed to load created message delivery"))
    }

    pub async fn transition_inter_agent_delivery(
        &self,
        delivery_id: &str,
        status: InterAgentMessageStatus,
        error: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            r#"
UPDATE inter_agent_message_deliveries
SET status = ?,
    attempt_count = attempt_count + CASE
        WHEN ? = 'delivered' THEN 1
        WHEN ? = 'dead_lettered' AND status = 'queued' THEN 1
        ELSE 0
    END,
    delivered_at = CASE WHEN ? = 'delivered' THEN COALESCE(delivered_at, ?) ELSE delivered_at END,
    acked_at = CASE WHEN ? = 'acked' THEN COALESCE(acked_at, ?) ELSE acked_at END,
    expired_at = CASE WHEN ? = 'expired' THEN COALESCE(expired_at, ?) ELSE expired_at END,
    dead_lettered_at = CASE WHEN ? = 'dead_lettered' THEN COALESCE(dead_lettered_at, ?) ELSE dead_lettered_at END,
    cancelled_at = CASE WHEN ? = 'cancelled' THEN COALESCE(cancelled_at, ?) ELSE cancelled_at END,
    last_error = ?, updated_at = ?
WHERE id = ? AND status NOT IN (?, ?, ?, ?)
            "#,
        )
            .bind(status.as_str())
            .bind(status.as_str())
            .bind(status.as_str())
            .bind(status.as_str())
            .bind(now)
            .bind(status.as_str())
            .bind(now)
            .bind(status.as_str())
            .bind(now)
            .bind(status.as_str())
            .bind(now)
            .bind(status.as_str())
            .bind(now)
            .bind(error)
            .bind(now)
            .bind(delivery_id)
            .bind(InterAgentMessageStatus::Acked.as_str())
            .bind(InterAgentMessageStatus::Expired.as_str())
            .bind(InterAgentMessageStatus::DeadLettered.as_str())
            .bind(InterAgentMessageStatus::Cancelled.as_str())
            .execute(self.pool.as_ref())
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn ack_inter_agent_message_delivery(
        &self,
        delivery_id: &str,
    ) -> anyhow::Result<bool> {
        self.transition_inter_agent_delivery(delivery_id, InterAgentMessageStatus::Acked, None)
            .await
    }

    pub async fn expire_inter_agent_message_delivery(
        &self,
        delivery_id: &str,
    ) -> anyhow::Result<bool> {
        self.transition_inter_agent_delivery(delivery_id, InterAgentMessageStatus::Expired, None)
            .await
    }

    pub async fn dead_letter_inter_agent_message_delivery(
        &self,
        delivery_id: &str,
        error: Option<&str>,
    ) -> anyhow::Result<bool> {
        self.transition_inter_agent_delivery(
            delivery_id,
            InterAgentMessageStatus::DeadLettered,
            error,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::test_support::unique_temp_dir;
    use std::sync::Arc;

    async fn test_runtime() -> anyhow::Result<Arc<StateRuntime>> {
        StateRuntime::init(unique_temp_dir(), "test-provider".to_string()).await
    }

    async fn create_active_worker_attempt(
        runtime: &StateRuntime,
        run_id: &str,
        task_id: &str,
        worker_thread_id: &str,
        max_attempts: i64,
    ) -> anyhow::Result<String> {
        runtime
            .create_runnable_swarm_run(
                &SwarmRunCreateParams {
                    id: run_id.to_string(),
                    thread_id: "thread-root".to_string(),
                    title: None,
                    spec_json: Some(serde_json::json!({"tasks":[task_id]})),
                    model_candidate_json: None,
                    fallback_reason: None,
                    max_concurrency: 1,
                    token_budget: Some(1_000),
                    runtime_budget_seconds: Some(600),
                    fail_fast: true,
                    cancel_policy: Some("propagate".to_string()),
                    deadline_at: None,
                },
                &[SwarmTaskCreateParams {
                    id: task_id.to_string(),
                    run_id: run_id.to_string(),
                    thread_id: "thread-root".to_string(),
                    assigned_thread_id: None,
                    order_index: 0,
                    depends_on_task_ids: Vec::new(),
                    task_kind: "worker".to_string(),
                    agent_type: Some("researcher".to_string()),
                    instructions: Some("research".to_string()),
                    result_token: "result-token".to_string(),
                    model_candidate_json: None,
                    model_route_json: None,
                    candidate_index: Some(0),
                    fallback_reason: None,
                    max_attempts,
                    deadline_at: None,
                }],
            )
            .await?;
        let claimed = runtime
            .claim_ready_swarm_task(run_id, "lease-owner", 30)
            .await?
            .expect("task should be claimable");
        assert_eq!(claimed.id, task_id);
        assert!(
            runtime
                .bind_swarm_task_thread(task_id, "lease-owner", worker_thread_id)
                .await?
        );
        let attempt_id = format!("{task_id}:attempt:{}", claimed.attempt_count);
        runtime
            .record_swarm_attempt_start(
                attempt_id.as_str(),
                run_id,
                task_id,
                worker_thread_id,
                Some("lease-owner"),
                Some(30),
                None,
                None,
            )
            .await?;
        Ok(attempt_id)
    }

    #[tokio::test]
    async fn runnable_swarm_run_is_created_atomically_with_its_tasks() -> anyhow::Result<()> {
        let runtime = test_runtime().await?;
        let run = runtime
            .create_runnable_swarm_run(
                &SwarmRunCreateParams {
                    id: "atomic-run".to_string(),
                    thread_id: "thread-root".to_string(),
                    title: Some("atomic run".to_string()),
                    spec_json: Some(serde_json::json!({"tasks":["atomic-task"]})),
                    model_candidate_json: None,
                    fallback_reason: None,
                    max_concurrency: 1,
                    token_budget: None,
                    runtime_budget_seconds: None,
                    fail_fast: true,
                    cancel_policy: Some("propagate".to_string()),
                    deadline_at: None,
                },
                &[SwarmTaskCreateParams {
                    id: "atomic-task".to_string(),
                    run_id: "atomic-run".to_string(),
                    thread_id: "thread-root".to_string(),
                    assigned_thread_id: None,
                    order_index: 0,
                    depends_on_task_ids: Vec::new(),
                    task_kind: "worker".to_string(),
                    agent_type: Some("researcher".to_string()),
                    instructions: Some("research".to_string()),
                    result_token: "atomic-result-token".to_string(),
                    model_candidate_json: None,
                    model_route_json: None,
                    candidate_index: None,
                    fallback_reason: None,
                    max_attempts: 1,
                    deadline_at: None,
                }],
            )
            .await?;

        assert_eq!(run.status, SwarmRunStatus::Running);
        assert!(runtime.get_swarm_task("atomic-task").await?.is_some());
        assert_eq!(
            runtime.list_active_swarm_runs("thread-root").await?.len(),
            1
        );
        runtime.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn worker_report_atomically_finalizes_attempt_task_and_usage() -> anyhow::Result<()> {
        let runtime = test_runtime().await?;
        let attempt_id = create_active_worker_attempt(
            runtime.as_ref(),
            "report-run",
            "report-task",
            "thread-worker",
            2,
        )
        .await?;
        let result = serde_json::json!({"answer": 42});

        assert!(
            runtime
                .finalize_swarm_worker_report(
                    attempt_id.as_str(),
                    "report-task",
                    SwarmTaskAttemptDisposition::Succeeded,
                    17,
                    3,
                    Some(&result),
                    None,
                )
                .await?
        );
        assert!(
            !runtime
                .finalize_swarm_worker_report(
                    attempt_id.as_str(),
                    "report-task",
                    SwarmTaskAttemptDisposition::Succeeded,
                    17,
                    3,
                    Some(&result),
                    None,
                )
                .await?
        );

        let task = runtime
            .get_swarm_task("report-task")
            .await?
            .expect("task exists");
        assert_eq!(task.status, SwarmTaskStatus::Completed);
        assert_eq!(task.assigned_thread_id, None);
        assert_eq!(task.token_usage, 17);
        assert_eq!(task.runtime_usage_seconds, 3);
        assert_eq!(task.result_json, Some(result.clone()));
        let attempt = runtime
            .get_swarm_attempt(attempt_id.as_str())
            .await?
            .expect("attempt exists");
        assert_eq!(attempt.status, SwarmAttemptStatus::Succeeded);
        assert_eq!(attempt.token_usage, 17);
        let run = runtime
            .get_swarm_run("report-run")
            .await?
            .expect("run exists");
        assert_eq!(run.token_usage, 17);
        runtime.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_returns_live_workers_and_wins_report_races() -> anyhow::Result<()> {
        let runtime = test_runtime().await?;
        let attempt_id = create_active_worker_attempt(
            runtime.as_ref(),
            "cancel-run",
            "cancel-task",
            "thread-worker",
            2,
        )
        .await?;

        let cancellation = runtime
            .cancel_swarm_run("cancel-run", "cancel test")
            .await?;
        assert!(cancellation.cancelled);
        assert_eq!(cancellation.worker_thread_ids, vec!["thread-worker"]);
        assert!(
            !runtime
                .finalize_swarm_worker_report(
                    attempt_id.as_str(),
                    "cancel-task",
                    SwarmTaskAttemptDisposition::Succeeded,
                    17,
                    3,
                    Some(&serde_json::json!({"late": true})),
                    None,
                )
                .await?
        );

        let task = runtime
            .get_swarm_task("cancel-task")
            .await?
            .expect("task exists");
        assert_eq!(task.status, SwarmTaskStatus::Cancelled);
        assert_eq!(task.assigned_thread_id, None);
        let attempt = runtime
            .get_swarm_attempt(attempt_id.as_str())
            .await?
            .expect("attempt exists");
        assert_eq!(attempt.status, SwarmAttemptStatus::Cancelled);
        let second = runtime
            .cancel_swarm_run("cancel-run", "duplicate cancel")
            .await?;
        assert!(!second.cancelled);
        assert!(second.worker_thread_ids.is_empty());
        runtime.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn swarm_run_task_and_checkpoint_round_trip() -> anyhow::Result<()> {
        let runtime = test_runtime().await?;
        let run = runtime
            .create_swarm_run(&SwarmRunCreateParams {
                id: "run-1".to_string(),
                thread_id: "thread-root".to_string(),
                title: Some("test run".to_string()),
                spec_json: Some(serde_json::json!({"mode":"smoke"})),
                model_candidate_json: Some(serde_json::json!([{"model":"gpt-4.1"}])),
                fallback_reason: Some("primary unavailable".to_string()),
                max_concurrency: 2,
                token_budget: Some(1000),
                runtime_budget_seconds: Some(600),
                fail_fast: true,
                cancel_policy: Some("cancel_children".to_string()),
                deadline_at: None,
            })
            .await?;
        assert_eq!(run.id, "run-1");

        let task = runtime
            .create_swarm_tasks(&[SwarmTaskCreateParams {
                id: "task-1".to_string(),
                run_id: run.id.clone(),
                thread_id: "thread-root".to_string(),
                assigned_thread_id: None,
                order_index: 0,
                depends_on_task_ids: Vec::new(),
                task_kind: "research".to_string(),
                agent_type: Some("worker".to_string()),
                instructions: Some("collect context".to_string()),
                result_token: "result-token".to_string(),
                model_candidate_json: Some(serde_json::json!([{"model":"gpt-4.1"}])),
                model_route_json: Some(serde_json::json!({"route":"responses"})),
                candidate_index: Some(0),
                fallback_reason: None,
                max_attempts: 3,
                deadline_at: None,
            }])
            .await?
            .pop()
            .expect("task created");
        assert_eq!(task.task_kind, "research");

        let claimed = runtime
            .claim_ready_swarm_task(run.id.as_str(), "lease-owner", 30)
            .await?
            .expect("task should be claimable");
        assert_eq!(claimed.id, task.id);
        assert_eq!(claimed.assigned_thread_id, None);

        assert!(
            runtime
                .bind_swarm_task_thread(task.id.as_str(), "lease-owner", "thread-worker")
                .await?
        );

        let checkpoint = runtime
            .create_swarm_checkpoint(
                "checkpoint-1",
                run.id.as_str(),
                Some(task.id.as_str()),
                "thread-worker",
                "snapshot",
                &serde_json::json!({"step": 1}),
            )
            .await?;
        assert_eq!(checkpoint.checkpoint_type, "snapshot");

        let result = serde_json::json!({"ok": true});
        let _ = runtime
            .complete_swarm_task(task.id.as_str(), Some(&result))
            .await?;
        let persisted = runtime
            .get_swarm_run(run.id.as_str())
            .await?
            .expect("run exists");
        assert_eq!(persisted.max_concurrency, 2);
        runtime.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn inter_agent_message_round_trip_has_no_body_column() -> anyhow::Result<()> {
        let runtime = test_runtime().await?;
        let (message, delivery) = runtime
            .enqueue_inter_agent_message_with_delivery(
                &SwarmMessageCreateParams {
                    id: "msg-1".to_string(),
                    run_id: None,
                    correlation_id: Some("corr-1".to_string()),
                    in_reply_to: None,
                    direct: true,
                    topic: Some("topic".to_string()),
                    sender_thread_id: "thread-a".to_string(),
                    target_thread_id: Some("thread-b".to_string()),
                    priority: 10,
                    ttl_seconds: Some(30),
                    rollout_pointer: Some("rollout#1".to_string()),
                    rollout_hash: Some("abc123".to_string()),
                    metadata_json: serde_json::json!({"kind":"note"}),
                },
                "delivery-1",
            )
            .await?;
        assert!(message.direct);
        assert_eq!(delivery.status, InterAgentMessageStatus::Queued);
        assert!(
            runtime
                .ack_inter_agent_message_delivery(delivery.id.as_str())
                .await?
        );
        runtime.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn ack_all_deliveries_skips_terminal_rows() -> anyhow::Result<()> {
        let runtime = test_runtime().await?;
        let message = runtime
            .enqueue_inter_agent_message(&SwarmMessageCreateParams {
                id: "msg-2".to_string(),
                run_id: None,
                correlation_id: None,
                in_reply_to: None,
                direct: true,
                topic: None,
                sender_thread_id: "thread-a".to_string(),
                target_thread_id: Some("thread-b".to_string()),
                priority: 1,
                ttl_seconds: None,
                rollout_pointer: None,
                rollout_hash: None,
                metadata_json: serde_json::json!({}),
            })
            .await?;
        let queued = runtime
            .create_inter_agent_message_delivery(
                "delivery-2",
                message.id.as_str(),
                "thread-a",
                "thread-b",
                None,
                None,
                None,
            )
            .await?;
        let _ = runtime
            .expire_inter_agent_message_delivery(queued.id.as_str())
            .await?;
        let updated = runtime
            .ack_inter_agent_message_deliveries_by_message_id(message.id.as_str())
            .await?;
        assert_eq!(updated, 0);
        runtime.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn expire_message_marks_deliveries_terminal() -> anyhow::Result<()> {
        let runtime = test_runtime().await?;
        let message = runtime
            .enqueue_inter_agent_message(&SwarmMessageCreateParams {
                id: "msg-3".to_string(),
                run_id: None,
                correlation_id: None,
                in_reply_to: None,
                direct: true,
                topic: None,
                sender_thread_id: "thread-a".to_string(),
                target_thread_id: Some("thread-b".to_string()),
                priority: 1,
                ttl_seconds: None,
                rollout_pointer: None,
                rollout_hash: None,
                metadata_json: serde_json::json!({}),
            })
            .await?;
        let delivery = runtime
            .create_inter_agent_message_delivery(
                "delivery-3",
                message.id.as_str(),
                "thread-a",
                "thread-b",
                None,
                None,
                None,
            )
            .await?;
        let expired = runtime
            .expire_inter_agent_message_and_deliveries_by_message_id(message.id.as_str())
            .await?;
        assert!(expired.0);
        assert_eq!(expired.1, 1);
        let reloaded = runtime
            .get_inter_agent_message(message.id.as_str())
            .await?
            .expect("message exists");
        assert_eq!(reloaded.status, InterAgentMessageStatus::Expired);
        let reloaded_delivery = runtime
            .get_inter_agent_message_delivery(delivery.id.as_str())
            .await?
            .expect("delivery exists");
        assert_eq!(reloaded_delivery.status, InterAgentMessageStatus::Expired);
        runtime.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn recipient_ack_does_not_ack_other_broadcast_deliveries() -> anyhow::Result<()> {
        let runtime = test_runtime().await?;
        let message = runtime
            .enqueue_inter_agent_message(&SwarmMessageCreateParams {
                id: "msg-broadcast".to_string(),
                run_id: None,
                correlation_id: Some("corr-broadcast".to_string()),
                in_reply_to: None,
                direct: false,
                topic: Some("review".to_string()),
                sender_thread_id: "thread-a".to_string(),
                target_thread_id: None,
                priority: 2,
                ttl_seconds: Some(60),
                rollout_pointer: None,
                rollout_hash: None,
                metadata_json: serde_json::json!({"kind":"review_request"}),
            })
            .await?;
        let delivery_b = runtime
            .create_inter_agent_message_delivery(
                "delivery-b",
                message.id.as_str(),
                "thread-a",
                "thread-b",
                Some("review"),
                None,
                None,
            )
            .await?;
        let delivery_c = runtime
            .create_inter_agent_message_delivery(
                "delivery-c",
                message.id.as_str(),
                "thread-a",
                "thread-c",
                Some("review"),
                None,
                None,
            )
            .await?;
        runtime
            .transition_inter_agent_message(
                message.id.as_str(),
                InterAgentMessageStatus::Delivered,
                None,
            )
            .await?;
        runtime
            .transition_inter_agent_delivery(
                delivery_b.id.as_str(),
                InterAgentMessageStatus::Delivered,
                None,
            )
            .await?;
        runtime
            .transition_inter_agent_delivery(
                delivery_c.id.as_str(),
                InterAgentMessageStatus::Delivered,
                None,
            )
            .await?;

        let (message_acked, deliveries_acked) = runtime
            .ack_inter_agent_message_for_target(message.id.as_str(), "thread-b")
            .await?;
        assert!(!message_acked);
        assert_eq!(deliveries_acked, 1);
        let reloaded_b = runtime
            .get_inter_agent_message_delivery(delivery_b.id.as_str())
            .await?
            .expect("delivery b exists");
        let reloaded_c = runtime
            .get_inter_agent_message_delivery(delivery_c.id.as_str())
            .await?
            .expect("delivery c exists");
        assert_eq!(reloaded_b.status, InterAgentMessageStatus::Acked);
        assert_eq!(reloaded_b.attempt_count, 1);
        assert_eq!(reloaded_c.status, InterAgentMessageStatus::Delivered);

        let (message_acked, deliveries_acked) = runtime
            .ack_inter_agent_message_for_target(message.id.as_str(), "thread-c")
            .await?;
        assert!(message_acked);
        assert_eq!(deliveries_acked, 1);
        let reloaded_message = runtime
            .get_inter_agent_message(message.id.as_str())
            .await?
            .expect("message exists");
        assert_eq!(reloaded_message.status, InterAgentMessageStatus::Acked);
        assert_eq!(reloaded_message.attempt_count, 1);
        runtime.close().await;
        Ok(())
    }
}
