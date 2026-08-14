use super::StateRuntime;
use serde::Deserialize;
use serde::Serialize;
use sqlx::QueryBuilder;
use sqlx::Row;
use sqlx::Sqlite;
use std::collections::HashSet;

const EMPATRA_ATOMIC_OPERATION_CAPACITY: i64 = 4_096;
const EMPATRA_ATOMIC_OPERATION_RETRY_HORIZON_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const EMPATRA_ATOMIC_PENDING_PUBLICATION_CAPACITY: i64 = 256;
const EMPATRA_ATOMIC_PENDING_PUBLICATION_BYTES: i64 = 64 * 1024 * 1024;
const EMPATRA_ATOMIC_SINGLE_PUBLICATION_BYTES: usize = 768 * 1024;
const EMPATRA_ATOMIC_PENDING_USAGE_SQL: &str = r#"
SELECT
    (SELECT COUNT(*) FROM empatra_atomic_operations WHERE state = 'accepted')
        + (SELECT COUNT(*) FROM empatra_atomic_publication_outbox),
    COALESCE((SELECT SUM(LENGTH(CAST(initial_events_json AS BLOB))) FROM empatra_atomic_operations WHERE state = 'accepted'), 0)
        + COALESCE((SELECT SUM(LENGTH(CAST(initial_events_json AS BLOB))) FROM empatra_atomic_publication_outbox), 0)
"#;
const EMPATRA_ATOMIC_AUTHORITY_MARKER_CONTENT: &[u8] =
    b"Empatra atomic thread publication requires the matching state database.\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmpatraAtomicOperationState {
    Reserved,
    Accepted,
    CleanupPending,
    Completed,
    Failed,
}

impl EmpatraAtomicOperationState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Accepted => "accepted",
            Self::CleanupPending => "cleanup_pending",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "reserved" => Ok(Self::Reserved),
            "accepted" => Ok(Self::Accepted),
            "cleanup_pending" => Ok(Self::CleanupPending),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => anyhow::bail!("invalid Empatra atomic operation state: {value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmpatraAtomicOperationRecord {
    pub operation_id: String,
    pub payload_digest: String,
    pub issued_at_ms: i64,
    pub thread_id: String,
    pub turn_id: String,
    pub state: EmpatraAtomicOperationState,
    pub error: Option<String>,
    pub initial_events_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmpatraAtomicPublication {
    pub operation_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub initial_events_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmpatraAtomicOperationClaim {
    Claimed(EmpatraAtomicOperationRecord),
    Existing(EmpatraAtomicOperationRecord),
}

impl StateRuntime {
    /// Returns whether `thread_id` is still behind Empatra's atomic publication barrier.
    pub async fn is_empatra_atomic_thread_reserved(&self, thread_id: &str) -> anyhow::Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM empatra_atomic_operations WHERE thread_id = ? AND state != 'completed' LIMIT 1",
        )
        .bind(thread_id)
        .fetch_optional(self.pool.as_ref())
        .await?
        .is_some())
    }

    pub async fn reserved_empatra_atomic_thread_ids(
        &self,
        thread_ids: &[String],
    ) -> anyhow::Result<HashSet<String>> {
        if thread_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let mut builder = QueryBuilder::<Sqlite>::new(
            "SELECT thread_id FROM empatra_atomic_operations WHERE state != 'completed' AND thread_id IN (",
        );
        let mut separated = builder.separated(", ");
        for thread_id in thread_ids {
            separated.push_bind(thread_id);
        }
        separated.push_unseparated(")");
        builder
            .build()
            .fetch_all(self.pool.as_ref())
            .await?
            .into_iter()
            .map(|row| row.try_get("thread_id").map_err(Into::into))
            .collect()
    }

    pub async fn claim_empatra_atomic_operation(
        &self,
        record: &EmpatraAtomicOperationRecord,
    ) -> anyhow::Result<EmpatraAtomicOperationClaim> {
        self.ensure_empatra_atomic_authority_marker().await?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "DELETE FROM empatra_atomic_operations WHERE state IN ('completed', 'failed') AND updated_at_ms < ? AND NOT EXISTS (SELECT 1 FROM empatra_atomic_publication_outbox AS outbox WHERE outbox.operation_id = empatra_atomic_operations.operation_id)",
        )
        .bind(now_ms - EMPATRA_ATOMIC_OPERATION_RETRY_HORIZON_MS)
        .execute(&mut *transaction)
        .await?;
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM empatra_atomic_operations WHERE operation_id = ?",
        )
        .bind(&record.operation_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if !exists {
            let count =
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM empatra_atomic_operations")
                    .fetch_one(&mut *transaction)
                    .await?;
            if count >= EMPATRA_ATOMIC_OPERATION_CAPACITY {
                anyhow::bail!(
                    "Empatra atomic operation capacity ({EMPATRA_ATOMIC_OPERATION_CAPACITY}) is exhausted"
                );
            }
        }
        let result = sqlx::query(
            r#"
INSERT INTO empatra_atomic_operations (
    operation_id, payload_digest, issued_at_ms, thread_id, turn_id, state, error,
    initial_events_json, created_at_ms, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(operation_id) DO NOTHING
"#,
        )
        .bind(&record.operation_id)
        .bind(&record.payload_digest)
        .bind(record.issued_at_ms)
        .bind(&record.thread_id)
        .bind(&record.turn_id)
        .bind(record.state.as_str())
        .bind(&record.error)
        .bind(&record.initial_events_json)
        .bind(now_ms)
        .bind(now_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        let persisted = self
            .empatra_atomic_operation(&record.operation_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("claimed Empatra atomic operation disappeared"))?;
        Ok(if result.rows_affected() == 1 {
            EmpatraAtomicOperationClaim::Claimed(persisted)
        } else {
            EmpatraAtomicOperationClaim::Existing(persisted)
        })
    }

    async fn ensure_empatra_atomic_authority_marker(&self) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        let path = super::empatra_atomic_authority_marker_path(&self.codex_home);
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                file.write_all(EMPATRA_ATOMIC_AUTHORITY_MARKER_CONTENT)
                    .await?;
                file.sync_all().await?;
                sync_parent_directory(&self.codex_home).await?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub async fn empatra_atomic_operation(
        &self,
        operation_id: &str,
    ) -> anyhow::Result<Option<EmpatraAtomicOperationRecord>> {
        let row = sqlx::query(
            r#"
SELECT operation_id, payload_digest, issued_at_ms, thread_id, turn_id, state, error, initial_events_json
FROM empatra_atomic_operations
WHERE operation_id = ?
"#,
        )
        .bind(operation_id)
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(|row| {
            let state: String = row.try_get("state")?;
            Ok(EmpatraAtomicOperationRecord {
                operation_id: row.try_get("operation_id")?,
                payload_digest: row.try_get("payload_digest")?,
                issued_at_ms: row.try_get("issued_at_ms")?,
                thread_id: row.try_get("thread_id")?,
                turn_id: row.try_get("turn_id")?,
                state: EmpatraAtomicOperationState::parse(&state)?,
                error: row.try_get("error")?,
                initial_events_json: row.try_get("initial_events_json")?,
            })
        })
        .transpose()
    }

    /// Returns bounded operations whose next transition is derivable entirely from durable
    /// state. Reserved operations are intentionally excluded because resuming them requires the
    /// exact canonical client request.
    pub async fn recoverable_empatra_atomic_operations(
        &self,
        limit: usize,
    ) -> anyhow::Result<Vec<EmpatraAtomicOperationRecord>> {
        let limit = limit.clamp(1, 256) as i64;
        let rows = sqlx::query(
            r#"
SELECT operation_id, payload_digest, issued_at_ms, thread_id, turn_id, state, error, initial_events_json
FROM empatra_atomic_operations
WHERE state IN ('accepted', 'cleanup_pending')
ORDER BY updated_at_ms ASC, operation_id ASC
LIMIT ?
"#,
        )
        .bind(limit)
        .fetch_all(self.pool.as_ref())
        .await?;
        rows.into_iter()
            .map(|row| {
                let state: String = row.try_get("state")?;
                Ok(EmpatraAtomicOperationRecord {
                    operation_id: row.try_get("operation_id")?,
                    payload_digest: row.try_get("payload_digest")?,
                    issued_at_ms: row.try_get("issued_at_ms")?,
                    thread_id: row.try_get("thread_id")?,
                    turn_id: row.try_get("turn_id")?,
                    state: EmpatraAtomicOperationState::parse(&state)?,
                    error: row.try_get("error")?,
                    initial_events_json: row.try_get("initial_events_json")?,
                })
            })
            .collect()
    }

    pub async fn accept_empatra_atomic_operation(
        &self,
        operation_id: &str,
        initial_events_json: &str,
    ) -> anyhow::Result<()> {
        if let Some(existing) = self.empatra_atomic_operation(operation_id).await?
            && matches!(
                existing.state,
                EmpatraAtomicOperationState::Accepted | EmpatraAtomicOperationState::Completed
            )
            && existing.initial_events_json.as_deref() == Some(initial_events_json)
        {
            return Ok(());
        }
        if initial_events_json.len() > EMPATRA_ATOMIC_SINGLE_PUBLICATION_BYTES {
            anyhow::bail!("Empatra atomic initial event bundle exceeds its durable bound");
        }
        let mut transaction = self.pool.begin().await?;
        let (pending_count, pending_bytes): (i64, i64) =
            sqlx::query_as(EMPATRA_ATOMIC_PENDING_USAGE_SQL)
                .fetch_one(&mut *transaction)
                .await?;
        if pending_count >= EMPATRA_ATOMIC_PENDING_PUBLICATION_CAPACITY
            || pending_bytes.saturating_add(initial_events_json.len() as i64)
                > EMPATRA_ATOMIC_PENDING_PUBLICATION_BYTES
        {
            anyhow::bail!("Empatra atomic publication backlog is at capacity");
        }
        let result = sqlx::query(
            "UPDATE empatra_atomic_operations SET state = 'accepted', initial_events_json = ?, updated_at_ms = ? WHERE operation_id = ? AND state = 'reserved'",
        )
        .bind(initial_events_json)
        .bind(chrono::Utc::now().timestamp_millis())
        .bind(operation_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 1 {
            transaction.commit().await?;
            return Ok(());
        }
        transaction.rollback().await?;
        let existing = self
            .empatra_atomic_operation(operation_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("Empatra atomic operation {operation_id} disappeared")
            })?;
        if matches!(
            existing.state,
            EmpatraAtomicOperationState::Accepted | EmpatraAtomicOperationState::Completed
        ) && existing.initial_events_json.as_deref() == Some(initial_events_json)
        {
            return Ok(());
        }
        anyhow::bail!("Empatra atomic operation {operation_id} cannot accept this turn")
    }

    pub async fn complete_and_enqueue_empatra_atomic_publication(
        &self,
        operation_id: &str,
    ) -> anyhow::Result<EmpatraAtomicPublication> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut transaction = self.pool.begin().await?;
        let record = sqlx::query(
            "SELECT thread_id, turn_id, initial_events_json, state FROM empatra_atomic_operations WHERE operation_id = ?",
        )
        .bind(operation_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Empatra atomic operation {operation_id} disappeared"))?;
        let state: String = record.try_get("state")?;
        let initial_events_json: Option<String> = record.try_get("initial_events_json")?;
        let publication = EmpatraAtomicPublication {
            operation_id: operation_id.to_string(),
            thread_id: record.try_get("thread_id")?,
            turn_id: record.try_get("turn_id")?,
            initial_events_json: initial_events_json.ok_or_else(|| {
                anyhow::anyhow!("Empatra atomic operation {operation_id} has no accepted turn")
            })?,
        };
        if state == "accepted" {
            sqlx::query("UPDATE empatra_atomic_operations SET state = 'completed', updated_at_ms = ? WHERE operation_id = ? AND state = 'accepted'")
                .bind(now_ms).bind(operation_id).execute(&mut *transaction).await?;
        } else if state != "completed" {
            anyhow::bail!("Empatra atomic operation {operation_id} is not accepted");
        }
        sqlx::query("INSERT INTO empatra_atomic_publication_outbox (operation_id, thread_id, turn_id, initial_events_json, created_at_ms) VALUES (?, ?, ?, ?, ?) ON CONFLICT(operation_id) DO NOTHING")
            .bind(operation_id).bind(&publication.thread_id).bind(&publication.turn_id)
            .bind(&publication.initial_events_json).bind(now_ms).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(publication)
    }

    pub async fn acknowledge_empatra_atomic_publication(
        &self,
        operation_id: &str,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("DELETE FROM empatra_atomic_publication_outbox WHERE operation_id = ?")
            .bind(operation_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "UPDATE empatra_atomic_operations SET initial_events_json = NULL WHERE operation_id = ? AND state = 'completed'",
        )
        .bind(operation_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn pending_empatra_atomic_publications(
        &self,
        limit: usize,
    ) -> anyhow::Result<Vec<EmpatraAtomicPublication>> {
        let limit = limit.clamp(1, 256) as i64;
        let rows = sqlx::query(
            "SELECT operation_id, thread_id, turn_id, initial_events_json FROM empatra_atomic_publication_outbox ORDER BY created_at_ms ASC, operation_id ASC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(self.pool.as_ref())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(EmpatraAtomicPublication {
                    operation_id: row.try_get("operation_id")?,
                    thread_id: row.try_get("thread_id")?,
                    turn_id: row.try_get("turn_id")?,
                    initial_events_json: row.try_get("initial_events_json")?,
                })
            })
            .collect()
    }

    pub async fn empatra_atomic_publication(
        &self,
        operation_id: &str,
    ) -> anyhow::Result<Option<EmpatraAtomicPublication>> {
        let row = sqlx::query(
            "SELECT operation_id, thread_id, turn_id, initial_events_json FROM empatra_atomic_publication_outbox WHERE operation_id = ?",
        )
        .bind(operation_id)
        .fetch_optional(self.pool.as_ref())
        .await?;
        row.map(|row| {
            Ok(EmpatraAtomicPublication {
                operation_id: row.try_get("operation_id")?,
                thread_id: row.try_get("thread_id")?,
                turn_id: row.try_get("turn_id")?,
                initial_events_json: row.try_get("initial_events_json")?,
            })
        })
        .transpose()
    }

    pub async fn begin_empatra_atomic_cleanup(
        &self,
        operation_id: &str,
        failure_code: &str,
    ) -> anyhow::Result<()> {
        validate_failure_code(failure_code)?;
        let result = sqlx::query(
            "UPDATE empatra_atomic_operations SET state = 'cleanup_pending', error = ?, updated_at_ms = ? WHERE operation_id = ? AND state = 'reserved'",
        )
        .bind(failure_code)
        .bind(chrono::Utc::now().timestamp_millis())
        .bind(operation_id)
        .execute(self.pool.as_ref())
        .await?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        let existing = self
            .empatra_atomic_operation(operation_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("Empatra atomic operation {operation_id} disappeared")
            })?;
        if existing.state == EmpatraAtomicOperationState::CleanupPending
            && existing.error.as_deref() == Some(failure_code)
        {
            return Ok(());
        }
        anyhow::bail!("Empatra atomic operation {operation_id} cannot begin cleanup")
    }

    pub async fn finish_empatra_atomic_cleanup(
        &self,
        operation_id: &str,
        failure_code: &str,
    ) -> anyhow::Result<()> {
        validate_failure_code(failure_code)?;
        let result = sqlx::query(
            "UPDATE empatra_atomic_operations SET state = 'failed', error = ?, updated_at_ms = ? WHERE operation_id = ? AND state = 'cleanup_pending'",
        )
        .bind(failure_code)
        .bind(chrono::Utc::now().timestamp_millis())
        .bind(operation_id)
        .execute(self.pool.as_ref())
        .await?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        let existing = self
            .empatra_atomic_operation(operation_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("Empatra atomic operation {operation_id} disappeared")
            })?;
        if existing.state == EmpatraAtomicOperationState::Failed
            && existing.error.as_deref() == Some(failure_code)
        {
            return Ok(());
        }
        anyhow::bail!("Empatra atomic operation {operation_id} cleanup is not pending")
    }
}

#[cfg(unix)]
async fn sync_parent_directory(path: &std::path::Path) -> anyhow::Result<()> {
    tokio::fs::File::open(path).await?.sync_all().await?;
    Ok(())
}

// Windows persists the newly created marker itself with `File::sync_all`; unlike Unix,
// opening a directory for a portable `FlushFileBuffers` equivalent is unsupported.
#[cfg(not(unix))]
async fn sync_parent_directory(_path: &std::path::Path) -> anyhow::Result<()> {
    Ok(())
}

fn validate_failure_code(failure_code: &str) -> anyhow::Result<()> {
    if failure_code.is_empty()
        || failure_code.len() > 128
        || failure_code.chars().any(char::is_control)
    {
        anyhow::bail!("invalid bounded Empatra atomic failure code");
    }
    Ok(())
}

#[cfg(test)]
#[path = "empatra_atomic_operations_tests.rs"]
mod tests;
