use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmRunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl SwarmRunStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(anyhow::anyhow!("invalid swarm run status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmTaskStatus {
    Pending,
    Ready,
    Running,
    Completed,
    Failed,
    Retryable,
    Escalated,
    Cancelled,
}

impl SwarmTaskStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Retryable => "retryable",
            Self::Escalated => "escalated",
            Self::Cancelled => "cancelled",
        }
    }
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "ready" => Ok(Self::Ready),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "retryable" => Ok(Self::Retryable),
            "escalated" => Ok(Self::Escalated),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(anyhow::anyhow!("invalid swarm task status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmAttemptStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Expired,
}

impl SwarmAttemptStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            _ => Err(anyhow::anyhow!("invalid swarm attempt status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InterAgentMessageStatus {
    Queued,
    Delivered,
    Acked,
    Expired,
    DeadLettered,
    Cancelled,
}

impl InterAgentMessageStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Delivered => "delivered",
            Self::Acked => "acked",
            Self::Expired => "expired",
            Self::DeadLettered => "dead_lettered",
            Self::Cancelled => "cancelled",
        }
    }
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "delivered" => Ok(Self::Delivered),
            "acked" => Ok(Self::Acked),
            "expired" => Ok(Self::Expired),
            "dead_lettered" => Ok(Self::DeadLettered),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(anyhow::anyhow!(
                "invalid inter-agent message status: {value}"
            )),
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Acked | Self::Expired | Self::DeadLettered | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SwarmRun {
    pub id: String,
    pub thread_id: String,
    pub status: SwarmRunStatus,
    pub title: Option<String>,
    pub spec_json: Option<Value>,
    pub model_candidate_json: Option<Value>,
    pub fallback_reason: Option<String>,
    pub max_concurrency: i64,
    pub token_budget: Option<i64>,
    pub runtime_budget_seconds: Option<i64>,
    pub fail_fast: bool,
    pub cancel_policy: Option<String>,
    pub token_usage: i64,
    pub runtime_usage_seconds: i64,
    pub deadline_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub result_json: Option<Value>,
    pub cancelled_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmRunProgress {
    pub total_tasks: usize,
    pub pending_tasks: usize,
    pub ready_tasks: usize,
    pub running_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub retryable_tasks: usize,
    pub escalated_tasks: usize,
    pub cancelled_tasks: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmTaskAttemptDisposition {
    Succeeded,
    Failed,
    Retryable,
    Escalated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmRunCancellation {
    pub cancelled: bool,
    pub worker_thread_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SwarmTask {
    pub id: String,
    pub run_id: String,
    pub thread_id: String,
    pub assigned_thread_id: Option<String>,
    pub order_index: i64,
    pub depends_on_task_ids: Vec<String>,
    pub task_kind: String,
    pub agent_type: Option<String>,
    pub instructions: Option<String>,
    pub result_token: String,
    pub status: SwarmTaskStatus,
    pub model_candidate_json: Option<Value>,
    pub model_route_json: Option<Value>,
    pub candidate_index: Option<i64>,
    pub fallback_reason: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub max_attempts: i64,
    pub attempt_count: i64,
    pub token_usage: i64,
    pub runtime_usage_seconds: i64,
    pub deadline_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub result_json: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SwarmAttempt {
    pub id: String,
    pub run_id: String,
    pub task_id: String,
    pub thread_id: String,
    pub status: SwarmAttemptStatus,
    pub lease_owner: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub model_candidate_json: Option<Value>,
    pub fallback_reason: Option<String>,
    pub token_usage: i64,
    pub runtime_usage_seconds: i64,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub last_error: Option<String>,
    pub result_json: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SwarmCheckpoint {
    pub id: String,
    pub run_id: String,
    pub task_id: Option<String>,
    pub thread_id: String,
    pub checkpoint_type: String,
    pub payload_json: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InterAgentMessage {
    pub id: String,
    pub run_id: Option<String>,
    pub correlation_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub direct: bool,
    pub topic: Option<String>,
    pub sender_thread_id: String,
    pub target_thread_id: Option<String>,
    pub priority: i64,
    pub ttl_seconds: Option<i64>,
    pub status: InterAgentMessageStatus,
    pub attempt_count: i64,
    pub expires_at: Option<DateTime<Utc>>,
    pub rollout_pointer: Option<String>,
    pub rollout_hash: Option<String>,
    pub last_error: Option<String>,
    pub metadata_json: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InterAgentMessageDelivery {
    pub id: String,
    pub message_id: String,
    pub sender_thread_id: String,
    pub target_thread_id: String,
    pub topic: Option<String>,
    pub status: InterAgentMessageStatus,
    pub attempt_count: i64,
    pub delivered_at: Option<DateTime<Utc>>,
    pub acked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub dead_lettered_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub rollout_pointer: Option<String>,
    pub rollout_hash: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SwarmRunCreateParams {
    pub id: String,
    pub thread_id: String,
    pub title: Option<String>,
    pub spec_json: Option<Value>,
    pub model_candidate_json: Option<Value>,
    pub fallback_reason: Option<String>,
    pub max_concurrency: i64,
    pub token_budget: Option<i64>,
    pub runtime_budget_seconds: Option<i64>,
    pub fail_fast: bool,
    pub cancel_policy: Option<String>,
    pub deadline_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SwarmTaskCreateParams {
    pub id: String,
    pub run_id: String,
    pub thread_id: String,
    pub assigned_thread_id: Option<String>,
    pub order_index: i64,
    pub depends_on_task_ids: Vec<String>,
    pub task_kind: String,
    pub agent_type: Option<String>,
    pub instructions: Option<String>,
    pub result_token: String,
    pub model_candidate_json: Option<Value>,
    pub model_route_json: Option<Value>,
    pub candidate_index: Option<i64>,
    pub fallback_reason: Option<String>,
    pub max_attempts: i64,
    pub deadline_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SwarmMessageCreateParams {
    pub id: String,
    pub run_id: Option<String>,
    pub correlation_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub direct: bool,
    pub topic: Option<String>,
    pub sender_thread_id: String,
    pub target_thread_id: Option<String>,
    pub priority: i64,
    pub ttl_seconds: Option<i64>,
    pub rollout_pointer: Option<String>,
    pub rollout_hash: Option<String>,
    pub metadata_json: Value,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct SwarmRunRow {
    pub(crate) id: String,
    pub(crate) thread_id: String,
    pub(crate) status: String,
    pub(crate) title: Option<String>,
    pub(crate) spec_json: Option<String>,
    pub(crate) model_candidate_json: Option<String>,
    pub(crate) fallback_reason: Option<String>,
    pub(crate) max_concurrency: i64,
    pub(crate) token_budget: Option<i64>,
    pub(crate) runtime_budget_seconds: Option<i64>,
    pub(crate) fail_fast: i64,
    pub(crate) cancel_policy: Option<String>,
    pub(crate) token_usage: i64,
    pub(crate) runtime_usage_seconds: i64,
    pub(crate) deadline_at: Option<i64>,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
    pub(crate) started_at: Option<i64>,
    pub(crate) completed_at: Option<i64>,
    pub(crate) last_error: Option<String>,
    pub(crate) result_json: Option<String>,
    pub(crate) cancelled_at: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct SwarmTaskRow {
    pub(crate) id: String,
    pub(crate) run_id: String,
    pub(crate) thread_id: String,
    pub(crate) assigned_thread_id: Option<String>,
    pub(crate) order_index: i64,
    pub(crate) depends_on_task_ids_json: String,
    pub(crate) task_kind: String,
    pub(crate) agent_type: Option<String>,
    pub(crate) instructions: Option<String>,
    pub(crate) result_token: String,
    pub(crate) status: String,
    pub(crate) model_candidate_json: Option<String>,
    pub(crate) model_route_json: Option<String>,
    pub(crate) candidate_index: Option<i64>,
    pub(crate) fallback_reason: Option<String>,
    pub(crate) lease_owner: Option<String>,
    pub(crate) lease_until: Option<i64>,
    pub(crate) max_attempts: i64,
    pub(crate) attempt_count: i64,
    pub(crate) token_usage: i64,
    pub(crate) runtime_usage_seconds: i64,
    pub(crate) deadline_at: Option<i64>,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
    pub(crate) started_at: Option<i64>,
    pub(crate) completed_at: Option<i64>,
    pub(crate) last_error: Option<String>,
    pub(crate) result_json: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct SwarmAttemptRow {
    pub(crate) id: String,
    pub(crate) run_id: String,
    pub(crate) task_id: String,
    pub(crate) thread_id: String,
    pub(crate) status: String,
    pub(crate) lease_owner: Option<String>,
    pub(crate) lease_until: Option<i64>,
    pub(crate) model_candidate_json: Option<String>,
    pub(crate) fallback_reason: Option<String>,
    pub(crate) token_usage: i64,
    pub(crate) runtime_usage_seconds: i64,
    pub(crate) created_at: i64,
    pub(crate) started_at: Option<i64>,
    pub(crate) completed_at: Option<i64>,
    pub(crate) updated_at: i64,
    pub(crate) last_error: Option<String>,
    pub(crate) result_json: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct SwarmCheckpointRow {
    pub(crate) id: String,
    pub(crate) run_id: String,
    pub(crate) task_id: Option<String>,
    pub(crate) thread_id: String,
    pub(crate) checkpoint_type: String,
    pub(crate) payload_json: String,
    pub(crate) created_at: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct InterAgentMessageRow {
    pub(crate) id: String,
    pub(crate) run_id: Option<String>,
    pub(crate) correlation_id: Option<String>,
    pub(crate) in_reply_to: Option<String>,
    pub(crate) direct: i64,
    pub(crate) topic: Option<String>,
    pub(crate) sender_thread_id: String,
    pub(crate) target_thread_id: Option<String>,
    pub(crate) priority: i64,
    pub(crate) ttl_seconds: Option<i64>,
    pub(crate) status: String,
    pub(crate) attempt_count: i64,
    pub(crate) expires_at: Option<i64>,
    pub(crate) rollout_pointer: Option<String>,
    pub(crate) rollout_hash: Option<String>,
    pub(crate) last_error: Option<String>,
    pub(crate) metadata_json: String,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct InterAgentMessageDeliveryRow {
    pub(crate) id: String,
    pub(crate) message_id: String,
    pub(crate) sender_thread_id: String,
    pub(crate) target_thread_id: String,
    pub(crate) topic: Option<String>,
    pub(crate) status: String,
    pub(crate) attempt_count: i64,
    pub(crate) delivered_at: Option<i64>,
    pub(crate) acked_at: Option<i64>,
    pub(crate) expired_at: Option<i64>,
    pub(crate) dead_lettered_at: Option<i64>,
    pub(crate) cancelled_at: Option<i64>,
    pub(crate) last_error: Option<String>,
    pub(crate) rollout_pointer: Option<String>,
    pub(crate) rollout_hash: Option<String>,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
}

macro_rules! ts {
    ($v:expr) => {
        $v.map(epoch_seconds_to_datetime).transpose()
    };
}
fn epoch_seconds_to_datetime(secs: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(secs, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid unix timestamp: {secs}"))
}
fn json_opt(value: Option<String>) -> Result<Option<Value>> {
    value
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(Into::into)
}

impl TryFrom<SwarmRunRow> for SwarmRun {
    type Error = anyhow::Error;
    fn try_from(v: SwarmRunRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v.id,
            thread_id: v.thread_id,
            status: SwarmRunStatus::parse(v.status.as_str())?,
            title: v.title,
            spec_json: json_opt(v.spec_json)?,
            model_candidate_json: json_opt(v.model_candidate_json)?,
            fallback_reason: v.fallback_reason,
            max_concurrency: v.max_concurrency,
            token_budget: v.token_budget,
            runtime_budget_seconds: v.runtime_budget_seconds,
            fail_fast: v.fail_fast != 0,
            cancel_policy: v.cancel_policy,
            token_usage: v.token_usage,
            runtime_usage_seconds: v.runtime_usage_seconds,
            deadline_at: ts!(v.deadline_at)?,
            created_at: epoch_seconds_to_datetime(v.created_at)?,
            updated_at: epoch_seconds_to_datetime(v.updated_at)?,
            started_at: ts!(v.started_at)?,
            completed_at: ts!(v.completed_at)?,
            last_error: v.last_error,
            result_json: json_opt(v.result_json)?,
            cancelled_at: ts!(v.cancelled_at)?,
        })
    }
}
impl TryFrom<SwarmTaskRow> for SwarmTask {
    type Error = anyhow::Error;
    fn try_from(v: SwarmTaskRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v.id,
            run_id: v.run_id,
            thread_id: v.thread_id,
            assigned_thread_id: v.assigned_thread_id,
            order_index: v.order_index,
            depends_on_task_ids: serde_json::from_str(v.depends_on_task_ids_json.as_str())?,
            task_kind: v.task_kind,
            agent_type: v.agent_type,
            instructions: v.instructions,
            result_token: v.result_token,
            status: SwarmTaskStatus::parse(v.status.as_str())?,
            model_candidate_json: json_opt(v.model_candidate_json)?,
            model_route_json: json_opt(v.model_route_json)?,
            candidate_index: v.candidate_index,
            fallback_reason: v.fallback_reason,
            lease_owner: v.lease_owner,
            lease_until: ts!(v.lease_until)?,
            max_attempts: v.max_attempts,
            attempt_count: v.attempt_count,
            token_usage: v.token_usage,
            runtime_usage_seconds: v.runtime_usage_seconds,
            deadline_at: ts!(v.deadline_at)?,
            created_at: epoch_seconds_to_datetime(v.created_at)?,
            updated_at: epoch_seconds_to_datetime(v.updated_at)?,
            started_at: ts!(v.started_at)?,
            completed_at: ts!(v.completed_at)?,
            last_error: v.last_error,
            result_json: json_opt(v.result_json)?,
        })
    }
}
impl TryFrom<SwarmAttemptRow> for SwarmAttempt {
    type Error = anyhow::Error;
    fn try_from(v: SwarmAttemptRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v.id,
            run_id: v.run_id,
            task_id: v.task_id,
            thread_id: v.thread_id,
            status: SwarmAttemptStatus::parse(v.status.as_str())?,
            lease_owner: v.lease_owner,
            lease_until: ts!(v.lease_until)?,
            model_candidate_json: json_opt(v.model_candidate_json)?,
            fallback_reason: v.fallback_reason,
            token_usage: v.token_usage,
            runtime_usage_seconds: v.runtime_usage_seconds,
            created_at: epoch_seconds_to_datetime(v.created_at)?,
            started_at: ts!(v.started_at)?,
            completed_at: ts!(v.completed_at)?,
            updated_at: epoch_seconds_to_datetime(v.updated_at)?,
            last_error: v.last_error,
            result_json: json_opt(v.result_json)?,
        })
    }
}
impl TryFrom<SwarmCheckpointRow> for SwarmCheckpoint {
    type Error = anyhow::Error;
    fn try_from(v: SwarmCheckpointRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v.id,
            run_id: v.run_id,
            task_id: v.task_id,
            thread_id: v.thread_id,
            checkpoint_type: v.checkpoint_type,
            payload_json: serde_json::from_str(v.payload_json.as_str())?,
            created_at: epoch_seconds_to_datetime(v.created_at)?,
        })
    }
}
impl TryFrom<InterAgentMessageRow> for InterAgentMessage {
    type Error = anyhow::Error;
    fn try_from(v: InterAgentMessageRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v.id,
            run_id: v.run_id,
            correlation_id: v.correlation_id,
            in_reply_to: v.in_reply_to,
            direct: v.direct != 0,
            topic: v.topic,
            sender_thread_id: v.sender_thread_id,
            target_thread_id: v.target_thread_id,
            priority: v.priority,
            ttl_seconds: v.ttl_seconds,
            status: InterAgentMessageStatus::parse(v.status.as_str())?,
            attempt_count: v.attempt_count,
            expires_at: ts!(v.expires_at)?,
            rollout_pointer: v.rollout_pointer,
            rollout_hash: v.rollout_hash,
            last_error: v.last_error,
            metadata_json: serde_json::from_str(v.metadata_json.as_str())?,
            created_at: epoch_seconds_to_datetime(v.created_at)?,
            updated_at: epoch_seconds_to_datetime(v.updated_at)?,
        })
    }
}
impl TryFrom<InterAgentMessageDeliveryRow> for InterAgentMessageDelivery {
    type Error = anyhow::Error;
    fn try_from(v: InterAgentMessageDeliveryRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v.id,
            message_id: v.message_id,
            sender_thread_id: v.sender_thread_id,
            target_thread_id: v.target_thread_id,
            topic: v.topic,
            status: InterAgentMessageStatus::parse(v.status.as_str())?,
            attempt_count: v.attempt_count,
            delivered_at: ts!(v.delivered_at)?,
            acked_at: ts!(v.acked_at)?,
            expired_at: ts!(v.expired_at)?,
            dead_lettered_at: ts!(v.dead_lettered_at)?,
            cancelled_at: ts!(v.cancelled_at)?,
            last_error: v.last_error,
            rollout_pointer: v.rollout_pointer,
            rollout_hash: v.rollout_hash,
            created_at: epoch_seconds_to_datetime(v.created_at)?,
            updated_at: epoch_seconds_to_datetime(v.updated_at)?,
        })
    }
}
