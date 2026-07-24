use super::*;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::next_thread_spawn_depth;
use crate::agent::role::DEFAULT_ROLE_NAME;
use crate::agent::role::available_role_names;
use crate::agent::role::resolve_role_config;
use crate::agent::status::is_final;
use crate::function_tool::FunctionCallError;
use crate::session::InputQueueActivity;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::skills::SkillMetadata;
use crate::skills::implicit_skill_invocation_seen_key;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::multi_agents_common::ResolvedSpawnAgentModelRoute;
use crate::tools::handlers::multi_agents_common::SpawnAgentModelCandidate;
use crate::tools::handlers::multi_agents_common::apply_spawn_agent_model_route;
use crate::tools::handlers::multi_agents_common::apply_spawn_agent_role;
use crate::tools::handlers::multi_agents_common::apply_spawn_agent_runtime_overrides;
use crate::tools::handlers::multi_agents_common::apply_spawn_agent_service_tier;
use crate::tools::handlers::multi_agents_common::build_agent_spawn_config;
use crate::tools::handlers::multi_agents_common::find_spawn_agent_model_name;
use crate::tools::handlers::multi_agents_common::function_arguments;
use crate::tools::handlers::multi_agents_common::thread_spawn_source;
use crate::tools::handlers::multi_agents_common::tool_output_code_mode_result;
use crate::tools::handlers::multi_agents_common::tool_output_json_text;
use crate::tools::handlers::multi_agents_common::tool_output_response_item;
use crate::tools::handlers::multi_agents_common::validate_task_title;
use crate::tools::handlers::multi_agents_spec::WaitAgentTimeoutOptions;
use crate::tools::handlers::multi_agents_v2::emit_sub_agent_activity;
use crate::tools::handlers::work_swarm_spec::StartWorkSwarmToolOptions;
use crate::tools::handlers::work_swarm_spec::create_cancel_work_swarm_tool;
use crate::tools::handlers::work_swarm_spec::create_get_work_swarm_status_tool;
use crate::tools::handlers::work_swarm_spec::create_report_work_swarm_result_tool;
use crate::tools::handlers::work_swarm_spec::create_start_work_swarm_tool;
use crate::tools::handlers::work_swarm_spec::create_wait_work_swarm_tool;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use chrono::DateTime;
use chrono::Utc;
use codex_models_manager::manager::RefreshStrategy;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::items::SubAgentActivityItem;
use codex_protocol::items::TurnItem;
use codex_protocol::items::WorkSwarmActivityItem;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentActivityKind;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::TurnWorkSwarmCommunicationKind;
use codex_protocol::protocol::TurnWorkSwarmCommunicationMessage;
use codex_protocol::protocol::TurnWorkSwarmCommunicationProgress;
use codex_protocol::protocol::TurnWorkSwarmCommunicationStatus;
use codex_protocol::protocol::TurnWorkSwarmProgressStatus;
use codex_protocol::protocol::TurnWorkSwarmTaskKind;
use codex_protocol::protocol::TurnWorkSwarmTaskProgress;
use codex_protocol::protocol::TurnWorkSwarmTaskStatus;
use codex_protocol::protocol::WorkSwarmProgress;
use codex_state::InterAgentMessageStatus;
use codex_state::SwarmAttemptStatus;
use codex_state::SwarmRun;
use codex_state::SwarmRunStatus;
use codex_state::SwarmTask;
use codex_state::SwarmTaskAttemptDisposition;
use codex_state::SwarmTaskStatus;
use codex_swarm::CellId;
use codex_swarm::ExecutionPolicy;
use codex_swarm::ExecutionSpec;
use codex_swarm::ExecutionSpecError;
use codex_swarm::RunId;
use codex_swarm::TaskId;
use codex_swarm::TaskKind;
use codex_swarm::TaskSpec;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::sleep_until;
use tracing::warn;
use uuid::Uuid;

const DEFAULT_LEASE_TTL_SECONDS: u64 = 60;
const MIN_LEASE_TTL_SECONDS: u64 = 5;
const MAX_LEASE_TTL_SECONDS: u64 = 3_600;
const MAX_WORK_SWARM_TASKS: usize = 64;
const MAX_TASK_ATTEMPTS: u32 = 8;
const RUNNER_POLL_INTERVAL: Duration = Duration::from_millis(500);
const WORK_SWARM_AGENT_NAME_PREFIX: &str = "work_swarm_";
const WORK_SWARM_SKILL_NAME: &str = "work-swarm:orchestrate-work-swarm";
const WORK_SWARM_PLUGIN_ID: &str = "work-swarm@empatra-work-swarm";

fn work_swarm_skill(turn: &TurnContext) -> Option<&SkillMetadata> {
    turn.turn_skills
        .snapshot
        .outcome()
        .skills
        .iter()
        .find(|skill| {
            skill.name == WORK_SWARM_SKILL_NAME
                && skill.plugin_id.as_deref() == Some(WORK_SWARM_PLUGIN_ID)
                && turn.turn_skills.snapshot.outcome().is_skill_enabled(skill)
        })
}

pub(crate) fn work_swarm_skill_available(turn: &TurnContext) -> bool {
    work_swarm_skill(turn).is_some()
}

async fn ensure_work_swarm_skill_read(turn: &TurnContext) -> Result<(), FunctionCallError> {
    let Some(skill) = work_swarm_skill(turn) else {
        return Err(FunctionCallError::RespondToModel(
            "start_work_swarm is unavailable because the required \
             work-swarm:orchestrate-work-swarm skill is missing or disabled. Enable Agents so \
             Empatra can install and enable the Work Swarm plugin."
                .to_string(),
        ));
    };
    let seen_key = implicit_skill_invocation_seen_key(skill);
    let was_read = turn
        .turn_skills
        .implicit_invocation_seen_skills
        .lock()
        .await
        .contains(&seen_key);
    if was_read {
        return Ok(());
    }

    Err(FunctionCallError::RespondToModel(format!(
        "Before start_work_swarm, completely read the required \
         work-swarm:orchestrate-work-swarm skill at {}. The runtime has not observed that read yet.",
        skill.path_to_skills_md.display()
    )))
}

static ACTIVE_RUNNERS: LazyLock<Mutex<HashMap<String, watch::Sender<bool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) struct StartWorkSwarmHandler {
    options: StartWorkSwarmToolOptions,
}

impl StartWorkSwarmHandler {
    pub(crate) fn new(options: StartWorkSwarmToolOptions) -> Self {
        Self { options }
    }
}

pub(crate) struct WaitWorkSwarmHandler {
    options: WaitAgentTimeoutOptions,
}

impl WaitWorkSwarmHandler {
    pub(crate) fn new(options: WaitAgentTimeoutOptions) -> Self {
        Self { options }
    }
}

#[derive(Default)]
pub(crate) struct GetWorkSwarmStatusHandler;

#[derive(Default)]
pub(crate) struct CancelWorkSwarmHandler;

#[derive(Default)]
pub(crate) struct ReportWorkSwarmResultHandler;

macro_rules! impl_work_swarm_handler {
    ($handler:ty, $tool_name:literal, $spec:path, $function:path) => {
        impl ToolExecutor<ToolInvocation> for $handler {
            fn tool_name(&self) -> ToolName {
                ToolName::plain($tool_name)
            }

            fn spec(&self) -> ToolSpec {
                $spec()
            }

            fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
                Box::pin(async move { $function(invocation).await.map(boxed_tool_output) })
            }
        }

        impl CoreToolRuntime for $handler {
            fn matches_kind(&self, payload: &ToolPayload) -> bool {
                matches!(payload, ToolPayload::Function { .. })
            }
        }
    };
}

impl ToolExecutor<ToolInvocation> for StartWorkSwarmHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("start_work_swarm")
    }

    fn spec(&self) -> ToolSpec {
        create_start_work_swarm_tool(self.options.clone())
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            handle_start_work_swarm(invocation)
                .await
                .map(boxed_tool_output)
        })
    }
}

impl CoreToolRuntime for StartWorkSwarmHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

impl ToolExecutor<ToolInvocation> for WaitWorkSwarmHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("wait_work_swarm")
    }

    fn spec(&self) -> ToolSpec {
        create_wait_work_swarm_tool(self.options)
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            handle_wait_work_swarm(invocation)
                .await
                .map(boxed_tool_output)
        })
    }
}

impl CoreToolRuntime for WaitWorkSwarmHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}
impl_work_swarm_handler!(
    GetWorkSwarmStatusHandler,
    "get_work_swarm_status",
    create_get_work_swarm_status_tool,
    handle_get_work_swarm_status
);
impl_work_swarm_handler!(
    CancelWorkSwarmHandler,
    "cancel_work_swarm",
    create_cancel_work_swarm_tool,
    handle_cancel_work_swarm
);
impl_work_swarm_handler!(
    ReportWorkSwarmResultHandler,
    "report_work_swarm_result",
    create_report_work_swarm_result_tool,
    handle_report_work_swarm_result
);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartWorkSwarmArgs {
    title: Option<String>,
    policy: WorkSwarmPolicyArgs,
    tasks: Vec<WorkSwarmTaskArgs>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkSwarmPolicyArgs {
    max_concurrency: usize,
    token_budget: Option<u64>,
    runtime_budget_seconds: Option<u64>,
    lease_ttl_seconds: Option<u64>,
    deadline_at: Option<String>,
    fail_fast: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkSwarmTaskArgs {
    id: String,
    task_title: String,
    kind: String,
    agent_type: Option<String>,
    instructions: String,
    dependencies: Option<Vec<String>>,
    cell: Option<String>,
    explicit_model: Option<String>,
    explicit_reasoning: Option<ReasoningEffort>,
    max_attempts: Option<u32>,
    deadline_at: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelWorkSwarmArgs {
    run_id: String,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkSwarmStatusArgs {
    run_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitWorkSwarmArgs {
    run_id: String,
    timeout_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportWorkSwarmResultArgs {
    run_id: String,
    task_id: String,
    result_token: String,
    result: Value,
    status: String,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct StartWorkSwarmResult {
    run_id: String,
    accepted: bool,
}

#[derive(Debug, Serialize)]
struct CancelWorkSwarmResult {
    run_id: String,
    cancelled: bool,
}

#[derive(Debug, Serialize)]
struct WorkSwarmStatusResult {
    run_id: String,
    status: String,
    total_tasks: usize,
    pending_tasks: usize,
    ready_tasks: usize,
    running_tasks: usize,
    completed_tasks: usize,
    failed_tasks: usize,
    retryable_tasks: usize,
    escalated_tasks: usize,
    cancelled_tasks: usize,
    token_budget: Option<i64>,
    token_usage: i64,
    runtime_budget_seconds: Option<i64>,
    runtime_usage_seconds: i64,
    fallback_reason: Option<String>,
    last_error: Option<String>,
    result: Option<Value>,
    tasks: Vec<WorkSwarmTaskStatusResult>,
}

#[derive(Debug, Serialize)]
struct WaitWorkSwarmResult {
    wait_outcome: String,
    #[serde(flatten)]
    run: WorkSwarmStatusResult,
}

#[derive(Debug, Serialize)]
struct WorkSwarmTaskStatusResult {
    task_id: String,
    kind: String,
    agent_type: Option<String>,
    status: String,
    attempt_count: i64,
    max_attempts: i64,
    model: Option<String>,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    fallback_reason: Option<String>,
    last_error: Option<String>,
    result: Option<Value>,
}

#[derive(Debug, Serialize)]
struct ReportWorkSwarmResult {
    accepted: bool,
    disposition: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReportDisposition {
    Succeeded,
    Failed,
    Retryable,
    Escalated,
}

#[derive(Debug)]
enum ClaimedTaskFailure {
    CapacityDeferred,
    Objective(String),
    Retryable(String),
    Fatal(anyhow::Error),
}

impl From<anyhow::Error> for ClaimedTaskFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Fatal(error)
    }
}

impl ReportDisposition {
    fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Retryable => "retryable",
            Self::Escalated => "escalated",
        }
    }
}

async fn handle_start_work_swarm(
    invocation: ToolInvocation,
) -> Result<StartWorkSwarmResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        ..
    } = invocation;
    ensure_orchestrator_source(&turn)?;
    ensure_work_swarm_skill_read(&turn).await?;
    let args: StartWorkSwarmArgs = parse_arguments(&function_arguments(payload)?)?;
    validate_request_limits(&args, &turn)?;
    let db = required_state_db(&session)?;
    let run_id = Uuid::new_v4().to_string();
    let execution_spec = build_execution_spec(&args, &turn, run_id.as_str())?;
    execution_spec.validate().map_err(execution_spec_error)?;

    let run_params = codex_state::SwarmRunCreateParams {
        id: run_id.clone(),
        thread_id: session.thread_id.to_string(),
        title: args
            .title
            .as_deref()
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(str::to_string),
        spec_json: Some(serde_json::to_value(&execution_spec).map_err(json_error)?),
        model_candidate_json: None,
        fallback_reason: None,
        max_concurrency: execution_spec.policy.max_concurrency as i64,
        token_budget: execution_spec
            .policy
            .token_budget
            .map(saturating_u64_to_i64),
        runtime_budget_seconds: execution_spec
            .policy
            .max_runtime
            .map(|duration| saturating_u64_to_i64(duration.as_secs())),
        fail_fast: execution_spec.policy.fail_fast,
        cancel_policy: Some("propagate".to_string()),
        deadline_at: execution_spec.policy.deadline.map(Into::into),
    };
    let tasks = build_task_params(run_id.as_str(), &session, &execution_spec);
    let run = db
        .create_runnable_swarm_run(&run_params, &tasks)
        .await
        .map_err(runtime_error)?;

    emit_work_swarm_progress(
        &session,
        &turn,
        run.id.as_str(),
        TurnWorkSwarmProgressStatus::Pending,
        None,
        None,
    )
    .await;
    let _runner = spawn_work_swarm_runner(Arc::clone(&session), Arc::clone(&turn), run.id.clone());

    Ok(StartWorkSwarmResult {
        run_id: run.id,
        accepted: true,
    })
}

async fn handle_get_work_swarm_status(
    invocation: ToolInvocation,
) -> Result<WorkSwarmStatusResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        ..
    } = invocation;
    ensure_orchestrator_source(&turn)?;
    let args: WorkSwarmStatusArgs = parse_arguments(&function_arguments(payload)?)?;
    let db = required_state_db(&session)?;
    let run = owned_swarm_run(&db, &session, args.run_id.as_str()).await?;
    if matches!(
        run.status,
        SwarmRunStatus::Pending | SwarmRunStatus::Running
    ) {
        let _runner =
            spawn_work_swarm_runner(Arc::clone(&session), Arc::clone(&turn), run.id.clone());
    }
    work_swarm_status_result(&db, run).await
}

async fn handle_wait_work_swarm(
    invocation: ToolInvocation,
) -> Result<WaitWorkSwarmResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        ..
    } = invocation;
    ensure_orchestrator_source(&turn)?;
    let args: WaitWorkSwarmArgs = parse_arguments(&function_arguments(payload)?)?;
    let timeout_ms = validated_wait_timeout_ms(args.timeout_ms, &turn)?;
    let db = required_state_db(&session)?;

    // Validate ownership before subscribing, then read again after the runner
    // subscription is installed. The second read closes the race where the run
    // reaches a terminal state between the first read and subscription.
    let mut run = owned_swarm_run(&db, &session, args.run_id.as_str()).await?;
    if let Some(wait_outcome) = terminal_wait_outcome(run.status) {
        return Ok(WaitWorkSwarmResult {
            wait_outcome: wait_outcome.to_string(),
            run: work_swarm_status_result(&db, run).await?,
        });
    }

    let mut runner =
        spawn_work_swarm_runner(Arc::clone(&session), Arc::clone(&turn), run.id.clone());
    let turn_state = session
        .input_queue
        .turn_state_for_sub_id(&session.active_turn, &turn.sub_id)
        .await;
    let (mut activity, mut pending_activity) = session
        .input_queue
        .subscribe_activity(turn_state.as_deref())
        .await;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
    let wait_outcome = loop {
        run = owned_swarm_run(&db, &session, args.run_id.as_str()).await?;
        if let Some(outcome) = terminal_wait_outcome(run.status) {
            break outcome;
        }

        match wait_for_work_swarm_wake(
            &mut runner,
            &mut activity,
            pending_activity.take(),
            deadline,
        )
        .await
        {
            WorkSwarmWaitWake::RunnerFinished => {
                // A runner can end because another process changed durable
                // state or because it recovered from a stale lease. Re-read
                // first; if the run is still active, install a fresh runner.
                run = owned_swarm_run(&db, &session, args.run_id.as_str()).await?;
                if terminal_wait_outcome(run.status).is_none() {
                    runner = spawn_work_swarm_runner(
                        Arc::clone(&session),
                        Arc::clone(&turn),
                        run.id.clone(),
                    );
                }
            }
            WorkSwarmWaitWake::Steered => break "steered",
            WorkSwarmWaitWake::TimedOut => break "timed_out",
        }
    };

    // A terminal transition concurrent with timeout or steering wins: callers
    // receive the completed pipeline result instead of a stale wait outcome.
    run = owned_swarm_run(&db, &session, args.run_id.as_str()).await?;
    let wait_outcome = terminal_wait_outcome(run.status).unwrap_or(wait_outcome);
    Ok(WaitWorkSwarmResult {
        wait_outcome: wait_outcome.to_string(),
        run: work_swarm_status_result(&db, run).await?,
    })
}

async fn work_swarm_status_result(
    db: &crate::StateDbHandle,
    run: SwarmRun,
) -> Result<WorkSwarmStatusResult, FunctionCallError> {
    let progress = db
        .get_swarm_run_progress(run.id.as_str())
        .await
        .map_err(runtime_error)?;
    let (_, tasks, _, _, _) = db
        .load_swarm_recovery_data(run.id.as_str())
        .await
        .map_err(runtime_error)?;
    let task_statuses = tasks
        .into_iter()
        .map(|task| WorkSwarmTaskStatusResult {
            task_id: local_task_id(run.id.as_str(), task.id.as_str()).to_string(),
            kind: task.task_kind,
            agent_type: task.agent_type,
            status: task.status.as_str().to_string(),
            attempt_count: task.attempt_count,
            max_attempts: task.max_attempts,
            model: task
                .model_candidate_json
                .as_ref()
                .and_then(|candidate| candidate.get("model"))
                .and_then(Value::as_str)
                .map(str::to_string),
            reasoning_effort: task
                .model_candidate_json
                .as_ref()
                .and_then(|candidate| candidate.get("reasoning_effort"))
                .and_then(Value::as_str)
                .map(str::to_string),
            service_tier: task
                .model_candidate_json
                .as_ref()
                .and_then(|candidate| candidate.get("service_tier"))
                .and_then(Value::as_str)
                .map(str::to_string),
            fallback_reason: task.fallback_reason,
            last_error: task.last_error,
            result: task.result_json,
        })
        .collect();
    Ok(WorkSwarmStatusResult {
        run_id: run.id,
        status: run.status.as_str().to_string(),
        total_tasks: progress.total_tasks,
        pending_tasks: progress.pending_tasks,
        ready_tasks: progress.ready_tasks,
        running_tasks: progress.running_tasks,
        completed_tasks: progress.completed_tasks,
        failed_tasks: progress.failed_tasks,
        retryable_tasks: progress.retryable_tasks,
        escalated_tasks: progress.escalated_tasks,
        cancelled_tasks: progress.cancelled_tasks,
        token_budget: run.token_budget,
        token_usage: run.token_usage,
        runtime_budget_seconds: run.runtime_budget_seconds,
        runtime_usage_seconds: run.runtime_usage_seconds,
        fallback_reason: run.fallback_reason,
        last_error: run.last_error,
        result: run.result_json,
        tasks: task_statuses,
    })
}

fn validated_wait_timeout_ms(
    requested: Option<i64>,
    turn: &TurnContext,
) -> Result<i64, FunctionCallError> {
    let min = turn.config.multi_agent_v2.min_wait_timeout_ms;
    let max = turn.config.multi_agent_v2.max_wait_timeout_ms;
    match requested {
        Some(value) if value < min => Err(FunctionCallError::RespondToModel(format!(
            "timeout_ms must be at least {min}"
        ))),
        Some(value) if value > max => Err(FunctionCallError::RespondToModel(format!(
            "timeout_ms must be at most {max}"
        ))),
        Some(value) => Ok(value),
        None => Ok(turn.config.multi_agent_v2.default_wait_timeout_ms),
    }
}

fn terminal_wait_outcome(status: SwarmRunStatus) -> Option<&'static str> {
    match status {
        SwarmRunStatus::Completed => Some("completed"),
        SwarmRunStatus::Failed => Some("failed"),
        SwarmRunStatus::Cancelled => Some("cancelled"),
        SwarmRunStatus::Pending | SwarmRunStatus::Running => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkSwarmWaitWake {
    RunnerFinished,
    Steered,
    TimedOut,
}

async fn wait_for_work_swarm_wake(
    runner: &mut watch::Receiver<bool>,
    activity: &mut watch::Receiver<InputQueueActivity>,
    pending_activity: Option<InputQueueActivity>,
    deadline: Instant,
) -> WorkSwarmWaitWake {
    if pending_activity == Some(InputQueueActivity::Steer) {
        return WorkSwarmWaitWake::Steered;
    }
    if *runner.borrow_and_update() {
        return WorkSwarmWaitWake::RunnerFinished;
    }

    let mut activity_open = true;
    loop {
        tokio::select! {
            changed = runner.changed() => {
                if changed.is_err() || *runner.borrow_and_update() {
                    return WorkSwarmWaitWake::RunnerFinished;
                }
            }
            changed = activity.changed(), if activity_open => {
                match changed {
                    Ok(()) if *activity.borrow_and_update() == InputQueueActivity::Steer => {
                        return WorkSwarmWaitWake::Steered;
                    }
                    Ok(()) => {}
                    Err(_) => activity_open = false,
                }
            }
            _ = sleep_until(deadline) => return WorkSwarmWaitWake::TimedOut,
        }
    }
}

async fn handle_cancel_work_swarm(
    invocation: ToolInvocation,
) -> Result<CancelWorkSwarmResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        ..
    } = invocation;
    ensure_orchestrator_source(&turn)?;
    let args: CancelWorkSwarmArgs = parse_arguments(&function_arguments(payload)?)?;
    let db = required_state_db(&session)?;
    let _ = owned_swarm_run(&db, &session, args.run_id.as_str()).await?;
    let reason = args
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .unwrap_or("cancelled by orchestrator");
    let cancellation = db
        .cancel_swarm_run(args.run_id.as_str(), reason)
        .await
        .map_err(runtime_error)?;
    if cancellation.cancelled {
        shutdown_work_swarm_worker_threads(&session, cancellation.worker_thread_ids).await;
        emit_work_swarm_progress(
            &session,
            &turn,
            args.run_id.as_str(),
            TurnWorkSwarmProgressStatus::Cancelled,
            None,
            Some(reason),
        )
        .await;
    }
    Ok(CancelWorkSwarmResult {
        run_id: args.run_id,
        cancelled: cancellation.cancelled,
    })
}

async fn handle_report_work_swarm_result(
    invocation: ToolInvocation,
) -> Result<ReportWorkSwarmResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        ..
    } = invocation;
    let args: ReportWorkSwarmResultArgs = parse_arguments(&function_arguments(payload)?)?;
    ensure_worker_source(&turn)?;
    if !args.result.is_object() {
        return Err(FunctionCallError::RespondToModel(
            "work swarm result must be a JSON object".to_string(),
        ));
    }
    let requested_disposition = parse_report_disposition(&args)?;
    let db = required_state_db(&session)?;
    let task = db
        .get_swarm_task(args.task_id.as_str())
        .await
        .map_err(runtime_error)?
        .ok_or_else(|| {
            FunctionCallError::RespondToModel(format!("work swarm task {} not found", args.task_id))
        })?;
    authenticate_worker_report(&task, &session, &args)?;

    let attempt_id = attempt_id(&task);
    let tokens_used = session
        .services
        .agent_control
        .child_token_usage(session.thread_id)
        .await;
    let disposition = if matches!(
        requested_disposition,
        ReportDisposition::Retryable | ReportDisposition::Escalated
    ) && task.attempt_count >= task.max_attempts
    {
        ReportDisposition::Failed
    } else {
        requested_disposition
    };
    let state_disposition = match disposition {
        ReportDisposition::Succeeded => SwarmTaskAttemptDisposition::Succeeded,
        ReportDisposition::Failed => SwarmTaskAttemptDisposition::Failed,
        ReportDisposition::Retryable => SwarmTaskAttemptDisposition::Retryable,
        ReportDisposition::Escalated => SwarmTaskAttemptDisposition::Escalated,
    };
    let transition_error = match disposition {
        ReportDisposition::Failed => Some(
            args.error
                .as_deref()
                .unwrap_or("worker reported objective failure"),
        ),
        ReportDisposition::Succeeded => None,
        ReportDisposition::Retryable | ReportDisposition::Escalated => args.error.as_deref(),
    };
    let transitioned = db
        .finalize_swarm_worker_report(
            attempt_id.as_str(),
            task.id.as_str(),
            state_disposition,
            tokens_used,
            0,
            Some(&args.result),
            transition_error,
        )
        .await
        .map_err(runtime_error)?;
    if !transitioned {
        return Err(FunctionCallError::RespondToModel(
            "work swarm attempt or task is no longer active".to_string(),
        ));
    }

    emit_work_swarm_progress(
        &session,
        &turn,
        args.run_id.as_str(),
        match disposition {
            ReportDisposition::Succeeded
            | ReportDisposition::Retryable
            | ReportDisposition::Escalated => TurnWorkSwarmProgressStatus::Running,
            ReportDisposition::Failed => TurnWorkSwarmProgressStatus::Failed,
        },
        Some(task.id.as_str()),
        transition_error,
    )
    .await;

    let control = session.services.agent_control.clone();
    let child_thread_id = session.thread_id;
    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        let _ = control.shutdown_live_agent(child_thread_id).await;
    });

    Ok(ReportWorkSwarmResult {
        accepted: true,
        disposition: disposition.as_str().to_string(),
    })
}

fn ensure_orchestrator_source(turn: &TurnContext) -> Result<(), FunctionCallError> {
    if matches!(&turn.session_source, SessionSource::SubAgent(_)) {
        return Err(FunctionCallError::RespondToModel(
            "work swarm workers cannot create, inspect, or cancel swarm runs".to_string(),
        ));
    }
    if turn
        .session_source
        .get_agent_path()
        .is_some_and(|path| !path.is_root())
    {
        return Err(FunctionCallError::RespondToModel(
            "only the root Work orchestrator can manage Work Swarm runs".to_string(),
        ));
    }
    Ok(())
}

fn ensure_worker_source(turn: &TurnContext) -> Result<(), FunctionCallError> {
    if is_work_swarm_worker_source(&turn.session_source) {
        return Ok(());
    }
    Err(FunctionCallError::RespondToModel(
        "report_work_swarm_result is only available to the assigned Work Swarm worker".to_string(),
    ))
}

pub(crate) fn is_work_swarm_worker_source(source: &SessionSource) -> bool {
    matches!(
        source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            agent_path: Some(agent_path),
            ..
        }) if agent_path.name().starts_with(WORK_SWARM_AGENT_NAME_PREFIX)
    )
}

fn authenticate_worker_report(
    task: &SwarmTask,
    session: &Session,
    args: &ReportWorkSwarmResultArgs,
) -> Result<(), FunctionCallError> {
    let current_thread_id = session.thread_id.to_string();
    if task.run_id != args.run_id
        || task.status != SwarmTaskStatus::Running
        || task
            .lease_until
            .is_none_or(|lease_until| lease_until <= Utc::now())
        || task.assigned_thread_id.as_deref() != Some(current_thread_id.as_str())
        || task.result_token != args.result_token
    {
        return Err(FunctionCallError::RespondToModel(
            "work swarm result token, task lease, or assigned worker does not match".to_string(),
        ));
    }
    Ok(())
}

fn parse_report_disposition(
    args: &ReportWorkSwarmResultArgs,
) -> Result<ReportDisposition, FunctionCallError> {
    match args.status.trim() {
        "succeeded" => Ok(ReportDisposition::Succeeded),
        "failed" => Ok(ReportDisposition::Failed),
        "retryable" => Ok(ReportDisposition::Retryable),
        "escalated" => Ok(ReportDisposition::Escalated),
        other => Err(FunctionCallError::RespondToModel(format!(
            "unknown work swarm result status: {other}"
        ))),
    }
}

fn validate_request_limits(
    args: &StartWorkSwarmArgs,
    turn: &TurnContext,
) -> Result<(), FunctionCallError> {
    if args.tasks.len() > MAX_WORK_SWARM_TASKS {
        return Err(FunctionCallError::RespondToModel(format!(
            "work swarm supports at most {MAX_WORK_SWARM_TASKS} tasks per run"
        )));
    }
    if let Some(max_threads) = turn.config.agent_max_threads
        && args.policy.max_concurrency > max_threads
    {
        return Err(FunctionCallError::RespondToModel(format!(
            "max_concurrency {} exceeds configured agent thread limit {max_threads}",
            args.policy.max_concurrency
        )));
    }
    if let Some(lease_ttl) = args.policy.lease_ttl_seconds
        && !(MIN_LEASE_TTL_SECONDS..=MAX_LEASE_TTL_SECONDS).contains(&lease_ttl)
    {
        return Err(FunctionCallError::RespondToModel(format!(
            "lease_ttl_seconds must be between {MIN_LEASE_TTL_SECONDS} and {MAX_LEASE_TTL_SECONDS}"
        )));
    }
    for task in &args.tasks {
        if task.id.len() > 96
            || task
                .agent_type
                .as_deref()
                .is_some_and(|agent_type| agent_type.len() > 64)
        {
            return Err(FunctionCallError::RespondToModel(
                "work swarm task id or agent_type is too long".to_string(),
            ));
        }
        if task.id.contains(':') {
            return Err(FunctionCallError::RespondToModel(
                "work swarm task ids must not contain a colon".to_string(),
            ));
        }
        if task
            .max_attempts
            .is_some_and(|value| value > MAX_TASK_ATTEMPTS)
        {
            return Err(FunctionCallError::RespondToModel(format!(
                "task {} exceeds the maximum of {MAX_TASK_ATTEMPTS} attempts",
                task.id
            )));
        }
        let agent_type = task
            .agent_type
            .as_deref()
            .map(str::trim)
            .unwrap_or(DEFAULT_ROLE_NAME);
        if agent_type.is_empty() {
            return Err(FunctionCallError::RespondToModel(
                "work swarm agent_type must be omitted or name a configured role".to_string(),
            ));
        }
        if resolve_role_config(&turn.config, agent_type).is_none() {
            return Err(FunctionCallError::RespondToModel(format!(
                "unknown agent_type '{agent_type}'; available roles: {}",
                available_role_names(&turn.config).join(", ")
            )));
        }
    }
    Ok(())
}

fn build_execution_spec(
    args: &StartWorkSwarmArgs,
    turn: &TurnContext,
    run_id: &str,
) -> Result<ExecutionSpec, FunctionCallError> {
    let policy = ExecutionPolicy {
        max_concurrency: args.policy.max_concurrency,
        token_budget: args.policy.token_budget,
        max_runtime: args.policy.runtime_budget_seconds.map(Duration::from_secs),
        deadline: parse_datetime(args.policy.deadline_at.as_deref())?.map(Into::into),
        fail_fast: args.policy.fail_fast.unwrap_or(true),
        lease_ttl: Duration::from_secs(
            args.policy
                .lease_ttl_seconds
                .unwrap_or(DEFAULT_LEASE_TTL_SECONDS),
        ),
        default_max_attempts: None,
    };
    let tasks = args
        .tasks
        .iter()
        .map(|task| {
            let agent_type = task
                .agent_type
                .as_deref()
                .map(str::trim)
                .unwrap_or(DEFAULT_ROLE_NAME);
            let routing = &turn.config.agent_model_routing;
            let route = routing
                .enabled
                .unwrap_or(!routing.routes.is_empty())
                .then(|| routing.routes.get(agent_type))
                .flatten();
            let route_attempts = route
                .and_then(|route| route.max_attempts)
                .or_else(|| route.map(|route| route.candidates.len().max(1) as u32))
                .unwrap_or(1);
            let max_attempts = task
                .max_attempts
                .unwrap_or(if task.explicit_model.is_some() {
                    1
                } else {
                    route_attempts
                })
                .min(MAX_TASK_ATTEMPTS);
            Ok(TaskSpec {
                id: TaskId(task.id.trim().to_string()),
                task_title: validate_task_title(task.task_title.as_str())?,
                kind: parse_task_kind(task.kind.as_str())?,
                agent_type: agent_type.to_string(),
                instructions: task.instructions.trim().to_string(),
                dependencies: task
                    .dependencies
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|id| TaskId(id.trim().to_string()))
                    .collect(),
                cell: task
                    .cell
                    .as_deref()
                    .map(str::trim)
                    .filter(|cell| !cell.is_empty())
                    .map(|cell| CellId(cell.to_string())),
                explicit_model: task
                    .explicit_model
                    .as_deref()
                    .map(str::trim)
                    .filter(|model| !model.is_empty())
                    .map(str::to_string),
                explicit_reasoning: task.explicit_reasoning.as_ref().map(ToString::to_string),
                max_attempts,
                deadline: parse_datetime(task.deadline_at.as_deref())?.map(Into::into),
            })
        })
        .collect::<Result<Vec<_>, FunctionCallError>>()?;
    Ok(ExecutionSpec {
        run_id: RunId(run_id.to_string()),
        policy,
        tasks,
    })
}

fn build_task_params(
    run_id: &str,
    session: &Session,
    execution_spec: &ExecutionSpec,
) -> Vec<codex_state::SwarmTaskCreateParams> {
    execution_spec
        .tasks
        .iter()
        .enumerate()
        .map(|(order_index, task)| codex_state::SwarmTaskCreateParams {
            id: stored_task_id(run_id, task.id.0.as_str()),
            run_id: run_id.to_string(),
            thread_id: session.thread_id.to_string(),
            assigned_thread_id: None,
            order_index: order_index as i64,
            depends_on_task_ids: task
                .dependencies
                .iter()
                .map(|dependency| stored_task_id(run_id, dependency.0.as_str()))
                .collect(),
            task_kind: task_kind_name(task.kind).to_string(),
            agent_type: Some(task.agent_type.clone()),
            instructions: Some(task.instructions.clone()),
            result_token: Uuid::new_v4().to_string(),
            model_candidate_json: None,
            model_route_json: None,
            candidate_index: None,
            fallback_reason: None,
            max_attempts: i64::from(task.max_attempts),
            deadline_at: task.deadline.map(Into::into),
        })
        .collect()
}

fn stored_task_id(run_id: &str, local_task_id: &str) -> String {
    format!("{run_id}:{local_task_id}")
}

fn local_task_id<'a>(run_id: &str, stored_task_id: &'a str) -> &'a str {
    stored_task_id
        .strip_prefix(format!("{run_id}:").as_str())
        .unwrap_or(stored_task_id)
}

fn task_kind_name(kind: TaskKind) -> &'static str {
    match kind {
        TaskKind::Worker => "worker",
        TaskKind::Reducer => "reducer",
        TaskKind::Reviewer => "reviewer",
    }
}

fn parse_task_kind(kind: &str) -> Result<TaskKind, FunctionCallError> {
    match kind.trim().to_ascii_lowercase().as_str() {
        "worker" => Ok(TaskKind::Worker),
        "reducer" => Ok(TaskKind::Reducer),
        "reviewer" => Ok(TaskKind::Reviewer),
        other => Err(FunctionCallError::RespondToModel(format!(
            "unknown work swarm task kind: {other}"
        ))),
    }
}

fn parse_datetime(value: Option<&str>) -> Result<Option<DateTime<Utc>>, FunctionCallError> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(value)
                .map(|datetime| datetime.with_timezone(&Utc))
                .map_err(|err| {
                    FunctionCallError::RespondToModel(format!("invalid RFC3339 timestamp: {err}"))
                })
        })
        .transpose()
}

fn spawn_work_swarm_runner(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    run_id: String,
) -> watch::Receiver<bool> {
    let mut runners = ACTIVE_RUNNERS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(existing) = runners.get(run_id.as_str()) {
        return existing.subscribe();
    }
    let (finished_tx, finished_rx) = watch::channel(false);
    runners.insert(run_id.clone(), finished_tx.clone());
    drop(runners);

    let runner = tokio::spawn(run_work_swarm(
        Arc::clone(&session),
        Arc::clone(&turn),
        run_id.clone(),
    ));
    tokio::spawn(async move {
        let result = match runner.await {
            Ok(result) => result,
            Err(err) => Err(anyhow::anyhow!("Work Swarm runner task terminated: {err}")),
        };
        if let Err(err) = result {
            let error = err.to_string();
            warn!("work swarm run {run_id} failed: {err:#}");
            if let Some(db) = session.state_db() {
                let _ = fail_run(&session, &turn, &db, run_id.as_str(), error.as_str()).await;
            } else {
                shutdown_running_work_swarm_children(&session, run_id.as_str()).await;
                emit_work_swarm_progress(
                    &session,
                    &turn,
                    run_id.as_str(),
                    TurnWorkSwarmProgressStatus::Failed,
                    None,
                    Some(error.as_str()),
                )
                .await;
            }
        }
        finished_tx.send_replace(true);
        ACTIVE_RUNNERS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(run_id.as_str());
    });
    finished_rx
}

/// Restarts durable Work Swarm runs when an orchestrator session resumes after a process crash.
/// The in-memory registry makes this safe to call at every step boundary.
pub(crate) async fn resume_active_work_swarms(session: &Arc<Session>, turn: &Arc<TurnContext>) {
    if ensure_orchestrator_source(turn).is_err() {
        return;
    }
    let Some(db) = session.state_db() else {
        return;
    };
    let thread_id = session.thread_id.to_string();
    match db.list_active_swarm_runs(thread_id.as_str()).await {
        Ok(runs) => {
            for run in runs {
                let _runner =
                    spawn_work_swarm_runner(Arc::clone(session), Arc::clone(turn), run.id);
            }
        }
        Err(err) => warn!("failed to recover active Work Swarm runs: {err:#}"),
    }
}

async fn run_work_swarm(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    run_id: String,
) -> anyhow::Result<()> {
    let db = session
        .state_db()
        .ok_or_else(|| anyhow::anyhow!("state db unavailable"))?;
    let execution_spec = load_execution_spec(&db, run_id.as_str()).await?;
    let lease_owner = format!("work_swarm:{run_id}");
    let lease_seconds = execution_spec.policy.lease_ttl.as_secs().max(1) as i64;

    loop {
        let Some(run) = db.get_swarm_run(run_id.as_str()).await? else {
            return Ok(());
        };
        if run.status == SwarmRunStatus::Cancelled {
            shutdown_running_work_swarm_children(&session, run_id.as_str()).await;
            return Ok(());
        }
        if matches!(
            run.status,
            SwarmRunStatus::Completed | SwarmRunStatus::Failed
        ) {
            return Ok(());
        }

        if let Some(reason) = run_limit_failure(&session, &db, &run).await? {
            fail_run(&session, &turn, &db, run_id.as_str(), reason.as_str()).await?;
            return Ok(());
        }

        reconcile_running_tasks(
            &session,
            &turn,
            &db,
            &run,
            lease_owner.as_str(),
            lease_seconds,
        )
        .await?;

        let progress = db.get_swarm_run_progress(run_id.as_str()).await?;
        if progress.failed_tasks > 0 && run.fail_fast {
            fail_run(
                &session,
                &turn,
                &db,
                run_id.as_str(),
                "a Work Swarm task failed and fail_fast is enabled",
            )
            .await?;
            return Ok(());
        }
        if progress.completed_tasks == progress.total_tasks && progress.total_tasks > 0 {
            let result = collect_completed_task_results(&db, run_id.as_str()).await?;
            db.mark_swarm_run_completed(run_id.as_str(), Some(&result))
                .await?;
            emit_work_swarm_progress(
                &session,
                &turn,
                run_id.as_str(),
                TurnWorkSwarmProgressStatus::Succeeded,
                None,
                None,
            )
            .await;
            return Ok(());
        }

        let mut spawned_any = false;
        let mut running = progress.running_tasks;
        let concurrency_limit = usize::try_from(run.max_concurrency.max(1)).unwrap_or(1);
        while running < concurrency_limit {
            let Some(task) = db
                .claim_ready_swarm_task(run_id.as_str(), lease_owner.as_str(), lease_seconds)
                .await?
            else {
                break;
            };
            spawned_any = true;
            match spawn_claimed_task(
                &session,
                &turn,
                &db,
                &run,
                &execution_spec,
                &task,
                lease_owner.as_str(),
                lease_seconds,
            )
            .await
            {
                Ok(()) => {
                    running += 1;
                }
                Err(ClaimedTaskFailure::CapacityDeferred) => {
                    break;
                }
                Err(ClaimedTaskFailure::Objective(error)) => {
                    handle_objective_attempt_failure(&session, &db, &task, error.as_str()).await?;
                    emit_work_swarm_progress(
                        &session,
                        &turn,
                        run_id.as_str(),
                        TurnWorkSwarmProgressStatus::Running,
                        Some(task.id.as_str()),
                        Some(error.as_str()),
                    )
                    .await;
                }
                Err(ClaimedTaskFailure::Retryable(error)) => {
                    handle_retryable_attempt_failure(&session, &db, &task, error.as_str()).await?;
                    emit_work_swarm_progress(
                        &session,
                        &turn,
                        run_id.as_str(),
                        TurnWorkSwarmProgressStatus::Running,
                        Some(task.id.as_str()),
                        Some(error.as_str()),
                    )
                    .await;
                }
                Err(ClaimedTaskFailure::Fatal(error)) => return Err(error),
            }
        }

        let progress = db.get_swarm_run_progress(run_id.as_str()).await?;
        if progress.failed_tasks > 0 && progress.running_tasks == 0 && !spawned_any {
            fail_run(
                &session,
                &turn,
                &db,
                run_id.as_str(),
                "Work Swarm cannot make further progress because one or more tasks failed",
            )
            .await?;
            return Ok(());
        }
        emit_work_swarm_progress(
            &session,
            &turn,
            run_id.as_str(),
            TurnWorkSwarmProgressStatus::Running,
            None,
            None,
        )
        .await;
        sleep(RUNNER_POLL_INTERVAL).await;
    }
}

async fn collect_completed_task_results(
    db: &crate::StateDbHandle,
    run_id: &str,
) -> anyhow::Result<Value> {
    let (_, tasks, _, _, _) = db.load_swarm_recovery_data(run_id).await?;
    let results = tasks
        .into_iter()
        .map(|task| {
            (
                local_task_id(run_id, task.id.as_str()).to_string(),
                task.result_json.unwrap_or(Value::Null),
            )
        })
        .collect::<BTreeMap<_, _>>();
    Ok(json!({ "tasks": results }))
}

async fn spawn_claimed_task(
    session: &Session,
    turn: &TurnContext,
    db: &crate::StateDbHandle,
    run: &SwarmRun,
    execution_spec: &ExecutionSpec,
    task: &SwarmTask,
    lease_owner: &str,
    lease_seconds: i64,
) -> Result<(), ClaimedTaskFailure> {
    let local_id = local_task_id(run.id.as_str(), task.id.as_str());
    let task_spec = execution_spec
        .tasks
        .iter()
        .find(|candidate| candidate.id.0 == local_id)
        .ok_or_else(|| {
            ClaimedTaskFailure::Fatal(anyhow::anyhow!(
                "task {local_id} is missing from persisted execution spec"
            ))
        })?;
    let dependency_results = load_dependency_results(db, task).await?;
    let mut child_config = build_agent_spawn_config(&session.get_base_instructions().await, turn)
        .map_err(|err| ClaimedTaskFailure::Objective(format!("{err:?}")))?;
    let route = if let Some(persisted_route) = task.model_route_json.clone() {
        serde_json::from_value::<ResolvedSpawnAgentModelRoute>(persisted_route).map_err(|err| {
            ClaimedTaskFailure::Fatal(anyhow::anyhow!(
                "persisted model route for task {} is invalid: {err}",
                task.id
            ))
        })?
    } else {
        apply_spawn_agent_model_route(
            session,
            turn,
            &mut child_config,
            task.agent_type.as_deref(),
            task_spec.explicit_model.as_deref(),
            task_spec
                .explicit_reasoning
                .as_deref()
                .and_then(|value| value.parse::<ReasoningEffort>().ok()),
            None,
        )
        .await
        .map_err(|err| ClaimedTaskFailure::Objective(format!("{err:?}")))?
    };
    let selected_index = usize::try_from(task.candidate_index.unwrap_or(0).max(0))
        .unwrap_or(0)
        .min(route.candidates.len().saturating_sub(1));
    let selected = route
        .candidates
        .get(selected_index)
        .cloned()
        .ok_or_else(|| {
            ClaimedTaskFailure::Fatal(anyhow::anyhow!(
                "persisted model route contains no compatible candidates"
            ))
        })?;
    apply_candidate(&mut child_config, &selected);
    apply_spawn_agent_role(session, &mut child_config, task.agent_type.as_deref())
        .await
        .map_err(|err| ClaimedTaskFailure::Objective(format!("{err:?}")))?;
    let selected_model = child_config.model.as_deref().ok_or_else(|| {
        ClaimedTaskFailure::Objective(
            "the resolved Work Swarm candidate does not select a model".to_string(),
        )
    })?;
    let available_models = session
        .services
        .models_manager
        .list_models(RefreshStrategy::Offline)
        .await;
    find_spawn_agent_model_name(&available_models, selected_model, turn.multi_agent_version)
        .map_err(|err| ClaimedTaskFailure::Objective(format!("{err:?}")))?;
    apply_spawn_agent_service_tier(
        session,
        &mut child_config,
        turn.config.service_tier.as_deref(),
        selected.service_tier.as_deref(),
    )
    .await
    .map_err(|err| ClaimedTaskFailure::Objective(format!("{err:?}")))?;
    apply_spawn_agent_runtime_overrides(&mut child_config, turn)
        .map_err(|err| ClaimedTaskFailure::Objective(format!("{err:?}")))?;

    let fallback_reason = if selected_index > 0 {
        Some(
            task.fallback_reason
                .as_deref()
                .unwrap_or("previous objective failure"),
        )
    } else {
        route.fallback_reason.as_deref()
    };
    let route_json = route_to_json(&route)?;
    let selected_json = json!({
        "model": child_config.model,
        "reasoning_effort": child_config.model_reasoning_effort.as_ref().map(ToString::to_string),
        "service_tier": child_config.service_tier,
    });
    if !db
        .update_swarm_task_routing(
            task.id.as_str(),
            lease_owner,
            &selected_json,
            &route_json,
            selected_index as i64,
            fallback_reason,
        )
        .await?
    {
        return Err(ClaimedTaskFailure::Retryable(
            "task lease was lost while selecting a model".to_string(),
        ));
    }

    let attempt_id = attempt_id(task);
    let pending_thread_id = format!("pending:{}", task.id);
    db.record_swarm_attempt_start(
        attempt_id.as_str(),
        run.id.as_str(),
        task.id.as_str(),
        pending_thread_id.as_str(),
        Some(lease_owner),
        Some(lease_seconds),
        Some(&selected_json),
        fallback_reason,
    )
    .await?;

    let worker_prompt = build_worker_prompt(run, task, task_spec, &dependency_results)?;
    let spawn_source = thread_spawn_source(
        session.thread_id,
        &turn.session_source,
        next_thread_spawn_depth(&turn.session_source),
        task.agent_type.as_deref(),
        Some(format!(
            "{WORK_SWARM_AGENT_NAME_PREFIX}{}",
            Uuid::now_v7().simple()
        )),
    )
    .map_err(|err| ClaimedTaskFailure::Fatal(anyhow::anyhow!("{err:?}")))?;
    let worker_agent_path = spawn_source.get_agent_path().ok_or_else(|| {
        ClaimedTaskFailure::Fatal(anyhow::anyhow!(
            "Work Swarm worker is missing a canonical agent path"
        ))
    })?;
    let spawned = match session
        .services
        .agent_control
        .spawn_agent_with_metadata(
            child_config,
            vec![codex_protocol::user_input::UserInput::Text {
                text: worker_prompt,
                text_elements: Vec::new(),
            }],
            Some(spawn_source),
            SpawnAgentOptions {
                parent_thread_id: Some(session.thread_id),
                environments: Some(turn.environments.to_selections()),
                ..Default::default()
            },
        )
        .await
    {
        Ok(spawned) => spawned,
        Err(CodexErr::AgentLimitReached { .. }) => {
            if !db
                .defer_swarm_task_for_capacity(task.id.as_str(), lease_owner, attempt_id.as_str())
                .await?
            {
                return Err(ClaimedTaskFailure::Fatal(anyhow::anyhow!(
                    "task lease was lost while deferring worker admission"
                )));
            }
            return Err(ClaimedTaskFailure::CapacityDeferred);
        }
        Err(err) => return Err(ClaimedTaskFailure::Retryable(err.to_string())),
    };
    let thread_id = spawned.thread_id;
    if !db
        .bind_swarm_attempt_thread(
            attempt_id.as_str(),
            lease_owner,
            thread_id.to_string().as_str(),
        )
        .await?
        || !db
            .bind_swarm_task_thread(
                task.id.as_str(),
                lease_owner,
                thread_id.to_string().as_str(),
            )
            .await?
    {
        let _ = session
            .services
            .agent_control
            .shutdown_live_agent(thread_id)
            .await;
        return Err(ClaimedTaskFailure::Retryable(
            "task lease was lost while binding its worker".to_string(),
        ));
    }

    emit_sub_agent_activity(
        session,
        turn,
        SubAgentActivityItem {
            id: format!("work_swarm:{}:{}", run.id, task.id),
            agent_thread_id: thread_id,
            agent_path: worker_agent_path,
            task_title: Some(task_spec.task_title.clone()),
            kind: SubAgentActivityKind::Started,
        },
    )
    .await;
    emit_work_swarm_progress(
        session,
        turn,
        run.id.as_str(),
        TurnWorkSwarmProgressStatus::Running,
        Some(task.id.as_str()),
        None,
    )
    .await;
    Ok(())
}

fn apply_candidate(config: &mut crate::config::Config, candidate: &SpawnAgentModelCandidate) {
    config.model = Some(candidate.model.clone());
    config.model_reasoning_effort = candidate.reasoning_effort.clone();
    config.service_tier = candidate.service_tier.clone();
}

fn route_to_json(route: &ResolvedSpawnAgentModelRoute) -> anyhow::Result<Value> {
    Ok(serde_json::to_value(route)?)
}

async fn load_dependency_results(
    db: &crate::StateDbHandle,
    task: &SwarmTask,
) -> anyhow::Result<BTreeMap<String, Value>> {
    let mut results = BTreeMap::new();
    for dependency_id in &task.depends_on_task_ids {
        let dependency = db
            .get_swarm_task(dependency_id.as_str())
            .await?
            .ok_or_else(|| anyhow::anyhow!("dependency {dependency_id} is missing"))?;
        if dependency.status != SwarmTaskStatus::Completed {
            return Err(anyhow::anyhow!(
                "dependency {dependency_id} is not completed"
            ));
        }
        results.insert(
            local_task_id(task.run_id.as_str(), dependency.id.as_str()).to_string(),
            dependency.result_json.unwrap_or(Value::Null),
        );
    }
    Ok(results)
}

fn build_worker_prompt(
    run: &SwarmRun,
    task: &SwarmTask,
    task_spec: &TaskSpec,
    dependency_results: &BTreeMap<String, Value>,
) -> anyhow::Result<String> {
    let dependencies = serde_json::to_string_pretty(dependency_results)?;
    Ok(format!(
        "You are a narrow specialist in an Empatra Work Swarm. The root orchestrator has already planned the work. Follow this packet exactly.\n\
Run ID: {}\n\
Task ID: {}\n\
Task title: {}\n\
Task kind: {}\n\
Agent type: {}\n\
Run title: {}\n\
\n\
Dependency results (authoritative JSON):\n{}\n\
\n\
Task instructions:\n{}\n\
\n\
Completion contract:\n\
- When you communicate with the orchestrator or another specialist, include run_id {} and a concise topic so the coordination is visible to the user.\n\
- Call report_work_swarm_result exactly once.\n\
- Pass run_id {}, task_id {}, and result_token {} unchanged.\n\
- Return a JSON object in result.\n\
- Use status succeeded for a valid result.\n\
- Use retryable only for a concrete transient execution failure.\n\
- Use escalated only for an objective model/provider/timeout/schema failure that warrants the next configured model.\n\
- Do not use confidence or task difficulty as an escalation signal.\n\
- Stop after reporting.",
        run.id,
        task.id,
        task_spec.task_title,
        task.task_kind,
        task.agent_type.as_deref().unwrap_or(DEFAULT_ROLE_NAME),
        run.title.as_deref().unwrap_or_default(),
        dependencies,
        task_spec.instructions,
        run.id,
        run.id,
        task.id,
        task.result_token,
    ))
}

async fn reconcile_running_tasks(
    session: &Session,
    turn: &TurnContext,
    db: &crate::StateDbHandle,
    run: &SwarmRun,
    lease_owner: &str,
    lease_seconds: i64,
) -> anyhow::Result<()> {
    let (_, tasks, _, _, _) = db.load_swarm_recovery_data(run.id.as_str()).await?;
    for task in tasks
        .into_iter()
        .filter(|task| task.status == SwarmTaskStatus::Running)
    {
        let now = Utc::now();
        if task.deadline_at.is_some_and(|deadline| deadline <= now) {
            terminate_active_attempt(session, db, &task, "task deadline exceeded", true).await?;
            emit_work_swarm_progress(
                session,
                turn,
                run.id.as_str(),
                TurnWorkSwarmProgressStatus::Running,
                Some(task.id.as_str()),
                Some("task deadline exceeded"),
            )
            .await;
            continue;
        }
        let attempt = db.get_swarm_attempt(attempt_id(&task).as_str()).await?;
        if let (Some(timeout), Some(started_at)) = (
            route_timeout_seconds(&task),
            attempt.as_ref().and_then(|attempt| attempt.started_at),
        ) && now.signed_duration_since(started_at).num_seconds().max(0) as u64 >= timeout
        {
            terminate_active_attempt(session, db, &task, "worker attempt timed out", true).await?;
            emit_work_swarm_progress(
                session,
                turn,
                run.id.as_str(),
                TurnWorkSwarmProgressStatus::Running,
                Some(task.id.as_str()),
                Some("worker attempt timed out"),
            )
            .await;
            continue;
        }

        let Some(thread_id) = task
            .assigned_thread_id
            .as_deref()
            .and_then(|value| ThreadId::from_string(value).ok())
        else {
            continue;
        };
        let status = session.services.agent_control.get_status(thread_id).await;
        if is_final(&status) {
            let error = format!("worker ended without a result report: {status:?}");
            terminate_active_attempt(session, db, &task, error.as_str(), true).await?;
        } else {
            db.renew_swarm_task_lease(task.id.as_str(), lease_owner, lease_seconds)
                .await?;
        }
    }
    db.expire_swarm_leases().await?;
    Ok(())
}

fn route_timeout_seconds(task: &SwarmTask) -> Option<u64> {
    task.model_route_json
        .as_ref()
        .and_then(|route| route.get("timeout_seconds"))
        .and_then(Value::as_u64)
}

async fn terminate_active_attempt(
    session: &Session,
    db: &crate::StateDbHandle,
    task: &SwarmTask,
    error: &str,
    objective_failure: bool,
) -> anyhow::Result<()> {
    let thread_id = task
        .assigned_thread_id
        .as_deref()
        .and_then(|value| ThreadId::from_string(value).ok());
    let tokens = if let Some(thread_id) = thread_id {
        session
            .services
            .agent_control
            .child_token_usage(thread_id)
            .await
    } else {
        0
    };
    record_failed_attempt(db, task, error, tokens).await?;
    if let Some(thread_id) = thread_id {
        let _ = session
            .services
            .agent_control
            .shutdown_live_agent(thread_id)
            .await;
    }
    if objective_failure && task.attempt_count < task.max_attempts {
        db.escalate_swarm_task(task.id.as_str(), Some(error))
            .await?;
    } else {
        db.fail_swarm_task(task.id.as_str(), error).await?;
    }
    Ok(())
}

async fn handle_objective_attempt_failure(
    session: &Session,
    db: &crate::StateDbHandle,
    task: &SwarmTask,
    error: &str,
) -> anyhow::Result<()> {
    record_failed_attempt(db, task, error, 0).await?;
    if task.attempt_count < task.max_attempts {
        db.escalate_swarm_task(task.id.as_str(), Some(error))
            .await?;
    } else {
        db.fail_swarm_task(task.id.as_str(), error).await?;
    }
    shutdown_failed_task_child(session, task).await;
    Ok(())
}

async fn handle_retryable_attempt_failure(
    session: &Session,
    db: &crate::StateDbHandle,
    task: &SwarmTask,
    error: &str,
) -> anyhow::Result<()> {
    record_failed_attempt(db, task, error, 0).await?;
    if task.attempt_count < task.max_attempts {
        db.retry_swarm_task(task.id.as_str(), Some(error)).await?;
    } else {
        db.fail_swarm_task(task.id.as_str(), error).await?;
    }
    shutdown_failed_task_child(session, task).await;
    Ok(())
}

async fn record_failed_attempt(
    db: &crate::StateDbHandle,
    task: &SwarmTask,
    error: &str,
    tokens: i64,
) -> anyhow::Result<()> {
    let attempt_id = attempt_id(task);
    if db.get_swarm_attempt(attempt_id.as_str()).await?.is_none() {
        let unstarted_thread_id = format!("unstarted:{}", task.id);
        db.record_swarm_attempt_start(
            attempt_id.as_str(),
            task.run_id.as_str(),
            task.id.as_str(),
            unstarted_thread_id.as_str(),
            task.lease_owner.as_deref(),
            None,
            task.model_candidate_json.as_ref(),
            task.fallback_reason.as_deref(),
        )
        .await?;
    }
    let _ = db
        .record_swarm_attempt_end(
            attempt_id.as_str(),
            SwarmAttemptStatus::Failed,
            tokens,
            0,
            None,
            Some(error),
        )
        .await?;
    Ok(())
}

async fn shutdown_failed_task_child(session: &Session, task: &SwarmTask) {
    if let Some(thread_id) = task
        .assigned_thread_id
        .as_deref()
        .and_then(|value| ThreadId::from_string(value).ok())
    {
        let _ = session
            .services
            .agent_control
            .shutdown_live_agent(thread_id)
            .await;
    }
}

fn attempt_id(task: &SwarmTask) -> String {
    format!("{}:attempt:{}", task.id, task.attempt_count)
}

async fn run_limit_failure(
    session: &Session,
    db: &crate::StateDbHandle,
    run: &SwarmRun,
) -> anyhow::Result<Option<String>> {
    let now = Utc::now();
    if run.deadline_at.is_some_and(|deadline| deadline <= now) {
        return Ok(Some("Work Swarm deadline exceeded".to_string()));
    }
    if let (Some(limit), Some(started_at)) = (run.runtime_budget_seconds, run.started_at)
        && now.signed_duration_since(started_at).num_seconds() >= limit
    {
        return Ok(Some("Work Swarm runtime budget exhausted".to_string()));
    }
    if let Some(limit) = run.token_budget {
        let (_, tasks, _, _, _) = db.load_swarm_recovery_data(run.id.as_str()).await?;
        let mut live_tokens = 0_i64;
        for task in tasks
            .iter()
            .filter(|task| task.status == SwarmTaskStatus::Running)
        {
            if let Some(thread_id) = task
                .assigned_thread_id
                .as_deref()
                .and_then(|value| ThreadId::from_string(value).ok())
            {
                live_tokens = live_tokens.saturating_add(
                    session
                        .services
                        .agent_control
                        .child_token_usage(thread_id)
                        .await,
                );
            }
        }
        return Ok(token_budget_failure(limit, run.token_usage, live_tokens));
    }
    Ok(None)
}

fn token_budget_failure(limit: i64, persisted_tokens: i64, live_tokens: i64) -> Option<String> {
    let observed_tokens = persisted_tokens.saturating_add(live_tokens);
    (observed_tokens >= limit).then(|| {
        format!(
            "Work Swarm token budget exhausted: observed {observed_tokens} tokens against a hard \
             limit of {limit}; usage includes active workers' full input context and output"
        )
    })
}

async fn fail_run(
    session: &Session,
    turn: &TurnContext,
    db: &crate::StateDbHandle,
    run_id: &str,
    reason: &str,
) -> anyhow::Result<()> {
    let (_, tasks, _, _, _) = db.load_swarm_recovery_data(run_id).await?;
    let mut active_attempt_token_usage = BTreeMap::new();
    let mut worker_thread_ids = Vec::new();
    for task in tasks
        .iter()
        .filter(|task| task.status == SwarmTaskStatus::Running)
    {
        let Some(worker_thread_id) = task.assigned_thread_id.as_deref() else {
            continue;
        };
        worker_thread_ids.push(worker_thread_id.to_string());
        let Some(thread_id) = ThreadId::from_string(worker_thread_id).ok() else {
            continue;
        };
        active_attempt_token_usage.insert(
            attempt_id(task),
            session
                .services
                .agent_control
                .child_token_usage(thread_id)
                .await
                .max(0),
        );
    }
    db.mark_swarm_run_failed(run_id, reason, &active_attempt_token_usage)
        .await?;
    shutdown_work_swarm_worker_threads(session, worker_thread_ids).await;
    emit_work_swarm_progress(
        session,
        turn,
        run_id,
        TurnWorkSwarmProgressStatus::Failed,
        None,
        Some(reason),
    )
    .await;
    Ok(())
}

fn bounded_work_swarm_preview(content: &str) -> String {
    const MAX_CHARS: usize = 1_024;
    let normalized = content.trim();
    let mut characters = normalized.chars();
    let mut preview = characters.by_ref().take(MAX_CHARS).collect::<String>();
    if characters.next().is_some() {
        preview.push('…');
    }
    preview
}

const WORK_SWARM_VISIBLE_MESSAGE_LIMIT: usize = 200;

async fn emit_work_swarm_progress(
    session: &Session,
    turn: &TurnContext,
    run_id: &str,
    _status: TurnWorkSwarmProgressStatus,
    task_id: Option<&str>,
    error: Option<&str>,
) {
    let Some(db) = session.state_db() else {
        return;
    };
    let Ok((Some(run), tasks, _, messages, _)) = db.load_swarm_recovery_data(run_id).await else {
        return;
    };
    let Ok(attempt_thread_ids) = db
        .list_latest_swarm_attempt_thread_ids_by_task(run_id)
        .await
    else {
        return;
    };
    let task = task_id
        .and_then(|task_id| tasks.iter().find(|task| task.id == task_id))
        .cloned();
    let model = task
        .as_ref()
        .and_then(|task| task.model_candidate_json.as_ref())
        .and_then(|candidate| candidate.get("model"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let agent_path = if let Some(thread_id) = task
        .as_ref()
        .and_then(|task| work_swarm_task_agent_thread_id(task, &attempt_thread_ids))
        .and_then(|value| ThreadId::from_string(value).ok())
    {
        session
            .services
            .agent_control
            .get_agent_metadata(thread_id)
            .and_then(|metadata| metadata.agent_path)
            .map(|path| path.to_string())
    } else {
        None
    };
    let status = match run.status {
        SwarmRunStatus::Pending => TurnWorkSwarmProgressStatus::Pending,
        SwarmRunStatus::Running => TurnWorkSwarmProgressStatus::Running,
        SwarmRunStatus::Completed => TurnWorkSwarmProgressStatus::Succeeded,
        SwarmRunStatus::Failed => TurnWorkSwarmProgressStatus::Failed,
        SwarmRunStatus::Cancelled => TurnWorkSwarmProgressStatus::Cancelled,
    };
    let task_titles = run
        .spec_json
        .as_ref()
        .and_then(|spec| serde_json::from_value::<ExecutionSpec>(spec.clone()).ok())
        .map(|spec| {
            spec.tasks
                .into_iter()
                .map(|task| (task.id.0, task.task_title))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let queued = tasks
        .iter()
        .filter(|task| {
            matches!(
                task.status,
                SwarmTaskStatus::Pending
                    | SwarmTaskStatus::Ready
                    | SwarmTaskStatus::Retryable
                    | SwarmTaskStatus::Escalated
            )
        })
        .count() as i64;
    let running = tasks
        .iter()
        .filter(|task| task.status == SwarmTaskStatus::Running)
        .count() as i64;
    let succeeded = tasks
        .iter()
        .filter(|task| task.status == SwarmTaskStatus::Completed)
        .count() as i64;
    let failed = tasks
        .iter()
        .filter(|task| task.status == SwarmTaskStatus::Failed)
        .count() as i64;
    let cancelled = tasks
        .iter()
        .filter(|task| task.status == SwarmTaskStatus::Cancelled)
        .count() as i64;
    let task_snapshots = tasks
        .iter()
        .map(|task| {
            let agent_thread_id = work_swarm_task_agent_thread_id(task, &attempt_thread_ids);
            let agent_path = agent_thread_id
                .and_then(|value| ThreadId::from_string(value).ok())
                .and_then(|thread_id| session.services.agent_control.get_agent_metadata(thread_id))
                .and_then(|metadata| metadata.agent_path)
                .map(|path| path.to_string());
            TurnWorkSwarmTaskProgress {
                id: local_task_id(run_id, task.id.as_str()).to_string(),
                task_title: task_titles
                    .get(local_task_id(run_id, task.id.as_str()))
                    .cloned(),
                kind: match task.task_kind.as_str() {
                    "reducer" => TurnWorkSwarmTaskKind::Reducer,
                    "reviewer" => TurnWorkSwarmTaskKind::Reviewer,
                    _ => TurnWorkSwarmTaskKind::Worker,
                },
                specialist: task.agent_type.clone(),
                status: match task.status {
                    SwarmTaskStatus::Pending => TurnWorkSwarmTaskStatus::Pending,
                    SwarmTaskStatus::Ready => TurnWorkSwarmTaskStatus::Ready,
                    SwarmTaskStatus::Running => TurnWorkSwarmTaskStatus::Running,
                    SwarmTaskStatus::Completed => TurnWorkSwarmTaskStatus::Succeeded,
                    SwarmTaskStatus::Failed => TurnWorkSwarmTaskStatus::Failed,
                    SwarmTaskStatus::Retryable => TurnWorkSwarmTaskStatus::Retryable,
                    SwarmTaskStatus::Escalated => TurnWorkSwarmTaskStatus::Escalated,
                    SwarmTaskStatus::Cancelled => TurnWorkSwarmTaskStatus::Cancelled,
                },
                summary: task.instructions.as_deref().map(bounded_work_swarm_preview),
                depends_on: task
                    .depends_on_task_ids
                    .iter()
                    .map(|dependency_id| local_task_id(run_id, dependency_id).to_string())
                    .collect(),
                attempt: task.attempt_count,
                max_attempts: task.max_attempts,
                tokens_used: task.token_usage,
                model: task
                    .model_candidate_json
                    .as_ref()
                    .and_then(|candidate| candidate.get("model"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                fallback_reason: task.fallback_reason.clone(),
                agent_path,
                agent_thread_id: agent_thread_id.map(str::to_string),
                error: task.last_error.clone(),
            }
        })
        .collect();
    let communication_messages = messages
        .iter()
        .rev()
        .take(WORK_SWARM_VISIBLE_MESSAGE_LIMIT)
        .rev()
        .filter_map(|message| {
            let sender_agent_path = message
                .metadata_json
                .get("author")
                .and_then(Value::as_str)?
                .to_string();
            let recipient_agent_path = message
                .metadata_json
                .get("recipient")
                .and_then(Value::as_str)?
                .to_string();
            let preview = message
                .metadata_json
                .get("message_preview")
                .and_then(Value::as_str)
                .map(str::to_string);
            let content_redacted = message
                .metadata_json
                .get("message_content_redacted")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let kind = match message
                .metadata_json
                .get("communication_kind")
                .and_then(Value::as_str)
                .unwrap_or("message")
            {
                "spawn" => TurnWorkSwarmCommunicationKind::Spawn,
                "followup" => TurnWorkSwarmCommunicationKind::Followup,
                "result" => TurnWorkSwarmCommunicationKind::Result,
                _ => TurnWorkSwarmCommunicationKind::Message,
            };
            let status = match message.status {
                InterAgentMessageStatus::Queued => TurnWorkSwarmCommunicationStatus::Queued,
                InterAgentMessageStatus::Delivered => TurnWorkSwarmCommunicationStatus::Delivered,
                InterAgentMessageStatus::Acked => TurnWorkSwarmCommunicationStatus::Acked,
                InterAgentMessageStatus::Expired => TurnWorkSwarmCommunicationStatus::Expired,
                InterAgentMessageStatus::DeadLettered => {
                    TurnWorkSwarmCommunicationStatus::DeadLettered
                }
                InterAgentMessageStatus::Cancelled => TurnWorkSwarmCommunicationStatus::Cancelled,
            };
            let sender_task_id = tasks
                .iter()
                .find(|task| {
                    work_swarm_task_agent_thread_id(task, &attempt_thread_ids)
                        == Some(message.sender_thread_id.as_str())
                })
                .map(|task| local_task_id(run_id, task.id.as_str()).to_string());
            let recipient_task_id = message.target_thread_id.as_deref().and_then(|thread_id| {
                tasks
                    .iter()
                    .find(|task| {
                        work_swarm_task_agent_thread_id(task, &attempt_thread_ids)
                            == Some(thread_id)
                    })
                    .map(|task| local_task_id(run_id, task.id.as_str()).to_string())
            });
            Some(TurnWorkSwarmCommunicationMessage {
                id: message.id.clone(),
                sender_agent_path,
                recipient_agent_path,
                sender_task_id,
                recipient_task_id,
                kind,
                topic: message.topic.clone(),
                status,
                preview,
                content_redacted,
                created_at_ms: message.created_at.timestamp_millis(),
                updated_at_ms: message.updated_at.timestamp_millis(),
                correlation_id: message.correlation_id.clone(),
                in_reply_to: message.in_reply_to.clone(),
            })
        })
        .collect();
    let communication = TurnWorkSwarmCommunicationProgress {
        queued: messages
            .iter()
            .filter(|message| message.status == InterAgentMessageStatus::Queued)
            .count() as i64,
        delivered: messages
            .iter()
            .filter(|message| message.status == InterAgentMessageStatus::Delivered)
            .count() as i64,
        acked: messages
            .iter()
            .filter(|message| message.status == InterAgentMessageStatus::Acked)
            .count() as i64,
        expired: messages
            .iter()
            .filter(|message| message.status == InterAgentMessageStatus::Expired)
            .count() as i64,
        dead_lettered: messages
            .iter()
            .filter(|message| message.status == InterAgentMessageStatus::DeadLettered)
            .count() as i64,
        cancelled: messages
            .iter()
            .filter(|message| message.status == InterAgentMessageStatus::Cancelled)
            .count() as i64,
        messages: communication_messages,
        messages_truncated: messages.len() > WORK_SWARM_VISIBLE_MESSAGE_LIMIT,
    };
    let is_terminal = matches!(
        status,
        TurnWorkSwarmProgressStatus::Succeeded
            | TurnWorkSwarmProgressStatus::Failed
            | TurnWorkSwarmProgressStatus::Cancelled
    );
    let item = TurnItem::WorkSwarmActivity(WorkSwarmActivityItem {
        id: format!("work_swarm:{run_id}"),
        progress: WorkSwarmProgress {
            run_id: run_id.to_string(),
            status,
            title: run.title,
            max_concurrency: run.max_concurrency,
            runtime_used_seconds: run.runtime_usage_seconds,
            runtime_budget_seconds: run.runtime_budget_seconds,
            total: tasks.len() as i64,
            queued,
            running,
            succeeded,
            failed,
            cancelled,
            skipped: 0,
            tokens_used: run.token_usage,
            token_budget: run.token_budget,
            task_id: task
                .as_ref()
                .map(|task| local_task_id(run_id, task.id.as_str()).to_string()),
            agent_path,
            model,
            fallback_reason: task
                .as_ref()
                .and_then(|task| task.fallback_reason.clone())
                .or(run.fallback_reason),
            error: error
                .map(str::to_string)
                .or_else(|| task.and_then(|task| task.last_error)),
            tasks: task_snapshots,
            communication,
        },
    });
    if is_terminal {
        session.emit_turn_item_completed(turn, item).await;
    } else {
        session.emit_turn_item_started(turn, &item).await;
    }
}

fn work_swarm_task_agent_thread_id<'a>(
    task: &'a SwarmTask,
    attempt_thread_ids: &'a BTreeMap<String, String>,
) -> Option<&'a str> {
    task.assigned_thread_id
        .as_deref()
        .filter(|thread_id| is_displayable_swarm_attempt_thread_id(thread_id))
        .or_else(|| {
            attempt_thread_ids
                .get(task.id.as_str())
                .map(String::as_str)
                .filter(|thread_id| is_displayable_swarm_attempt_thread_id(thread_id))
        })
}

fn is_displayable_swarm_attempt_thread_id(thread_id: &str) -> bool {
    !thread_id.starts_with("pending:") && !thread_id.starts_with("unstarted:")
}

async fn shutdown_running_work_swarm_children(session: &Session, run_id: &str) {
    let Some(db) = session.state_db() else {
        return;
    };
    let Ok((_, tasks, _, _, _)) = db.load_swarm_recovery_data(run_id).await else {
        return;
    };
    shutdown_work_swarm_worker_threads(
        session,
        tasks
            .into_iter()
            .filter_map(|task| task.assigned_thread_id)
            .collect(),
    )
    .await;
}

async fn shutdown_work_swarm_worker_threads(session: &Session, worker_thread_ids: Vec<String>) {
    for worker_thread_id in worker_thread_ids {
        if let Ok(thread_id) = ThreadId::from_string(worker_thread_id.as_str()) {
            let _ = session
                .services
                .agent_control
                .shutdown_live_agent(thread_id)
                .await;
        }
    }
}

async fn load_execution_spec(
    db: &crate::StateDbHandle,
    run_id: &str,
) -> anyhow::Result<ExecutionSpec> {
    let run = db
        .get_swarm_run(run_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("work swarm run {run_id} not found"))?;
    serde_json::from_value(
        run.spec_json
            .ok_or_else(|| anyhow::anyhow!("work swarm run {run_id} has no execution spec"))?,
    )
    .map_err(Into::into)
}

async fn owned_swarm_run(
    db: &crate::StateDbHandle,
    session: &Session,
    run_id: &str,
) -> Result<SwarmRun, FunctionCallError> {
    let run = db
        .get_swarm_run(run_id)
        .await
        .map_err(runtime_error)?
        .ok_or_else(|| {
            FunctionCallError::RespondToModel(format!("work swarm run {run_id} not found"))
        })?;
    if run.thread_id != session.thread_id.to_string() {
        return Err(FunctionCallError::RespondToModel(
            "work swarm run belongs to another orchestrator thread".to_string(),
        ));
    }
    Ok(run)
}

fn required_state_db(session: &Session) -> Result<crate::StateDbHandle, FunctionCallError> {
    session.state_db().ok_or_else(|| {
        FunctionCallError::Fatal("sqlite state db is unavailable for this session".to_string())
    })
}

fn execution_spec_error(err: ExecutionSpecError) -> FunctionCallError {
    FunctionCallError::RespondToModel(err.to_string())
}

fn runtime_error(err: anyhow::Error) -> FunctionCallError {
    FunctionCallError::RespondToModel(err.to_string())
}

fn json_error(err: serde_json::Error) -> FunctionCallError {
    FunctionCallError::RespondToModel(err.to_string())
}

fn saturating_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

macro_rules! impl_tool_output {
    ($output:ty, $tool_name:literal) => {
        impl ToolOutput for $output {
            fn log_preview(&self) -> String {
                tool_output_json_text(self, $tool_name)
            }

            fn success_for_logging(&self) -> bool {
                true
            }

            fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
                tool_output_response_item(call_id, payload, self, Some(true), $tool_name)
            }

            fn code_mode_result(&self, _payload: &ToolPayload) -> Value {
                tool_output_code_mode_result(self, $tool_name)
            }
        }
    };
}

impl_tool_output!(StartWorkSwarmResult, "start_work_swarm");
impl_tool_output!(CancelWorkSwarmResult, "cancel_work_swarm");
impl_tool_output!(WorkSwarmStatusResult, "get_work_swarm_status");
impl_tool_output!(WaitWorkSwarmResult, "wait_work_swarm");
impl_tool_output!(ReportWorkSwarmResult, "report_work_swarm_result");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::tests::make_session_and_context;
    use crate::session::turn_context::TurnSkillsContext;
    use codex_core_skills::HostSkillsSnapshot;
    use codex_core_skills::SkillLoadOutcome;
    use codex_protocol::protocol::SkillScope;

    fn work_swarm_skill_metadata() -> SkillMetadata {
        let path = codex_utils_absolute_path::AbsolutePathBuf::try_from(
            std::env::current_dir()
                .expect("current directory")
                .join("work-swarm-SKILL.md"),
        )
        .expect("skill path should be absolute");
        SkillMetadata {
            name: WORK_SWARM_SKILL_NAME.to_string(),
            description: "Orchestrate Work Swarms.".to_string(),
            short_description: None,
            interface: None,
            dependencies: None,
            policy: None,
            path_to_skills_md: path,
            scope: SkillScope::User,
            plugin_id: Some(WORK_SWARM_PLUGIN_ID.to_string()),
        }
    }

    #[tokio::test]
    async fn work_swarm_start_requires_an_observed_skill_read() {
        let (_session, mut turn) = make_session_and_context().await;
        let skill = work_swarm_skill_metadata();
        let mut outcome = SkillLoadOutcome::default();
        outcome.skills = vec![skill.clone()];
        turn.turn_skills = TurnSkillsContext::new(HostSkillsSnapshot::new(Arc::new(outcome)));

        assert!(ensure_work_swarm_skill_read(&turn).await.is_err());

        turn.turn_skills
            .implicit_invocation_seen_skills
            .lock()
            .await
            .insert(implicit_skill_invocation_seen_key(&skill));

        assert!(ensure_work_swarm_skill_read(&turn).await.is_ok());
    }

    #[tokio::test]
    async fn omitted_agent_type_resolves_to_the_default_role() {
        let (_session, turn) = make_session_and_context().await;
        let args: StartWorkSwarmArgs = serde_json::from_value(json!({
            "policy": {
                "max_concurrency": 1
            },
            "tasks": [{
                "id": "synthesis",
                "task_title": "Synthesize findings",
                "kind": "reducer",
                "instructions": "Combine the dependency results."
            }]
        }))
        .expect("request should deserialize without agent_type");

        validate_request_limits(&args, &turn).expect("default role should validate");
        let spec = build_execution_spec(&args, &turn, "run")
            .expect("execution spec should use the default role");

        assert_eq!(spec.tasks[0].agent_type, DEFAULT_ROLE_NAME);
        assert_eq!(spec.tasks[0].kind, TaskKind::Reducer);
    }

    #[test]
    fn token_budget_failure_reports_live_usage_that_crossed_the_limit() {
        assert_eq!(
            token_budget_failure(12_000, 0, 14_598).as_deref(),
            Some(
                "Work Swarm token budget exhausted: observed 14598 tokens against a hard limit of \
                 12000; usage includes active workers' full input context and output"
            )
        );
        assert_eq!(token_budget_failure(12_000, 2_000, 9_999), None);
    }

    #[test]
    fn stored_task_ids_are_run_scoped_and_reversible() {
        let stored = stored_task_id("run", "review");
        assert_eq!(stored, "run:review");
        assert_eq!(local_task_id("run", stored.as_str()), "review");
    }

    #[test]
    fn report_status_rejects_unknown_values() {
        let args = ReportWorkSwarmResultArgs {
            run_id: "run".to_string(),
            task_id: "run:task".to_string(),
            result_token: "token".to_string(),
            result: json!({}),
            status: "confident".to_string(),
            error: None,
        };
        assert!(parse_report_disposition(&args).is_err());
    }

    #[test]
    fn work_swarm_status_serializes_run_and_task_results() {
        let result = WorkSwarmStatusResult {
            run_id: "run".to_string(),
            status: "completed".to_string(),
            total_tasks: 1,
            pending_tasks: 0,
            ready_tasks: 0,
            running_tasks: 0,
            completed_tasks: 1,
            failed_tasks: 0,
            retryable_tasks: 0,
            escalated_tasks: 0,
            cancelled_tasks: 0,
            token_budget: None,
            token_usage: 0,
            runtime_budget_seconds: None,
            runtime_usage_seconds: 0,
            fallback_reason: None,
            last_error: None,
            result: Some(json!({"answer": 42})),
            tasks: vec![WorkSwarmTaskStatusResult {
                task_id: "task".to_string(),
                kind: "reducer".to_string(),
                agent_type: Some("worker".to_string()),
                status: "completed".to_string(),
                attempt_count: 1,
                max_attempts: 1,
                model: Some("model".to_string()),
                reasoning_effort: None,
                service_tier: None,
                fallback_reason: None,
                last_error: None,
                result: Some(json!({"summary": "done"})),
            }],
        };

        let value = serde_json::to_value(result).expect("status should serialize");
        assert_eq!(value["result"]["answer"], 42);
        assert_eq!(value["tasks"][0]["result"]["summary"], "done");
    }

    #[test]
    fn wait_work_swarm_result_flattens_the_terminal_snapshot() {
        let result = WaitWorkSwarmResult {
            wait_outcome: "completed".to_string(),
            run: WorkSwarmStatusResult {
                run_id: "run".to_string(),
                status: "completed".to_string(),
                total_tasks: 0,
                pending_tasks: 0,
                ready_tasks: 0,
                running_tasks: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                retryable_tasks: 0,
                escalated_tasks: 0,
                cancelled_tasks: 0,
                token_budget: None,
                token_usage: 0,
                runtime_budget_seconds: None,
                runtime_usage_seconds: 0,
                fallback_reason: None,
                last_error: None,
                result: Some(json!({"answer": "done"})),
                tasks: Vec::new(),
            },
        };

        let value = serde_json::to_value(result).expect("wait result should serialize");
        assert_eq!(value["wait_outcome"], "completed");
        assert_eq!(value["run_id"], "run");
        assert_eq!(value["status"], "completed");
        assert_eq!(value["result"]["answer"], "done");
        assert!(value.get("run").is_none());
    }

    #[test]
    fn terminal_wait_outcome_covers_every_terminal_run_status() {
        assert_eq!(
            terminal_wait_outcome(SwarmRunStatus::Completed),
            Some("completed")
        );
        assert_eq!(
            terminal_wait_outcome(SwarmRunStatus::Failed),
            Some("failed")
        );
        assert_eq!(
            terminal_wait_outcome(SwarmRunStatus::Cancelled),
            Some("cancelled")
        );
        assert_eq!(terminal_wait_outcome(SwarmRunStatus::Pending), None);
        assert_eq!(terminal_wait_outcome(SwarmRunStatus::Running), None);
    }

    #[tokio::test]
    async fn swarm_wait_wakes_when_the_runner_finishes() {
        let (runner_tx, mut runner_rx) = watch::channel(false);
        let (_activity_tx, mut activity_rx) = watch::channel(InputQueueActivity::Mailbox);
        runner_tx.send_replace(true);

        let wake = wait_for_work_swarm_wake(
            &mut runner_rx,
            &mut activity_rx,
            Some(InputQueueActivity::Mailbox),
            Instant::now() + Duration::from_secs(1),
        )
        .await;

        assert_eq!(wake, WorkSwarmWaitWake::RunnerFinished);
    }

    #[tokio::test]
    async fn swarm_wait_is_interrupted_by_pending_user_steer() {
        let (_runner_tx, mut runner_rx) = watch::channel(false);
        let (_activity_tx, mut activity_rx) = watch::channel(InputQueueActivity::Mailbox);

        let wake = wait_for_work_swarm_wake(
            &mut runner_rx,
            &mut activity_rx,
            Some(InputQueueActivity::Steer),
            Instant::now() + Duration::from_secs(1),
        )
        .await;

        assert_eq!(wake, WorkSwarmWaitWake::Steered);
    }

    #[tokio::test]
    async fn swarm_wait_times_out_without_runner_or_user_activity() {
        let (_runner_tx, mut runner_rx) = watch::channel(false);
        let (_activity_tx, mut activity_rx) = watch::channel(InputQueueActivity::Mailbox);

        let wake =
            wait_for_work_swarm_wake(&mut runner_rx, &mut activity_rx, None, Instant::now()).await;

        assert_eq!(wake, WorkSwarmWaitWake::TimedOut);
    }

    #[test]
    fn work_swarm_workers_are_identified_by_their_canonical_thread_spawn_path() {
        let worker = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: Some(
                codex_protocol::AgentPath::root()
                    .join("work_swarm_019f")
                    .unwrap(),
            ),
            agent_nickname: None,
            agent_role: Some("reviewer".to_string()),
        });
        let ordinary = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 1,
            agent_path: Some(codex_protocol::AgentPath::root().join("reviewer").unwrap()),
            agent_nickname: None,
            agent_role: Some("reviewer".to_string()),
        });

        assert!(is_work_swarm_worker_source(&worker));
        assert!(!is_work_swarm_worker_source(&ordinary));
        assert!(!is_work_swarm_worker_source(&SessionSource::Exec));
    }

    #[test]
    fn task_progress_thread_id_falls_back_to_latest_attempt_thread() {
        let task = test_swarm_task("run:task", SwarmTaskStatus::Completed, None);
        let attempt_thread_ids = BTreeMap::from([(
            "run:task".to_string(),
            "00000000-0000-4000-8000-000000000001".to_string(),
        )]);

        assert_eq!(
            work_swarm_task_agent_thread_id(&task, &attempt_thread_ids),
            Some("00000000-0000-4000-8000-000000000001")
        );
    }

    #[test]
    fn task_progress_thread_id_keeps_live_assignment_while_running() {
        let task = test_swarm_task(
            "run:task",
            SwarmTaskStatus::Running,
            Some("00000000-0000-4000-8000-000000000002"),
        );
        let attempt_thread_ids = BTreeMap::from([(
            "run:task".to_string(),
            "00000000-0000-4000-8000-000000000001".to_string(),
        )]);

        assert_eq!(
            work_swarm_task_agent_thread_id(&task, &attempt_thread_ids),
            Some("00000000-0000-4000-8000-000000000002")
        );
    }

    #[test]
    fn task_progress_thread_id_does_not_surface_provisional_attempt_threads() {
        let task = test_swarm_task(
            "run:task",
            SwarmTaskStatus::Running,
            Some("pending:run:task"),
        );
        let attempt_thread_ids =
            BTreeMap::from([("run:task".to_string(), "unstarted:run:task".to_string())]);

        assert_eq!(
            work_swarm_task_agent_thread_id(&task, &attempt_thread_ids),
            None
        );
    }

    fn test_swarm_task(
        id: &str,
        status: SwarmTaskStatus,
        assigned_thread_id: Option<&str>,
    ) -> SwarmTask {
        let now = Utc::now();
        SwarmTask {
            id: id.to_string(),
            run_id: "run".to_string(),
            thread_id: "thread-root".to_string(),
            assigned_thread_id: assigned_thread_id.map(str::to_string),
            order_index: 0,
            depends_on_task_ids: Vec::new(),
            task_kind: "worker".to_string(),
            agent_type: Some("worker".to_string()),
            instructions: Some("work".to_string()),
            result_token: "result-token".to_string(),
            status,
            model_candidate_json: None,
            model_route_json: None,
            candidate_index: Some(0),
            fallback_reason: None,
            lease_owner: None,
            lease_until: None,
            max_attempts: 3,
            attempt_count: 1,
            token_usage: 0,
            runtime_usage_seconds: 0,
            deadline_at: None,
            created_at: now,
            updated_at: now,
            started_at: Some(now),
            completed_at: None,
            last_error: None,
            result_json: None,
        }
    }
}
