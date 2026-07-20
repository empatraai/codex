use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RunId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TaskId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AttemptId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CellId(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Worker,
    Reducer,
    Reviewer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: TaskId,
    pub kind: TaskKind,
    pub agent_type: String,
    pub instructions: String,
    pub dependencies: Vec<TaskId>,
    pub cell: Option<CellId>,
    pub explicit_model: Option<String>,
    pub explicit_reasoning: Option<String>,
    pub max_attempts: u32,
    pub deadline: Option<SystemTime>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSpec {
    pub run_id: RunId,
    pub policy: ExecutionPolicy,
    pub tasks: Vec<TaskSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    pub max_concurrency: usize,
    pub token_budget: Option<u64>,
    pub max_runtime: Option<Duration>,
    pub deadline: Option<SystemTime>,
    pub fail_fast: bool,
    pub lease_ttl: Duration,
    pub default_max_attempts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionSpecError {
    EmptyRunId,
    EmptyTasks,
    InvalidMaxConcurrency,
    EmptyTaskId { task: String },
    DuplicateTaskId { task: String },
    MissingDependency { task: String, dependency: String },
    CycleDetected { path: Vec<String> },
    EmptyAgentType { task: String },
    EmptyInstructions { task: String },
    InvalidMaxAttempts { task: String },
    InvalidLeaseTtl,
    InvalidDefaultMaxAttempts,
}

impl fmt::Display for ExecutionSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRunId => write!(formatter, "empty run id"),
            Self::EmptyTasks => write!(formatter, "execution spec must contain at least one task"),
            Self::InvalidMaxConcurrency => write!(formatter, "invalid max concurrency"),
            Self::EmptyTaskId { task } => write!(formatter, "empty task id for {task}"),
            Self::DuplicateTaskId { task } => write!(formatter, "duplicate task id {task}"),
            Self::MissingDependency { task, dependency } => {
                write!(formatter, "missing dependency {dependency} for task {task}")
            }
            Self::CycleDetected { path } => {
                write!(formatter, "execution spec cycle detected")?;
                for node in path {
                    write!(formatter, " -> {node}")?;
                }
                Ok(())
            }
            Self::EmptyAgentType { task } => write!(formatter, "empty agent type for {task}"),
            Self::EmptyInstructions { task } => write!(formatter, "empty instructions for {task}"),
            Self::InvalidMaxAttempts { task } => {
                write!(formatter, "invalid max attempts for {task}")
            }
            Self::InvalidLeaseTtl => write!(formatter, "invalid lease ttl"),
            Self::InvalidDefaultMaxAttempts => {
                write!(formatter, "invalid default max attempts")
            }
        }
    }
}

impl ExecutionSpec {
    pub fn validate(&self) -> Result<(), ExecutionSpecError> {
        if self.run_id.0.trim().is_empty() {
            return Err(ExecutionSpecError::EmptyRunId);
        }
        self.policy.validate()?;
        if self.tasks.is_empty() {
            return Err(ExecutionSpecError::EmptyTasks);
        }

        let mut ids = BTreeSet::new();
        for task in &self.tasks {
            validate_task_shape(task)?;
            if !ids.insert(task.id.0.clone()) {
                return Err(ExecutionSpecError::DuplicateTaskId {
                    task: task.id.0.clone(),
                });
            }
        }

        let task_ids = ids;
        for task in &self.tasks {
            for dependency in &task.dependencies {
                if !task_ids.contains(&dependency.0) {
                    return Err(ExecutionSpecError::MissingDependency {
                        task: task.id.0.clone(),
                        dependency: dependency.0.clone(),
                    });
                }
            }
        }

        if let Some(path) = detect_cycle(&self.tasks) {
            return Err(ExecutionSpecError::CycleDetected { path });
        }

        Ok(())
    }

    pub fn ready_tasks(&self, completed: &BTreeSet<TaskId>) -> Vec<TaskId> {
        self.ready_tasks_with_status_map(&BTreeMap::new(), completed)
    }

    pub fn ready_tasks_with_status_map(
        &self,
        task_statuses: &BTreeMap<TaskId, TaskStatus>,
        completed: &BTreeSet<TaskId>,
    ) -> Vec<TaskId> {
        let mut ready = Vec::new();
        for task in &self.tasks {
            if completed.contains(&task.id) {
                continue;
            }
            if task_statuses
                .get(&task.id)
                .is_some_and(|status| !status.is_runnable())
            {
                continue;
            }
            if task
                .dependencies
                .iter()
                .all(|dependency| completed.contains(dependency))
            {
                ready.push(task.id.clone());
            }
        }
        ready
    }
}

impl ExecutionPolicy {
    pub fn validate(&self) -> Result<(), ExecutionSpecError> {
        if self.max_concurrency == 0 {
            return Err(ExecutionSpecError::InvalidMaxConcurrency);
        }
        if self.lease_ttl.is_zero() {
            return Err(ExecutionSpecError::InvalidLeaseTtl);
        }
        if self.default_max_attempts == Some(0) {
            return Err(ExecutionSpecError::InvalidDefaultMaxAttempts);
        }
        Ok(())
    }
}

fn validate_task_shape(task: &TaskSpec) -> Result<(), ExecutionSpecError> {
    if task.id.0.trim().is_empty() {
        return Err(ExecutionSpecError::EmptyTaskId {
            task: "<empty>".to_string(),
        });
    }
    if task.agent_type.trim().is_empty() {
        return Err(ExecutionSpecError::EmptyAgentType {
            task: task.id.0.clone(),
        });
    }
    if task.instructions.trim().is_empty() {
        return Err(ExecutionSpecError::EmptyInstructions {
            task: task.id.0.clone(),
        });
    }
    if task.max_attempts == 0 {
        return Err(ExecutionSpecError::InvalidMaxAttempts {
            task: task.id.0.clone(),
        });
    }
    Ok(())
}

fn detect_cycle(tasks: &[TaskSpec]) -> Option<Vec<String>> {
    let by_id: BTreeMap<_, _> = tasks.iter().map(|task| (task.id.0.clone(), task)).collect();
    let mut marks = BTreeMap::<String, Visit>::new();
    let mut stack = Vec::new();

    for task in tasks {
        if let Some(path) = visit(task, &by_id, &mut marks, &mut stack) {
            return Some(path);
        }
    }
    None
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Visit {
    Visiting,
    Done,
}

fn visit<'a>(
    task: &'a TaskSpec,
    by_id: &BTreeMap<String, &'a TaskSpec>,
    marks: &mut BTreeMap<String, Visit>,
    stack: &mut Vec<String>,
) -> Option<Vec<String>> {
    match marks.get(&task.id.0) {
        Some(Visit::Done) => return None,
        Some(Visit::Visiting) => {
            let start = stack.iter().position(|id| id == &task.id.0).unwrap_or(0);
            return Some(
                stack[start..]
                    .iter()
                    .cloned()
                    .chain(std::iter::once(task.id.0.clone()))
                    .collect(),
            );
        }
        None => {}
    }

    marks.insert(task.id.0.clone(), Visit::Visiting);
    stack.push(task.id.0.clone());
    for dependency in &task.dependencies {
        let Some(next) = by_id.get(&dependency.0) else {
            continue;
        };
        if let Some(path) = visit(next, by_id, marks, stack) {
            return Some(path);
        }
    }
    stack.pop();
    marks.insert(task.id.0.clone(), Visit::Done);
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCandidate {
    pub model: String,
    pub reasoning: Option<String>,
    pub service_tier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRoute {
    pub candidates: Vec<ModelCandidate>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDecision {
    pub selected: Option<ModelCandidate>,
    pub route: ModelRoute,
}

impl ModelRoute {
    pub fn decide(&self, available_models: &BTreeSet<String>) -> RouteDecision {
        let selected = self
            .candidates
            .iter()
            .find(|candidate| available_models.contains(&candidate.model))
            .cloned();
        RouteDecision {
            selected,
            route: self.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RunStatus {
    Planned,
    Running,
    Cancelling,
    Cancelled,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskStatus {
    Pending,
    Ready,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Expired,
    Blocked,
}

impl TaskStatus {
    pub const fn is_runnable(self) -> bool {
        matches!(self, Self::Pending | Self::Ready)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AttemptStatus {
    Pending,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    Unclaimed,
    Claimed { expires_at: SystemTime },
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseTransition {
    Claimed,
    Renewed,
    Expired,
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRecord {
    pub task: TaskStatus,
    pub attempt: AttemptStatus,
    pub lease: LeaseState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseDecision {
    Granted(LeaseTransition),
    Rejected,
}

impl LeaseRecord {
    pub fn claim(&mut self, now: SystemTime, ttl: Duration) -> LeaseDecision {
        match self.lease {
            LeaseState::Unclaimed | LeaseState::Expired => {
                self.lease = LeaseState::Claimed {
                    expires_at: now + ttl,
                };
                self.task = TaskStatus::Claimed;
                self.attempt = AttemptStatus::Claimed;
                LeaseDecision::Granted(LeaseTransition::Claimed)
            }
            LeaseState::Claimed { expires_at } if expires_at <= now => {
                self.lease = LeaseState::Claimed {
                    expires_at: now + ttl,
                };
                self.task = TaskStatus::Claimed;
                self.attempt = AttemptStatus::Claimed;
                LeaseDecision::Granted(LeaseTransition::Claimed)
            }
            LeaseState::Claimed { .. } => LeaseDecision::Rejected,
        }
    }

    pub fn renew(&mut self, now: SystemTime, ttl: Duration) -> LeaseDecision {
        match self.lease {
            LeaseState::Claimed { expires_at } if expires_at > now => {
                self.lease = LeaseState::Claimed {
                    expires_at: now + ttl,
                };
                self.task = TaskStatus::Running;
                self.attempt = AttemptStatus::Running;
                LeaseDecision::Granted(LeaseTransition::Renewed)
            }
            LeaseState::Claimed { .. } => LeaseDecision::Rejected,
            LeaseState::Unclaimed | LeaseState::Expired => LeaseDecision::Rejected,
        }
    }

    pub fn expire(&mut self, now: SystemTime) -> LeaseDecision {
        match self.lease {
            LeaseState::Claimed { expires_at } if expires_at <= now => {
                self.lease = LeaseState::Expired;
                self.task = TaskStatus::Expired;
                self.attempt = AttemptStatus::Aborted;
                LeaseDecision::Granted(LeaseTransition::Expired)
            }
            LeaseState::Claimed { .. } => LeaseDecision::Rejected,
            LeaseState::Expired => LeaseDecision::Granted(LeaseTransition::Expired),
            LeaseState::Unclaimed => LeaseDecision::Rejected,
        }
    }

    pub fn release(&mut self) -> LeaseDecision {
        self.lease = LeaseState::Unclaimed;
        self.task = TaskStatus::Pending;
        self.attempt = AttemptStatus::Pending;
        LeaseDecision::Granted(LeaseTransition::Released)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptPolicy {
    pub max_attempts: u32,
    pub escalation_threshold: u32,
}

impl AttemptPolicy {
    pub fn should_escalate(self, attempt_index: u32, objective_failure: bool) -> bool {
        objective_failure
            && attempt_index >= self.escalation_threshold
            && attempt_index < self.max_attempts
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetPolicy {
    pub max_tokens: u64,
    pub max_runtime: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetAdmission {
    Admitted,
    RejectedTokens,
    RejectedRuntime,
}

impl BudgetPolicy {
    pub fn admit(self, requested_tokens: u64, requested_runtime: Duration) -> BudgetAdmission {
        if requested_tokens > self.max_tokens {
            return BudgetAdmission::RejectedTokens;
        }
        if requested_runtime > self.max_runtime {
            return BudgetAdmission::RejectedRuntime;
        }
        BudgetAdmission::Admitted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunProjection {
    pub run_id: RunId,
    pub status: RunStatus,
    pub total_tasks: usize,
    pub ready_tasks: usize,
    pub pending_tasks: usize,
    pub running_tasks: usize,
    pub succeeded_tasks: usize,
    pub failed_tasks: usize,
    pub cancelled_tasks: usize,
    pub blocked_tasks: usize,
    pub skipped_tasks: usize,
    pub selected_model: Option<String>,
    pub fallback_reason: Option<String>,
    pub token_budget: Option<u64>,
    pub token_budget_used: u64,
    pub runtime_budget_used: Duration,
    pub cancellation_requested: bool,
}
