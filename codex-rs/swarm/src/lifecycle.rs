use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskState {
    Proposed,
    Scheduled,
    Ready,
    Running,
    Waiting,
    Blocked,
    AwaitingApproval,
    Parked,
    Completed,
    Failed,
    Cancelled,
    Lost,
}

impl TaskState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return true;
        }

        match self {
            Self::Proposed => matches!(next, Self::Scheduled | Self::Cancelled),
            Self::Scheduled => matches!(
                next,
                Self::Ready | Self::Blocked | Self::Parked | Self::Failed | Self::Cancelled
            ),
            Self::Ready => matches!(
                next,
                Self::Running
                    | Self::Blocked
                    | Self::AwaitingApproval
                    | Self::Parked
                    | Self::Failed
                    | Self::Cancelled
            ),
            Self::Running => matches!(
                next,
                Self::Scheduled
                    | Self::Waiting
                    | Self::Blocked
                    | Self::AwaitingApproval
                    | Self::Parked
                    | Self::Completed
                    | Self::Failed
                    | Self::Cancelled
                    | Self::Lost
            ),
            Self::Waiting => matches!(
                next,
                Self::Ready
                    | Self::Blocked
                    | Self::Parked
                    | Self::Failed
                    | Self::Cancelled
                    | Self::Lost
            ),
            Self::Blocked => matches!(
                next,
                Self::Scheduled | Self::Ready | Self::Parked | Self::Failed | Self::Cancelled
            ),
            Self::AwaitingApproval => matches!(
                next,
                Self::Ready | Self::Parked | Self::Failed | Self::Cancelled
            ),
            Self::Parked => matches!(
                next,
                Self::Scheduled | Self::Ready | Self::Failed | Self::Cancelled
            ),
            Self::Lost => matches!(next, Self::Scheduled | Self::Failed | Self::Cancelled),
            Self::Completed | Self::Failed | Self::Cancelled => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionOutcome {
    Changed { from: TaskState, to: TaskState },
    Unchanged(TaskState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTaskTransition {
    pub from: TaskState,
    pub to: TaskState,
}

impl fmt::Display for InvalidTaskTransition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid swarm task transition from {:?} to {:?}",
            self.from, self.to
        )
    }
}

impl Error for InvalidTaskTransition {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskLifecycle {
    state: TaskState,
}

impl Default for TaskLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskLifecycle {
    pub const fn new() -> Self {
        Self {
            state: TaskState::Proposed,
        }
    }

    pub const fn restore(state: TaskState) -> Self {
        Self { state }
    }

    pub const fn state(self) -> TaskState {
        self.state
    }

    pub fn transition_to(
        &mut self,
        next: TaskState,
    ) -> Result<TransitionOutcome, InvalidTaskTransition> {
        let from = self.state;
        if from == next {
            return Ok(TransitionOutcome::Unchanged(from));
        }
        if !from.can_transition_to(next) {
            return Err(InvalidTaskTransition { from, to: next });
        }

        self.state = next;
        Ok(TransitionOutcome::Changed { from, to: next })
    }
}
