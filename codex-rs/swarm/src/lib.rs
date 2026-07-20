//! Pure Work Studio swarm planning and policy contracts.
//!
//! Runtime ownership remains in the existing Codex multi-agent control plane.

mod boundaries;
mod domain;
mod kernel;
mod lifecycle;
mod pattern;
mod plan;
mod wait_graph;

pub use boundaries::ExecutionCapacity;
pub use boundaries::SwarmBoundaries;
pub use domain::DomainProfile;
pub use domain::EvidencePolicy;
pub use domain::WorkDomain;
pub use domain::domain_profile;
pub use kernel::AttemptId;
pub use kernel::AttemptPolicy;
pub use kernel::AttemptStatus;
pub use kernel::BudgetAdmission;
pub use kernel::BudgetPolicy;
pub use kernel::CellId;
pub use kernel::ExecutionPolicy;
pub use kernel::ExecutionSpec;
pub use kernel::ExecutionSpecError;
pub use kernel::LeaseDecision;
pub use kernel::LeaseRecord;
pub use kernel::LeaseState;
pub use kernel::LeaseTransition;
pub use kernel::ModelCandidate;
pub use kernel::ModelRoute;
pub use kernel::RouteDecision;
pub use kernel::RunId;
pub use kernel::RunProjection;
pub use kernel::RunStatus;
pub use kernel::TaskId;
pub use kernel::TaskKind;
pub use kernel::TaskSpec;
pub use kernel::TaskStatus;
pub use lifecycle::InvalidTaskTransition;
pub use lifecycle::TaskLifecycle;
pub use lifecycle::TaskState;
pub use lifecycle::TransitionOutcome;
pub use pattern::ALL_PATTERNS;
pub use pattern::PatternDefinition;
pub use pattern::PatternTopology;
pub use pattern::SwarmPattern;
pub use pattern::pattern_definition;
pub use plan::Capability;
pub use plan::CapabilitySet;
pub use plan::PlanError;
pub use plan::PlanningEnvironment;
pub use plan::RiskOverlay;
pub use plan::SessionScope;
pub use plan::SwarmPlan;
pub use plan::SwarmSpec;
pub use plan::plan_swarm;
pub use wait_graph::WaitEdgeOutcome;
pub use wait_graph::WaitGraph;
pub use wait_graph::WaitGraphError;
pub use wait_graph::WaitNode;
pub use wait_graph::WaitNodeKind;
pub use wait_graph::WaitOwnership;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
