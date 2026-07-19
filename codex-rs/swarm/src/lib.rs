//! Pure Work Studio swarm planning and policy contracts.
//!
//! Runtime ownership remains in the existing Codex multi-agent control plane.

mod boundaries;
mod domain;
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
