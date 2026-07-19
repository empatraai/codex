use crate::DomainProfile;
use crate::EvidencePolicy;
use crate::ExecutionCapacity;
use crate::SwarmBoundaries;
use crate::SwarmPattern;
use crate::pattern_definition;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionScope {
    Work,
    Life,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Read,
    Write,
    Execute,
    Network,
    ExternalEffect,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CapabilitySet(u8);

impl CapabilitySet {
    pub fn from_capabilities(capabilities: &[Capability]) -> Self {
        capabilities
            .iter()
            .fold(Self::default(), |set, capability| set.with(*capability))
    }

    pub const fn with(self, capability: Capability) -> Self {
        Self(self.0 | capability_bit(capability))
    }

    pub const fn contains(self, capability: Capability) -> bool {
        self.0 & capability_bit(capability) != 0
    }

    pub const fn intersect(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }
}

const fn capability_bit(capability: Capability) -> u8 {
    1 << capability as u8
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RiskOverlay {
    pub high_stakes: bool,
    pub ambiguous_scope: bool,
    pub broad_surface: bool,
    pub external_effects: bool,
}

impl RiskOverlay {
    pub const fn is_high_risk(self) -> bool {
        self.high_stakes || self.external_effects
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarmSpec {
    pub session_scope: SessionScope,
    pub pattern: SwarmPattern,
    pub domain: DomainProfile,
    pub risk: RiskOverlay,
    pub requested_capabilities: CapabilitySet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanningEnvironment {
    pub parent_capabilities: CapabilitySet,
    pub capacity: ExecutionCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarmPlan {
    pub requested_pattern: SwarmPattern,
    pub effective_pattern: SwarmPattern,
    pub evidence_policy: EvidencePolicy,
    pub boundaries: SwarmBoundaries,
    pub child_capabilities: CapabilitySet,
    pub sealed_first_pass: bool,
    pub requires_independent_review: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanError {
    NonWorkSession,
    NoExecutionCapacity,
}

pub fn plan_swarm(
    spec: SwarmSpec,
    environment: PlanningEnvironment,
) -> Result<SwarmPlan, PlanError> {
    if spec.session_scope != SessionScope::Work {
        return Err(PlanError::NonWorkSession);
    }

    let effective_pattern = if spec.pattern == SwarmPattern::Direct && spec.risk.is_high_risk() {
        SwarmPattern::AdversarialReview
    } else {
        spec.pattern
    };
    let definition = pattern_definition(effective_pattern);
    if effective_pattern != SwarmPattern::Direct
        && (environment.capacity.max_active_agents == 0
            || environment.capacity.max_total_agents == 0
            || environment.capacity.max_depth == 0)
    {
        return Err(PlanError::NoExecutionCapacity);
    }

    let boundaries =
        SwarmBoundaries::derive(definition, spec.domain, spec.risk, environment.capacity);
    let requires_independent_review = spec.risk.is_high_risk()
        || spec.domain.prefers_independent_passes
        || effective_pattern == SwarmPattern::AdversarialReview;

    Ok(SwarmPlan {
        requested_pattern: spec.pattern,
        effective_pattern,
        evidence_policy: spec.domain.evidence,
        boundaries,
        child_capabilities: spec
            .requested_capabilities
            .intersect(environment.parent_capabilities),
        sealed_first_pass: requires_independent_review && boundaries.logical_agents > 1,
        requires_independent_review,
    })
}
