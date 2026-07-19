use super::*;

fn environment() -> PlanningEnvironment {
    PlanningEnvironment {
        parent_capabilities: CapabilitySet::from_capabilities(&[
            Capability::Read,
            Capability::Write,
        ]),
        capacity: ExecutionCapacity::default(),
    }
}

fn spec(pattern: SwarmPattern, domain: WorkDomain) -> SwarmSpec {
    SwarmSpec {
        session_scope: SessionScope::Work,
        pattern,
        domain: domain_profile(domain),
        risk: RiskOverlay::default(),
        requested_capabilities: CapabilitySet::from_capabilities(&[Capability::Read]),
    }
}

#[test]
fn rejects_non_work_sessions() {
    let mut input = spec(SwarmPattern::ScoutSynthesize, WorkDomain::Research);
    input.session_scope = SessionScope::Life;

    assert_eq!(
        plan_swarm(input, environment()),
        Err(PlanError::NonWorkSession)
    );
}

#[test]
fn direct_work_stays_solo() {
    let plan = plan_swarm(
        spec(SwarmPattern::Direct, WorkDomain::Software),
        environment(),
    )
    .unwrap();

    assert_eq!(plan.effective_pattern, SwarmPattern::Direct);
    assert_eq!(plan.boundaries, SwarmBoundaries::DIRECT);
}

#[test]
fn child_capabilities_are_strictly_attenuated() {
    let mut input = spec(SwarmPattern::PlannerWorkersReviewer, WorkDomain::Software);
    input.requested_capabilities = CapabilitySet::from_capabilities(&[
        Capability::Read,
        Capability::Execute,
        Capability::ExternalEffect,
    ]);

    let plan = plan_swarm(input, environment()).unwrap();

    assert!(plan.child_capabilities.contains(Capability::Read));
    assert!(!plan.child_capabilities.contains(Capability::Execute));
    assert!(!plan.child_capabilities.contains(Capability::ExternalEffect));
}

#[test]
fn high_risk_direct_request_adds_adversarial_review_and_caps_concurrency() {
    let mut input = spec(SwarmPattern::Direct, WorkDomain::Security);
    input.risk.high_stakes = true;

    let plan = plan_swarm(input, environment()).unwrap();

    assert_eq!(plan.effective_pattern, SwarmPattern::AdversarialReview);
    assert!(plan.requires_independent_review);
    assert!(plan.sealed_first_pass);
    assert!(plan.boundaries.max_active_agents <= 3);
}

#[test]
fn large_mixed_phase_graph_is_partitioned_into_cells() {
    let mut input = spec(SwarmPattern::PhaseGraph, WorkDomain::Mixed);
    input.risk.broad_surface = true;

    let plan = plan_swarm(input, environment()).unwrap();

    assert!(plan.boundaries.logical_agents > plan.boundaries.cell_size);
    assert!(plan.boundaries.cell_count > 1);
    assert!(plan.boundaries.initial_active_agents < plan.boundaries.logical_agents);
}

#[test]
fn registries_cover_every_pattern_and_domain_policy() {
    assert_eq!(ALL_PATTERNS.len(), 10);
    assert_eq!(
        domain_profile(WorkDomain::Research).evidence,
        EvidencePolicy::IndependentSources
    );
    assert_eq!(
        domain_profile(WorkDomain::Software).evidence,
        EvidencePolicy::BuildAndTest
    );
}
