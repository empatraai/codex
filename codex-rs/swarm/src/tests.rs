use super::*;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;
use std::time::SystemTime;

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

#[test]
fn task_lifecycle_enforces_scheduler_order_and_idempotent_replay() {
    let mut lifecycle = TaskLifecycle::new();
    assert_eq!(
        lifecycle.transition_to(TaskState::Proposed),
        Ok(TransitionOutcome::Unchanged(TaskState::Proposed))
    );
    assert_eq!(
        lifecycle.transition_to(TaskState::Scheduled),
        Ok(TransitionOutcome::Changed {
            from: TaskState::Proposed,
            to: TaskState::Scheduled,
        })
    );
    lifecycle.transition_to(TaskState::Ready).unwrap();
    lifecycle.transition_to(TaskState::Running).unwrap();
    lifecycle.transition_to(TaskState::Waiting).unwrap();
    lifecycle.transition_to(TaskState::Ready).unwrap();
    lifecycle.transition_to(TaskState::Running).unwrap();
    lifecycle.transition_to(TaskState::Completed).unwrap();

    assert_eq!(lifecycle.state(), TaskState::Completed);
    assert!(lifecycle.state().is_terminal());
    assert_eq!(
        lifecycle.transition_to(TaskState::Completed),
        Ok(TransitionOutcome::Unchanged(TaskState::Completed))
    );
}

#[test]
fn task_lifecycle_rejects_skips_and_terminal_reopen() {
    let mut lifecycle = TaskLifecycle::new();
    assert_eq!(
        lifecycle.transition_to(TaskState::Running),
        Err(InvalidTaskTransition {
            from: TaskState::Proposed,
            to: TaskState::Running,
        })
    );

    lifecycle.transition_to(TaskState::Scheduled).unwrap();
    lifecycle.transition_to(TaskState::Ready).unwrap();
    lifecycle.transition_to(TaskState::Cancelled).unwrap();
    assert_eq!(
        lifecycle.transition_to(TaskState::Scheduled),
        Err(InvalidTaskTransition {
            from: TaskState::Cancelled,
            to: TaskState::Scheduled,
        })
    );
}

#[test]
fn retry_and_recovery_return_work_to_the_scheduler() {
    let mut retryable = TaskLifecycle::restore(TaskState::Running);
    retryable.transition_to(TaskState::Scheduled).unwrap();
    assert_eq!(retryable.state(), TaskState::Scheduled);

    let mut lost = TaskLifecycle::restore(TaskState::Lost);
    lost.transition_to(TaskState::Scheduled).unwrap();
    assert_eq!(lost.state(), TaskState::Scheduled);
}

#[test]
fn wait_graph_rejects_scheduler_cycles_with_the_cycle_path() {
    let task_a = WaitNode::scheduler(WaitNodeKind::Task, "a");
    let task_b = WaitNode::scheduler(WaitNodeKind::Task, "b");
    let join = WaitNode::scheduler(WaitNodeKind::Join, "join");
    let mut graph = WaitGraph::default();

    graph.add_wait(task_a.clone(), task_b.clone()).unwrap();
    graph.add_wait(task_b.clone(), join.clone()).unwrap();
    assert_eq!(
        graph.add_wait(join.clone(), task_a.clone()),
        Err(WaitGraphError::CycleDetected {
            path: vec![
                join,
                task_a,
                task_b,
                WaitNode::scheduler(WaitNodeKind::Join, "join")
            ],
        })
    );
}

#[test]
fn external_waits_do_not_create_false_scheduler_deadlocks() {
    let task = WaitNode::scheduler(WaitNodeKind::Task, "task");
    let human = WaitNode::external(WaitNodeKind::Human, "user-approval");
    let mut graph = WaitGraph::default();

    assert_eq!(
        graph.add_wait(task.clone(), human.clone()),
        Ok(WaitEdgeOutcome::Added)
    );
    assert_eq!(graph.add_wait(human, task), Ok(WaitEdgeOutcome::Added));
}

#[test]
fn wait_edges_are_idempotent_and_cleanup_removes_both_directions() {
    let task = WaitNode::scheduler(WaitNodeKind::Task, "task");
    let message = WaitNode::scheduler(WaitNodeKind::Message, "message");
    let join = WaitNode::scheduler(WaitNodeKind::Join, "join");
    let mut graph = WaitGraph::default();

    assert_eq!(
        graph.add_wait(task.clone(), message.clone()),
        Ok(WaitEdgeOutcome::Added)
    );
    assert_eq!(
        graph.add_wait(task.clone(), message.clone()),
        Ok(WaitEdgeOutcome::Unchanged)
    );
    graph.add_wait(message.clone(), join).unwrap();

    assert_eq!(graph.remove_node(&message), 2);
    assert_eq!(graph.dependencies(&task).count(), 0);
}

#[test]
fn wait_graph_rejects_empty_durable_identity() {
    let mut graph = WaitGraph::default();
    assert_eq!(
        graph.add_wait(
            WaitNode::scheduler(WaitNodeKind::Task, " "),
            WaitNode::scheduler(WaitNodeKind::Resource, "provider"),
        ),
        Err(WaitGraphError::EmptyNodeId {
            kind: WaitNodeKind::Task,
        })
    );
}

#[test]
fn human_and_external_waits_cannot_be_misclassified_as_scheduler_owned() {
    let mut graph = WaitGraph::default();
    assert_eq!(
        graph.add_wait(
            WaitNode::scheduler(WaitNodeKind::Task, "task"),
            WaitNode::scheduler(WaitNodeKind::Human, "user"),
        ),
        Err(WaitGraphError::InvalidOwnership {
            kind: WaitNodeKind::Human,
            ownership: WaitOwnership::Scheduler,
        })
    );
}

#[test]
fn execution_spec_validates_dag_shape_and_deterministic_readiness() {
    let spec = ExecutionSpec {
        run_id: RunId("run-1".to_string()),
        policy: ExecutionPolicy {
            max_concurrency: 2,
            token_budget: Some(1_000),
            max_runtime: Some(Duration::from_secs(60)),
            deadline: None,
            fail_fast: true,
            lease_ttl: Duration::from_secs(30),
            default_max_attempts: Some(3),
        },
        tasks: vec![
            TaskSpec {
                id: TaskId("root".to_string()),
                task_title: "Plan work".to_string(),
                kind: TaskKind::Worker,
                agent_type: "planner".to_string(),
                instructions: "plan the work".to_string(),
                dependencies: vec![],
                cell: Some(CellId("alpha".to_string())),
                explicit_model: None,
                explicit_reasoning: None,
                max_attempts: 2,
                deadline: None,
            },
            TaskSpec {
                id: TaskId("review".to_string()),
                task_title: "Review plan".to_string(),
                kind: TaskKind::Reviewer,
                agent_type: "reviewer".to_string(),
                instructions: "check the plan".to_string(),
                dependencies: vec![TaskId("root".to_string())],
                cell: None,
                explicit_model: Some("gpt-4.1".to_string()),
                explicit_reasoning: Some("low".to_string()),
                max_attempts: 3,
                deadline: None,
            },
        ],
    };

    spec.validate().unwrap();

    let ready = spec.ready_tasks(&BTreeSet::from([TaskId("root".to_string())]));
    assert_eq!(ready, vec![TaskId("review".to_string())]);

    let mut statuses = BTreeMap::new();
    statuses.insert(TaskId("review".to_string()), TaskStatus::Running);
    let ready =
        spec.ready_tasks_with_status_map(&statuses, &BTreeSet::from([TaskId("root".to_string())]));
    assert!(ready.is_empty());
}

#[test]
fn execution_spec_rejects_cycles_and_missing_dependencies() {
    let cycle = ExecutionSpec {
        run_id: RunId("run-2".to_string()),
        policy: ExecutionPolicy {
            max_concurrency: 1,
            token_budget: None,
            max_runtime: None,
            deadline: None,
            fail_fast: false,
            lease_ttl: Duration::from_secs(5),
            default_max_attempts: None,
        },
        tasks: vec![
            TaskSpec {
                id: TaskId("a".to_string()),
                task_title: "Run A".to_string(),
                kind: TaskKind::Worker,
                agent_type: "worker".to_string(),
                instructions: "a".to_string(),
                dependencies: vec![TaskId("b".to_string())],
                cell: None,
                explicit_model: None,
                explicit_reasoning: None,
                max_attempts: 1,
                deadline: None,
            },
            TaskSpec {
                id: TaskId("b".to_string()),
                task_title: "Run B".to_string(),
                kind: TaskKind::Reducer,
                agent_type: "reducer".to_string(),
                instructions: "b".to_string(),
                dependencies: vec![TaskId("a".to_string())],
                cell: None,
                explicit_model: None,
                explicit_reasoning: None,
                max_attempts: 1,
                deadline: None,
            },
        ],
    };

    assert!(matches!(
        cycle.validate(),
        Err(ExecutionSpecError::CycleDetected { .. })
    ));

    let missing = ExecutionSpec {
        run_id: RunId("run-3".to_string()),
        policy: ExecutionPolicy {
            max_concurrency: 1,
            token_budget: None,
            max_runtime: None,
            deadline: None,
            fail_fast: false,
            lease_ttl: Duration::from_secs(5),
            default_max_attempts: None,
        },
        tasks: vec![TaskSpec {
            id: TaskId("solo".to_string()),
            task_title: "Run solo".to_string(),
            kind: TaskKind::Worker,
            agent_type: "worker".to_string(),
            instructions: "solo".to_string(),
            dependencies: vec![TaskId("absent".to_string())],
            cell: None,
            explicit_model: None,
            explicit_reasoning: None,
            max_attempts: 1,
            deadline: None,
        }],
    };

    assert!(matches!(
        missing.validate(),
        Err(ExecutionSpecError::MissingDependency { .. })
    ));
}

#[test]
fn route_decision_prefers_ordered_candidates_that_are_available() {
    let route = ModelRoute {
        reason: "cheap-first".to_string(),
        candidates: vec![
            ModelCandidate {
                model: "missing".to_string(),
                reasoning: None,
                service_tier: None,
            },
            ModelCandidate {
                model: "present".to_string(),
                reasoning: Some("low".to_string()),
                service_tier: Some("standard".to_string()),
            },
        ],
    };
    let decision = route.decide(&BTreeSet::from(["present".to_string()]));

    assert_eq!(
        decision.selected,
        Some(ModelCandidate {
            model: "present".to_string(),
            reasoning: Some("low".to_string()),
            service_tier: Some("standard".to_string()),
        })
    );
}

#[test]
fn lease_claim_expire_and_retry_policy_are_deterministic() {
    let now = SystemTime::UNIX_EPOCH;
    let mut record = LeaseRecord {
        task: TaskStatus::Pending,
        attempt: AttemptStatus::Pending,
        lease: LeaseState::Unclaimed,
    };

    assert_eq!(
        record.claim(now, Duration::from_secs(30)),
        LeaseDecision::Granted(LeaseTransition::Claimed)
    );
    assert_eq!(
        record.renew(now, Duration::from_secs(60)),
        LeaseDecision::Granted(LeaseTransition::Renewed)
    );
    assert_eq!(
        record.expire(now),
        LeaseDecision::Rejected,
        "lease should not expire before deadline"
    );

    let policy = AttemptPolicy {
        max_attempts: 3,
        escalation_threshold: 2,
    };
    assert!(policy.should_escalate(2, true));
    assert!(!policy.should_escalate(1, true));
}

#[test]
fn budget_policy_rejects_overruns() {
    let policy = BudgetPolicy {
        max_tokens: 100,
        max_runtime: Duration::from_secs(10),
    };

    assert_eq!(
        policy.admit(101, Duration::from_secs(1)),
        BudgetAdmission::RejectedTokens
    );
    assert_eq!(
        policy.admit(50, Duration::from_secs(11)),
        BudgetAdmission::RejectedRuntime
    );
    assert_eq!(
        policy.admit(50, Duration::from_secs(1)),
        BudgetAdmission::Admitted
    );
}

#[test]
fn execution_policy_rejects_zero_bounds_and_projection_tracks_counts() {
    assert_eq!(
        ExecutionPolicy {
            max_concurrency: 0,
            token_budget: None,
            max_runtime: None,
            deadline: None,
            fail_fast: false,
            lease_ttl: Duration::from_secs(1),
            default_max_attempts: None,
        }
        .validate(),
        Err(ExecutionSpecError::InvalidMaxConcurrency)
    );

    assert_eq!(
        ExecutionPolicy {
            max_concurrency: 1,
            token_budget: None,
            max_runtime: None,
            deadline: None,
            fail_fast: false,
            lease_ttl: Duration::from_secs(0),
            default_max_attempts: None,
        }
        .validate(),
        Err(ExecutionSpecError::InvalidLeaseTtl)
    );

    let projection = RunProjection {
        run_id: RunId("run-4".to_string()),
        status: RunStatus::Running,
        total_tasks: 5,
        ready_tasks: 1,
        pending_tasks: 1,
        running_tasks: 1,
        succeeded_tasks: 1,
        failed_tasks: 0,
        cancelled_tasks: 1,
        blocked_tasks: 0,
        skipped_tasks: 0,
        selected_model: Some("cheap-model".to_string()),
        fallback_reason: Some("fallback".to_string()),
        token_budget: Some(2_000),
        token_budget_used: 500,
        runtime_budget_used: Duration::from_secs(12),
        cancellation_requested: false,
    };

    assert_eq!(projection.total_tasks, 5);
    assert_eq!(projection.succeeded_tasks, 1);
    assert_eq!(projection.cancelled_tasks, 1);
    assert_eq!(projection.token_budget, Some(2_000));
}
