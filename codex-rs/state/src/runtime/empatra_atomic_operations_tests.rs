use super::*;
use crate::runtime::test_support::unique_temp_dir;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn claims_one_exact_payload_and_preserves_terminal_evidence_across_restart()
-> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(home.clone(), "test-provider".to_string()).await?;
    let claim = EmpatraAtomicOperationRecord {
        operation_id: "workspace-command-1".to_string(),
        payload_digest: "digest-a".to_string(),
        issued_at_ms: 1_700_000_000_000,
        thread_id: "018bcfe5-6800-7000-8000-000000000001".to_string(),
        turn_id: "018bcfe5-6800-7000-8000-000000000002".to_string(),
        state: EmpatraAtomicOperationState::Reserved,
        error: None,
        initial_events_json: None,
    };

    assert_eq!(
        runtime.claim_empatra_atomic_operation(&claim).await?,
        EmpatraAtomicOperationClaim::Claimed(claim.clone())
    );
    runtime
        .accept_empatra_atomic_operation(&claim.operation_id, "event")
        .await?;
    runtime
        .complete_and_enqueue_empatra_atomic_publication(&claim.operation_id)
        .await?;

    let restarted = StateRuntime::init(home, "test-provider".to_string()).await?;
    assert_eq!(
        restarted
            .empatra_atomic_operation(&claim.operation_id)
            .await?,
        Some(EmpatraAtomicOperationRecord {
            state: EmpatraAtomicOperationState::Completed,
            initial_events_json: Some("event".to_string()),
            ..claim
        })
    );
    Ok(())
}

#[tokio::test]
async fn claiming_an_existing_operation_never_overwrites_its_payload() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test-provider".to_string()).await?;
    let original = EmpatraAtomicOperationRecord {
        operation_id: "workspace-command-1".to_string(),
        payload_digest: "digest-a".to_string(),
        issued_at_ms: 1_700_000_000_000,
        thread_id: "018bcfe5-6800-7000-8000-000000000001".to_string(),
        turn_id: "018bcfe5-6800-7000-8000-000000000002".to_string(),
        state: EmpatraAtomicOperationState::Reserved,
        error: None,
        initial_events_json: None,
    };
    assert!(matches!(
        runtime.claim_empatra_atomic_operation(&original).await?,
        EmpatraAtomicOperationClaim::Claimed(_)
    ));
    let conflicting = EmpatraAtomicOperationRecord {
        payload_digest: "digest-b".to_string(),
        ..original.clone()
    };

    assert_eq!(
        runtime.claim_empatra_atomic_operation(&conflicting).await?,
        EmpatraAtomicOperationClaim::Existing(original)
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_claims_have_one_dispatch_owner() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(home.clone(), "test-provider".to_string()).await?;
    let claim = EmpatraAtomicOperationRecord {
        operation_id: "workspace-command-race".to_string(),
        payload_digest: "digest".to_string(),
        issued_at_ms: 1_700_000_000_000,
        thread_id: "018bcfe5-6800-7000-8000-000000000011".to_string(),
        turn_id: "018bcfe5-6800-7000-8000-000000000012".to_string(),
        state: EmpatraAtomicOperationState::Reserved,
        error: None,
        initial_events_json: None,
    };
    let (left, right) = tokio::join!(
        runtime.claim_empatra_atomic_operation(&claim),
        runtime.claim_empatra_atomic_operation(&claim),
    );
    let outcomes = [left?, right?];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, EmpatraAtomicOperationClaim::Claimed(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, EmpatraAtomicOperationClaim::Existing(_)))
            .count(),
        1
    );
    assert_eq!(
        tokio::fs::read(super::super::empatra_atomic_authority_marker_path(&home)).await?,
        EMPATRA_ATOMIC_AUTHORITY_MARKER_CONTENT
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_distinct_claims_create_one_fixed_authority_marker() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(home.clone(), "test-provider".to_string()).await?;
    let mut claims = tokio::task::JoinSet::new();
    for index in 0..32 {
        let runtime = runtime.clone();
        claims.spawn(async move {
            runtime
                .claim_empatra_atomic_operation(&EmpatraAtomicOperationRecord {
                    operation_id: format!("workspace-command-{index}"),
                    payload_digest: format!("digest-{index}"),
                    issued_at_ms: 1_700_000_000_000 + index,
                    thread_id: format!("018bcfe5-6800-7000-8000-{index:012}"),
                    turn_id: format!("018bcfe5-6800-7001-8000-{index:012}"),
                    state: EmpatraAtomicOperationState::Reserved,
                    error: None,
                    initial_events_json: None,
                })
                .await
        });
    }
    while let Some(claim) = claims.join_next().await {
        assert!(matches!(claim??, EmpatraAtomicOperationClaim::Claimed(_)));
    }

    assert_eq!(
        tokio::fs::read(super::super::empatra_atomic_authority_marker_path(&home)).await?,
        EMPATRA_ATOMIC_AUTHORITY_MARKER_CONTENT
    );
    Ok(())
}

#[tokio::test]
async fn terminal_gc_frees_capacity_but_reserved_operations_are_never_pruned() -> anyhow::Result<()>
{
    let runtime = StateRuntime::init(unique_temp_dir(), "test-provider".to_string()).await?;
    let old = chrono::Utc::now().timestamp_millis() - EMPATRA_ATOMIC_OPERATION_RETRY_HORIZON_MS - 1;
    let mut transaction = runtime.pool.begin().await?;
    for index in 0..EMPATRA_ATOMIC_OPERATION_CAPACITY {
        let state = if index < 2 { "completed" } else { "reserved" };
        sqlx::query(
            "INSERT INTO empatra_atomic_operations (operation_id, payload_digest, issued_at_ms, thread_id, turn_id, state, error, created_at_ms, updated_at_ms) VALUES (?, 'digest', 1, ?, ?, ?, NULL, ?, ?)",
        )
        .bind(format!("operation-{index}"))
        .bind(format!("thread-{index}"))
        .bind(format!("turn-{index}"))
        .bind(state)
        .bind(old)
        .bind(old)
        .execute(&mut *transaction)
        .await?;
    }
    sqlx::query(
        "INSERT INTO empatra_atomic_publication_outbox (operation_id, thread_id, turn_id, initial_events_json, created_at_ms) VALUES ('operation-0', 'thread-0', 'turn-0', 'event', ?)",
    )
    .bind(old)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    let replacement = EmpatraAtomicOperationRecord {
        operation_id: "replacement".to_string(),
        payload_digest: "digest".to_string(),
        issued_at_ms: 2,
        thread_id: "replacement-thread".to_string(),
        turn_id: "replacement-turn".to_string(),
        state: EmpatraAtomicOperationState::Reserved,
        error: None,
        initial_events_json: None,
    };
    assert!(matches!(
        runtime.claim_empatra_atomic_operation(&replacement).await?,
        EmpatraAtomicOperationClaim::Claimed(_)
    ));
    assert!(
        runtime
            .empatra_atomic_operation("operation-0")
            .await?
            .is_some()
    );
    assert!(
        runtime
            .empatra_atomic_operation("operation-1")
            .await?
            .is_none()
    );

    let overflow = EmpatraAtomicOperationRecord {
        operation_id: "overflow".to_string(),
        ..replacement
    };
    assert!(
        runtime
            .claim_empatra_atomic_operation(&overflow)
            .await
            .expect_err("reserved capacity must be bounded")
            .to_string()
            .contains("capacity")
    );
    Ok(())
}

#[tokio::test]
async fn publication_outbox_survives_completion_until_acknowledged() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test-provider".to_string()).await?;
    let record = EmpatraAtomicOperationRecord {
        operation_id: "workspace-command-outbox".to_string(),
        payload_digest: "digest".to_string(),
        issued_at_ms: 1_700_000_000_000,
        thread_id: "018bcfe5-6800-7000-8000-000000000031".to_string(),
        turn_id: "018bcfe5-6800-7000-8000-000000000032".to_string(),
        state: EmpatraAtomicOperationState::Reserved,
        error: None,
        initial_events_json: None,
    };
    runtime.claim_empatra_atomic_operation(&record).await?;
    runtime
        .accept_empatra_atomic_operation(&record.operation_id, "event")
        .await?;
    let publication = runtime
        .complete_and_enqueue_empatra_atomic_publication(&record.operation_id)
        .await?;

    assert_eq!(
        runtime.pending_empatra_atomic_publications(256).await?,
        vec![publication]
    );
    runtime
        .acknowledge_empatra_atomic_publication(&record.operation_id)
        .await?;
    assert!(
        runtime
            .pending_empatra_atomic_publications(256)
            .await?
            .is_empty()
    );
    assert_eq!(
        runtime
            .empatra_atomic_operation(&record.operation_id)
            .await?
            .expect("completed operation")
            .initial_events_json,
        None,
        "acknowledging publication must release the bounded event payload"
    );
    Ok(())
}

#[tokio::test]
async fn publication_backlog_counts_utf8_bytes_instead_of_characters() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test-provider".to_string()).await?;
    let record = EmpatraAtomicOperationRecord {
        operation_id: "workspace-command-utf8-quota".to_string(),
        payload_digest: "digest".to_string(),
        issued_at_ms: 1_700_000_000_000,
        thread_id: "018bcfe5-6800-7000-8000-000000000051".to_string(),
        turn_id: "018bcfe5-6800-7000-8000-000000000052".to_string(),
        state: EmpatraAtomicOperationState::Reserved,
        error: None,
        initial_events_json: None,
    };
    runtime.claim_empatra_atomic_operation(&record).await?;

    let unicode = "🙂".repeat(32);
    runtime
        .accept_empatra_atomic_operation(&record.operation_id, &unicode)
        .await?;
    let (pending_count, pending_bytes): (i64, i64) =
        sqlx::query_as(EMPATRA_ATOMIC_PENDING_USAGE_SQL)
            .fetch_one(runtime.pool.as_ref())
            .await?;
    assert_eq!(pending_count, 1);
    assert_eq!(pending_bytes, unicode.len() as i64);
    assert_eq!(pending_bytes, 4 * unicode.chars().count() as i64);
    Ok(())
}

#[tokio::test]
async fn cleanup_failure_is_terminal_only_after_confirmed_cleanup() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test-provider".to_string()).await?;
    let record = EmpatraAtomicOperationRecord {
        operation_id: "workspace-command-cleanup".to_string(),
        payload_digest: "digest".to_string(),
        issued_at_ms: 1_700_000_000_000,
        thread_id: "018bcfe5-6800-7000-8000-000000000041".to_string(),
        turn_id: "018bcfe5-6800-7000-8000-000000000042".to_string(),
        state: EmpatraAtomicOperationState::Reserved,
        error: None,
        initial_events_json: None,
    };
    runtime.claim_empatra_atomic_operation(&record).await?;
    runtime
        .begin_empatra_atomic_cleanup(&record.operation_id, "initial_turn_failed")
        .await?;
    assert_eq!(
        runtime
            .empatra_atomic_operation(&record.operation_id)
            .await?
            .expect("cleanup operation")
            .state,
        EmpatraAtomicOperationState::CleanupPending
    );
    runtime
        .finish_empatra_atomic_cleanup(&record.operation_id, "initial_turn_failed")
        .await?;
    assert_eq!(
        runtime
            .empatra_atomic_operation(&record.operation_id)
            .await?
            .expect("failed operation")
            .state,
        EmpatraAtomicOperationState::Failed
    );
    Ok(())
}

#[tokio::test]
async fn startup_recovery_lists_only_durably_derivable_nonterminal_transitions()
-> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test-provider".to_string()).await?;
    let record = |operation_id: &str, suffix: u64| EmpatraAtomicOperationRecord {
        operation_id: operation_id.to_string(),
        payload_digest: "digest".to_string(),
        issued_at_ms: 1_700_000_000_000,
        thread_id: format!("018bcfe5-6800-7000-8000-{suffix:012}"),
        turn_id: format!("018bcfe5-6800-7001-8000-{suffix:012}"),
        state: EmpatraAtomicOperationState::Reserved,
        error: None,
        initial_events_json: None,
    };
    let reserved = record("reserved", 71);
    let accepted = record("accepted", 72);
    let cleanup = record("cleanup", 73);
    runtime.claim_empatra_atomic_operation(&reserved).await?;
    runtime.claim_empatra_atomic_operation(&accepted).await?;
    runtime.claim_empatra_atomic_operation(&cleanup).await?;
    runtime
        .accept_empatra_atomic_operation(&accepted.operation_id, "event")
        .await?;
    runtime
        .begin_empatra_atomic_cleanup(&cleanup.operation_id, "submit_failed")
        .await?;

    let recoverable = runtime.recoverable_empatra_atomic_operations(256).await?;
    assert_eq!(
        recoverable
            .iter()
            .map(|operation| (&operation.operation_id, operation.state))
            .collect::<Vec<_>>(),
        vec![
            (
                &accepted.operation_id,
                EmpatraAtomicOperationState::Accepted
            ),
            (
                &cleanup.operation_id,
                EmpatraAtomicOperationState::CleanupPending
            ),
        ]
    );
    assert!(
        recoverable
            .iter()
            .all(|operation| operation.operation_id != reserved.operation_id),
        "reserved operations require the exact client request and must remain hidden"
    );
    Ok(())
}

#[tokio::test]
async fn reserved_thread_publication_state_is_queryable() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test-provider".to_string()).await?;
    let record = EmpatraAtomicOperationRecord {
        operation_id: "workspace-command-publication".to_string(),
        payload_digest: "digest".to_string(),
        issued_at_ms: 1_700_000_000_000,
        thread_id: "018bcfe5-6800-7000-8000-000000000021".to_string(),
        turn_id: "018bcfe5-6800-7000-8000-000000000022".to_string(),
        state: EmpatraAtomicOperationState::Reserved,
        error: None,
        initial_events_json: None,
    };
    runtime.claim_empatra_atomic_operation(&record).await?;
    assert!(
        runtime
            .is_empatra_atomic_thread_reserved(&record.thread_id)
            .await?
    );
    runtime
        .accept_empatra_atomic_operation(&record.operation_id, "event")
        .await?;
    assert!(
        runtime
            .is_empatra_atomic_thread_reserved(&record.thread_id)
            .await?
    );
    runtime
        .complete_and_enqueue_empatra_atomic_publication(&record.operation_id)
        .await?;
    assert!(
        !runtime
            .is_empatra_atomic_thread_reserved(&record.thread_id)
            .await?
    );
    Ok(())
}
