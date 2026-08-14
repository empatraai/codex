use super::*;
use codex_app_server_protocol::EmpatraInitialTurnParams;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadStartParams;

pub(super) struct EmpatraAtomicCoordinator {
    empatra_atomic_operation_locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    empatra_installation_id: String,
    state_db: Option<StateDbHandle>,
    thread_processor: ThreadRequestProcessor,
    turn_processor: TurnRequestProcessor,
}

enum AtomicThreadStart {
    Create {
        params: ThreadStartParams,
        request_context: RequestContext,
    },
    Fork(ThreadForkParams),
}

struct AtomicCommand {
    operation_id: String,
    issued_at_ms: i64,
    identity: crate::empatra_atomic::AtomicIdentity,
    thread: AtomicThreadStart,
    turn: EmpatraInitialTurnParams,
    thread_start_failure: &'static str,
}

struct AtomicCommandResult {
    operation_id: String,
    thread_id: String,
    turn_id: String,
}

enum ExistingOperation {
    Continue,
    Return(AtomicCommandResult),
}

impl EmpatraAtomicCoordinator {
    pub(super) fn new(
        empatra_installation_id: String,
        state_db: Option<StateDbHandle>,
        thread_processor: ThreadRequestProcessor,
        turn_processor: TurnRequestProcessor,
    ) -> Self {
        Self {
            empatra_atomic_operation_locks: Arc::new(Mutex::new(HashMap::new())),
            empatra_installation_id,
            state_db,
            thread_processor,
            turn_processor,
        }
    }

    async fn empatra_operation_lock(&self, operation_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.empatra_atomic_operation_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(operation_id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(operation_id.to_string(), Arc::downgrade(&lock));
        lock
    }

    async fn claim_empatra_operation(
        &self,
        operation_id: &str,
        issued_at_ms: i64,
        identity: &crate::empatra_atomic::AtomicIdentity,
    ) -> Result<(StateDbHandle, EmpatraAtomicOperationClaim), JSONRPCErrorError> {
        let state_db = self.state_db.clone().ok_or_else(|| {
            crate::error_code::internal_error(
                "Empatra atomic creation requires the durable state database",
            )
        })?;
        let claim = state_db
            .claim_empatra_atomic_operation(&EmpatraAtomicOperationRecord {
                operation_id: operation_id.to_string(),
                payload_digest: identity.payload_digest.clone(),
                issued_at_ms,
                thread_id: identity.thread_id.to_string(),
                turn_id: identity.turn_id.clone(),
                state: EmpatraAtomicOperationState::Reserved,
                error: None,
                initial_events_json: None,
            })
            .await
            .map_err(|error| {
                crate::error_code::internal_error(format!(
                    "failed to reserve Empatra atomic operation: {error}"
                ))
            })?;
        let record = match &claim {
            EmpatraAtomicOperationClaim::Claimed(record)
            | EmpatraAtomicOperationClaim::Existing(record) => record,
        };
        if record.payload_digest != identity.payload_digest
            || record.issued_at_ms != issued_at_ms
            || record.thread_id != identity.thread_id.to_string()
            || record.turn_id != identity.turn_id
        {
            return Err(invalid_request(format!(
                "operationId {operation_id} was already used with a different payload"
            )));
        }
        Ok((state_db, claim))
    }

    async fn publish_empatra_outbox(
        &self,
        state_db: &StateDbHandle,
        operation_id: &str,
        request_id: &ConnectionRequestId,
        supports_openai_form_elicitation: bool,
    ) -> Result<(), JSONRPCErrorError> {
        let Some(publication) = state_db
            .empatra_atomic_publication(operation_id)
            .await
            .map_err(|error| {
                crate::error_code::internal_error(format!(
                    "failed to read atomic publication: {error}"
                ))
            })?
        else {
            return Ok(());
        };
        let thread_id = ThreadId::from_string(&publication.thread_id).map_err(|error| {
            crate::error_code::internal_error(format!(
                "invalid atomic publication thread id: {error}"
            ))
        })?;
        let events: Vec<Event> =
            serde_json::from_str(&publication.initial_events_json).map_err(|error| {
                crate::error_code::internal_error(format!(
                    "invalid atomic publication event bundle: {error}"
                ))
            })?;
        let started = match self.thread_processor.empatra_loaded_thread(thread_id).await {
            Ok(started) => started,
            Err(_) => {
                self.thread_processor
                    .reconcile_empatra_atomic_thread(
                        request_id,
                        thread_id,
                        &publication.turn_id,
                        None,
                        supports_openai_form_elicitation,
                    )
                    .await?
                    .0
            }
        };
        self.thread_processor
            .publish_empatra_atomic_thread(request_id.connection_id, started, events)
            .await?;
        state_db
            .acknowledge_empatra_atomic_publication(operation_id)
            .await
            .map_err(|error| {
                crate::error_code::internal_error(format!(
                    "failed to acknowledge atomic publication: {error}"
                ))
            })?;
        Ok(())
    }

    async fn fail_empatra_before_acceptance(
        &self,
        state_db: &StateDbHandle,
        operation_id: &str,
        thread_id: ThreadId,
        failure_code: &'static str,
    ) -> Result<(), JSONRPCErrorError> {
        state_db
            .begin_empatra_atomic_cleanup(operation_id, failure_code)
            .await
            .map_err(|error| {
                crate::error_code::internal_error(format!(
                    "failed to begin atomic cleanup: {error}"
                ))
            })?;
        self.thread_processor
            .cleanup_empatra_atomic_thread(thread_id)
            .await?;
        state_db
            .finish_empatra_atomic_cleanup(operation_id, failure_code)
            .await
            .map_err(|error| {
                crate::error_code::internal_error(format!(
                    "failed to finish atomic cleanup: {error}"
                ))
            })
    }

    async fn resume_existing_operation(
        &self,
        state_db: &StateDbHandle,
        record: &EmpatraAtomicOperationRecord,
        request_id: &ConnectionRequestId,
        supports_openai_form_elicitation: bool,
    ) -> Result<ExistingOperation, JSONRPCErrorError> {
        let result = || AtomicCommandResult {
            operation_id: record.operation_id.clone(),
            thread_id: record.thread_id.clone(),
            turn_id: record.turn_id.clone(),
        };
        match record.state {
            EmpatraAtomicOperationState::Completed => {
                self.publish_empatra_outbox(
                    state_db,
                    &record.operation_id,
                    request_id,
                    supports_openai_form_elicitation,
                )
                .await?;
                Ok(ExistingOperation::Return(result()))
            }
            EmpatraAtomicOperationState::Failed => Err(invalid_request(
                record
                    .error
                    .clone()
                    .unwrap_or_else(|| "atomic creation failed".to_string()),
            )),
            EmpatraAtomicOperationState::Accepted => {
                state_db
                    .complete_and_enqueue_empatra_atomic_publication(&record.operation_id)
                    .await
                    .map_err(|error| {
                        crate::error_code::internal_error(format!(
                            "failed to complete accepted atomic operation: {error}"
                        ))
                    })?;
                self.publish_empatra_outbox(
                    state_db,
                    &record.operation_id,
                    request_id,
                    supports_openai_form_elicitation,
                )
                .await?;
                Ok(ExistingOperation::Return(result()))
            }
            EmpatraAtomicOperationState::CleanupPending => {
                let failure_code = record.error.as_deref().unwrap_or("atomic_cleanup_failed");
                let thread_id = ThreadId::from_string(&record.thread_id).map_err(|error| {
                    crate::error_code::internal_error(format!("invalid cleanup thread id: {error}"))
                })?;
                self.thread_processor
                    .cleanup_empatra_atomic_thread(thread_id)
                    .await?;
                state_db
                    .finish_empatra_atomic_cleanup(&record.operation_id, failure_code)
                    .await
                    .map_err(|error| {
                        crate::error_code::internal_error(format!(
                            "failed to finish recovered atomic cleanup: {error}"
                        ))
                    })?;
                Err(invalid_request(failure_code))
            }
            EmpatraAtomicOperationState::Reserved => Ok(ExistingOperation::Continue),
        }
    }

    async fn await_empatra_initial_turn_bundle(
        &self,
        started: &AtomicStartedThread,
        turn_id: &str,
        expected_input: &[codex_app_server_protocol::UserInput],
        mut durability_receipt: tokio::sync::oneshot::Receiver<
            Result<tokio::sync::oneshot::Sender<()>, String>,
        >,
    ) -> Result<(Vec<Event>, tokio::sync::oneshot::Sender<()>), JSONRPCErrorError> {
        const MAX_INITIAL_EVENTS: usize = 256;
        let mut events = Vec::new();
        let mut event_bytes = 0usize;
        let mut turn_started = false;
        let mut release = None;
        timeout(Duration::from_secs(30), async {
            while !turn_started || release.is_none() {
                tokio::select! {
                    receipt = &mut durability_receipt, if release.is_none() => {
                        release = Some(receipt
                            .map_err(|_| crate::error_code::internal_error("atomic initial-turn durability receipt was dropped"))?
                            .map_err(|_| crate::error_code::internal_error("atomic initial-turn persistence failed"))?);
                    }
                    event = started.thread.next_event(), if !turn_started => {
                        let event = event.map_err(|error| crate::error_code::internal_error(format!("failed waiting for atomic initial events: {error}")))?;
                        event_bytes = event_bytes.saturating_add(
                            serde_json::to_vec(&event)
                                .map_err(|error| crate::error_code::internal_error(format!("failed to bound atomic initial event: {error}")))?
                                .len(),
                        );
                        if events.len() >= MAX_INITIAL_EVENTS || event_bytes > crate::empatra_atomic::MAX_INITIAL_EVENT_BYTES {
                            return Err(crate::error_code::internal_error("atomic initial event bundle exceeded its bound"));
                        }
                        turn_started = matches!(
                            &event.msg,
                            EventMsg::TurnStarted(started) if started.turn_id == turn_id
                        );
                        events.push(event);
                    }
                }
            }
            Ok::<_, JSONRPCErrorError>(())
        })
        .await
        .map_err(|_| crate::error_code::internal_error("timed out waiting for durable atomic initial input"))??;

        self.thread_processor
            .verify_empatra_atomic_initial_turn(started.thread_id, turn_id, expected_input)
            .await?;
        Ok((
            events,
            release.ok_or_else(|| {
                crate::error_code::internal_error("atomic initial-turn release gate is missing")
            })?,
        ))
    }

    async fn execute_atomic_command(
        &self,
        request_id: ConnectionRequestId,
        command: AtomicCommand,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        supports_openai_form_elicitation: bool,
    ) -> Result<AtomicCommandResult, JSONRPCErrorError> {
        let AtomicCommand {
            operation_id,
            issued_at_ms,
            identity,
            thread,
            turn,
            thread_start_failure,
        } = command;
        let expected_input = turn.input.clone();
        let operation_lock = self.empatra_operation_lock(&operation_id).await;
        let _guard = operation_lock.lock().await;
        let (state_db, claim) = self
            .claim_empatra_operation(&operation_id, issued_at_ms, &identity)
            .await?;
        if let EmpatraAtomicOperationClaim::Existing(record) = &claim
            && let ExistingOperation::Return(result) = self
                .resume_existing_operation(
                    &state_db,
                    record,
                    &request_id,
                    supports_openai_form_elicitation,
                )
                .await?
        {
            return Ok(result);
        }

        let started = match claim {
            EmpatraAtomicOperationClaim::Claimed(_) => {
                let result = match thread {
                    AtomicThreadStart::Create {
                        params,
                        request_context,
                    } => {
                        self.thread_processor
                            .empatra_thread_start(
                                request_id.clone(),
                                params,
                                app_server_client_name.clone(),
                                app_server_client_version.clone(),
                                supports_openai_form_elicitation,
                                request_context,
                                identity.thread_id,
                            )
                            .await
                    }
                    AtomicThreadStart::Fork(params) => {
                        self.thread_processor
                            .empatra_thread_fork(
                                request_id.clone(),
                                params,
                                app_server_client_name.clone(),
                                app_server_client_version.clone(),
                                supports_openai_form_elicitation,
                                identity.thread_id,
                            )
                            .await
                    }
                };
                match result {
                    Ok(started) => started,
                    Err(error) => {
                        self.fail_empatra_before_acceptance(
                            &state_db,
                            &operation_id,
                            identity.thread_id,
                            thread_start_failure,
                        )
                        .await?;
                        return Err(error);
                    }
                }
            }
            EmpatraAtomicOperationClaim::Existing(_) => {
                let (started, persisted_events) = self
                    .thread_processor
                    .reconcile_empatra_atomic_thread(
                        &request_id,
                        identity.thread_id,
                        &identity.turn_id,
                        Some(&expected_input),
                        supports_openai_form_elicitation,
                    )
                    .await?;
                if let AtomicInitialEvidence::Complete(ref events) = persisted_events {
                    let events_json = serde_json::to_string(&events).map_err(|error| {
                        crate::error_code::internal_error(format!(
                            "failed to persist recovered atomic turn evidence: {error}"
                        ))
                    })?;
                    state_db
                        .accept_empatra_atomic_operation(&operation_id, &events_json)
                        .await
                        .map_err(|error| {
                            crate::error_code::internal_error(format!(
                                "failed to accept recovered atomic turn: {error}"
                            ))
                        })?;
                    state_db
                        .complete_and_enqueue_empatra_atomic_publication(&operation_id)
                        .await
                        .map_err(|error| {
                            crate::error_code::internal_error(format!(
                                "failed to publish recovered atomic turn: {error}"
                            ))
                        })?;
                    self.publish_empatra_outbox(
                        &state_db,
                        &operation_id,
                        &request_id,
                        supports_openai_form_elicitation,
                    )
                    .await?;
                    return Ok(AtomicCommandResult {
                        operation_id,
                        thread_id: identity.thread_id.to_string(),
                        turn_id: identity.turn_id,
                    });
                }
                if matches!(persisted_events, AtomicInitialEvidence::Partial) {
                    self.fail_empatra_before_acceptance(
                        &state_db,
                        &operation_id,
                        identity.thread_id,
                        "partial_initial_turn_evidence",
                    )
                    .await?;
                    return Err(invalid_request("partial_initial_turn_evidence"));
                }
                started
            }
        };

        let durability_receipt = match self
            .turn_processor
            .empatra_turn_start(
                request_id.clone(),
                started.thread_id,
                turn,
                identity.turn_id.clone(),
                app_server_client_name,
                app_server_client_version,
                supports_openai_form_elicitation,
            )
            .await
        {
            Ok((_, receipt)) => receipt,
            Err(error) => {
                self.fail_empatra_before_acceptance(
                    &state_db,
                    &operation_id,
                    identity.thread_id,
                    "initial_turn_submit_failed",
                )
                .await?;
                return Err(error);
            }
        };
        let (initial_events, release_initial_turn) = match self
            .await_empatra_initial_turn_bundle(
                &started,
                &identity.turn_id,
                &expected_input,
                durability_receipt,
            )
            .await
        {
            Ok(events) => events,
            Err(error) => {
                self.fail_empatra_before_acceptance(
                    &state_db,
                    &operation_id,
                    identity.thread_id,
                    "initial_turn_persistence_failed",
                )
                .await?;
                return Err(error);
            }
        };
        let events_json = serde_json::to_string(&initial_events).map_err(|error| {
            crate::error_code::internal_error(format!(
                "failed to persist atomic turn evidence: {error}"
            ))
        })?;
        state_db
            .accept_empatra_atomic_operation(&operation_id, &events_json)
            .await
            .map_err(|error| {
                crate::error_code::internal_error(format!("failed to accept atomic turn: {error}"))
            })?;
        state_db
            .complete_and_enqueue_empatra_atomic_publication(&operation_id)
            .await
            .map_err(|error| {
                crate::error_code::internal_error(format!(
                    "failed to publish atomic operation: {error}"
                ))
            })?;
        self.publish_empatra_outbox(
            &state_db,
            &operation_id,
            &request_id,
            supports_openai_form_elicitation,
        )
        .await?;
        // Publication is the commit point. A dropped release receiver means the durable,
        // nonempty turn reconstructs as interrupted; retry returns these IDs without resubmit.
        let _ = release_initial_turn.send(());
        Ok(AtomicCommandResult {
            operation_id,
            thread_id: identity.thread_id.to_string(),
            turn_id: identity.turn_id,
        })
    }

    pub(super) async fn drain_pending_empatra_publications(
        &self,
        request_id: &ConnectionRequestId,
        supports_openai_form_elicitation: bool,
    ) -> Result<(), JSONRPCErrorError> {
        let Some(state_db) = self.state_db.as_ref() else {
            return Ok(());
        };
        loop {
            let recoverable = state_db
                .recoverable_empatra_atomic_operations(256)
                .await
                .map_err(|error| {
                    crate::error_code::internal_error(format!(
                        "failed to list recoverable atomic operations: {error}"
                    ))
                })?;
            if recoverable.is_empty() {
                break;
            }
            for operation in recoverable {
                match operation.state {
                    EmpatraAtomicOperationState::Accepted => {
                        state_db
                            .complete_and_enqueue_empatra_atomic_publication(
                                &operation.operation_id,
                            )
                            .await
                            .map_err(|error| {
                                crate::error_code::internal_error(format!(
                                    "failed to recover accepted atomic operation: {error}"
                                ))
                            })?;
                    }
                    EmpatraAtomicOperationState::CleanupPending => {
                        let thread_id =
                            ThreadId::from_string(&operation.thread_id).map_err(|error| {
                                crate::error_code::internal_error(format!(
                                    "invalid recovered cleanup thread id: {error}"
                                ))
                            })?;
                        self.thread_processor
                            .cleanup_empatra_atomic_thread(thread_id)
                            .await?;
                        state_db
                            .finish_empatra_atomic_cleanup(
                                &operation.operation_id,
                                operation
                                    .error
                                    .as_deref()
                                    .unwrap_or("atomic_cleanup_failed"),
                            )
                            .await
                            .map_err(|error| {
                                crate::error_code::internal_error(format!(
                                    "failed to finish recovered atomic cleanup: {error}"
                                ))
                            })?;
                    }
                    EmpatraAtomicOperationState::Reserved
                    | EmpatraAtomicOperationState::Completed
                    | EmpatraAtomicOperationState::Failed => {}
                }
            }
        }
        loop {
            let pending = state_db
                .pending_empatra_atomic_publications(256)
                .await
                .map_err(|error| {
                    crate::error_code::internal_error(format!(
                        "failed to list atomic publications: {error}"
                    ))
                })?;
            if pending.is_empty() {
                return Ok(());
            }
            for publication in pending {
                self.publish_empatra_outbox(
                    state_db,
                    &publication.operation_id,
                    request_id,
                    supports_openai_form_elicitation,
                )
                .await?;
            }
        }
    }

    pub(super) async fn empatra_thread_create_and_start(
        &self,
        request_id: ConnectionRequestId,
        params: EmpatraThreadCreateAndStartParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        supports_openai_form_elicitation: bool,
        request_context: RequestContext,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let identity =
            crate::empatra_atomic::create_identity(&self.empatra_installation_id, &params)?;
        let result = self
            .execute_atomic_command(
                request_id,
                AtomicCommand {
                    operation_id: params.operation_id,
                    issued_at_ms: params.issued_at_ms,
                    identity,
                    thread: AtomicThreadStart::Create {
                        params: params.thread,
                        request_context,
                    },
                    turn: params.turn,
                    thread_start_failure: "thread_start_failed",
                },
                app_server_client_name,
                app_server_client_version,
                supports_openai_form_elicitation,
            )
            .await?;
        Ok(Some(
            EmpatraThreadCreateAndStartResponse {
                operation_id: result.operation_id,
                thread_id: result.thread_id,
                turn_id: result.turn_id,
            }
            .into(),
        ))
    }

    pub(super) async fn empatra_thread_fork_and_start(
        &self,
        request_id: ConnectionRequestId,
        params: EmpatraThreadForkAndStartParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        supports_openai_form_elicitation: bool,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let identity =
            crate::empatra_atomic::fork_identity(&self.empatra_installation_id, &params)?;
        let result = self
            .execute_atomic_command(
                request_id,
                AtomicCommand {
                    operation_id: params.operation_id,
                    issued_at_ms: params.issued_at_ms,
                    identity,
                    thread: AtomicThreadStart::Fork(params.thread),
                    turn: params.turn,
                    thread_start_failure: "thread_fork_failed",
                },
                app_server_client_name,
                app_server_client_version,
                supports_openai_form_elicitation,
            )
            .await?;
        Ok(Some(
            EmpatraThreadForkAndStartResponse {
                operation_id: result.operation_id,
                thread_id: result.thread_id,
                turn_id: result.turn_id,
            }
            .into(),
        ))
    }
}
