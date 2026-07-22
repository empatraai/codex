use super::*;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::items::CollabAgentTool as CoreCollabAgentTool;
use codex_protocol::items::CollabAgentToolCallItem;
use codex_protocol::items::CollabAgentToolCallStatus as CoreCollabAgentToolCallStatus;
use codex_protocol::items::SubAgentActivityItem;
use codex_protocol::items::TurnItem as CoreTurnItem;
use codex_protocol::items::WorkSwarmActivityItem;
use codex_protocol::protocol::AgentStatus as CoreAgentStatus;
use codex_protocol::protocol::CollabAgentRef;
use codex_protocol::protocol::SubAgentActivityKind as CoreSubAgentActivityKind;
use codex_protocol::protocol::TurnWorkSwarmCommunicationProgress as CoreTurnWorkSwarmCommunicationProgress;
use codex_protocol::protocol::TurnWorkSwarmProgressStatus as CoreTurnWorkSwarmProgressStatus;
use codex_protocol::protocol::WorkSwarmProgress;
use pretty_assertions::assert_eq;
use std::collections::HashMap;

#[test]
fn converts_collab_agent_tool_call_into_thread_item() {
    let sender_thread_id = ThreadId::new();
    let receiver_thread_id = ThreadId::new();
    let core_item = CoreTurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
        id: "collab-1".to_string(),
        tool: CoreCollabAgentTool::SpawnAgent,
        status: CoreCollabAgentToolCallStatus::Completed,
        sender_thread_id,
        receiver_thread_ids: vec![receiver_thread_id],
        receiver_agents: vec![CollabAgentRef {
            thread_id: receiver_thread_id,
            agent_nickname: Some("researcher".to_string()),
            agent_role: Some("explorer".to_string()),
        }],
        prompt: Some("map the protocol".to_string()),
        model: Some("test-model".to_string()),
        reasoning_effort: Some(ReasoningEffort::High),
        agents_states: HashMap::from([(
            receiver_thread_id,
            CoreAgentStatus::Completed(Some("done".to_string())),
        )]),
    });

    assert_eq!(
        ThreadItem::from(core_item),
        ThreadItem::CollabAgentToolCall {
            id: "collab-1".to_string(),
            tool: CollabAgentTool::SpawnAgent,
            status: CollabAgentToolCallStatus::Completed,
            sender_thread_id: sender_thread_id.to_string(),
            receiver_thread_ids: vec![receiver_thread_id.to_string()],
            prompt: Some("map the protocol".to_string()),
            model: Some("test-model".to_string()),
            reasoning_effort: Some(ReasoningEffort::High),
            agents_states: HashMap::from([(
                receiver_thread_id.to_string(),
                CollabAgentState {
                    status: CollabAgentStatus::Completed,
                    message: Some("done".to_string()),
                },
            )]),
        }
    );
}

#[test]
fn converts_sub_agent_activity_into_thread_item() {
    let agent_thread_id = ThreadId::new();
    let core_item = CoreTurnItem::SubAgentActivity(SubAgentActivityItem {
        id: "activity-1".to_string(),
        kind: CoreSubAgentActivityKind::Interrupted,
        agent_thread_id,
        agent_path: AgentPath::try_from("/root/researcher").expect("valid agent path"),
    });

    assert_eq!(
        ThreadItem::from(core_item),
        ThreadItem::SubAgentActivity {
            id: "activity-1".to_string(),
            kind: SubAgentActivityKind::Interrupted,
            agent_thread_id: agent_thread_id.to_string(),
            agent_path: "/root/researcher".to_string(),
        }
    );
}

#[test]
fn converts_work_swarm_activity_into_thread_item() {
    let core_item = CoreTurnItem::WorkSwarmActivity(WorkSwarmActivityItem {
        id: "work_swarm:run-1".to_string(),
        progress: WorkSwarmProgress {
            run_id: "run-1".to_string(),
            status: CoreTurnWorkSwarmProgressStatus::Running,
            title: Some("Release pipeline".to_string()),
            max_concurrency: 3,
            runtime_used_seconds: 12,
            runtime_budget_seconds: Some(90),
            total: 2,
            queued: 0,
            running: 1,
            succeeded: 1,
            failed: 0,
            cancelled: 0,
            skipped: 0,
            tokens_used: 42,
            token_budget: Some(100),
            task_id: None,
            agent_path: None,
            model: Some("gpt-test".to_string()),
            fallback_reason: None,
            error: None,
            tasks: Vec::new(),
            communication: CoreTurnWorkSwarmCommunicationProgress {
                queued: 0,
                delivered: 0,
                acked: 0,
                expired: 0,
                dead_lettered: 0,
                cancelled: 0,
                messages: Vec::new(),
                messages_truncated: false,
            },
        },
    });

    let item = ThreadItem::from(core_item);
    assert_eq!(item.id(), "work_swarm:run-1");
    let ThreadItem::WorkSwarmActivity {
        run_id,
        status,
        running,
        succeeded,
        model,
        ..
    } = item
    else {
        panic!("expected work swarm activity");
    };
    assert_eq!(run_id, "run-1");
    assert_eq!(status, TurnWorkSwarmProgressStatus::Running);
    assert_eq!(running, 1);
    assert_eq!(succeeded, 1);
    assert_eq!(model.as_deref(), Some("gpt-test"));
}
