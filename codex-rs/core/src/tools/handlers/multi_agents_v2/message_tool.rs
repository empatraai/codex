//! Shared argument parsing and dispatch for the v2 agent messaging tools.
//!
//! `send_message` and `followup_task` share the same submission path and differ only in whether the
//! resulting `InterAgentCommunication` should wake the target immediately.

use super::*;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::tools::context::FunctionToolOutput;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::InterAgentCommunicationPriority;
use serde_json::json;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MessageDeliveryMode {
    QueueOnly,
    TriggerTurn,
}

impl MessageDeliveryMode {
    /// Returns whether the produced communication should start a turn immediately.
    fn apply(self, communication: InterAgentCommunication) -> InterAgentCommunication {
        match self {
            Self::QueueOnly => InterAgentCommunication {
                trigger_turn: false,
                ..communication
            },
            Self::TriggerTurn => InterAgentCommunication {
                trigger_turn: true,
                ..communication
            },
        }
    }
}

const MAX_MESSAGE_METADATA_ID_LEN: usize = 128;
const MAX_MESSAGE_TOPIC_LEN: usize = 128;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Input for the MultiAgentV2 `send_message` tool.
pub(crate) struct SendMessageArgs {
    pub(crate) target: String,
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) run_id: Option<String>,
    #[serde(default)]
    pub(crate) correlation_id: Option<String>,
    #[serde(default)]
    pub(crate) in_reply_to: Option<String>,
    #[serde(default)]
    pub(crate) topic: Option<String>,
    #[serde(default)]
    pub(crate) priority: Option<InterAgentCommunicationPriority>,
    #[serde(default)]
    pub(crate) ttl_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Input for the MultiAgentV2 `followup_task` tool.
pub(crate) struct FollowupTaskArgs {
    pub(crate) target: String,
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) run_id: Option<String>,
    #[serde(default)]
    pub(crate) correlation_id: Option<String>,
    #[serde(default)]
    pub(crate) in_reply_to: Option<String>,
    #[serde(default)]
    pub(crate) topic: Option<String>,
    #[serde(default)]
    pub(crate) priority: Option<InterAgentCommunicationPriority>,
    #[serde(default)]
    pub(crate) ttl_seconds: Option<i64>,
}

pub(super) fn message_content(message: String) -> Result<String, FunctionCallError> {
    if message.trim().is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "Empty message can't be sent to an agent".to_string(),
        ));
    }
    Ok(message)
}

fn bounded_optional_string(
    value: Option<String>,
    field_name: &str,
    max_len: usize,
) -> Result<Option<String>, FunctionCallError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(FunctionCallError::RespondToModel(format!(
            "{field_name} can't be empty"
        )));
    }
    if value.len() > max_len {
        return Err(FunctionCallError::RespondToModel(format!(
            "{field_name} is too long"
        )));
    }
    Ok(Some(value.to_string()))
}

fn trimmed_required_string(value: String, field_name: &str) -> Result<String, FunctionCallError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(FunctionCallError::RespondToModel(format!(
            "{field_name} can't be empty"
        )));
    }
    Ok(value.to_string())
}

fn validate_ttl_seconds(ttl_seconds: Option<i64>) -> Result<Option<i64>, FunctionCallError> {
    match ttl_seconds {
        None => Ok(None),
        Some(ttl_seconds) if (1..=86_400).contains(&ttl_seconds) => Ok(Some(ttl_seconds)),
        Some(_) => Err(FunctionCallError::RespondToModel(
            "ttl_seconds must be between 1 and 86400".to_string(),
        )),
    }
}

fn apply_message_metadata(
    communication: &mut InterAgentCommunication,
    run_id: Option<String>,
    correlation_id: Option<String>,
    in_reply_to: Option<String>,
    topic: Option<String>,
    priority: Option<InterAgentCommunicationPriority>,
    ttl_seconds: Option<i64>,
) {
    communication.run_id = run_id;
    communication.correlation_id = correlation_id;
    communication.in_reply_to = in_reply_to;
    communication.topic = topic;
    communication.priority = priority;
    communication.ttl_seconds = ttl_seconds;
}

/// Handles the shared MultiAgentV2 message flow for both `send_message` and `followup_task`.
pub(crate) async fn handle_message_string_tool(
    invocation: ToolInvocation,
    mode: MessageDeliveryMode,
    target: String,
    message: String,
    run_id: Option<String>,
    correlation_id: Option<String>,
    in_reply_to: Option<String>,
    topic: Option<String>,
    priority: Option<InterAgentCommunicationPriority>,
    ttl_seconds: Option<i64>,
) -> Result<FunctionToolOutput, FunctionCallError> {
    let target = trimmed_required_string(target, "target")?;
    let message = message_content(message)?;
    let run_id = bounded_optional_string(run_id, "run_id", MAX_MESSAGE_METADATA_ID_LEN)?;
    let correlation_id = bounded_optional_string(
        correlation_id,
        "correlation_id",
        MAX_MESSAGE_METADATA_ID_LEN,
    )?;
    let in_reply_to =
        bounded_optional_string(in_reply_to, "in_reply_to", MAX_MESSAGE_METADATA_ID_LEN)?;
    let topic = bounded_optional_string(topic, "topic", MAX_MESSAGE_TOPIC_LEN)?;
    let ttl_seconds = validate_ttl_seconds(ttl_seconds)?;
    let ToolInvocation {
        session,
        turn,
        call_id,
        ..
    } = invocation;
    let receiver_thread_id = resolve_agent_target(&session, &turn, &target).await?;
    let receiver_agent = session
        .services
        .agent_control
        .ensure_agent_known(receiver_thread_id)
        .map_err(|err| collab_agent_error(receiver_thread_id, err))?;
    if mode == MessageDeliveryMode::TriggerTurn
        && receiver_agent
            .agent_path
            .as_ref()
            .is_some_and(AgentPath::is_root)
    {
        return Err(FunctionCallError::RespondToModel(
            "Follow-up tasks can't target the root agent".to_string(),
        ));
    }
    let receiver_agent_path = receiver_agent.agent_path.clone().ok_or_else(|| {
        FunctionCallError::RespondToModel("target agent is missing an agent_path".to_string())
    })?;
    let resume_config = build_agent_resume_config(turn.as_ref())?;
    session
        .services
        .agent_control
        .ensure_v2_agent_loaded(resume_config, receiver_thread_id)
        .await
        .map_err(|err| collab_agent_error(receiver_thread_id, err))?;
    let author = turn
        .session_source
        .get_agent_path()
        .unwrap_or_else(AgentPath::root);
    let communication =
        communication_from_tool_message(author, receiver_agent_path.clone(), message);
    let mut communication = communication;
    let message_id = uuid::Uuid::now_v7().to_string();
    communication.id = Some(message_id.clone());
    apply_message_metadata(
        &mut communication,
        run_id,
        correlation_id,
        in_reply_to,
        topic,
        priority,
        ttl_seconds,
    );
    let kind = match mode {
        MessageDeliveryMode::QueueOnly => AgentCommunicationKind::Message,
        MessageDeliveryMode::TriggerTurn => AgentCommunicationKind::Followup,
    };
    let context = AgentCommunicationContext::new(kind, session.thread_id);
    let result = session
        .services
        .agent_control
        .send_inter_agent_communication(receiver_thread_id, mode.apply(communication), context)
        .await
        .map_err(|err| collab_agent_error(receiver_thread_id, err));
    result?;
    emit_sub_agent_activity(
        &session,
        &turn,
        SubAgentActivityItem {
            id: call_id,
            agent_thread_id: receiver_thread_id,
            agent_path: receiver_agent_path,
            kind: SubAgentActivityKind::Interacted,
        },
    )
    .await;

    Ok(FunctionToolOutput::from_text(
        json!({
            "delivered": true,
            "message_id": message_id,
        })
        .to_string(),
        Some(true),
    ))
}

#[cfg(test)]
mod tests {
    use super::validate_ttl_seconds;

    #[test]
    fn ttl_validation_accepts_omission_and_inclusive_boundaries() {
        assert_eq!(validate_ttl_seconds(None).unwrap(), None);
        assert_eq!(validate_ttl_seconds(Some(1)).unwrap(), Some(1));
        assert_eq!(validate_ttl_seconds(Some(86_400)).unwrap(), Some(86_400));
    }

    #[test]
    fn ttl_validation_rejects_values_outside_delivery_bounds() {
        assert!(validate_ttl_seconds(Some(0)).is_err());
        assert!(validate_ttl_seconds(Some(86_401)).is_err());
    }
}
