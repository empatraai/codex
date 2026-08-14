use codex_app_server_protocol::EmpatraInitialTurnParams;
use codex_app_server_protocol::EmpatraThreadCreateAndStartParams;
use codex_app_server_protocol::EmpatraThreadForkAndStartParams;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::UserInput;
use codex_protocol::ThreadId;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::time::SystemTime;
use uuid::Uuid;

use crate::error_code::invalid_request;

const MAX_OPERATION_ID_BYTES: usize = 256;
const MAX_INITIAL_INPUT_ITEMS: usize = 256;
pub(crate) const MAX_INITIAL_INPUT_BYTES: usize = 256 * 1024;
pub(crate) const MAX_INITIAL_EVENT_BYTES: usize = 2 * MAX_INITIAL_INPUT_BYTES + 64 * 1024;
const MAX_CLOCK_SKEW_MS: i64 = 24 * 60 * 60 * 1_000;
const UUID_V7_MAX_TIMESTAMP_MS: i64 = (1_i64 << 48) - 1;

pub(crate) struct AtomicIdentity {
    pub(crate) payload_digest: String,
    pub(crate) thread_id: ThreadId,
    pub(crate) turn_id: String,
}

pub(crate) fn create_identity(
    installation_id: &str,
    params: &EmpatraThreadCreateAndStartParams,
) -> Result<AtomicIdentity, JSONRPCErrorError> {
    validate_common(&params.operation_id, params.issued_at_ms, &params.turn)?;
    if params.thread.allow_provider_model_fallback {
        return Err(invalid_request(
            "Empatra atomic creation forbids provider model fallback",
        ));
    }
    if params.thread.ephemeral == Some(true) {
        return Err(invalid_request(
            "Empatra atomic creation requires durable thread history",
        ));
    }
    identity(
        "create",
        installation_id,
        &params.operation_id,
        params.issued_at_ms,
        &(params.issued_at_ms, &params.thread, &params.turn),
    )
}

pub(crate) fn fork_identity(
    installation_id: &str,
    params: &EmpatraThreadForkAndStartParams,
) -> Result<AtomicIdentity, JSONRPCErrorError> {
    validate_common(&params.operation_id, params.issued_at_ms, &params.turn)?;
    if params.thread.ephemeral {
        return Err(invalid_request(
            "Empatra atomic creation requires durable thread history",
        ));
    }
    identity(
        "fork",
        installation_id,
        &params.operation_id,
        params.issued_at_ms,
        &(params.issued_at_ms, &params.thread, &params.turn),
    )
}

fn validate_common(
    operation_id: &str,
    issued_at_ms: i64,
    turn: &EmpatraInitialTurnParams,
) -> Result<(), JSONRPCErrorError> {
    if operation_id.is_empty()
        || operation_id.len() > MAX_OPERATION_ID_BYTES
        || operation_id.trim() != operation_id
        || operation_id.chars().any(char::is_control)
    {
        return Err(invalid_request(
            "operationId must be 1..256 UTF-8 bytes, trimmed, and contain no control characters",
        ));
    }
    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|error| invalid_request(format!("system clock is before Unix epoch: {error}")))?
        .as_millis()
        .min(i64::MAX as u128) as i64;
    if !(0..=UUID_V7_MAX_TIMESTAMP_MS).contains(&issued_at_ms)
        || issued_at_ms > now_ms.saturating_add(MAX_CLOCK_SKEW_MS)
    {
        return Err(invalid_request(
            "issuedAtMs is outside the UUIDv7 timestamp range or too far in the future",
        ));
    }
    if turn.input.is_empty() || turn.input.len() > MAX_INITIAL_INPUT_ITEMS {
        return Err(invalid_request("initial input must contain 1..256 items"));
    }
    let encoded = serde_json::to_vec(&turn.input)
        .map_err(|error| invalid_request(format!("invalid initial input: {error}")))?;
    if encoded.len() > MAX_INITIAL_INPUT_BYTES {
        return Err(invalid_request(
            "initial input exceeds the 256 KiB encoded limit",
        ));
    }
    // #122 owns transport of controller-local paths and capability references.
    if turn.input.iter().any(
        |item| !matches!(item, UserInput::Text { text_elements, .. } if text_elements.is_empty()),
    ) {
        return Err(invalid_request(
            "Empatra atomic creation currently accepts text input only",
        ));
    }
    if !turn
        .input
        .iter()
        .any(|item| matches!(item, UserInput::Text { text, .. } if !text.is_empty()))
    {
        return Err(invalid_request("initial text input must not be empty"));
    }
    Ok(())
}

fn identity<T: Serialize>(
    operation_kind: &str,
    installation_id: &str,
    operation_id: &str,
    issued_at_ms: i64,
    payload: &T,
) -> Result<AtomicIdentity, JSONRPCErrorError> {
    let canonical = canonical_json(payload)?;
    let payload_digest = hex_digest(&[
        b"empatra.atomic.payload.v1\0",
        operation_kind.as_bytes(),
        b"\0",
        &canonical,
    ]);
    let thread_uuid = deterministic_uuid_v7(
        issued_at_ms,
        b"empatra.atomic.thread.v1\0",
        installation_id,
        operation_id,
    );
    let turn_uuid = deterministic_uuid_v7(
        issued_at_ms,
        b"empatra.atomic.turn.v1\0",
        installation_id,
        operation_id,
    );
    let thread_id = ThreadId::from_string(&thread_uuid.to_string())
        .map_err(|error| invalid_request(format!("failed to derive thread id: {error}")))?;
    Ok(AtomicIdentity {
        payload_digest,
        thread_id,
        turn_id: turn_uuid.to_string(),
    })
}

fn deterministic_uuid_v7(
    issued_at_ms: i64,
    domain: &[u8],
    installation_id: &str,
    operation_id: &str,
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(installation_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(operation_id.as_bytes());
    let digest = hasher.finalize();
    let timestamp = issued_at_ms as u64;
    let mut bytes = [0_u8; 16];
    bytes[..6].copy_from_slice(&timestamp.to_be_bytes()[2..]);
    bytes[6..].copy_from_slice(&digest[..10]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, JSONRPCErrorError> {
    let value = serde_json::to_value(value)
        .map_err(|error| invalid_request(format!("invalid atomic payload: {error}")))?;
    serde_json::to_vec(&sort_json(value))
        .map_err(|error| invalid_request(format!("invalid atomic payload: {error}")))
}

fn sort_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json).collect()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, sort_json(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        value => value,
    }
}

fn hex_digest(parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::ThreadStartParams;

    fn params() -> EmpatraThreadCreateAndStartParams {
        EmpatraThreadCreateAndStartParams {
            operation_id: "command-1".to_string(),
            issued_at_ms: 1_700_000_000_123,
            thread: ThreadStartParams::default(),
            turn: EmpatraInitialTurnParams {
                client_user_message_id: None,
                input: vec![UserInput::Text {
                    text: "hello".to_string(),
                    text_elements: vec![],
                }],
                cwd: None,
                approval_policy: None,
                sandbox_policy: None,
                permissions: None,
                model: None,
                effort: None,
                collaboration_mode: None,
            },
        }
    }

    #[test]
    fn identity_is_stable_uuidv7_and_payload_bound() {
        let params = params();
        let first = create_identity("installation", &params).unwrap();
        let second = create_identity("installation", &params).unwrap();
        assert_eq!(first.thread_id, second.thread_id);
        assert_eq!(first.turn_id, second.turn_id);
        assert_eq!(first.payload_digest, second.payload_digest);
        let uuid = Uuid::parse_str(&first.thread_id.to_string()).unwrap();
        assert_eq!(uuid.get_version_num(), 7);
        assert_eq!(uuid.get_timestamp().unwrap().to_unix().1 / 1_000_000, 123);
        assert_ne!(first.thread_id.to_string(), first.turn_id);

        let mut changed = params;
        changed.turn.input = vec![UserInput::Text {
            text: "changed".to_string(),
            text_elements: vec![],
        }];
        assert_ne!(
            first.payload_digest,
            create_identity("installation", &changed)
                .unwrap()
                .payload_digest
        );
    }

    #[test]
    fn validation_rejects_unbounded_or_path_bearing_requests() {
        let mut value = params();
        value.operation_id = " command-1".to_string();
        assert!(create_identity("installation", &value).is_err());
        let mut value = params();
        value.turn.input.clear();
        assert!(create_identity("installation", &value).is_err());
        let mut value = params();
        value.issued_at_ms = UUID_V7_MAX_TIMESTAMP_MS;
        assert!(create_identity("installation", &value).is_err());
        let mut value = params();
        value.turn.input = vec![UserInput::Text {
            text: String::new(),
            text_elements: vec![],
        }];
        assert!(create_identity("installation", &value).is_err());
        let mut value = params();
        value.turn.input = vec![UserInput::Text {
            text: "hello".to_string(),
            text_elements: vec![codex_app_server_protocol::TextElement::new(
                codex_app_server_protocol::ByteRange { start: 0, end: 5 },
                Some("path".to_string()),
            )],
        }];
        assert!(create_identity("installation", &value).is_err());
        let mut value = params();
        value.thread.ephemeral = Some(true);
        assert!(create_identity("installation", &value).is_err());
    }
}
