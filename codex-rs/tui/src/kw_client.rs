//! TCP client for KuiWeaving daemon communication.
//!
//! Connects to KuiWeaving daemon via TCP (localhost:9528),
//! sends turn/interrupt/status messages, receives streaming events.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Top-level KuiWeaving daemon client.
pub struct KwClient {
    reader: BufReader<TcpStream>,
    addr: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum KwEvent {
    #[serde(rename = "thinking_delta")]
    Thinking { turn_id: String, delta: String },
    #[serde(rename = "tool_call")]
    ToolCall {
        turn_id: String,
        tool_name: String,
        #[serde(default)]
        tool_input: serde_json::Value,
        #[serde(default)]
        source: String,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        turn_id: String,
        tool_name: String,
        result: String,
        #[serde(default)]
        success: bool,
        #[serde(default)]
        duration_ms: u64,
    },
    #[serde(rename = "turn_complete")]
    TurnComplete {
        turn_id: String,
        final_response: String,
        #[serde(default)]
        importance: f64,
        #[serde(default)]
        tool_count: u32,
        #[serde(default)]
        spine_updates: serde_json::Value,
    },
    #[serde(rename = "loop_event")]
    LoopEvent {
        #[serde(rename = "loop")]
        loop_name: String,
        payload: serde_json::Value,
    },
    #[serde(rename = "status_ack")]
    StatusAck {
        loops: serde_json::Value,
    },
    #[serde(rename = "error")]
    Error {
        turn_id: Option<String>,
        code: String,
        message: String,
    },
}

#[derive(Debug, Serialize)]
struct KwMessage {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    working_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    codex_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_context: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_ctx_delta: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
}

impl KwClient {
    /// Connect to KuiWeaving daemon at the given address (e.g. "127.0.0.1:9528").
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to KuiWeaving daemon at {}",
                    addr
                )
            })?;

        Ok(Self {
            reader: BufReader::new(stream),
            addr: addr.to_string(),
        })
    }

    /// Send handshake with Codex version and plugin context.
    pub async fn handshake(
        &mut self,
        codex_version: &str,
        plugin_context: serde_json::Value,
    ) -> Result<KwEvent> {
        let msg = KwMessage {
            msg_type: "handshake".to_string(),
            codex_version: Some(codex_version.to_string()),
            plugin_context: Some(plugin_context),
            turn_id: None,
            input: None,
            working_dir: None,
            session_id: None,
            plugin_ctx_delta: None,
            history: None,
            mode: None,
        };
        self.send_msg(&msg).await?;
        self.read_event().await
    }

    /// Send a user turn and return an event stream.
    pub async fn send_turn(
        &mut self,
        turn_id: &str,
        input: &str,
        working_dir: &std::path::Path,
        session_id: &str,
    ) -> Result<KwTurnStream> {
        let msg = KwMessage {
            msg_type: "turn".to_string(),
            turn_id: Some(turn_id.to_string()),
            input: Some(input.to_string()),
            working_dir: Some(working_dir.display().to_string()),
            session_id: Some(session_id.to_string()),
            codex_version: None,
            plugin_context: None,
            plugin_ctx_delta: None,
            history: None,
            mode: None,
        };
        self.send_msg(&msg).await?;

        Ok(KwTurnStream {
            client_ref: self as *const Self,
            turn_id: turn_id.to_string(),
        })
    }

    /// Send interrupt for an active turn.
    pub async fn interrupt(&mut self, turn_id: &str) -> Result<()> {
        let msg = KwMessage {
            msg_type: "interrupt".to_string(),
            turn_id: Some(turn_id.to_string()),
            input: None,
            working_dir: None,
            session_id: None,
            codex_version: None,
            plugin_context: None,
            plugin_ctx_delta: None,
            history: None,
            mode: None,
        };
        self.send_msg(&msg).await
    }

    /// Send a status ping and get daemon status.
    pub async fn status(&mut self) -> Result<KwEvent> {
        let msg = KwMessage {
            msg_type: "status".to_string(),
            turn_id: None,
            input: None,
            working_dir: None,
            session_id: None,
            codex_version: None,
            plugin_context: None,
            plugin_ctx_delta: None,
            history: None,
            mode: None,
        };
        self.send_msg(&msg).await?;
        self.read_event().await
    }

    /// Read one JSON-line event from the socket.
    pub async fn read_event(&mut self) -> Result<KwEvent> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            anyhow::bail!("Empty response from KuiWeaving daemon");
        }
        serde_json::from_str(trimmed).context("Failed to parse KuiWeaving event")
    }

    async fn send_msg(&mut self, msg: &KwMessage) -> Result<()> {
        let mut json = serde_json::to_string(msg)?;
        json.push('\n');
        self.reader.get_mut().write_all(json.as_bytes()).await?;
        self.reader.get_mut().flush().await?;
        Ok(())
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }
}

/// Streaming iterator over a single turn's events.
pub struct KwTurnStream {
    client_ref: *const KwClient,
    turn_id: String,
}

unsafe impl Send for KwTurnStream {}
unsafe impl Sync for KwTurnStream {}

impl KwTurnStream {
    /// Read the next event in this turn. Returns None when turn completes or errors.
    pub async fn next_event(&self) -> Option<KwEvent> {
        let client = unsafe { &mut *(self.client_ref as *mut KwClient) };
        loop {
            match client.read_event().await {
                Ok(event @ KwEvent::TurnComplete { .. }) => return Some(event),
                Ok(event) => return Some(event),
                Err(_) => return None,
            }
        }
    }
}
