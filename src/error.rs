use thiserror::Error;

#[derive(Debug, Error)]
pub enum RebindError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("RPC timed out: {0}")]
    Timeout(String),

    #[error("server error [{code}]: {message}")]
    Server { code: String, message: String },

    #[error("websocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

impl RebindError {
    pub fn connection(msg: impl Into<String>) -> Self {
        Self::Connection(msg.into())
    }

    pub fn timeout(cmd: impl Into<String>) -> Self {
        Self::Timeout(cmd.into())
    }

    pub fn server(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Server {
            code: code.into(),
            message: message.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, RebindError>;
