//! Rebind remote access client.
//!
//! ```no_run
//! use rebind_client::RebindClient;
//!
//! #[tokio::main]
//! async fn main() -> rebind_client::Result<()> {
//!     let mut client = RebindClient::connect("ws://127.0.0.1:19561").await?;
//!
//!     client.hid_move(30, -5);
//!     let (x, y) = client.system_mouse().await?;
//!     println!("mouse at {x},{y}");
//!
//!     client.close().await;
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod types;

mod client;

pub use client::RebindClient;
pub use error::{RebindError, Result};
pub use types::*;
