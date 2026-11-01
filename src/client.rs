use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::error::{RebindError, Result};
use crate::types::*;

type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>;

/// Async WebSocket client for the Rebind remote access protocol.
///
/// All HID write methods are fire-and-forget (synchronous, queue internally).
/// All read methods are async and return typed results.
///
/// # Example
///
/// ```no_run
/// use rebind_client::RebindClient;
///
/// #[tokio::main]
/// async fn main() -> rebind_client::Result<()> {
///     let mut client = RebindClient::connect("ws://127.0.0.1:19561").await?;
///     client.hid_move(30, -5);
///     let (x, y) = client.system_mouse().await?;
///     println!("{x} {y}");
///     client.close().await;
///     Ok(())
/// }
/// ```
pub struct RebindClient {
    sender: mpsc::UnboundedSender<Message>,
    pending: PendingMap,
    next_id: Arc<AtomicU64>,
    timeout_ms: u64,
    // event stream senders keyed by event name
    event_senders: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Value>>>>,
    // background task handle — kept alive until close()
    _task: tokio::task::JoinHandle<()>,
}

impl RebindClient {
    /// Connect to a Rebind relay. Authenticates if `token` is non-empty.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_options(url, "", 5000).await
    }

    pub async fn connect_with_token(url: &str, token: &str) -> Result<Self> {
        Self::connect_with_options(url, token, 5000).await
    }

    pub async fn connect_with_options(url: &str, token: &str, timeout_ms: u64) -> Result<Self> {
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let event_senders: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Value>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let next_id = Arc::new(AtomicU64::new(1));

        // read the hello banner
        let banner = match read.next().await {
            Some(Ok(Message::Text(raw))) => serde_json::from_str::<Value>(&raw)?,
            _ => return Err(RebindError::connection("no hello banner received")),
        };
        if banner.get("t").and_then(|v| v.as_str()) != Some("hello") {
            return Err(RebindError::connection("unexpected banner"));
        }

        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        // writer task
        let writer_task = {
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    if write.send(msg).await.is_err() {
                        break;
                    }
                }
            })
        };

        let pending_reader = pending.clone();
        let event_senders_reader = event_senders.clone();

        // reader task
        let reader_task = tokio::spawn(async move {
            while let Some(Ok(frame)) = read.next().await {
                let Message::Text(raw) = frame else {
                    continue;
                };
                let Ok(msg) = serde_json::from_str::<Value>(&raw) else {
                    continue;
                };
                if let Some(id) = msg.get("id").and_then(|v| v.as_u64()) {
                    let mut map = pending_reader.lock().await;
                    if let Some(tx) = map.remove(&id) {
                        let _ = tx.send(msg);
                    }
                } else if let Some(t) = msg.get("t").and_then(|v| v.as_str()) {
                    let senders = event_senders_reader.lock().await;
                    if let Some(sender) = senders.get(t) {
                        let _ = sender.send(msg);
                    }
                }
            }
            // stop the writer first so its receiver drops and later RPCs see a
            // closed sender, then fail in-flight RPCs and end every event
            // receiver by dropping their senders
            writer_task.abort();
            let _ = writer_task.await;
            pending_reader.lock().await.clear();
            event_senders_reader.lock().await.clear();
        });

        let client = Self {
            sender: tx.clone(),
            pending,
            next_id,
            timeout_ms,
            event_senders,
            _task: reader_task,
        };

        // authenticate if token provided
        if !token.is_empty() {
            let result = client.rpc(json!({ "t": "auth", "token": token })).await?;
            if result.get("ok").and_then(|v| v.as_bool()) != Some(true) {
                return Err(RebindError::server("bad_token", "server rejected token"));
            }
        }

        Ok(client)
    }

    /// Close the connection gracefully.
    pub async fn close(self) {
        let _ = self.sender.send(Message::Close(None));
        self._task.abort();
    }

    // ── HID writes (fire-and-forget) ──────────────────────────────────────

    pub fn hid_down(&self, code: &str) {
        self.one_shot(json!({ "t": "hid.down", "code": code }));
    }

    pub fn hid_up(&self, code: &str) {
        self.one_shot(json!({ "t": "hid.up", "code": code }));
    }

    pub fn hid_press(&self, code: &str, hold_ms: u32) {
        self.one_shot(json!({ "t": "hid.press", "code": code, "hold_ms": hold_ms }));
    }

    pub fn hid_type(&self, text: &str) {
        self.one_shot(json!({ "t": "hid.type", "text": text }));
    }

    pub fn hid_move(&self, dx: i32, dy: i32) {
        self.one_shot(json!({ "t": "hid.move", "dx": dx, "dy": dy }));
    }

    pub fn hid_move_to(&self, x: i32, y: i32) {
        self.one_shot(json!({ "t": "hid.move_to", "x": x, "y": y }));
    }

    pub fn hid_scroll(&self, delta: i32) {
        self.one_shot(json!({ "t": "hid.scroll", "delta": delta }));
    }

    // ── reads ─────────────────────────────────────────────────────────────

    pub async fn screen_pixel(&self, x: i32, y: i32) -> Result<Pixel> {
        let r = self
            .rpc(json!({ "t": "screen.pixel", "x": x, "y": y }))
            .await?;
        Ok(Pixel {
            r: r["r"].as_u64().unwrap_or(0) as u8,
            g: r["g"].as_u64().unwrap_or(0) as u8,
            b: r["b"].as_u64().unwrap_or(0) as u8,
        })
    }

    pub async fn screen_resolution(&self) -> Result<Resolution> {
        let r = self.rpc(json!({ "t": "screen.resolution" })).await?;
        Ok(Resolution {
            width: r["width"].as_u64().unwrap_or(0) as u32,
            height: r["height"].as_u64().unwrap_or(0) as u32,
        })
    }

    pub async fn system_mouse(&self) -> Result<(i32, i32)> {
        let r = self.rpc(json!({ "t": "system.mouse" })).await?;
        Ok((
            r["x"].as_i64().unwrap_or(0) as i32,
            r["y"].as_i64().unwrap_or(0) as i32,
        ))
    }

    pub async fn system_window(&self) -> Result<WindowInfo> {
        let r = self.rpc(json!({ "t": "system.window" })).await?;
        let w = &r["window"];
        Ok(WindowInfo {
            title: w["title"].as_str().unwrap_or("").to_string(),
            process: w["process"].as_str().unwrap_or("").to_string(),
            x: w["x"].as_i64().unwrap_or(0) as i32,
            y: w["y"].as_i64().unwrap_or(0) as i32,
            width: w["width"].as_i64().unwrap_or(0) as i32,
            height: w["height"].as_i64().unwrap_or(0) as i32,
        })
    }

    pub async fn system_time(&self) -> Result<u64> {
        let r = self.rpc(json!({ "t": "system.time" })).await?;
        Ok(r["time_ms"].as_u64().unwrap_or(0))
    }

    pub async fn input_keys(&self) -> Result<Vec<String>> {
        let r = self.rpc(json!({ "t": "input.keys" })).await?;
        Ok(r["keys"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect())
    }

    pub async fn input_is_down(&self, code: &str) -> Result<bool> {
        let r = self
            .rpc(json!({ "t": "input.is_down", "code": code }))
            .await?;
        Ok(r["down"].as_bool().unwrap_or(false))
    }

    pub async fn input_modifiers(&self) -> Result<Modifiers> {
        let r = self.rpc(json!({ "t": "input.modifiers" })).await?;
        let m = &r["modifiers"];
        Ok(Modifiers {
            shift: m["shift"].as_bool().unwrap_or(false),
            ctrl: m["ctrl"].as_bool().unwrap_or(false),
            alt: m["alt"].as_bool().unwrap_or(false),
            win: m["win"].as_bool().unwrap_or(false),
        })
    }

    pub async fn clipboard_get(&self) -> Result<String> {
        let r = self.rpc(json!({ "t": "clipboard.get" })).await?;
        Ok(r["text"].as_str().unwrap_or("").to_string())
    }

    pub async fn clipboard_set(&self, text: &str) -> Result<()> {
        self.rpc(json!({ "t": "clipboard.set", "text": text }))
            .await?;
        Ok(())
    }

    pub async fn window_list(&self, filter: Option<&str>) -> Result<Vec<WindowInfo>> {
        let req = match filter {
            Some(f) => json!({ "t": "window.list", "filter": f }),
            None => json!({ "t": "window.list" }),
        };
        let r = self.rpc(req).await?;
        let windows = r["windows"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|w| WindowInfo {
                title: w["title"].as_str().unwrap_or("").to_string(),
                process: w["process"].as_str().unwrap_or("").to_string(),
                x: w["x"].as_i64().unwrap_or(0) as i32,
                y: w["y"].as_i64().unwrap_or(0) as i32,
                width: w["width"].as_i64().unwrap_or(0) as i32,
                height: w["height"].as_i64().unwrap_or(0) as i32,
            })
            .collect();
        Ok(windows)
    }

    pub async fn window_find(&self, title: &str) -> Result<Option<i64>> {
        let r = self
            .rpc(json!({ "t": "window.find", "title": title }))
            .await?;
        Ok(r["handle"].as_i64())
    }

    pub async fn window_activate(&self, handle: i64) -> Result<()> {
        self.rpc(json!({ "t": "window.activate", "handle": handle }))
            .await?;
        Ok(())
    }

    pub async fn ping(&self) -> Result<u64> {
        let r = self.rpc(json!({ "t": "ping" })).await?;
        Ok(r["time_ms"].as_u64().unwrap_or(0))
    }

    /// Send any server command and return its reply (without the id), including commands
    /// without a typed method. `args` must be a JSON object; its `t` and `id`
    /// are replaced by `command` and the correlation id.
    ///
    /// ```no_run
    /// # async fn run(client: &rebind_client::RebindClient) -> rebind_client::Result<()> {
    /// let r = client.call("hash.sha256", serde_json::json!({ "data": "hello" })).await?;
    /// println!("{}", r["digest"]);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call(&self, command: &str, mut args: Value) -> Result<Value> {
        let Some(obj) = args.as_object_mut() else {
            return Err(RebindError::Json(serde::de::Error::custom(
                "call args must be a JSON object",
            )));
        };
        obj.insert("t".to_string(), json!(command));
        let mut reply = self.rpc(args).await?;
        if let Some(obj) = reply.as_object_mut() {
            obj.remove("id");
        }
        Ok(reply)
    }

    // ── event streams ─────────────────────────────────────────────────────

    /// Subscribe to mouse position events. Returns a receiver that yields
    /// `Point` values until the connection closes. Dropping the receiver
    /// stops delivery locally; the server keeps sending until disconnect.
    pub async fn mouse_events(&self) -> Result<mpsc::UnboundedReceiver<Point>> {
        let raw_rx = self.subscribe_raw("mouse").await?;
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut raw = raw_rx;
            while let Some(v) = raw.recv().await {
                let x = v["x"].as_i64().unwrap_or(0) as i32;
                let y = v["y"].as_i64().unwrap_or(0) as i32;
                if tx.send(Point { x, y }).is_err() {
                    break;
                }
            }
        });
        Ok(rx)
    }

    /// Subscribe to window focus change events.
    pub async fn window_events(&self) -> Result<mpsc::UnboundedReceiver<WindowInfo>> {
        let raw_rx = self.subscribe_raw("window").await?;
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut raw = raw_rx;
            while let Some(v) = raw.recv().await {
                let w = &v["window"];
                let info = WindowInfo {
                    title: w["title"].as_str().unwrap_or("").to_string(),
                    process: w["process"].as_str().unwrap_or("").to_string(),
                    x: w["x"].as_i64().unwrap_or(0) as i32,
                    y: w["y"].as_i64().unwrap_or(0) as i32,
                    width: w["width"].as_i64().unwrap_or(0) as i32,
                    height: w["height"].as_i64().unwrap_or(0) as i32,
                };
                if tx.send(info).is_err() {
                    break;
                }
            }
        });
        Ok(rx)
    }

    // ── internals ─────────────────────────────────────────────────────────

    fn one_shot(&self, mut msg: Value) {
        // fire-and-forget: no id field
        if let Some(obj) = msg.as_object_mut() {
            obj.remove("id");
        }
        let text = serde_json::to_string(&msg).unwrap_or_default();
        let _ = self.sender.send(Message::Text(text.into()));
    }

    async fn rpc(&self, mut msg: Value) -> Result<Value> {
        if self.sender.is_closed() {
            return Err(RebindError::connection("not connected"));
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if let Some(obj) = msg.as_object_mut() {
            obj.insert("id".to_string(), json!(id));
        }

        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let text = serde_json::to_string(&msg)?;
        if self.sender.send(Message::Text(text.into())).is_err() {
            self.pending.lock().await.remove(&id);
            return Err(RebindError::connection("not connected"));
        }

        let resp = match timeout(Duration::from_millis(self.timeout_ms), rx).await {
            Ok(reply) => reply
                .map_err(|_| RebindError::connection("connection closed while waiting for RPC"))?,
            Err(_) => {
                self.pending.lock().await.remove(&id);
                return Err(RebindError::timeout(
                    msg.get("t")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string(),
                ));
            }
        };

        if let Some(err) = resp.get("error") {
            let code = err["code"].as_str().unwrap_or("unknown");
            let message = err["message"].as_str().unwrap_or("");
            return Err(RebindError::server(code, message));
        }

        Ok(resp)
    }

    async fn subscribe_raw(&self, event: &str) -> Result<mpsc::UnboundedReceiver<Value>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.event_senders
            .lock()
            .await
            .insert(event.to_string(), tx);
        self.rpc(json!({ "t": "subscribe", "events": [event] }))
            .await?;
        Ok(rx)
    }
}
