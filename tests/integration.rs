//! Integration tests for RebindClient using an in-process mock WebSocket server.
//! Tests run fully offline — no live relay required.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::{accept_async, tungstenite::Message};

use rebind_client::{RebindClient, RebindError};

// ── mock server ───────────────────────────────────────────────────────────────

#[derive(Clone)]
struct MockConfig {
    token: String,
    reply_delay_ms: u64,
    ping_returns_error: bool,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            token: String::new(),
            reply_delay_ms: 0,
            ping_returns_error: false,
        }
    }
}

struct MockServer {
    addr: SocketAddr,
    auth_count: Arc<Mutex<u32>>,
    subscribe_count: Arc<Mutex<u32>>,
    hid_frames: Arc<Mutex<Vec<Value>>>,
    event_tx: broadcast::Sender<Value>,
    shutdown_tx: broadcast::Sender<()>,
}

impl MockServer {
    async fn start(cfg: MockConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let auth_count = Arc::new(Mutex::new(0u32));
        let subscribe_count = Arc::new(Mutex::new(0u32));
        let hid_frames = Arc::new(Mutex::new(Vec::new()));
        let (event_tx, _) = broadcast::channel(64);
        let (shutdown_tx, _) = broadcast::channel(1);

        let auth_count_srv = auth_count.clone();
        let subscribe_count_srv = subscribe_count.clone();
        let hid_frames_srv = hid_frames.clone();
        let event_tx_srv = event_tx.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let cfg_srv = cfg;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Ok((stream, _)) = listener.accept() => {
                        let auth = auth_count_srv.clone();
                        let subs = subscribe_count_srv.clone();
                        let hid = hid_frames_srv.clone();
                        let ev_tx = event_tx_srv.clone();
                        let cfg = cfg_srv.clone();
                        tokio::spawn(handle_connection(stream, auth, subs, hid, ev_tx, cfg));
                    }
                    _ = shutdown_rx.recv() => break,
                }
            }
        });

        Self {
            addr,
            auth_count,
            subscribe_count,
            hid_frames,
            event_tx,
            shutdown_tx,
        }
    }

    fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    fn auth_count(&self) -> u32 {
        *self.auth_count.lock().unwrap()
    }

    fn subscribe_count(&self) -> u32 {
        *self.subscribe_count.lock().unwrap()
    }

    fn push_mouse(&self, x: i32, y: i32) {
        let _ = self.event_tx.send(json!({ "t": "mouse", "x": x, "y": y }));
    }

    fn stop(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

async fn handle_connection(
    stream: TcpStream,
    auth_count: Arc<Mutex<u32>>,
    subscribe_count: Arc<Mutex<u32>>,
    hid_frames: Arc<Mutex<Vec<Value>>>,
    event_tx: broadcast::Sender<Value>,
    cfg: MockConfig,
) {
    let ws = accept_async(stream).await.unwrap();
    let (mut write, mut read) = ws.split();

    // send hello banner
    write
        .send(Message::Text(
            json!({
                "t": "hello",
                "protocol": "1.0.0",
                "auth_required": !cfg.token.is_empty()
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    let mut subscriptions: HashMap<String, bool> = HashMap::new();
    let mut event_rx = event_tx.subscribe();
    let mut authed = cfg.token.is_empty();

    loop {
        tokio::select! {
            Some(Ok(Message::Text(raw))) = read.next() => {
                let Ok(req) = serde_json::from_str::<Value>(&raw) else { continue };
                let cmd = req["t"].as_str().unwrap_or("");

                if cfg.reply_delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(cfg.reply_delay_ms)).await;
                }

                if !cfg.token.is_empty() && !authed && cmd != "auth" && cmd != "hello" {
                    send_err(&mut write, &req, "unauthenticated", "send auth first").await;
                    continue;
                }

                match cmd {
                    "hello" => send_reply(&mut write, &req, json!({ "protocol": "1.0.0" })).await,
                    "auth" => {
                        *auth_count.lock().unwrap() += 1;
                        if cfg.token.is_empty() || req["token"].as_str() == Some(&cfg.token) {
                            authed = true;
                            send_reply(&mut write, &req, json!({ "ok": true })).await;
                        } else {
                            send_err(&mut write, &req, "bad_token", "token does not match").await;
                        }
                    }
                    "ping" => {
                        if cfg.ping_returns_error {
                            send_err(&mut write, &req, "simulated", "ping error").await;
                        } else {
                            send_reply(&mut write, &req, json!({ "pong": true, "time_ms": 1000u64 })).await;
                        }
                    }
                    "screen.pixel" => {
                        let x = req["x"].as_i64().unwrap_or(0);
                        let y = req["y"].as_i64().unwrap_or(0);
                        if x < 0 || y < 0 {
                            send_err(&mut write, &req, "screen_error", "negative coordinates").await;
                        } else {
                            send_reply(&mut write, &req, json!({
                                "r": (x * y) & 0xFF,
                                "g": x & 0xFF,
                                "b": y & 0xFF,
                            })).await;
                        }
                    }
                    "screen.resolution" => send_reply(&mut write, &req, json!({ "width": 1920, "height": 1080 })).await,
                    "system.mouse" => send_reply(&mut write, &req, json!({ "x": 100, "y": 200 })).await,
                    "system.window" => send_reply(&mut write, &req, json!({
                        "window": { "title": "Mock Window", "process": "mock.exe", "x": 0, "y": 0, "width": 800, "height": 600 }
                    })).await,
                    "system.time" => send_reply(&mut write, &req, json!({ "time_ms": 1000u64 })).await,
                    "input.keys" => send_reply(&mut write, &req, json!({ "keys": [] })).await,
                    "input.is_down" => send_reply(&mut write, &req, json!({ "down": false })).await,
                    "input.modifiers" => send_reply(&mut write, &req, json!({
                        "modifiers": { "shift": false, "ctrl": false, "alt": false, "win": false }
                    })).await,
                    "clipboard.get" => send_reply(&mut write, &req, json!({ "text": "mock clipboard" })).await,
                    "clipboard.set" => send_reply(&mut write, &req, json!({ "ok": true })).await,
                    "window.list" => send_reply(&mut write, &req, json!({ "windows": [] })).await,
                    "window.find" => send_reply(&mut write, &req, json!({ "handle": null })).await,
                    "window.activate" | "window.move" => send_reply(&mut write, &req, json!({ "ok": true })).await,
                    "subscribe" => {
                        *subscribe_count.lock().unwrap() += 1;
                        if let Some(events) = req["events"].as_array() {
                            for e in events {
                                if let Some(s) = e.as_str() {
                                    subscriptions.insert(s.to_string(), true);
                                }
                            }
                        }
                        send_reply(&mut write, &req, json!({ "ok": true })).await;
                    }
                    "unsubscribe" => {
                        if let Some(events) = req["events"].as_array() {
                            for e in events {
                                if let Some(s) = e.as_str() {
                                    subscriptions.remove(s);
                                }
                            }
                        }
                        send_reply(&mut write, &req, json!({ "ok": true })).await;
                    }
                    // fire-and-forget HID — no reply
                    "hid.down" | "hid.up" | "hid.press" | "hid.type"
                    | "hid.move" | "hid.move_to" | "hid.scroll" => {
                        hid_frames.lock().unwrap().push(req.clone());
                    }
                    _ => send_err(&mut write, &req, "unknown_command", &format!("unknown '{cmd}'")).await,
                }
            }
            Ok(event) = event_rx.recv() => {
                let t = event["t"].as_str().unwrap_or("");
                if subscriptions.contains_key(t) {
                    let _ = write.send(Message::Text(event.to_string().into())).await;
                }
            }
            else => break,
        }
    }
}

async fn send_reply<W>(write: &mut W, req: &Value, mut payload: Value)
where
    W: futures_util::Sink<Message, Error = tungstenite::Error> + Unpin,
{
    if req["id"].is_null() || req.get("id").is_none() {
        return;
    }
    payload["id"] = req["id"].clone();
    let _ = write.send(Message::Text(payload.to_string().into())).await;
}

async fn send_err<W>(write: &mut W, req: &Value, code: &str, message: &str)
where
    W: futures_util::Sink<Message, Error = tungstenite::Error> + Unpin,
{
    if req.get("id").is_none() {
        return;
    }
    let msg = json!({ "id": req["id"], "error": { "code": code, "message": message } });
    let _ = write.send(Message::Text(msg.to_string().into())).await;
}

// ── helpers ───────────────────────────────────────────────────────────────────

async fn wait_for<F>(pred: F, timeout_ms: u64)
where
    F: Fn() -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("wait_for timed out after {timeout_ms}ms");
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_auth_success() {
    let srv = MockServer::start(MockConfig {
        token: "secret".into(),
        ..Default::default()
    })
    .await;
    let client = RebindClient::connect_with_token(&srv.url(), "secret")
        .await
        .unwrap();
    assert_eq!(srv.auth_count(), 1);
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_auth_failure() {
    let srv = MockServer::start(MockConfig {
        token: "secret".into(),
        ..Default::default()
    })
    .await;
    let result = RebindClient::connect_with_token(&srv.url(), "wrong").await;
    assert!(matches!(result, Err(RebindError::Server { .. })));
    srv.stop();
}

#[tokio::test]
async fn test_ping() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let ms = client.ping().await.unwrap();
    assert!(ms > 0);
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_screen_pixel() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let px = client.screen_pixel(10, 20).await.unwrap();
    assert_eq!(px.r, ((10 * 20) & 0xFF) as u8);
    assert_eq!(px.g, 10);
    assert_eq!(px.b, 20);
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_screen_pixel_server_error() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let result = client.screen_pixel(-1, -1).await;
    match result {
        Err(RebindError::Server { code, .. }) => assert_eq!(code, "screen_error"),
        other => panic!("expected Server error, got {other:?}"),
    }
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_screen_resolution() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let res = client.screen_resolution().await.unwrap();
    assert_eq!(res.width, 1920);
    assert_eq!(res.height, 1080);
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_system_mouse() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let (x, y) = client.system_mouse().await.unwrap();
    assert_eq!(x, 100);
    assert_eq!(y, 200);
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_system_window() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let win = client.system_window().await.unwrap();
    assert_eq!(win.title, "Mock Window");
    assert_eq!(win.process, "mock.exe");
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_input_keys() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let keys = client.input_keys().await.unwrap();
    assert!(keys.is_empty());
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_input_is_down() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    assert!(!client.input_is_down("A").await.unwrap());
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_clipboard_get() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let text = client.clipboard_get().await.unwrap();
    assert_eq!(text, "mock clipboard");
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_clipboard_set() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    client.clipboard_set("hello").await.unwrap();
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_window_list() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let windows = client.window_list(None).await.unwrap();
    assert!(windows.is_empty());
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_window_find_none() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let handle = client.window_find("Nonexistent").await.unwrap();
    assert!(handle.is_none());
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_hid_methods_send_exact_frames() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    client.hid_down("A");
    client.hid_up("A");
    client.hid_press("A", 20);
    client.hid_type("hello");
    client.hid_move(10, 20);
    client.hid_move_to(100, 200);
    client.hid_scroll(3);
    // The RPC reply is an ordered barrier after the one-shot frames.
    client.ping().await.unwrap();
    assert_eq!(
        *srv.hid_frames.lock().unwrap(),
        vec![
            json!({ "t": "hid.down", "code": "A" }),
            json!({ "t": "hid.up", "code": "A" }),
            json!({ "t": "hid.press", "code": "A", "hold_ms": 20 }),
            json!({ "t": "hid.type", "text": "hello" }),
            json!({ "t": "hid.move", "dx": 10, "dy": 20 }),
            json!({ "t": "hid.move_to", "x": 100, "y": 200 }),
            json!({ "t": "hid.scroll", "delta": 3 }),
        ]
    );
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_timeout() {
    let srv = MockServer::start(MockConfig {
        reply_delay_ms: 500,
        ..Default::default()
    })
    .await;
    let client = RebindClient::connect_with_options(&srv.url(), "", 50)
        .await
        .unwrap();
    let result = client.ping().await;
    assert!(matches!(result, Err(RebindError::Timeout(_))));
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_concurrent_rpcs() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();

    let mut handles = vec![];
    // wrap client in Arc so we can share it across tasks
    let client = Arc::new(client);
    for i in 0i32..20 {
        let c = client.clone();
        handles.push(tokio::spawn(async move { c.screen_pixel(i, i * 2).await }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        let px = h.await.unwrap().unwrap();
        assert_eq!(px.g, i as u8);
    }
    srv.stop();
}

#[tokio::test]
async fn test_mouse_events() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let mut rx = client.mouse_events().await.unwrap();

    wait_for(|| srv.subscribe_count() >= 1, 2000).await;

    srv.push_mouse(1, 2);
    srv.push_mouse(3, 4);
    srv.push_mouse(5, 6);

    let p1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let p2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let p3 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!((p1.x, p1.y), (1, 2));
    assert_eq!((p2.x, p2.y), (3, 4));
    assert_eq!((p3.x, p3.y), (5, 6));

    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_call_returns_reply() {
    let srv = MockServer::start(MockConfig::default()).await;
    let client = RebindClient::connect(&srv.url()).await.unwrap();
    let r = client.call("system.time", json!({})).await.unwrap();
    assert_eq!(r["time_ms"], 1000);
    // caller args cannot override the command or the correlation id
    let r = client
        .call("ping", json!({ "t": "bogus", "id": "x" }))
        .await
        .unwrap();
    assert_eq!(r["pong"], true);
    client.close().await;
    srv.stop();
}

#[tokio::test]
async fn test_rpc_in_flight_fails_fast_when_connection_drops() {
    // server that sends the banner, reads one request, then hangs up
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        ws.send(Message::Text(json!({ "t": "hello" }).to_string().into()))
            .await
            .unwrap();
        let _ = ws.next().await;
    });

    let client = RebindClient::connect(&url).await.unwrap();
    let started = std::time::Instant::now();
    let err = client.ping().await.unwrap_err();
    assert!(matches!(err, RebindError::Connection(_)), "{err:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn test_rpc_after_connection_drop_fails_fast() {
    // server that sends the banner, then hangs up
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        ws.send(Message::Text(json!({ "t": "hello" }).to_string().into()))
            .await
            .unwrap();
    });

    let client = RebindClient::connect(&url).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = std::time::Instant::now();
    let err = client.ping().await.unwrap_err();
    assert!(matches!(err, RebindError::Connection(_)), "{err:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
}
