# rebind-client

Rust client for the [Rebind](https://rebind.gg) remote access WebSocket protocol.
Async, typed, built on tokio.

## Install

```toml
[dependencies]
rebind-client = "0.2"
```

## Quick start

```rust
use rebind_client::RebindClient;

#[tokio::main]
async fn main() -> rebind_client::Result<()> {
    let client = RebindClient::connect("ws://127.0.0.1:19561").await?;

    client.hid_move(30, -5);
    client.hid_press("Mouse1", 20);
    client.hid_type("hello\n");

    let (x, y) = client.system_mouse().await?;
    let pixel = client.screen_pixel(x, y).await?;
    println!("pixel at {x},{y} = rgb({},{},{})", pixel.r, pixel.g, pixel.b);

    let mut events = client.mouse_events().await?;
    while let Some(pos) = events.recv().await {
        println!("{} {}", pos.x, pos.y);
    }

    client.close().await;
    Ok(())
}
```

## Read input state

```rust
let keys = client.input_keys().await?;
let mods = client.input_modifiers().await?;
let shift_held = client.input_is_down("LShift").await?;
```

## Screen sampling

```rust
let (w, h) = {
    let res = client.screen_resolution().await?;
    (res.width, res.height)
};

// sample center pixel
let px = client.screen_pixel((w / 2) as i32, (h / 2) as i32).await?;

// react to a specific on-screen color (e.g. a UI state in a QA test)
if px.r > 200 && px.g < 50 && px.b < 50 {
    client.hid_press("Mouse1", 20);
}
```

## Window management

```rust
if let Some(handle) = client.window_find("Counter-Strike 2").await? {
    client.window_activate(handle).await?;
}
```

## Clipboard

```rust
let text = client.clipboard_get().await?;
client.clipboard_set("hello from rust").await?;
```

## Any server command

Commands without a typed method are reachable through `call`, which takes a
JSON object of arguments and returns the reply:

```rust
use serde_json::json;

let r = client.call("hash.sha256", json!({ "data": "hello" })).await?;
println!("{}", r["digest"]);
```

## Auth

```rust
let client = RebindClient::connect_with_token(
    "ws://127.0.0.1:19561",
    "my-secret",
).await?;
```

## Concurrent RPCs

The client is cheaply clonable via `Arc` for use across tasks:

```rust
use std::sync::Arc;

let client = Arc::new(RebindClient::connect("ws://127.0.0.1:19561").await?);

let c1 = client.clone();
let c2 = client.clone();

let (mouse, window) = tokio::join!(
    c1.system_mouse(),
    c2.system_window(),
);
```

## Links

- crates.io: [rebind-client](https://crates.io/crates/rebind-client)
- GitHub: [usinput/rebind-client-rs](https://github.com/usinput/rebind-client-rs)
- Docs: [docs.rebind.gg](https://docs.rebind.gg)
