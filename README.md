# rebind-client

Rust client for the [Rebind](https://rebind.gg) remote access WebSocket protocol.
Async, typed, built on tokio.

## Install

```toml
[dependencies]
rebind-client = "0.1"
```

## Example

```rust
use rebind_client::RebindClient;

#[tokio::main]
async fn main() -> rebind_client::Result<()> {
    let client = RebindClient::connect("ws://127.0.0.1:19561").await?;

    client.hid_move(30, -5);
    client.hid_press("Mouse1", 20);

    let (x, y) = client.system_mouse().await?;
    let pixel = client.screen_pixel(x, y).await?;

    let mut events = client.mouse_events().await?;
    while let Some(pos) = events.recv().await {
        println!("{} {}", pos.x, pos.y);
    }

    client.close().await;
    Ok(())
}
```

## Links

- GitHub: [usinput/rebind-client-rs](https://github.com/usinput/rebind-client-rs)
- Docs: [docs.rebind.gg](https://docs.rebind.gg)
