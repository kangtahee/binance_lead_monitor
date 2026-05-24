# Binance Lead Monitor

Independent Binance spot/perp last-trade monitor. It does not import or call the
order slave, order executor, Polymarket code, or the existing dashboard.

## Run

```powershell
cd binance_lead_monitor
cargo run --release
```

Default address:

```text
http://127.0.0.1:8090
```

Optional environment variables:

```powershell
$env:LEAD_MONITOR_ADDR = "0.0.0.0:8090"
$env:SYMBOLS = "BTCUSDT,ETHUSDT"
cargo run --release
```

## Data

- Spot stream: `wss://stream.binance.com:9443/stream?...@trade`
- Perp stream: `wss://fstream.binance.com/stream?...@trade`
- Chart X axis can use local receive time or Binance trade time.
- Chart Y axis is raw last-trade price.

The chart keeps the most recent points in browser memory and supports dragging,
mouse wheel zoom, hover tooltip, and live-follow mode.
