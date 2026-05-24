use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    extract::{
        ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Html,
    routing::get,
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use rustls::crypto::ring::default_provider;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message as BinanceMessage};
use tracing::{error, info, warn};

#[derive(Clone, Copy, Debug)]
enum Venue {
    Spot,
    Perp,
}

impl Venue {
    fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Perp => "perp",
        }
    }

    fn ws_url(self, streams: &str) -> String {
        match self {
            Self::Spot => format!("wss://stream.binance.com:9443/stream?streams={streams}"),
            Self::Perp => format!("wss://fstream.binance.com/stream?streams={streams}"),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct TradeTick {
    #[serde(rename = "type")]
    message_type: &'static str,
    symbol: String,
    asset: String,
    venue: &'static str,
    price: f64,
    quantity: f64,
    trade_id: u64,
    exchange_event_time_ms: i64,
    exchange_trade_time_ms: i64,
    receive_time_ms: i64,
    exchange_to_receive_ms: i64,
    buyer_maker: bool,
}

#[derive(Clone, Default, Debug, Serialize)]
struct LatestBySymbol {
    spot: Option<TradeTick>,
    perp: Option<TradeTick>,
}

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<String>,
    latest: Arc<RwLock<HashMap<String, LatestBySymbol>>>,
    symbols: Arc<Vec<String>>,
    started_at_ms: i64,
}

#[derive(Debug, Deserialize)]
struct CombinedTrade {
    data: RawTrade,
}

#[derive(Debug, Deserialize)]
struct RawTrade {
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    #[serde(rename = "m")]
    buyer_maker: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .with_target(false)
        .init();

    let symbols = Arc::new(parse_symbols());
    let addr: SocketAddr = env::var("LEAD_MONITOR_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8090".to_string())
        .parse()
        .context("parse LEAD_MONITOR_ADDR")?;

    let (tx, _) = broadcast::channel(8192);
    let latest = Arc::new(RwLock::new(HashMap::new()));
    let state = AppState {
        tx: tx.clone(),
        latest: latest.clone(),
        symbols: symbols.clone(),
        started_at_ms: now_ms(),
    };

    tokio::spawn(run_binance_stream(
        Venue::Spot,
        symbols.clone(),
        tx.clone(),
        latest.clone(),
    ));
    tokio::spawn(run_binance_stream(
        Venue::Perp,
        symbols.clone(),
        tx.clone(),
        latest.clone(),
    ));

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/ws", get(ws_handler))
        .route("/api/status", get(status_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Binance lead monitor listening on http://{addr}");
    axum::serve(listener, app).await?;

    Ok(())
}

fn parse_symbols() -> Vec<String> {
    env::var("SYMBOLS")
        .unwrap_or_else(|_| "BTCUSDT,ETHUSDT".to_string())
        .split(',')
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect()
}

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn status_handler(State(state): State<AppState>) -> Json<serde_json::Value> {
    let latest = state.latest.read().await.clone();
    Json(json!({
        "type": "status",
        "started_at_ms": state.started_at_ms,
        "server_time_ms": now_ms(),
        "symbols": state.symbols.as_ref(),
        "latest": latest,
    }))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> axum::response::Response {
    ws.on_upgrade(|socket| handle_web_socket(socket, state))
}

async fn handle_web_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let latest = state.latest.read().await.clone();
    let init = json!({
        "type": "init",
        "started_at_ms": state.started_at_ms,
        "server_time_ms": now_ms(),
        "symbols": state.symbols.as_ref(),
        "latest": latest,
    })
    .to_string();

    if sender.send(AxumMessage::Text(init.into())).await.is_err() {
        return;
    }

    let mut rx = state.tx.subscribe();
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(msg) => {
                        if sender.send(AxumMessage::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let msg = json!({
                            "type": "lagged",
                            "skipped": skipped,
                            "server_time_ms": now_ms(),
                        }).to_string();
                        let _ = sender.send(AxumMessage::Text(msg.into())).await;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(AxumMessage::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }
}

async fn run_binance_stream(
    venue: Venue,
    symbols: Arc<Vec<String>>,
    tx: broadcast::Sender<String>,
    latest: Arc<RwLock<HashMap<String, LatestBySymbol>>>,
) {
    let streams = symbols
        .iter()
        .map(|symbol| format!("{}@trade", symbol.to_ascii_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let url = venue.ws_url(&streams);

    loop {
        info!("connecting {} stream: {}", venue.as_str(), url);
        match consume_stream(venue, &url, tx.clone(), latest.clone()).await {
            Ok(()) => warn!("{} stream closed, reconnecting", venue.as_str()),
            Err(err) => warn!("{} stream error: {err:#}", venue.as_str()),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn consume_stream(
    venue: Venue,
    url: &str,
    tx: broadcast::Sender<String>,
    latest: Arc<RwLock<HashMap<String, LatestBySymbol>>>,
) -> Result<()> {
    let (socket, _) = connect_async(url).await.context("connect Binance ws")?;
    info!("{} stream connected", venue.as_str());

    let (mut writer, mut reader) = socket.split();
    while let Some(message) = reader.next().await {
        match message.context("read Binance ws")? {
            BinanceMessage::Text(text) => {
                if let Err(err) = handle_trade_message(venue, &text, &tx, &latest).await {
                    error!("{} parse error: {err:#}", venue.as_str());
                }
            }
            BinanceMessage::Ping(payload) => {
                writer
                    .send(BinanceMessage::Pong(payload))
                    .await
                    .context("send pong")?;
            }
            BinanceMessage::Close(_) => break,
            _ => {}
        }
    }

    Ok(())
}

async fn handle_trade_message(
    venue: Venue,
    text: &str,
    tx: &broadcast::Sender<String>,
    latest: &Arc<RwLock<HashMap<String, LatestBySymbol>>>,
) -> Result<()> {
    let parsed: CombinedTrade = serde_json::from_str(text).context("decode combined trade")?;
    let price = parsed.data.price.parse::<f64>().context("parse price")?;
    let quantity = parsed
        .data
        .quantity
        .parse::<f64>()
        .context("parse quantity")?;
    let receive_time_ms = now_ms();
    let symbol = parsed.data.symbol.to_ascii_uppercase();
    let tick = TradeTick {
        message_type: "trade",
        asset: asset_from_symbol(&symbol),
        symbol: symbol.clone(),
        venue: venue.as_str(),
        price,
        quantity,
        trade_id: parsed.data.trade_id,
        exchange_event_time_ms: parsed.data.event_time_ms,
        exchange_trade_time_ms: parsed.data.trade_time_ms,
        receive_time_ms,
        exchange_to_receive_ms: receive_time_ms - parsed.data.event_time_ms,
        buyer_maker: parsed.data.buyer_maker,
    };

    {
        let mut latest = latest.write().await;
        let entry = latest.entry(symbol).or_default();
        let previous_price = match venue {
            Venue::Spot => entry.spot.as_ref().map(|tick| tick.price),
            Venue::Perp => entry.perp.as_ref().map(|tick| tick.price),
        };
        log_large_raw_move(venue, previous_price, &tick, text);
        match venue {
            Venue::Spot => entry.spot = Some(tick.clone()),
            Venue::Perp => entry.perp = Some(tick.clone()),
        }
    }

    let msg = serde_json::to_string(&tick).context("encode tick")?;
    let _ = tx.send(msg);

    Ok(())
}

fn asset_from_symbol(symbol: &str) -> String {
    symbol
        .strip_suffix("USDT")
        .unwrap_or(symbol)
        .to_ascii_uppercase()
}

fn log_large_raw_move(venue: Venue, previous_price: Option<f64>, tick: &TradeTick, raw_text: &str) {
    let Some(previous_price) = previous_price else {
        return;
    };
    if previous_price <= 0.0 || tick.price <= 0.0 {
        return;
    }

    let pct_move = ((tick.price - previous_price) / previous_price).abs();
    if pct_move < 0.01 {
        return;
    }

    warn!(
        "[{}] raw large move: symbol={} prev={:.8} price={:.8} move={:.4}% trade_id={} event_time={} trade_time={} receive_time={} raw={}",
        venue.as_str(),
        tick.symbol,
        previous_price,
        tick.price,
        pct_move * 100.0,
        tick.trade_id,
        tick.exchange_event_time_ms,
        tick.exchange_trade_time_ms,
        tick.receive_time_ms,
        raw_text,
    );
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
