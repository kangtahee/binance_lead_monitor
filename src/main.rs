use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    env,
    fmt,
    hash::Hash,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
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
use serde_json::{json, Value};
use statrs::distribution::{ContinuousCDF, Normal};
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use tracing::{debug, error, info, warn};

const PRICE_TO_BEAT_INTERVAL_MS: i64 = 300 * 1000;
const THEORY_MARKET_OPEN_GUARD_MS: i64 = 20 * 1000;
const EVENT_WINDOW_MS: i64 = 1_000;
const MIN_VOLATILITY: f64 = 0.000005;
const MAX_VOLATILITY: f64 = 0.01;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[serde(rename_all = "UPPERCASE")]
enum Asset {
    Btc,
    Eth,
}

impl Asset {
    fn all() -> Vec<Self> {
        vec![Self::Btc, Self::Eth]
    }

    fn from_user_value(value: &str) -> Option<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "BTC" | "BTCUSDT" | "BTC-USDT" | "BTC-USDT-SWAP" => Some(Self::Btc),
            "ETH" | "ETHUSDT" | "ETH-USDT" | "ETH-USDT-SWAP" => Some(Self::Eth),
            _ => None,
        }
    }

    fn from_binance_symbol(symbol: &str) -> Option<Self> {
        Self::from_user_value(symbol)
    }

    fn from_okx_inst_id(inst_id: &str) -> Option<Self> {
        Self::from_user_value(inst_id)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Eth => "ETH",
        }
    }

    fn binance_symbol(self) -> &'static str {
        match self {
            Self::Btc => "BTCUSDT",
            Self::Eth => "ETHUSDT",
        }
    }

    fn okx_spot_inst(self) -> &'static str {
        match self {
            Self::Btc => "BTC-USDT",
            Self::Eth => "ETH-USDT",
        }
    }

    fn okx_swap_inst(self) -> &'static str {
        match self {
            Self::Btc => "BTC-USDT-SWAP",
            Self::Eth => "ETH-USDT-SWAP",
        }
    }

    fn slug_prefix(self) -> &'static str {
        match self {
            Self::Btc => "btc-updown-5m",
            Self::Eth => "eth-updown-5m",
        }
    }

    fn min_theory_move(self) -> f64 {
        match self {
            Self::Btc => 0.0,
            Self::Eth => 0.1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
enum Venue {
    BinanceSpot,
    BinancePerp,
    OkxSpot,
    OkxPerp,
}

impl Venue {
    fn as_str(self) -> &'static str {
        match self {
            Self::BinanceSpot => "binance_spot",
            Self::BinancePerp => "binance_perp",
            Self::OkxSpot => "okx_spot",
            Self::OkxPerp => "okx_perp",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::BinanceSpot => "Binance Spot",
            Self::BinancePerp => "Binance Perp",
            Self::OkxSpot => "OKX Spot",
            Self::OkxPerp => "OKX Perp",
        }
    }

    fn short_label(self) -> &'static str {
        match self {
            Self::BinanceSpot => "BN Spot",
            Self::BinancePerp => "BN Perp",
            Self::OkxSpot => "OKX Spot",
            Self::OkxPerp => "OKX Perp",
        }
    }

    fn is_binance(self) -> bool {
        matches!(self, Self::BinanceSpot | Self::BinancePerp)
    }

    fn is_okx(self) -> bool {
        matches!(self, Self::OkxSpot | Self::OkxPerp)
    }

    fn binance_ws_url(self, streams: &str) -> String {
        match self {
            Self::BinanceSpot => format!("wss://stream.binance.com:9443/stream?streams={streams}"),
            Self::BinancePerp => format!("wss://fstream.binance.com/stream?streams={streams}"),
            _ => unreachable!("not a Binance venue"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[serde(rename_all = "UPPERCASE")]
enum Direction {
    Up,
    Down,
}

impl Direction {
    fn from_prices(s_old: f64, s_new: f64) -> Option<Self> {
        if s_new > s_old {
            Some(Self::Up)
        } else if s_new < s_old {
            Some(Self::Down)
        } else {
            None
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Up => "UP",
            Self::Down => "DOWN",
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct SourceKey {
    asset: Asset,
    venue: Venue,
}

#[derive(Clone, Debug)]
struct TradeRecord {
    price: f64,
    time_ms: i64,
    receive_time_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
struct TradeTick {
    #[serde(rename = "type")]
    message_type: &'static str,
    symbol: String,
    asset: String,
    venue: &'static str,
    venue_label: &'static str,
    stream: String,
    price: f64,
    raw_price: String,
    quantity: f64,
    raw_quantity: String,
    trade_id: String,
    exchange_event_time_ms: i64,
    exchange_trade_time_ms: i64,
    receive_time_ms: i64,
    exchange_to_receive_ms: i64,
    buyer_maker: Option<bool>,
    side: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct InvalidTick {
    #[serde(rename = "type")]
    message_type: &'static str,
    symbol: String,
    asset: String,
    venue: &'static str,
    venue_label: &'static str,
    stream: String,
    raw_price: String,
    raw_quantity: String,
    trade_id: String,
    exchange_time_ms: i64,
    receive_time_ms: i64,
    reason: String,
    raw: String,
}

#[derive(Clone, Default, Debug, Serialize)]
struct LatestBySymbol {
    binance_spot: Option<TradeTick>,
    binance_perp: Option<TradeTick>,
    okx_spot: Option<TradeTick>,
    okx_perp: Option<TradeTick>,
}

#[derive(Clone, Default, Debug, Serialize)]
struct PolymarketSnapshot {
    asset: String,
    slug: String,
    up_token_id: String,
    down_token_id: String,
    up_price: f64,
    down_price: f64,
    up_bid: String,
    up_ask: String,
    down_bid: String,
    down_ask: String,
    market_timestamp_ms: i64,
    receive_time_ms: i64,
    latency_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
struct TtTriggerRecord {
    venue: &'static str,
    venue_label: &'static str,
    receive_time_ms: i64,
    exchange_trade_time_ms: i64,
    lag_ms: i64,
    price: f64,
    s_old: f64,
    s_old_time_ms: i64,
    s0: f64,
    tau: f64,
    sigma: f64,
    theory_price_cents: f64,
    real_price_cents: f64,
    spread_cents: f64,
    threshold_cents: f64,
}

#[derive(Clone, Debug, Serialize)]
struct TtEventGroup {
    id: u64,
    asset: String,
    direction: Direction,
    market_slug: String,
    first_venue: &'static str,
    first_venue_label: &'static str,
    first_receive_time_ms: i64,
    deadline_ms: i64,
    triggers: BTreeMap<String, TtTriggerRecord>,
}

#[derive(Clone, Debug)]
struct TtTriggerCandidate {
    direction: Direction,
    s_old: f64,
    s_old_time_ms: i64,
    sigma_entry: f64,
    theory_price_cents: f64,
    real_price_cents: f64,
    spread_cents: f64,
    threshold_cents: f64,
}

#[derive(Clone, Debug, Default)]
struct TtSourceState {
    recent: VecDeque<TradeRecord>,
    price_to_beat: Option<f64>,
    price_to_beat_slot: Option<i64>,
    last_trigger_by_direction: HashMap<Direction, i64>,
}

#[derive(Debug, Default)]
struct SharedData {
    latest: HashMap<String, LatestBySymbol>,
    polymarket: HashMap<Asset, PolymarketSnapshot>,
    tt_sources: HashMap<SourceKey, TtSourceState>,
    tt_events: Vec<TtEventGroup>,
    invalid_counts: HashMap<String, u64>,
    next_event_id: u64,
}

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<String>,
    shared: Arc<RwLock<SharedData>>,
    assets: Arc<Vec<Asset>>,
    started_at_ms: i64,
}

#[derive(Debug, Deserialize)]
struct CombinedTrade {
    stream: String,
    data: RawBinanceTrade,
}

#[derive(Debug, Deserialize)]
struct RawBinanceTrade {
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

#[derive(Debug, Deserialize)]
struct OkxTradesEnvelope {
    arg: Option<OkxArg>,
    data: Option<Vec<RawOkxTrade>>,
    event: Option<String>,
    code: Option<String>,
    msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OkxArg {
    #[serde(rename = "instId")]
    inst_id: String,
}

#[derive(Debug, Deserialize)]
struct RawOkxTrade {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "tradeId")]
    trade_id: String,
    px: String,
    sz: String,
    side: Option<String>,
    ts: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .with_target(false)
        .init();

    let assets = Arc::new(parse_assets());
    let addr: SocketAddr = env::var("LEAD_MONITOR_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8090".to_string())
        .parse()
        .context("parse LEAD_MONITOR_ADDR")?;

    let (tx, _) = broadcast::channel(16384);
    let shared = Arc::new(RwLock::new(SharedData::default()));
    let state = AppState {
        tx: tx.clone(),
        shared: shared.clone(),
        assets: assets.clone(),
        started_at_ms: now_ms(),
    };

    for asset in assets.iter().copied() {
        tokio::spawn(run_polymarket_stream(asset, tx.clone(), shared.clone()));
    }

    tokio::spawn(run_binance_stream(
        Venue::BinanceSpot,
        assets.clone(),
        tx.clone(),
        shared.clone(),
    ));
    tokio::spawn(run_binance_stream(
        Venue::BinancePerp,
        assets.clone(),
        tx.clone(),
        shared.clone(),
    ));
    tokio::spawn(run_okx_stream(
        Venue::OkxSpot,
        assets.clone(),
        tx.clone(),
        shared.clone(),
    ));
    tokio::spawn(run_okx_stream(
        Venue::OkxPerp,
        assets.clone(),
        tx.clone(),
        shared.clone(),
    ));

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/ws", get(ws_handler))
        .route("/api/status", get(status_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("lead monitor listening on http://{addr}");
    axum::serve(listener, app).await?;

    Ok(())
}

fn parse_assets() -> Vec<Asset> {
    env::var("ASSETS")
        .or_else(|_| env::var("SYMBOLS"))
        .unwrap_or_else(|_| "BTC,ETH".to_string())
        .split(',')
        .filter_map(Asset::from_user_value)
        .fold(Vec::new(), |mut acc, asset| {
            if !acc.contains(&asset) {
                acc.push(asset);
            }
            acc
        })
        .into_iter()
        .collect::<Vec<_>>()
        .pipe(|assets| if assets.is_empty() { Asset::all() } else { assets })
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn status_handler(State(state): State<AppState>) -> Json<Value> {
    let shared = state.shared.read().await;
    let assets: Vec<_> = state.assets.iter().map(|asset| asset.as_str()).collect();
    Json(json!({
        "type": "status",
        "started_at_ms": state.started_at_ms,
        "server_time_ms": now_ms(),
        "assets": assets,
        "symbols": state.assets.iter().map(|asset| asset.binance_symbol()).collect::<Vec<_>>(),
        "latest": shared.latest,
        "polymarket": shared.polymarket,
        "tt_events": shared.tt_events,
        "invalid_counts": shared.invalid_counts,
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
    let shared = state.shared.read().await;
    let init = json!({
        "type": "init",
        "started_at_ms": state.started_at_ms,
        "server_time_ms": now_ms(),
        "assets": state.assets.iter().map(|asset| asset.as_str()).collect::<Vec<_>>(),
        "symbols": state.assets.iter().map(|asset| asset.binance_symbol()).collect::<Vec<_>>(),
        "latest": shared.latest,
        "polymarket": shared.polymarket,
        "tt_events": shared.tt_events,
        "invalid_counts": shared.invalid_counts,
    })
    .to_string();
    drop(shared);

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
    assets: Arc<Vec<Asset>>,
    tx: broadcast::Sender<String>,
    shared: Arc<RwLock<SharedData>>,
) {
    debug_assert!(venue.is_binance());
    let streams = assets
        .iter()
        .map(|asset| format!("{}@trade", asset.binance_symbol().to_ascii_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let url = venue.binance_ws_url(&streams);

    loop {
        info!("connecting {} stream: {}", venue.label(), url);
        match consume_binance_stream(venue, &url, tx.clone(), shared.clone()).await {
            Ok(()) => warn!("{} stream closed, reconnecting", venue.label()),
            Err(err) => warn!("{} stream error: {err:#}", venue.label()),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn consume_binance_stream(
    venue: Venue,
    url: &str,
    tx: broadcast::Sender<String>,
    shared: Arc<RwLock<SharedData>>,
) -> Result<()> {
    let (socket, _) = connect_async(url).await.context("connect Binance ws")?;
    info!("{} stream connected", venue.label());

    let (mut writer, mut reader) = socket.split();
    while let Some(message) = reader.next().await {
        match message.context("read Binance ws")? {
            WsMessage::Text(text) => {
                if let Err(err) = handle_binance_trade_message(venue, &text, &tx, &shared).await {
                    error!("{} parse error: {err:#}", venue.label());
                }
            }
            WsMessage::Ping(payload) => {
                writer.send(WsMessage::Pong(payload)).await.context("send pong")?;
            }
            WsMessage::Close(_) => break,
            _ => {}
        }
    }

    Ok(())
}

async fn handle_binance_trade_message(
    venue: Venue,
    text: &str,
    tx: &broadcast::Sender<String>,
    shared: &Arc<RwLock<SharedData>>,
) -> Result<()> {
    let parsed: CombinedTrade = serde_json::from_str(text).context("decode combined trade")?;
    let receive_time_ms = now_ms();
    let price = parsed.data.price.parse::<f64>().unwrap_or(0.0);
    let quantity = parsed.data.quantity.parse::<f64>().unwrap_or(0.0);
    let symbol = parsed.data.symbol.to_ascii_uppercase();
    let asset = Asset::from_binance_symbol(&symbol)
        .ok_or_else(|| anyhow!("unsupported Binance symbol {symbol}"))?;

    if !valid_trade_price(price, quantity) {
        let invalid = InvalidTick {
            message_type: "invalid_tick",
            asset: asset.as_str().to_string(),
            symbol,
            venue: venue.as_str(),
            venue_label: venue.label(),
            stream: parsed.stream,
            raw_price: parsed.data.price,
            raw_quantity: parsed.data.quantity,
            trade_id: parsed.data.trade_id.to_string(),
            exchange_time_ms: parsed.data.trade_time_ms,
            receive_time_ms,
            reason: invalid_reason(price, quantity),
            raw: text.to_string(),
        };
        handle_invalid_tick(invalid, tx, shared).await?;
        return Ok(());
    }

    let tick = TradeTick {
        message_type: "trade",
        asset: asset.as_str().to_string(),
        symbol: asset.binance_symbol().to_string(),
        venue: venue.as_str(),
        venue_label: venue.label(),
        stream: parsed.stream,
        price,
        raw_price: parsed.data.price,
        quantity,
        raw_quantity: parsed.data.quantity,
        trade_id: parsed.data.trade_id.to_string(),
        exchange_event_time_ms: parsed.data.event_time_ms,
        exchange_trade_time_ms: parsed.data.trade_time_ms,
        receive_time_ms,
        exchange_to_receive_ms: receive_time_ms - parsed.data.event_time_ms,
        buyer_maker: Some(parsed.data.buyer_maker),
        side: None,
    };

    handle_valid_trade(asset, venue, tick, text, tx, shared).await
}

async fn run_okx_stream(
    venue: Venue,
    assets: Arc<Vec<Asset>>,
    tx: broadcast::Sender<String>,
    shared: Arc<RwLock<SharedData>>,
) {
    debug_assert!(venue.is_okx());
    let args = assets
        .iter()
        .map(|asset| {
            let inst_id = match venue {
                Venue::OkxSpot => asset.okx_spot_inst(),
                Venue::OkxPerp => asset.okx_swap_inst(),
                _ => unreachable!("not an OKX venue"),
            };
            json!({ "channel": "trades", "instId": inst_id })
        })
        .collect::<Vec<_>>();
    let subscribe = json!({ "op": "subscribe", "args": args }).to_string();
    let url = "wss://ws.okx.com:8443/ws/v5/public";

    loop {
        info!("connecting {} stream: {}", venue.label(), url);
        match consume_okx_stream(venue, url, &subscribe, tx.clone(), shared.clone()).await {
            Ok(()) => warn!("{} stream closed, reconnecting", venue.label()),
            Err(err) => warn!("{} stream error: {err:#}", venue.label()),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn consume_okx_stream(
    venue: Venue,
    url: &str,
    subscribe: &str,
    tx: broadcast::Sender<String>,
    shared: Arc<RwLock<SharedData>>,
) -> Result<()> {
    let (socket, _) = connect_async(url).await.context("connect OKX ws")?;
    info!("{} stream connected", venue.label());

    let (mut writer, mut reader) = socket.split();
    writer
        .send(WsMessage::Text(subscribe.to_string().into()))
        .await
        .context("send OKX subscribe")?;

    while let Some(message) = reader.next().await {
        match message.context("read OKX ws")? {
            WsMessage::Text(text) => {
                let text_ref = text.as_ref();
                if text_ref == "ping" {
                    writer
                        .send(WsMessage::Text("pong".to_string().into()))
                        .await
                        .context("send OKX pong text")?;
                    continue;
                }
                if let Err(err) = handle_okx_trade_message(venue, text_ref, &tx, &shared).await {
                    error!("{} parse error: {err:#}", venue.label());
                }
            }
            WsMessage::Ping(payload) => {
                writer.send(WsMessage::Pong(payload)).await.context("send pong")?;
            }
            WsMessage::Close(_) => break,
            _ => {}
        }
    }

    Ok(())
}

async fn handle_okx_trade_message(
    venue: Venue,
    text: &str,
    tx: &broadcast::Sender<String>,
    shared: &Arc<RwLock<SharedData>>,
) -> Result<()> {
    let parsed: OkxTradesEnvelope = serde_json::from_str(text).context("decode OKX trade")?;
    if let Some(event) = parsed.event.as_deref() {
        if event != "subscribe" {
            warn!(
                "{} OKX event: event={} code={:?} msg={:?}",
                venue.label(),
                event,
                parsed.code,
                parsed.msg
            );
        }
        return Ok(());
    }

    let Some(data) = parsed.data else {
        return Ok(());
    };
    let stream = parsed
        .arg
        .as_ref()
        .map(|arg| arg.inst_id.clone())
        .unwrap_or_default();

    for raw in data {
        let receive_time_ms = now_ms();
        let asset = Asset::from_okx_inst_id(&raw.inst_id)
            .ok_or_else(|| anyhow!("unsupported OKX instId {}", raw.inst_id))?;
        let price = raw.px.parse::<f64>().unwrap_or(0.0);
        let quantity = raw.sz.parse::<f64>().unwrap_or(0.0);
        let trade_time_ms = raw.ts.parse::<i64>().unwrap_or(0);

        if !valid_trade_price(price, quantity) {
            let invalid = InvalidTick {
                message_type: "invalid_tick",
                asset: asset.as_str().to_string(),
                symbol: asset.binance_symbol().to_string(),
                venue: venue.as_str(),
                venue_label: venue.label(),
                stream: raw.inst_id,
                raw_price: raw.px,
                raw_quantity: raw.sz,
                trade_id: raw.trade_id,
                exchange_time_ms: trade_time_ms,
                receive_time_ms,
                reason: invalid_reason(price, quantity),
                raw: text.to_string(),
            };
            handle_invalid_tick(invalid, tx, shared).await?;
            continue;
        }

        let tick = TradeTick {
            message_type: "trade",
            asset: asset.as_str().to_string(),
            symbol: asset.binance_symbol().to_string(),
            venue: venue.as_str(),
            venue_label: venue.label(),
            stream: if stream.is_empty() { raw.inst_id } else { stream.clone() },
            price,
            raw_price: raw.px,
            quantity,
            raw_quantity: raw.sz,
            trade_id: raw.trade_id,
            exchange_event_time_ms: trade_time_ms,
            exchange_trade_time_ms: trade_time_ms,
            receive_time_ms,
            exchange_to_receive_ms: receive_time_ms - trade_time_ms,
            buyer_maker: None,
            side: raw.side,
        };

        handle_valid_trade(asset, venue, tick, text, tx, shared).await?;
    }

    Ok(())
}

fn valid_trade_price(price: f64, quantity: f64) -> bool {
    price.is_finite() && price > 0.0 && quantity.is_finite() && quantity > 0.0
}

fn invalid_reason(price: f64, quantity: f64) -> String {
    if !price.is_finite() || price <= 0.0 {
        "invalid_price".to_string()
    } else if !quantity.is_finite() || quantity <= 0.0 {
        "invalid_quantity".to_string()
    } else {
        "invalid_trade".to_string()
    }
}

async fn handle_invalid_tick(
    invalid: InvalidTick,
    tx: &broadcast::Sender<String>,
    shared: &Arc<RwLock<SharedData>>,
) -> Result<()> {
    if log_invalid_ticks_enabled() {
        warn!(
            "[{}] raw invalid tick: symbol={} reason={} raw_price={} raw_quantity={} trade_id={} exchange_time={} receive_time={} raw={}",
            invalid.venue,
            invalid.symbol,
            invalid.reason,
            invalid.raw_price,
            invalid.raw_quantity,
            invalid.trade_id,
            invalid.exchange_time_ms,
            invalid.receive_time_ms,
            invalid.raw,
        );
    } else {
        debug!(
            "[{}] ignored invalid tick: symbol={} reason={} raw_price={} raw_quantity={} trade_id={}",
            invalid.venue,
            invalid.symbol,
            invalid.reason,
            invalid.raw_price,
            invalid.raw_quantity,
            invalid.trade_id,
        );
    }

    {
        let mut shared = shared.write().await;
        let key = format!("{}:{}:{}", invalid.asset, invalid.venue, invalid.reason);
        *shared.invalid_counts.entry(key).or_default() += 1;
    }

    let msg = serde_json::to_string(&invalid).context("encode invalid tick")?;
    let _ = tx.send(msg);
    Ok(())
}

fn log_invalid_ticks_enabled() -> bool {
    env::var("LEAD_MONITOR_LOG_INVALID_TICKS")
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

async fn handle_valid_trade(
    asset: Asset,
    venue: Venue,
    tick: TradeTick,
    raw_text: &str,
    tx: &broadcast::Sender<String>,
    shared: &Arc<RwLock<SharedData>>,
) -> Result<()> {
    let mut event_update = None;

    {
        let mut shared = shared.write().await;
        let latest_entry = shared.latest.entry(tick.symbol.clone()).or_default();
        let previous_price = latest_price_for_venue(latest_entry, venue);
        log_large_raw_move(venue, previous_price, &tick, raw_text);
        set_latest_for_venue(latest_entry, venue, tick.clone());

        let source_key = SourceKey { asset, venue };
        let pm = shared.polymarket.get(&asset).cloned().unwrap_or_default();
        let source = shared.tt_sources.entry(source_key).or_default();
        update_price_to_beat_from_trade(source, tick.price, tick.exchange_trade_time_ms, venue, asset);

        let trade = TradeRecord {
            price: tick.price,
            time_ms: tick.exchange_trade_time_ms,
            receive_time_ms: tick.receive_time_ms,
        };
        source.recent.push_back(trade.clone());
        let keep_cutoff_ms = trade.time_ms.saturating_sub(2_000);
        while source
            .recent
            .front()
            .is_some_and(|entry| entry.time_ms < keep_cutoff_ms)
        {
            source.recent.pop_front();
        }

        if let Some(candidate) = evaluate_strict_tt(asset, source, &trade, &pm) {
            let allow_source_trigger = source
                .last_trigger_by_direction
                .get(&candidate.direction)
                .is_none_or(|last| trade.receive_time_ms.saturating_sub(*last) >= EVENT_WINDOW_MS);
            if allow_source_trigger {
                source
                    .last_trigger_by_direction
                    .insert(candidate.direction, trade.receive_time_ms);
                let trigger = build_trigger_record(venue, &trade, &candidate, source.price_to_beat.unwrap_or(0.0));
                let group = record_tt_trigger(&mut shared, asset, venue, candidate.direction, &pm, trigger);
                event_update = Some(group);
            }
        }
    }

    let msg = serde_json::to_string(&tick).context("encode tick")?;
    let _ = tx.send(msg);
    if let Some(group) = event_update {
        let update = json!({
            "type": "tt_event",
            "event": group,
            "server_time_ms": now_ms(),
        })
        .to_string();
        let _ = tx.send(update);
    }

    Ok(())
}

fn latest_price_for_venue(entry: &LatestBySymbol, venue: Venue) -> Option<f64> {
    match venue {
        Venue::BinanceSpot => entry.binance_spot.as_ref(),
        Venue::BinancePerp => entry.binance_perp.as_ref(),
        Venue::OkxSpot => entry.okx_spot.as_ref(),
        Venue::OkxPerp => entry.okx_perp.as_ref(),
    }
    .map(|tick| tick.price)
}

fn set_latest_for_venue(entry: &mut LatestBySymbol, venue: Venue, tick: TradeTick) {
    match venue {
        Venue::BinanceSpot => entry.binance_spot = Some(tick),
        Venue::BinancePerp => entry.binance_perp = Some(tick),
        Venue::OkxSpot => entry.okx_spot = Some(tick),
        Venue::OkxPerp => entry.okx_perp = Some(tick),
    }
}

fn update_price_to_beat_from_trade(
    source: &mut TtSourceState,
    price: f64,
    trade_time_ms: i64,
    venue: Venue,
    asset: Asset,
) {
    let slot = trade_time_ms.div_euclid(PRICE_TO_BEAT_INTERVAL_MS);
    let ms_since_slot_start = trade_time_ms.rem_euclid(PRICE_TO_BEAT_INTERVAL_MS);

    if source.price_to_beat_slot.is_none() {
        source.price_to_beat_slot = Some(slot);
        info!(
            "[{} {}] skip startup slot for price_to_beat: slot={}, elapsed={}ms",
            asset.as_str(),
            venue.short_label(),
            slot,
            ms_since_slot_start
        );
        return;
    }

    if source.price_to_beat_slot != Some(slot) {
        source.price_to_beat = Some(price);
        source.price_to_beat_slot = Some(slot);
        info!(
            "[{} {}] price_to_beat set: slot={}, price={:.8}",
            asset.as_str(),
            venue.short_label(),
            slot,
            price
        );
    }
}

fn evaluate_strict_tt(
    asset: Asset,
    source: &TtSourceState,
    trade: &TradeRecord,
    pm: &PolymarketSnapshot,
) -> Option<TtTriggerCandidate> {
    if market_elapsed_ms(trade.time_ms) < THEORY_MARKET_OPEN_GUARD_MS {
        return None;
    }

    let price_to_beat = source.price_to_beat?;
    let slot_end_ms = (trade.time_ms.div_euclid(PRICE_TO_BEAT_INTERVAL_MS) + 1) * PRICE_TO_BEAT_INTERVAL_MS;
    let tau = (slot_end_ms - trade.time_ms).max(0) as f64 / 1000.0;
    if price_to_beat <= 0.0 || tau <= 0.0 || pm.up_price <= 0.0 || pm.down_price <= 0.0 {
        return None;
    }

    let price_1s_ago = price_at_or_before(&source.recent, trade.time_ms.saturating_sub(1_000))?;
    let mut selected_candidate = None;
    let _ = select_sold_trade(&source.recent, |sold_trade| {
        if let Some(candidate) = build_theory_trigger_candidate(
            asset,
            trade,
            sold_trade.price,
            sold_trade.time_ms,
            pm,
            price_to_beat,
            tau,
        ) {
            let direction_ok = match candidate.direction {
                Direction::Up => trade.price > price_1s_ago,
                Direction::Down => trade.price < price_1s_ago,
            };
            if direction_ok {
                selected_candidate = Some(candidate);
                return true;
            }
        }
        false
    });

    selected_candidate
}

fn build_theory_trigger_candidate(
    asset: Asset,
    trade: &TradeRecord,
    s_old: f64,
    s_old_time_ms: i64,
    pm: &PolymarketSnapshot,
    price_to_beat: f64,
    tau: f64,
) -> Option<TtTriggerCandidate> {
    if (trade.price - s_old).abs() < asset.min_theory_move() {
        return None;
    }

    let direction = Direction::from_prices(s_old, trade.price)?;
    let buy_real = buy_real_price(pm, direction)?;
    let real_price_cents = buy_real * 100.0;
    let threshold_cents = entry_threshold_cents(real_price_cents)?;
    let sigma_entry = infer_implied_volatility(
        price_to_beat,
        s_old,
        tau,
        pm.up_price,
        pm.down_price,
    )?;
    let (theory_price, _) = calculate_theory_price_for_direction(
        price_to_beat,
        trade.price,
        tau,
        sigma_entry,
        direction,
    );
    let theory_price_cents = theory_price * 100.0;
    let spread_cents = theory_price_cents - real_price_cents;

    if spread_cents < threshold_cents {
        return None;
    }

    Some(TtTriggerCandidate {
        direction,
        s_old,
        s_old_time_ms,
        sigma_entry,
        theory_price_cents,
        real_price_cents,
        spread_cents,
        threshold_cents,
    })
}

fn build_trigger_record(
    venue: Venue,
    trade: &TradeRecord,
    candidate: &TtTriggerCandidate,
    price_to_beat: f64,
) -> TtTriggerRecord {
    let slot_end_ms = (trade.time_ms.div_euclid(PRICE_TO_BEAT_INTERVAL_MS) + 1) * PRICE_TO_BEAT_INTERVAL_MS;
    TtTriggerRecord {
        venue: venue.as_str(),
        venue_label: venue.label(),
        receive_time_ms: trade.receive_time_ms,
        exchange_trade_time_ms: trade.time_ms,
        lag_ms: 0,
        price: trade.price,
        s_old: candidate.s_old,
        s_old_time_ms: candidate.s_old_time_ms,
        s0: price_to_beat,
        tau: (slot_end_ms - trade.time_ms).max(0) as f64 / 1000.0,
        sigma: candidate.sigma_entry,
        theory_price_cents: candidate.theory_price_cents,
        real_price_cents: candidate.real_price_cents,
        spread_cents: candidate.spread_cents,
        threshold_cents: candidate.threshold_cents,
    }
}

fn record_tt_trigger(
    shared: &mut SharedData,
    asset: Asset,
    venue: Venue,
    direction: Direction,
    pm: &PolymarketSnapshot,
    mut trigger: TtTriggerRecord,
) -> TtEventGroup {
    let venue_key = venue.as_str().to_string();
    if let Some(group) = shared.tt_events.iter_mut().rev().find(|group| {
        group.asset == asset.as_str()
            && group.direction == direction
            && trigger.receive_time_ms >= group.first_receive_time_ms
            && trigger.receive_time_ms.saturating_sub(group.first_receive_time_ms) <= EVENT_WINDOW_MS
    }) {
        trigger.lag_ms = trigger.receive_time_ms.saturating_sub(group.first_receive_time_ms);
        group.triggers.entry(venue_key).or_insert(trigger);
        return group.clone();
    }

    let id = shared.next_event_id;
    shared.next_event_id += 1;
    let first_receive_time_ms = trigger.receive_time_ms;
    trigger.lag_ms = 0;
    let mut triggers = BTreeMap::new();
    triggers.insert(venue_key, trigger);
    let group = TtEventGroup {
        id,
        asset: asset.as_str().to_string(),
        direction,
        market_slug: pm.slug.clone(),
        first_venue: venue.as_str(),
        first_venue_label: venue.label(),
        first_receive_time_ms,
        deadline_ms: first_receive_time_ms + EVENT_WINDOW_MS,
        triggers,
    };
    shared.tt_events.push(group.clone());
    if shared.tt_events.len() > 200 {
        let overflow = shared.tt_events.len() - 200;
        shared.tt_events.drain(0..overflow);
    }

    group
}

fn market_elapsed_ms(trade_time_ms: i64) -> i64 {
    trade_time_ms.rem_euclid(PRICE_TO_BEAT_INTERVAL_MS)
}

fn entry_threshold_cents(buy_real_cents: f64) -> Option<f64> {
    if buy_real_cents <= 85.0 {
        Some(10.0)
    } else if buy_real_cents <= 90.0 {
        Some(7.0)
    } else if buy_real_cents <= 93.0 {
        Some(5.0)
    } else if buy_real_cents <= 95.0 {
        Some(3.0)
    } else {
        None
    }
}

fn buy_real_price(pm: &PolymarketSnapshot, direction: Direction) -> Option<f64> {
    let price = match direction {
        Direction::Up => pm.up_price,
        Direction::Down => pm.down_price,
    };
    (price > 0.0).then_some(price)
}

fn select_sold_trade<'a, F>(trades: &'a VecDeque<TradeRecord>, mut qualifies: F) -> Option<&'a TradeRecord>
where
    F: FnMut(&TradeRecord) -> bool,
{
    if trades.len() < 2 {
        return None;
    }

    if let Some(previous) = trades.iter().rev().nth(1) {
        if qualifies(previous) {
            return Some(previous);
        }
    }

    let lookback_target_ms = trades.back()?.time_ms.saturating_sub(10);
    if let Some(candidate) = trades
        .iter()
        .rev()
        .skip(1)
        .find(|entry| entry.time_ms <= lookback_target_ms)
    {
        if qualifies(candidate) {
            return Some(candidate);
        }
    }

    None
}

fn price_at_or_before(trades: &VecDeque<TradeRecord>, target_time_ms: i64) -> Option<f64> {
    trades
        .iter()
        .rev()
        .find(|entry| entry.time_ms <= target_time_ms)
        .map(|entry| entry.price)
}

fn standard_normal() -> Normal {
    Normal::new(0.0, 1.0).unwrap()
}

fn is_valid_sigma(sigma: f64) -> bool {
    sigma.is_finite() && (MIN_VOLATILITY..=MAX_VOLATILITY).contains(&sigma)
}

fn calculate_up_probability(s_t: f64, s0: f64, tau: f64, sigma: f64) -> f64 {
    if tau <= 0.0 || sigma <= 0.0 {
        return if s_t > s0 { 1.0 } else { 0.0 };
    }

    let x = (s_t / s0).ln();
    let sqrt_tau = tau.sqrt();
    let d = (x - 0.5 * sigma * sigma * tau) / (sigma * sqrt_tau);
    standard_normal().cdf(d)
}

fn compute_implied_vol(s_t: f64, s0: f64, tau: f64, p_obs: f64) -> Option<(f64, f64)> {
    if s_t <= 0.0 || s0 <= 0.0 || tau <= 0.0 {
        return None;
    }

    let p_clamped = p_obs.clamp(1e-6, 1.0 - 1e-6);
    let x = (s_t / s0).ln();
    let z = standard_normal().inverse_cdf(p_clamped);
    let discriminant = z * z + 2.0 * x;
    if discriminant < -1e-12 {
        return None;
    }

    let sqrt_disc = discriminant.max(0.0).sqrt();
    let y1 = -z + sqrt_disc;
    let y2 = -z - sqrt_disc;
    let y = if y1 > 0.0 && y2 > 0.0 {
        y1.min(y2)
    } else if y1 > 0.0 {
        y1
    } else if y2 > 0.0 {
        y2
    } else {
        return None;
    };

    let sigma = y / tau.sqrt();
    if sigma <= 0.0 || !sigma.is_finite() {
        return None;
    }

    Some((sigma, y))
}

fn infer_implied_volatility(
    s0: f64,
    s_old: f64,
    tau: f64,
    up_price: f64,
    down_price: f64,
) -> Option<f64> {
    if s0 <= 0.0 || s_old <= 0.0 || tau <= 0.0 {
        return None;
    }

    let price_for_inference = if s_old >= s0 {
        up_price
    } else {
        1.0 - down_price
    };

    compute_implied_vol(s_old, s0, tau, price_for_inference)
        .map(|(sigma, _)| sigma)
        .filter(|sigma| is_valid_sigma(*sigma))
}

fn calculate_theory_price_for_direction(
    s0: f64,
    s_new: f64,
    tau: f64,
    sigma: f64,
    direction: Direction,
) -> (f64, f64) {
    if s0 <= 0.0 || s_new <= 0.0 || tau <= 0.0 || !is_valid_sigma(sigma) {
        return (0.0, 0.0);
    }

    let theory_up = calculate_up_probability(s_new, s0, tau, sigma);
    let theory_price = match direction {
        Direction::Up => theory_up,
        Direction::Down => 1.0 - theory_up,
    };
    (theory_price, sigma)
}

async fn run_polymarket_stream(
    asset: Asset,
    tx: broadcast::Sender<String>,
    shared: Arc<RwLock<SharedData>>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build reqwest client");

    loop {
        let slug = current_polymarket_slug(asset);
        match resolve_polymarket_market(&client, asset, &slug).await {
            Ok((up_token_id, down_token_id)) => {
                {
                    let mut shared = shared.write().await;
                    let entry = shared.polymarket.entry(asset).or_default();
                    entry.asset = asset.as_str().to_string();
                    entry.slug = slug.clone();
                    entry.up_token_id = up_token_id.clone();
                    entry.down_token_id = down_token_id.clone();
                }
                info!(
                    "[Polymarket {}] resolved {} UP={} DOWN={}",
                    asset.as_str(),
                    slug,
                    up_token_id,
                    down_token_id
                );

                match consume_polymarket_stream(
                    asset,
                    slug.clone(),
                    up_token_id,
                    down_token_id,
                    tx.clone(),
                    shared.clone(),
                )
                .await
                {
                    Ok(()) => warn!("[Polymarket {}] stream closed, reconnecting", asset.as_str()),
                    Err(err) => warn!("[Polymarket {}] stream error: {err:#}", asset.as_str()),
                }
            }
            Err(err) => warn!("[Polymarket {}] resolve {} failed: {err:#}", asset.as_str(), slug),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn current_polymarket_slug(asset: Asset) -> String {
    let now_sec = now_ms() / 1000;
    let slot_start = (now_sec / 300) * 300;
    format!("{}-{}", asset.slug_prefix(), slot_start)
}

async fn resolve_polymarket_market(
    client: &reqwest::Client,
    asset: Asset,
    slug: &str,
) -> Result<(String, String)> {
    let url = format!("https://gamma-api.polymarket.com/markets?slug={slug}");
    let text = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .context("Gamma status")?
        .text()
        .await
        .context("Gamma body")?;
    let data: Value = serde_json::from_str(&text).context("Gamma JSON")?;
    let markets = data
        .as_array()
        .cloned()
        .or_else(|| data.get("markets").and_then(Value::as_array).cloned())
        .ok_or_else(|| anyhow!("Gamma response has no markets array for {}", asset.as_str()))?;
    let market = markets
        .first()
        .ok_or_else(|| anyhow!("Gamma returned empty markets for {slug}"))?;
    let token_ids = market
        .get("clobTokenIds")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Gamma market missing clobTokenIds for {slug}"))?;
    let ids: Vec<String> = serde_json::from_str(token_ids).context("parse clobTokenIds")?;
    if ids.len() < 2 {
        return Err(anyhow!("Gamma clobTokenIds has fewer than 2 ids for {slug}"));
    }
    Ok((ids[0].clone(), ids[1].clone()))
}

async fn consume_polymarket_stream(
    asset: Asset,
    slug: String,
    up_token_id: String,
    down_token_id: String,
    tx: broadcast::Sender<String>,
    shared: Arc<RwLock<SharedData>>,
) -> Result<()> {
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    let (socket, _) = connect_async(url).await.context("connect Polymarket ws")?;
    info!("[Polymarket {}] stream connected: {}", asset.as_str(), slug);

    let subscribe = json!({
        "type": "market",
        "operation": "subscribe",
        "markets": [],
        "assets_ids": [up_token_id, down_token_id],
        "initial_dump": true
    })
    .to_string();
    let slot_end_ms = ((now_ms() / PRICE_TO_BEAT_INTERVAL_MS) + 1) * PRICE_TO_BEAT_INTERVAL_MS;

    let (mut writer, mut reader) = socket.split();
    writer
        .send(WsMessage::Text(subscribe.into()))
        .await
        .context("send Polymarket subscribe")?;

    while let Some(message) = reader.next().await {
        if now_ms() >= slot_end_ms {
            break;
        }
        match message.context("read Polymarket ws")? {
            WsMessage::Text(text) => {
                if let Err(err) = handle_polymarket_message(asset, text.as_ref(), &tx, &shared).await {
                    debug!("[Polymarket {}] parse skipped: {err:#}", asset.as_str());
                }
            }
            WsMessage::Ping(payload) => {
                writer.send(WsMessage::Pong(payload)).await.context("send pong")?;
            }
            WsMessage::Close(_) => break,
            _ => {}
        }
    }

    Ok(())
}

async fn handle_polymarket_message(
    asset: Asset,
    text: &str,
    tx: &broadcast::Sender<String>,
    shared: &Arc<RwLock<SharedData>>,
) -> Result<()> {
    let value: Value = serde_json::from_str(text).context("parse Polymarket JSON")?;
    let messages = match value {
        Value::Array(items) => items,
        item @ Value::Object(_) => vec![item],
        _ => return Ok(()),
    };

    let mut changed = false;
    for message in messages {
        let Some(event_type) = message.get("event_type").and_then(Value::as_str) else {
            continue;
        };
        match event_type {
            "book" => {
                let asset_id = value_to_string(message.get("asset_id"));
                let bids = message.get("bids").and_then(Value::as_array).cloned().unwrap_or_default();
                let asks = message.get("asks").and_then(Value::as_array).cloned().unwrap_or_default();
                let best_bid = best_bid_from_levels(&bids);
                let best_ask = best_ask_from_levels(&asks);
                let timestamp = parse_i64_value(message.get("timestamp")).unwrap_or_else(now_ms);
                changed |= update_polymarket_asset(
                    asset,
                    &asset_id,
                    best_bid,
                    best_ask,
                    timestamp,
                    shared,
                )
                .await;
            }
            "price_change" => {
                let timestamp = parse_i64_value(message.get("timestamp")).unwrap_or_else(now_ms);
                let changes = message
                    .get("price_changes")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for change in changes {
                    let asset_id = value_to_string(change.get("asset_id"));
                    let best_bid = parse_f64_value(change.get("best_bid"));
                    let best_ask = parse_f64_value(change.get("best_ask"));
                    changed |= update_polymarket_asset(
                        asset,
                        &asset_id,
                        best_bid,
                        best_ask,
                        timestamp,
                        shared,
                    )
                    .await;
                }
            }
            "best_bid_ask" => {
                let asset_id = value_to_string(message.get("asset_id"));
                let best_bid = parse_f64_value(message.get("best_bid"));
                let best_ask = parse_f64_value(message.get("best_ask"));
                let timestamp = parse_i64_value(message.get("timestamp")).unwrap_or_else(now_ms);
                changed |= update_polymarket_asset(
                    asset,
                    &asset_id,
                    best_bid,
                    best_ask,
                    timestamp,
                    shared,
                )
                .await;
            }
            _ => {}
        }
    }

    if changed {
        let snapshot = {
            let shared = shared.read().await;
            shared.polymarket.get(&asset).cloned()
        };
        if let Some(snapshot) = snapshot {
            let msg = json!({
                "type": "polymarket",
                "asset": asset.as_str(),
                "data": snapshot,
                "server_time_ms": now_ms(),
            })
            .to_string();
            let _ = tx.send(msg);
        }
    }

    Ok(())
}

async fn update_polymarket_asset(
    asset: Asset,
    asset_id: &str,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    timestamp: i64,
    shared: &Arc<RwLock<SharedData>>,
) -> bool {
    if asset_id.is_empty() {
        return false;
    }

    let receive_time_ms = now_ms();
    let mut shared = shared.write().await;
    let entry = shared.polymarket.entry(asset).or_default();
    let mut changed = false;

    if asset_id == entry.up_token_id {
        if let Some(bid) = best_bid {
            entry.up_bid = format_decimal(bid);
            changed = true;
        }
        if let Some(ask) = best_ask {
            entry.up_ask = format_decimal(ask);
            entry.up_price = if ask > 0.0 { ask } else { 0.0 };
            changed = true;
        }
    } else if asset_id == entry.down_token_id {
        if let Some(bid) = best_bid {
            entry.down_bid = format_decimal(bid);
            changed = true;
        }
        if let Some(ask) = best_ask {
            entry.down_ask = format_decimal(ask);
            entry.down_price = if ask > 0.0 { ask } else { 0.0 };
            changed = true;
        }
    }

    if changed {
        entry.asset = asset.as_str().to_string();
        entry.market_timestamp_ms = timestamp;
        entry.receive_time_ms = receive_time_ms;
        entry.latency_ms = receive_time_ms.saturating_sub(timestamp);
    }

    changed
}

fn best_bid_from_levels(levels: &[Value]) -> Option<f64> {
    levels
        .iter()
        .filter_map(|level| parse_f64_value(level.get("price")))
        .filter(|price| price.is_finite() && *price > 0.0)
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

fn best_ask_from_levels(levels: &[Value]) -> Option<f64> {
    levels
        .iter()
        .filter_map(|level| parse_f64_value(level.get("price")))
        .filter(|price| price.is_finite() && *price > 0.0)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

fn value_to_string(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(value) => value.to_string().trim_matches('"').to_string(),
        None => String::new(),
    }
}

fn parse_f64_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn parse_i64_value(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::String(s) => s.parse::<i64>().ok(),
        Value::Number(n) => n.as_i64(),
        _ => None,
    }
}

fn format_decimal(value: f64) -> String {
    if value == 0.0 {
        "0".to_string()
    } else {
        format!("{value:.4}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

fn log_large_raw_move(venue: Venue, previous_price: Option<f64>, tick: &TradeTick, raw_text: &str) {
    if !tick.price.is_finite() || tick.price <= 0.0 {
        warn!(
            "[{}] raw invalid price after validation: symbol={} price={:.12} raw_price={} trade_id={} event_time={} trade_time={} receive_time={} raw={}",
            venue.as_str(),
            tick.symbol,
            tick.price,
            tick.raw_price,
            tick.trade_id,
            tick.exchange_event_time_ms,
            tick.exchange_trade_time_ms,
            tick.receive_time_ms,
            raw_text,
        );
        return;
    }

    let Some(previous_price) = previous_price else {
        return;
    };
    if previous_price <= 0.0 {
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
