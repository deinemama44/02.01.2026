#!/usr/bin/env python3
"""
OLYMPUS - Production Crypto Trading Bot for Bitget Futures
Single-file implementation. Python 3.9+ compatible.
All 22 bug fixes applied, all 15 new features implemented.
"""

import os
import sys
import json
import time
import signal
import hashlib
import hmac
import logging
import asyncio
import statistics
import math
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple
from logging.handlers import RotatingFileHandler
from collections import deque
from functools import wraps

try:
    import numpy as np
except ImportError:
    np = None

# ===========================================================================
# CONFIGURATION
# ===========================================================================

CFG = {
    "sprint_hours": 96,
    "risk_per_trade": 0.06,           # AGGRO: 6% pro Trade
    "max_risk_per_trade": 0.12,       # AGGRO: bis 12%
    "max_concurrent": 6,              # AGGRO: 6 gleichzeitig
    "leverage_min": 18,               # AGGRO: min 18x
    "leverage_max": 30,               # AGGRO: bis 30x
    "daily_loss_limit": 0.20,         # AGGRO: 20% Tageslimit
    "max_drawdown": 0.35,             # AGGRO: 35% DD erlaubt
    "drawdown_recovery_threshold": 0.25,
    "drawdown_recovery_size_mult": 0.7,
    "drawdown_recovery_trades": 5,
    "portfolio_heat_max": 0.40,       # AGGRO: 40% Heat
    "tp1_r": 0.6,                     # Schnellerer erster TP
    "tp1_pct": 0.20,                  # Weniger rausnehmen
    "tp2_r": 2.0,                     # Schnellerer voller TP
    "trail_activate_r": 1.0,          # Frueherer Trail
    "trail_atr_mult": 2.0,            # Engerer Trail
    "pyramid_r": 1.2,                 # Frueher pyramiden
    "pyramid_size_pct": 0.50,         # Mehr dazu
    "max_trade_duration_hours": 3,    # Kuerzer halten
    "max_trade_duration_min_r": 0.3,
    "hard_timeout_hours": 5,
    "cycle_sec": 5,                   # AGGRO: schnellerer Scan
    "cache_ttl": 15,                  # Frischere Daten
    "health_ping_sec": 30,
    "cooldown_after_win": 3,          # AGGRO: fast kein Cooldown
    "cooldown_after_loss": 15,        # AGGRO: kurzer Cooldown
    "cooldown_base": 5,               # AGGRO: minimal
    "ml_retrain_hours": 3,            # Oefter trainieren
    "ml_min_samples": 30,             # Weniger Samples noetig
    "ml_confidence_threshold": 0.42,  # AGGRO: niedrigere Schwelle
    "timeframes": ["1m", "5m", "15m", "1h"],
    "mtf_min_score": 1.5,             # AGGRO: weniger Confluence noetig
    "sessions": {"asia": (0, 8), "london": (8, 14), "ny": (14, 21), "off": (21, 24)},
    "session_vol_mult": {"asia": 0.9, "london": 1.0, "ny": 1.1, "off": 0.7},
    "funding_threshold": 0.001,       # AGGRO: hoehere Funding toleriert
    "margin_ratio_warn": 0.15,
    "margin_reduce_pct": 0.30,
    "correlation_threshold": 0.85,    # AGGRO: weniger Blocking
    "correlation_window": 30,
    "symbols": [
        # Majors
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "BNB/USDT:USDT", "XRP/USDT:USDT", "ADA/USDT:USDT",
        # Layer1 / Layer2
        "AVAX/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT",
        "APT/USDT:USDT", "SUI/USDT:USDT", "TIA/USDT:USDT",
        "INJ/USDT:USDT", "ATOM/USDT:USDT", "ARB/USDT:USDT",
        "OP/USDT:USDT", "MATIC/USDT:USDT", "STRK/USDT:USDT",
        "SEI/USDT:USDT", "TON/USDT:USDT",
        # DeFi / Infra
        "LINK/USDT:USDT", "UNI/USDT:USDT", "AAVE/USDT:USDT",
        "LDO/USDT:USDT", "FET/USDT:USDT", "RNDR/USDT:USDT",
        "DYDX/USDT:USDT", "GMX/USDT:USDT",
        # Memes / Hot
        "DOGE/USDT:USDT", "WIF/USDT:USDT", "PEPE/USDT:USDT",
        "BONK/USDT:USDT", "FLOKI/USDT:USDT", "SHIB/USDT:USDT",
        "ORDI/USDT:USDT",
        # Oldies but Goldies
        "LTC/USDT:USDT", "BCH/USDT:USDT", "ETC/USDT:USDT",
        "TRX/USDT:USDT", "FIL/USDT:USDT", "ICP/USDT:USDT",
    ],
    # ----- News -----
    "news_enabled": True,
    "news_check_sec": 300,                 # alle 5 min
    "news_lookback_min": 60,               # 60 min relevant
    "news_alert_importance": 7,            # >=7/10 -> Telegram
    "news_sentiment_window": 30,           # min news im Sentiment-Pool
    # ----- Liquidations -----
    "liq_enabled": True,
    "liq_check_sec": 60,
    "liq_alert_usd": 1_000_000,            # >=1M USDT cluster -> Alert
    "liq_hunt_threshold_usd": 500_000,     # Konter-Trade Schwelle
    "liq_hunt_window_sec": 180,            # Liq < 3 min alt
    # ----- Open Interest -----
    "oi_enabled": True,
    "oi_check_sec": 300,
    "oi_change_threshold": 0.04,           # 4% in 1h = signifikant
    "oi_history_max": 24,                  # 24 snapshots
    # ----- Long/Short Ratio -----
    "lsr_enabled": True,
    "lsr_check_sec": 600,
    "lsr_extreme_threshold": 2.5,          # >2.5 oder <0.4 = extrem
    # ----- Sentiment -----
    "sentiment_size_boost_max": 1.25,      # bis +25% Size
    "sentiment_size_cut_min": 0.5,         # bis -50% Size
    # ----- Telegram-Reporting -----
    "tg_hourly_summary": True,
    "tg_summary_interval_sec": 3600,
    "tg_top_n": 10,
    # ----- Symbol-Ranking -----
    "rank_top_n": 25,                      # nur Top-25 pro Cycle scannen
    "dash_port": 8080,
    "dash_host": "0.0.0.0",
    "webhook_port": 8081,
    "db_path": "olympus.db",
    "log_file": "olympus.log",
    "log_max_bytes": 10_000_000,
    "log_backups": 5,
}


# ===========================================================================
# LOGGING
# ===========================================================================

log = logging.getLogger("olympus")
log.setLevel(logging.DEBUG)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
_fh = RotatingFileHandler(
    CFG["log_file"], maxBytes=CFG["log_max_bytes"], backupCount=CFG["log_backups"]
)
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(_fmt)
log.addHandler(_fh)
_sh = logging.StreamHandler()
_sh.setLevel(logging.INFO)
_sh.setFormatter(_fmt)
log.addHandler(_sh)


# ===========================================================================
# ENVIRONMENT
# ===========================================================================


def _load_env():
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip("'\""))


_load_env()

API_KEY = os.environ.get("BITGET_API_KEY", "")
API_SECRET = os.environ.get("BITGET_API_SECRET", "")
API_PASSPHRASE = os.environ.get("BITGET_PASSPHRASE", "")
TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
DASH_TOKEN = os.environ.get("DASHBOARD_TOKEN", "olympus_secret_token")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
CRYPTOPANIC_TOKEN = os.environ.get("CRYPTOPANIC_TOKEN", "")  # optional

if not WEBHOOK_SECRET:
    log.warning("WEBHOOK_SECRET not set - webhook disabled")


# ===========================================================================
# GLOBAL STATE
# ===========================================================================

bot_active = True
bot_paused = False
sprint_start = time.time()
consec_losses = 0
consec_wins = 0
daily_pnl = 0.0
daily_pnl_date = ""
peak_equity = 0.0
current_drawdown = 0.0
drawdown_recovery_remaining = 0
exchange = None
db = None
symbols_info: Dict[str, Any] = {}
data_cache: Dict[str, Any] = {}
position_cache: List[Dict] = []
position_cache_time = 0.0
correlation_matrix: Dict[str, Dict[str, float]] = {}
ml_model = None
ml_win_rate = 0.5
equity_history: List[Tuple[float, float]] = []
slippage_log: List[Dict] = []
active_trades: Dict[str, Dict] = {}
grid_levels: Dict[str, List[Dict]] = {}
shutdown_event = None
last_health_ping = 0.0

# ----- News / Sentiment / Liquidations / OI / LSR -----
news_cache: List[Dict] = []                     # latest news items
last_news_check = 0.0
last_news_alert_id = ""                         # avoid spam
liquidations_log: deque = deque(maxlen=500)     # recent liqs
last_liq_check = 0.0
last_liq_alert_ts = 0.0
oi_history: Dict[str, deque] = {}               # symbol -> deque of (ts, oi)
last_oi_check = 0.0
ls_ratio: Dict[str, float] = {}                 # symbol -> long/short
last_lsr_check = 0.0
sentiment_score = 0.0                           # global market sentiment [-1..+1]
symbol_sentiment: Dict[str, float] = {}         # per-symbol [-1..+1]
last_hourly_summary = 0.0
strategy_stats: Dict[str, Dict[str, float]] = {}  # per-strategy pnl/wins/losses
risk_override: Optional[float] = None           # manual /setrisk
symbol_blacklist: set = set()


# ===========================================================================
# HELPERS (Bug #19 fix: sf logs unexpected, Bug #20 fix: retry specific exc)
# ===========================================================================


def sf(value, fmt=".4f", default="N/A"):
    """Safe format with debug logging for unexpected values."""
    try:
        if value is None:
            return default
        return f"{float(value):{fmt}}"
    except (TypeError, ValueError) as e:
        log.debug("sf() unexpected value: %r type=%s err=%s", value, type(value).__name__, e)
        return default


def retry(max_attempts=3, delay=1.0, exceptions=(IOError, OSError, ConnectionError)):
    """Retry decorator - only catches specific exceptions (Bug #20)."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except (SystemExit, KeyboardInterrupt, asyncio.CancelledError):
                    raise
                except exceptions as e:
                    last_exc = e
                    if attempt < max_attempts - 1:
                        log.warning("Retry %d/%d for %s: %s", attempt + 1, max_attempts, func.__name__, e)
                        await asyncio.sleep(delay * (attempt + 1))
            if last_exc:
                raise last_exc
        return wrapper
    return decorator


def get_session() -> str:
    """Feature #2: detect current trading session."""
    hour = datetime.now(timezone.utc).hour
    for name, (start, end) in CFG["sessions"].items():
        if start <= hour < end:
            return name
    return "off"


def session_size_mult() -> float:
    """Feature #2: position size multiplier based on session."""
    return CFG["session_vol_mult"].get(get_session(), 0.8)


def cooldown_seconds() -> int:
    """Feature #13: smart cooldown based on recent performance."""
    if consec_losses >= 3:
        return CFG["cooldown_after_loss"] * 2
    elif consec_losses >= 1:
        return CFG["cooldown_after_loss"]
    elif consec_wins >= 2:
        return max(5, CFG["cooldown_after_win"] // 2)
    return CFG["cooldown_base"]


# ===========================================================================
# DATABASE (Bug #8, #14, #22, Feature #10, #15)
# ===========================================================================

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL, side TEXT NOT NULL,
    entry_price REAL, exit_price REAL, size REAL,
    pnl REAL, r_multiple REAL, strategy TEXT,
    opened_at TEXT, closed_at TEXT,
    status TEXT DEFAULT 'open',
    sl_original REAL, sl_dist_original REAL,
    weighted_entry REAL, slippage REAL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS equity_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL, equity REAL NOT NULL, drawdown REAL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS daily_stats (
    date TEXT PRIMARY KEY, pnl REAL DEFAULT 0,
    consec_losses INTEGER DEFAULT 0, consec_wins INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS bot_state (
    key TEXT PRIMARY KEY, value TEXT
);
"""


async def db_init():
    global db
    try:
        import aiosqlite
    except ImportError:
        log.error("aiosqlite not installed - DB disabled")
        return
    db = await aiosqlite.connect(CFG["db_path"])
    db.row_factory = aiosqlite.Row
    await db.executescript(DB_SCHEMA)
    await db.commit()
    log.info("Database initialized")


async def db_close():
    if db:
        await db.close()


async def db_save_state(key: str, value: Any):
    if not db:
        return
    await db.execute(
        "INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)",
        (key, json.dumps(value)),
    )
    await db.commit()


async def db_load_state(key: str, default=None) -> Any:
    if not db:
        return default
    async with db.execute("SELECT value FROM bot_state WHERE key=?", (key,)) as cur:
        row = await cur.fetchone()
        if row:
            return json.loads(row[0])
    return default


async def db_open_trade(symbol, side, entry, size, strategy, sl_orig, sl_dist, slippage=0.0):
    if not db:
        return 0
    cur = await db.execute(
        "INSERT INTO trades (symbol,side,entry_price,size,strategy,opened_at,"
        "status,sl_original,sl_dist_original,weighted_entry,slippage) "
        "VALUES (?,?,?,?,?,?,'open',?,?,?,?)",
        (symbol, side, entry, size, strategy,
         datetime.now(timezone.utc).isoformat(), sl_orig, sl_dist, entry, slippage),
    )
    await db.commit()
    return cur.lastrowid


async def db_close_trade(trade_id, exit_price, pnl, r_mult):
    if not db:
        return
    await db.execute(
        "UPDATE trades SET exit_price=?,pnl=?,r_multiple=?,closed_at=?,"
        "status='closed' WHERE id=?",
        (exit_price, pnl, r_mult, datetime.now(timezone.utc).isoformat(), trade_id),
    )
    await db.commit()


async def db_close_all_open():
    """Bug #8: close all DB records on emergency close."""
    if not db:
        return
    await db.execute(
        "UPDATE trades SET status='closed', closed_at=? WHERE status='open'",
        (datetime.now(timezone.utc).isoformat(),),
    )
    await db.commit()


async def db_update_weighted_entry(trade_id, new_entry):
    if not db:
        return
    await db.execute("UPDATE trades SET weighted_entry=? WHERE id=?", (new_entry, trade_id))
    await db.commit()


async def db_save_equity(equity, dd):
    if not db:
        return
    await db.execute(
        "INSERT INTO equity_snapshots (ts,equity,drawdown) VALUES (?,?,?)",
        (datetime.now(timezone.utc).isoformat(), equity, dd),
    )
    await db.commit()


async def db_save_daily():
    if not db:
        return
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await db.execute(
        "INSERT OR REPLACE INTO daily_stats (date,pnl,consec_losses,consec_wins) VALUES (?,?,?,?)",
        (today, daily_pnl, consec_losses, consec_wins),
    )
    await db.commit()


async def db_load_daily():
    """Bug #14, #22: restore daily stats and consec_losses on restart."""
    global daily_pnl, daily_pnl_date, consec_losses, consec_wins
    if not db:
        return
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with db.execute("SELECT * FROM daily_stats WHERE date=?", (today,)) as cur:
        row = await cur.fetchone()
        if row:
            daily_pnl = float(row["pnl"]) if row["pnl"] else 0.0
            consec_losses = int(row["consec_losses"]) if row["consec_losses"] else 0
            consec_wins = int(row["consec_wins"]) if row["consec_wins"] else 0
            daily_pnl_date = today
            log.info("Restored: pnl=%.2f, losses=%d, wins=%d", daily_pnl, consec_losses, consec_wins)


async def db_win_rate(lookback=100) -> float:
    """Bug #16: actual historical win rate for Kelly formula."""
    if not db:
        return 0.5
    async with db.execute(
        "SELECT pnl FROM trades WHERE status='closed' ORDER BY id DESC LIMIT ?",
        (lookback,),
    ) as cur:
        rows = await cur.fetchall()
    if not rows or len(rows) < 10:
        return 0.5
    wins = sum(1 for r in rows if r["pnl"] and float(r["pnl"]) > 0)
    return wins / len(rows)


# ===========================================================================
# EXCHANGE LAYER (Bug #10, #11, #12 fixes)
# ===========================================================================


async def init_exchange():
    global exchange
    try:
        import ccxt.async_support as ccxt_async
    except ImportError:
        log.error("ccxt not installed")
        return
    exchange = ccxt_async.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": API_PASSPHRASE,
        "options": {"defaultType": "swap", "defaultSettle": "USDT"},
        "enableRateLimit": True,
    })
    try:
        await exchange.load_markets()
        global symbols_info
        for sym in CFG["symbols"]:
            if sym in exchange.markets:
                symbols_info[sym] = exchange.markets[sym]
        log.info("Exchange ready, %d symbols", len(symbols_info))
    except Exception as e:
        log.error("Market load failed: %s", e)

    # FIX Bitget 40774: account is in hedge mode but we send one-way orders.
    # Try to switch account to one-way mode (so we don't need holdSide param).
    try:
        await exchange.set_position_mode(False)  # False = one-way / unilateral
        log.info("Bitget position mode: one-way (unilateral)")
    except Exception as e:
        # Some Bitget versions need productType param; we will fall back to
        # supplying holdSide manually in place_order.
        log.warning("set_position_mode failed (%s) - will use holdSide fallback", e)


def _bitget_hold_side(side: str, params: Optional[Dict] = None) -> Dict:
    """Build Bitget v2 params with holdSide for hedge-mode accounts.

    side='buy' -> long, 'sell' -> short. If reduceOnly=True, holdSide is the
    side of the EXISTING position (opposite of the close order).
    """
    p = dict(params or {})
    if p.get("reduceOnly"):
        # closing a long uses sell with holdSide=long; closing a short uses buy with holdSide=short
        p["holdSide"] = "long" if side == "sell" else "short"
        p.setdefault("tradeSide", "close")
    else:
        p["holdSide"] = "long" if side == "buy" else "short"
        p.setdefault("tradeSide", "open")
    return p


async def close_exchange():
    if exchange:
        try:
            await exchange.close()
        except Exception:
            pass


@retry(max_attempts=3, delay=2.0, exceptions=(Exception,))
async def fetch_ohlcv(symbol: str, timeframe: str = "5m", limit: int = 100):
    """Fetch candles with cache (Bug #10, #11: longer TTL, batched)."""
    cache_key = f"{symbol}_{timeframe}"
    now = time.time()
    if cache_key in data_cache:
        ct, cd = data_cache[cache_key]
        if now - ct < CFG["cache_ttl"]:
            return cd
    if not exchange:
        return []
    data = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    data_cache[cache_key] = (now, data)
    return data


async def fetch_positions_once() -> List[Dict]:
    """Bug #12: fetch positions ONCE per cycle and reuse."""
    global position_cache, position_cache_time
    now = time.time()
    if now - position_cache_time < CFG["cache_ttl"]:
        return position_cache
    if not exchange:
        return []
    try:
        positions = await exchange.fetch_positions()
        position_cache = [p for p in positions if float(p.get("contracts", 0)) > 0]
        position_cache_time = now
        return position_cache
    except Exception as e:
        log.error("fetch_positions error: %s", e)
        return position_cache


async def fetch_balance() -> float:
    if not exchange:
        return 0.0
    try:
        bal = await exchange.fetch_balance({"type": "swap"})
        return float(bal.get("total", {}).get("USDT", 0))
    except Exception as e:
        log.error("fetch_balance error: %s", e)
        return 0.0


async def fetch_ticker(symbol: str) -> Dict:
    cache_key = "tick_" + symbol
    now = time.time()
    if cache_key in data_cache:
        ct, cd = data_cache[cache_key]
        if now - ct < CFG["cache_ttl"]:
            return cd
    if not exchange:
        return {}
    try:
        ticker = await exchange.fetch_ticker(symbol)
        data_cache[cache_key] = (now, ticker)
        return ticker
    except Exception as e:
        log.error("fetch_ticker %s: %s", symbol, e)
        return {}


async def fetch_funding_rate(symbol: str) -> float:
    """Feature #3: check funding rate before opening."""
    try:
        if not exchange:
            return 0.0
        info = await exchange.fetch_funding_rate(symbol)
        return float(info.get("fundingRate", 0))
    except Exception:
        return 0.0


async def get_margin_ratio() -> float:
    """Feature #4: anti-liquidation guard."""
    try:
        if not exchange:
            return 1.0
        bal = await exchange.fetch_balance({"type": "swap"})
        total = float(bal.get("total", {}).get("USDT", 1))
        used = float(bal.get("used", {}).get("USDT", 0))
        if total <= 0:
            return 1.0
        return (total - used) / total
    except Exception:
        return 1.0


async def set_leverage(symbol: str, leverage: int):
    try:
        if exchange:
            await exchange.set_leverage(leverage, symbol)
    except Exception as e:
        log.debug("set_leverage %s %dx: %s", symbol, leverage, e)


async def place_order(symbol, side, amount, order_type="market", price=None, params=None):
    if not exchange:
        return None
    params = params or {}
    try:
        order = await exchange.create_order(symbol, order_type, side, amount, price, params)
        return order
    except Exception as e:
        msg = str(e)
        # Bitget 40774: hedge-mode account needs holdSide. Auto-retry with it.
        if "40774" in msg or "unilateral position" in msg:
            try:
                p2 = _bitget_hold_side(side, params)
                log.info("Retrying %s with holdSide=%s tradeSide=%s",
                         symbol, p2.get("holdSide"), p2.get("tradeSide"))
                order = await exchange.create_order(symbol, order_type, side, amount, price, p2)
                return order
            except Exception as e2:
                log.error("place_order(retry) %s %s %s: %s", symbol, side, amount, e2)
                return None
        log.error("place_order %s %s %s: %s", symbol, side, amount, e)
        return None


async def cancel_order(symbol: str, order_id: str) -> bool:
    try:
        if exchange:
            await exchange.cancel_order(order_id, symbol)
            return True
    except Exception as e:
        log.debug("cancel_order %s: %s", order_id, e)
    return False


async def place_smart(symbol, side, amount, limit_price=None):
    """Bug #21: position check before market fallback to prevent doubles."""
    if limit_price:
        order = await place_order(symbol, side, amount, "limit", limit_price)
        if order:
            await asyncio.sleep(3)
            try:
                status = await exchange.fetch_order(order["id"], symbol)
                if status["status"] == "closed":
                    return status
                cancelled = await cancel_order(symbol, order["id"])
                if not cancelled:
                    positions = await fetch_positions_once()
                    for p in positions:
                        if p["symbol"] == symbol and float(p.get("contracts", 0)) > 0:
                            log.warning("Already filled on %s, skip market", symbol)
                            return status
            except Exception:
                pass
    # Market fallback with position check (Bug #21)
    positions = await fetch_positions_once()
    for p in positions:
        if p["symbol"] == symbol and float(p.get("contracts", 0)) > 0:
            log.warning("Position exists on %s, skipping market", symbol)
            return None
    return await place_order(symbol, side, amount, "market")


# ===========================================================================
# INDICATORS
# ===========================================================================


def calc_ema(closes: List[float], period: int) -> List[float]:
    if len(closes) < period:
        return closes[:]
    ema = [sum(closes[:period]) / period]
    mult = 2 / (period + 1)
    for price in closes[period:]:
        ema.append((price - ema[-1]) * mult + ema[-1])
    return ema


def calc_rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = deltas[-period:]
    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]
    avg_gain = sum(gains) / period if gains else 0.001
    avg_loss = sum(losses) / period if losses else 0.001
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_atr(highs, lows, closes, period=14) -> float:
    if len(closes) < period + 1:
        return abs(highs[-1] - lows[-1]) if highs else 0.0
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    return sum(trs[-period:]) / period


def calc_macd(closes):
    if len(closes) < 26:
        return 0.0, 0.0, 0.0
    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    ml = min(len(ema12), len(ema26))
    macd_line = [ema12[-ml + i] - ema26[-ml + i] for i in range(ml)]
    if len(macd_line) < 9:
        return macd_line[-1] if macd_line else 0, 0, 0
    sig = calc_ema(macd_line, 9)
    hist = macd_line[-1] - sig[-1] if sig else 0
    return macd_line[-1], sig[-1] if sig else 0, hist


def calc_bbands(closes, period=20, std_mult=2.0):
    if len(closes) < period:
        return closes[-1], closes[-1], closes[-1]
    window = closes[-period:]
    mid = sum(window) / period
    std = statistics.stdev(window) if len(window) > 1 else 0
    return mid + std_mult * std, mid, mid - std_mult * std


def calc_adx(highs, lows, closes, period=14) -> float:
    if len(closes) < period + 1:
        return 25.0
    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(highs)):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if up > down and up > 0 else 0)
        minus_dm.append(down if down > up and down > 0 else 0)
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        tr_list.append(tr)
    if len(tr_list) < period:
        return 25.0
    atr = sum(tr_list[-period:]) / period
    if atr == 0:
        return 25.0
    plus_di = 100 * sum(plus_dm[-period:]) / period / atr
    minus_di = 100 * sum(minus_dm[-period:]) / period / atr
    denom = plus_di + minus_di + 0.001
    return abs(plus_di - minus_di) / denom * 100


def calc_volume_profile(candles, bins=20) -> Dict[str, float]:
    """Feature #11: Volume Profile with POC."""
    if not candles or len(candles) < 10:
        return {"poc": 0, "vah": 0, "val": 0}
    prices = [(c[2] + c[3]) / 2 for c in candles]
    volumes = [c[5] for c in candles]
    min_p, max_p = min(prices), max(prices)
    if max_p == min_p:
        return {"poc": min_p, "vah": min_p, "val": min_p}
    bin_size = (max_p - min_p) / bins
    vol_at = [0.0] * bins
    for p, v in zip(prices, volumes):
        idx = min(int((p - min_p) / bin_size), bins - 1)
        vol_at[idx] += v
    poc_idx = vol_at.index(max(vol_at))
    poc = min_p + (poc_idx + 0.5) * bin_size
    total_vol = sum(vol_at)
    sorted_bins = sorted(range(bins), key=lambda i: vol_at[i], reverse=True)
    cum_vol = 0
    va_bins = []
    for idx in sorted_bins:
        cum_vol += vol_at[idx]
        va_bins.append(idx)
        if cum_vol >= total_vol * 0.7:
            break
    val_price = min_p + min(va_bins) * bin_size
    vah_price = min_p + (max(va_bins) + 1) * bin_size
    return {"poc": poc, "vah": vah_price, "val": val_price}


def detect_rsi_divergence(closes, period=14) -> str:
    """Feature #12: RSI divergence detection."""
    if len(closes) < period + 20:
        return "none"
    rsi_vals = []
    for i in range(20, 0, -1):
        rsi_vals.append(calc_rsi(closes[:-i], period))
    rsi_vals.append(calc_rsi(closes, period))
    recent_closes = closes[-21:]
    if len(recent_closes) < 10:
        return "none"
    if (recent_closes[-1] < min(recent_closes[-10:-1]) and
            rsi_vals[-1] > min(rsi_vals[-10:-1])):
        return "bullish"
    if (recent_closes[-1] > max(recent_closes[-10:-1]) and
            rsi_vals[-1] < max(rsi_vals[-10:-1])):
        return "bearish"
    return "none"


# ===========================================================================
# NEWS / LIQUIDATIONS / OI / LSR / SENTIMENT (NEW)
# ===========================================================================

# Symbol -> base ticker (e.g. "BTC/USDT:USDT" -> "BTC")
def _base_of(symbol: str) -> str:
    try:
        return symbol.split("/")[0].upper()
    except Exception:
        return symbol


# Bullish / bearish keyword maps for news sentiment
_BULL_KW = (
    "approve", "approval", "etf", "bullish", "rally", "surge", "soar",
    "moon", "all-time high", "ath", "partnership", "adopt", "adoption",
    "upgrade", "burn", "buyback", "institutional", "inflow", "halving",
    "pump", "breakout", "listing", "listed", "launch", "mainnet",
)
_BEAR_KW = (
    "hack", "exploit", "rug", "scam", "lawsuit", "sue", "ban", "banned",
    "outflow", "dump", "crash", "plunge", "bearish", "investigation",
    "fraud", "default", "liquidat", "sell-off", "selloff", "halt",
    "delist", "delisted", "exploit", "vulnerab", "downtime",
)


def _score_text(text: str) -> int:
    """+1 per bullish word, -1 per bearish. Returns net score."""
    if not text:
        return 0
    t = text.lower()
    s = 0
    for kw in _BULL_KW:
        if kw in t:
            s += 1
    for kw in _BEAR_KW:
        if kw in t:
            s -= 1
    return s


async def fetch_news() -> List[Dict]:
    """Pull crypto news from CryptoPanic (free) - parsed for sentiment."""
    global news_cache, last_news_check
    if not CFG["news_enabled"]:
        return []
    now = time.time()
    if now - last_news_check < CFG["news_check_sec"]:
        return news_cache
    last_news_check = now
    try:
        import aiohttp
    except ImportError:
        return news_cache
    base = "https://cryptopanic.com/api/v1/posts/"
    params = {"public": "true", "kind": "news"}
    if CRYPTOPANIC_TOKEN:
        params["auth_token"] = CRYPTOPANIC_TOKEN
        params.pop("public", None)
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{base}?{qs}"
    items: List[Dict] = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    log.debug("cryptopanic %d", resp.status)
                    return news_cache
                data = await resp.json()
        for r in data.get("results", []):
            title = r.get("title", "") or ""
            currencies = [c.get("code", "") for c in (r.get("currencies") or [])]
            published = r.get("published_at", "")
            votes = r.get("votes") or {}
            importance = (votes.get("important", 0) or 0) + (votes.get("positive", 0) or 0) + (votes.get("negative", 0) or 0)
            score = _score_text(title)
            # CryptoPanic vote-based hints
            if (votes.get("positive", 0) or 0) > (votes.get("negative", 0) or 0):
                score += 1
            elif (votes.get("negative", 0) or 0) > (votes.get("positive", 0) or 0):
                score -= 1
            items.append({
                "id": r.get("id") or r.get("slug", title[:30]),
                "title": title,
                "url": r.get("url", ""),
                "source": (r.get("source") or {}).get("title", ""),
                "currencies": currencies,
                "published": published,
                "importance": importance,
                "score": score,
                "ts": now,
            })
    except Exception as e:
        log.debug("fetch_news error: %s", e)
        return news_cache
    # keep latest 200
    news_cache = items[:200]
    log.info("News refreshed: %d items", len(news_cache))
    return news_cache


def news_for_symbol(symbol: str, lookback_sec: Optional[int] = None) -> List[Dict]:
    """Filter news cache for a symbol."""
    base = _base_of(symbol)
    cutoff = time.time() - (lookback_sec or CFG["news_lookback_min"] * 60)
    out = []
    for n in news_cache:
        if n["ts"] < cutoff:
            continue
        if base in [c.upper() for c in n.get("currencies", [])]:
            out.append(n)
    return out


def compute_symbol_sentiment(symbol: str) -> float:
    """Aggregate per-symbol sentiment in [-1, +1] from recent news."""
    items = news_for_symbol(symbol)
    if not items:
        return 0.0
    total = sum(i["score"] for i in items)
    norm = max(1, len(items) * 2)  # rough normalisation
    val = max(-1.0, min(1.0, total / norm))
    return val


def compute_market_sentiment() -> float:
    """Global market sentiment from all recent news, in [-1, +1]."""
    if not news_cache:
        return 0.0
    cutoff = time.time() - CFG["news_lookback_min"] * 60
    recent = [n for n in news_cache if n["ts"] >= cutoff]
    if not recent:
        return 0.0
    total = sum(n["score"] for n in recent)
    norm = max(1, len(recent) * 2)
    return max(-1.0, min(1.0, total / norm))


async def refresh_sentiment():
    """Refresh symbol+market sentiment caches."""
    global sentiment_score, symbol_sentiment
    sentiment_score = compute_market_sentiment()
    new_map = {}
    for sym in CFG["symbols"]:
        new_map[sym] = compute_symbol_sentiment(sym)
    symbol_sentiment = new_map


async def fetch_liquidations():
    """Binance USDT-M public liquidation WebSocket (free, real-time).

    Connects once and streams forced-order events. Since main loop calls this
    repeatedly, we keep a singleton task that runs until shutdown.
    """
    global last_liq_check
    if not CFG["liq_enabled"]:
        return
    last_liq_check = time.time()
    if getattr(fetch_liquidations, "_running", False):
        return
    fetch_liquidations._running = True

    async def _stream():
        try:
            import aiohttp
        except ImportError:
            return
        url = "wss://fstream.binance.com/ws/!forceOrder@arr"
        backoff = 1
        while not (shutdown_event and shutdown_event.is_set()):
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_read=30)
                async with aiohttp.ClientSession(timeout=timeout) as sess:
                    async with sess.ws_connect(url, heartbeat=60) as ws:
                        log.info("Liq stream connected")
                        backoff = 1
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            try:
                                payload = json.loads(msg.data)
                                o = payload.get("o", payload)
                                sym_raw = o.get("s", "")
                                if not sym_raw.endswith("USDT"):
                                    continue
                                base = sym_raw[:-4]
                                price = float(o.get("ap", o.get("p", 0)) or 0)
                                qty = float(o.get("q", 0) or 0)
                                value = price * qty
                                if value < 5_000:
                                    continue
                                # SELL liq = long got rekt; BUY liq = short got rekt
                                side = "long" if o.get("S") == "SELL" else "short"
                                ts = float(o.get("T", time.time() * 1000)) / 1000.0
                                liquidations_log.append({
                                    "symbol": f"{base}/USDT:USDT",
                                    "side": side, "price": price, "qty": qty,
                                    "value_usd": value, "ts": ts,
                                    "source": "binance_ws",
                                })
                            except Exception:
                                continue
            except Exception as e:
                log.debug("Liq stream error: %s, retry in %ds", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(60, backoff * 2)

    asyncio.create_task(_stream())


def liq_cluster_for(symbol: str, window_sec: int = None) -> Tuple[float, float]:
    """Sum (long-liq-value, short-liq-value) for symbol in window."""
    window_sec = window_sec or CFG["liq_hunt_window_sec"]
    cutoff = time.time() - window_sec
    long_val, short_val = 0.0, 0.0
    for l in liquidations_log:
        if l["symbol"] != symbol or l["ts"] < cutoff:
            continue
        if l["side"] == "long":
            long_val += l["value_usd"]
        else:
            short_val += l["value_usd"]
    return long_val, short_val


def total_liq_window(window_sec: int = 300) -> Dict[str, float]:
    """Total liq value per symbol in window."""
    cutoff = time.time() - window_sec
    out: Dict[str, float] = {}
    for l in liquidations_log:
        if l["ts"] < cutoff:
            continue
        out[l["symbol"]] = out.get(l["symbol"], 0) + l["value_usd"]
    return out


async def fetch_open_interest_all():
    """Snapshot OI for all symbols and store history."""
    global last_oi_check
    if not CFG["oi_enabled"] or not exchange:
        return
    now = time.time()
    if now - last_oi_check < CFG["oi_check_sec"]:
        return
    last_oi_check = now
    if not hasattr(exchange, "fetch_open_interest"):
        return
    for sym in CFG["symbols"]:
        try:
            oi = await exchange.fetch_open_interest(sym)
            value = float(oi.get("openInterestAmount", 0) or oi.get("openInterestValue", 0) or 0)
            if value <= 0:
                continue
            if sym not in oi_history:
                oi_history[sym] = deque(maxlen=CFG["oi_history_max"])
            oi_history[sym].append((now, value))
        except Exception:
            continue


def oi_change(symbol: str, lookback_sec: int = 3600) -> float:
    """Returns relative OI change over lookback. 0 if no data."""
    hist = oi_history.get(symbol)
    if not hist or len(hist) < 2:
        return 0.0
    now = time.time()
    cutoff = now - lookback_sec
    older = None
    for t, v in hist:
        if t >= cutoff:
            older = (t, v)
            break
    if older is None:
        older = hist[0]
    latest = hist[-1]
    if older[1] <= 0:
        return 0.0
    return (latest[1] - older[1]) / older[1]


async def fetch_long_short_ratio():
    """Pull global long/short account ratio from Binance for major pairs."""
    global last_lsr_check
    if not CFG["lsr_enabled"]:
        return
    now = time.time()
    if now - last_lsr_check < CFG["lsr_check_sec"]:
        return
    last_lsr_check = now
    try:
        import aiohttp
    except ImportError:
        return
    base_url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
    async with aiohttp.ClientSession() as session:
        for sym in CFG["symbols"]:
            base = _base_of(sym)
            params = f"symbol={base}USDT&period=15m&limit=1"
            try:
                async with session.get(f"{base_url}?{params}", timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                if data and isinstance(data, list):
                    ls_ratio[sym] = float(data[0].get("longShortRatio", 1.0))
            except Exception:
                continue
            await asyncio.sleep(0.05)  # be nice


async def fetch_top_trader_ratio():
    """Smart Money: Binance topLongShortPositionRatio (FREE, public).

    Top-trader position ratio shows what large/pro traders are positioned.
    Stored in ls_ratio[symbol+'_top'].
    """
    if not CFG["lsr_enabled"]:
        return
    try:
        import aiohttp
    except ImportError:
        return
    base_url = "https://fapi.binance.com/futures/data/topLongShortPositionRatio"
    async with aiohttp.ClientSession() as session:
        for sym in CFG["symbols"]:
            base = _base_of(sym)
            params = f"symbol={base}USDT&period=15m&limit=1"
            try:
                async with session.get(f"{base_url}?{params}", timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                if data and isinstance(data, list):
                    ls_ratio[sym + "_top"] = float(data[0].get("longShortRatio", 1.0))
            except Exception:
                continue
            await asyncio.sleep(0.05)


async def fetch_taker_ratio():
    """Aggressive Buy/Sell Pressure: Binance takerlongshortRatio (FREE, public).

    Taker buy/sell volume ratio - shows aggressor side.
    >1 = buyers aggressive, <1 = sellers aggressive.
    Stored in ls_ratio[symbol+'_taker'].
    """
    if not CFG["lsr_enabled"]:
        return
    try:
        import aiohttp
    except ImportError:
        return
    base_url = "https://fapi.binance.com/futures/data/takerlongshortRatio"
    async with aiohttp.ClientSession() as session:
        for sym in CFG["symbols"]:
            base = _base_of(sym)
            params = f"symbol={base}USDT&period=5m&limit=1"
            try:
                async with session.get(f"{base_url}?{params}", timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                if data and isinstance(data, list):
                    ls_ratio[sym + "_taker"] = float(data[0].get("buySellRatio", 1.0))
            except Exception:
                continue
            await asyncio.sleep(0.05)


async def fetch_oi_history_binance():
    """Pull Binance public OI history (better than ccxt for trend detection)."""
    if not CFG["oi_enabled"]:
        return
    try:
        import aiohttp
    except ImportError:
        return
    base_url = "https://fapi.binance.com/futures/data/openInterestHist"
    async with aiohttp.ClientSession() as session:
        for sym in CFG["symbols"]:
            base = _base_of(sym)
            params = f"symbol={base}USDT&period=15m&limit=8"  # 2h history
            try:
                async with session.get(f"{base_url}?{params}", timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                if not isinstance(data, list):
                    continue
                if sym not in oi_history:
                    oi_history[sym] = deque(maxlen=CFG["oi_history_max"])
                oi_history[sym].clear()
                for entry in data:
                    ts = float(entry.get("timestamp", 0)) / 1000.0
                    val = float(entry.get("sumOpenInterest", 0))
                    if val > 0:
                        oi_history[sym].append((ts, val))
            except Exception:
                continue
            await asyncio.sleep(0.05)


async def order_book_imbalance(symbol: str, depth: int = 20) -> float:
    """Order Flow Imbalance: (bids - asks) / (bids + asks) on top N levels.

    +1 = pure buying pressure, -1 = pure selling pressure.
    """
    if not exchange:
        return 0.0
    try:
        ob = await exchange.fetch_order_book(symbol, limit=depth)
        bids = sum(b[1] for b in ob.get("bids", [])[:depth])
        asks = sum(a[1] for a in ob.get("asks", [])[:depth])
        if bids + asks <= 0:
            return 0.0
        return (bids - asks) / (bids + asks)
    except Exception:
        return 0.0


def smart_money_bias(symbol: str) -> float:
    """Combine top-trader ratio + taker ratio into [-1, +1] bias.

    > 0 = smart money / aggressors leaning long
    < 0 = leaning short
    """
    top = ls_ratio.get(symbol + "_top", 0.0)
    tak = ls_ratio.get(symbol + "_taker", 0.0)
    parts = []
    if top > 0:
        # log-scale around 1.0 -> bias
        parts.append(max(-1.0, min(1.0, math.log(top + 1e-9))))
    if tak > 0:
        parts.append(max(-1.0, min(1.0, math.log(tak + 1e-9))))
    if not parts:
        return 0.0
    return sum(parts) / len(parts)


def opportunity_score(symbol: str, candles: List) -> float:
    """Rank symbol by trading opportunity. Higher = better.

    Combines: volatility, trend, vol-spike, sentiment, liquidation-cluster,
    OI change, and smart-money bias.
    """
    if not candles or len(candles) < 30:
        return 0.0
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    volumes = [c[5] for c in candles]
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    if price <= 0:
        return 0.0
    atr_pct = atr / price
    adx = calc_adx(highs, lows, closes)
    avg_vol = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else 1
    vol_spike = volumes[-1] / avg_vol if avg_vol > 0 else 1
    sent = symbol_sentiment.get(symbol, 0.0)
    long_liq, short_liq = liq_cluster_for(symbol)
    liq_factor = (long_liq + short_liq) / 100_000
    oi_chg = abs(oi_change(symbol))
    smart = abs(smart_money_bias(symbol))
    score = (
        atr_pct * 100 * 1.5
        + adx / 25.0
        + min(vol_spike, 5) * 0.5
        + abs(sent) * 1.5
        + min(liq_factor, 50) * 0.05
        + oi_chg * 30
        + smart * 1.5
    )
    return score


# ===========================================================================
# MULTI-TIMEFRAME CONFLUENCE (Feature #1)
# ===========================================================================


async def mtf_score(symbol: str) -> Tuple[int, str]:
    """Score signal confluence across timeframes."""
    bull, bear = 0.0, 0.0
    for tf in CFG["timeframes"]:
        candles = await fetch_ohlcv(symbol, tf, 100)
        if not candles or len(candles) < 30:
            continue
        closes = [c[4] for c in candles]
        rsi = calc_rsi(closes)
        _, _, hist = calc_macd(closes)
        ema_f = calc_ema(closes, 9)
        ema_s = calc_ema(closes, 21)
        if ema_f and ema_s and ema_f[-1] > ema_s[-1]:
            bull += 1
        elif ema_f and ema_s:
            bear += 1
        if hist > 0:
            bull += 0.5
        else:
            bear += 0.5
        if rsi < 35:
            bull += 0.5
        elif rsi > 65:
            bear += 0.5
    if bull >= CFG["mtf_min_score"] and bull > bear:
        return int(bull), "long"
    if bear >= CFG["mtf_min_score"] and bear > bull:
        return int(bear), "short"
    return 0, "none"


# ===========================================================================
# CORRELATION (Feature #6: dynamic rolling correlation)
# ===========================================================================


async def update_correlations():
    global correlation_matrix
    if np is None:
        return
    returns_data = {}
    for symbol in CFG["symbols"]:
        candles = await fetch_ohlcv(symbol, "1h", CFG["correlation_window"] + 1)
        if candles and len(candles) > 10:
            closes = [c[4] for c in candles]
            rets = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes))]
            returns_data[symbol] = rets
    sym_list = list(returns_data.keys())
    correlation_matrix = {}
    for i, s1 in enumerate(sym_list):
        correlation_matrix[s1] = {}
        for j, s2 in enumerate(sym_list):
            if i == j:
                correlation_matrix[s1][s2] = 1.0
                continue
            r1, r2 = returns_data[s1], returns_data[s2]
            ml = min(len(r1), len(r2))
            if ml < 10:
                correlation_matrix[s1][s2] = 0.0
                continue
            try:
                corr = float(np.corrcoef(r1[-ml:], r2[-ml:])[0, 1])
                correlation_matrix[s1][s2] = corr if not math.isnan(corr) else 0.0
            except Exception:
                correlation_matrix[s1][s2] = 0.0


def is_correlated_blocked(symbol: str) -> bool:
    """Block trades on correlated assets."""
    if symbol not in correlation_matrix:
        return False
    for open_sym in active_trades:
        if open_sym in correlation_matrix.get(symbol, {}):
            if abs(correlation_matrix[symbol][open_sym]) > CFG["correlation_threshold"]:
                log.info("Blocked %s: correlated with open %s", symbol, open_sym)
                return True
    return False


# ===========================================================================
# ML MODEL (Bug #1 fix: warmup_ml properly implemented)
# ===========================================================================


def build_features(candles) -> List[float]:
    if not candles or len(candles) < 30:
        return []
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    volumes = [c[5] for c in candles]
    rsi = calc_rsi(closes)
    atr = calc_atr(highs, lows, closes)
    _, _, hist = calc_macd(closes)
    adx = calc_adx(highs, lows, closes)
    bb_u, bb_m, bb_l = calc_bbands(closes)
    price = closes[-1]
    feats = [
        rsi / 100.0,
        atr / price if price > 0 else 0,
        hist / price if price > 0 else 0,
        adx / 100.0,
        (price - bb_l) / (bb_u - bb_l) if bb_u != bb_l else 0.5,
        (closes[-1] - closes[-5]) / closes[-5] if len(closes) >= 5 and closes[-5] > 0 else 0,
        (closes[-1] - closes[-10]) / closes[-10] if len(closes) >= 10 and closes[-10] > 0 else 0,
        (volumes[-1] / (sum(volumes[-20:]) / 20)) if len(volumes) >= 20 and sum(volumes[-20:]) > 0 else 1.0,
        1.0 if closes[-1] > closes[-2] else 0.0,
        (highs[-1] - lows[-1]) / closes[-1] if closes[-1] > 0 else 0,
    ]
    return feats


async def warmup_ml():
    """Bug #1 fix: properly implemented ML warmup."""
    global ml_model, ml_win_rate
    try:
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    except ImportError:
        log.warning("sklearn not available - ML disabled")
        return
    model_path = Path(__file__).parent / "ml_model.pkl"
    if model_path.exists():
        try:
            with open(model_path, "rb") as f:
                ml_model = pickle.load(f)
            log.info("ML model loaded from disk")
            ml_win_rate = await db_win_rate()
            return
        except Exception as e:
            log.warning("Failed to load ML model: %s", e)
    await train_ml()


async def train_ml():
    """Train ML ensemble model."""
    global ml_model, ml_win_rate
    try:
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
        from sklearn.ensemble import VotingClassifier
    except ImportError:
        return
    X_all, y_all = [], []
    for symbol in CFG["symbols"][:8]:
        candles = await fetch_ohlcv(symbol, "5m", 500)
        if not candles or len(candles) < 100:
            continue
        for i in range(50, len(candles) - 10):
            features = build_features(candles[:i])
            if not features or len(features) != 10:
                continue
            future_high = max(c[2] for c in candles[i:i + 10])
            current = candles[i][4]
            label = 1 if (future_high - current) / current > 0.003 else 0
            X_all.append(features)
            y_all.append(label)
    if len(X_all) < CFG["ml_min_samples"]:
        log.warning("Not enough ML samples: %d", len(X_all))
        return
    try:
        gb = GradientBoostingClassifier(n_estimators=50, max_depth=4, random_state=42)
        rf = RandomForestClassifier(n_estimators=50, max_depth=4, random_state=42)
        ensemble = VotingClassifier(estimators=[("gb", gb), ("rf", rf)], voting="soft")
        ensemble.fit(X_all, y_all)
        ml_model = ensemble
        model_path = Path(__file__).parent / "ml_model.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(ml_model, f)
        log.info("ML model trained on %d samples", len(X_all))
        ml_win_rate = await db_win_rate()
    except Exception as e:
        log.error("ML training failed: %s", e)


def ml_predict(features) -> Tuple[float, float]:
    if ml_model is None or not features or len(features) != 10:
        return 0.5, 0.5
    try:
        proba = ml_model.predict_proba([features])[0]
        return float(proba[1]), float(max(proba))
    except Exception:
        return 0.5, 0.5


# ===========================================================================
# STRATEGIES (5 regime-based)
# ===========================================================================


async def strategy_trend(symbol, candles):
    if len(candles) < 50:
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    ema9 = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    ema50 = calc_ema(closes, 50)
    if not ema9 or not ema21 or not ema50:
        return None
    adx = calc_adx(highs, lows, closes)
    if adx < 20:
        return None
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    # Long signal
    if (ema9[-1] > ema21[-1] > ema50[-1] and len(ema9) > 2 and
            len(ema21) > 1 and ema9[-2] <= ema21[-2]):
        return {"side": "buy", "strategy": "trend", "confidence": min(adx / 50, 0.9),
                "sl": price - 2.0 * atr, "tp": price + 3.0 * atr, "atr": atr}
    # Short signal
    if (ema9[-1] < ema21[-1] < ema50[-1] and len(ema9) > 2 and
            len(ema21) > 1 and ema9[-2] >= ema21[-2]):
        return {"side": "sell", "strategy": "trend", "confidence": min(adx / 50, 0.9),
                "sl": price + 2.0 * atr, "tp": price - 3.0 * atr, "atr": atr}
    return None


async def strategy_momentum(symbol, candles):
    if len(candles) < 30:
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    rsi = calc_rsi(closes)
    _, _, hist = calc_macd(closes)
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    if rsi < 30 and hist > 0:
        div = detect_rsi_divergence(closes)
        conf = 0.7 if div == "bullish" else 0.6
        return {"side": "buy", "strategy": "momentum", "confidence": conf,
                "sl": price - 1.8 * atr, "tp": price + 2.5 * atr, "atr": atr}
    if rsi > 70 and hist < 0:
        div = detect_rsi_divergence(closes)
        conf = 0.7 if div == "bearish" else 0.6
        return {"side": "sell", "strategy": "momentum", "confidence": conf,
                "sl": price + 1.8 * atr, "tp": price - 2.5 * atr, "atr": atr}
    return None


async def strategy_breakout(symbol, candles):
    if len(candles) < 40:
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    volumes = [c[5] for c in candles]
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    high_20 = max(highs[-21:-1])
    low_20 = min(lows[-21:-1])
    avg_vol = sum(volumes[-20:]) / 20 if volumes else 1
    vol_spike = volumes[-1] > avg_vol * 1.5
    if price > high_20 and vol_spike:
        return {"side": "buy", "strategy": "breakout", "confidence": 0.65,
                "sl": price - 2.0 * atr, "tp": price + 3.5 * atr, "atr": atr}
    if price < low_20 and vol_spike:
        return {"side": "sell", "strategy": "breakout", "confidence": 0.65,
                "sl": price + 2.0 * atr, "tp": price - 3.5 * atr, "atr": atr}
    return None


async def strategy_scalp(symbol, candles):
    if len(candles) < 25:
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    bb_u, bb_m, bb_l = calc_bbands(closes)
    rsi = calc_rsi(closes, 7)
    if price <= bb_l and rsi < 25:
        return {"side": "buy", "strategy": "scalp", "confidence": 0.6,
                "sl": price - 1.2 * atr, "tp": bb_m, "atr": atr}
    if price >= bb_u and rsi > 75:
        return {"side": "sell", "strategy": "scalp", "confidence": 0.6,
                "sl": price + 1.2 * atr, "tp": bb_m, "atr": atr}
    return None


async def strategy_reversal(symbol, candles):
    """Reversal at key levels with RSI divergence (Feature #12)."""
    if len(candles) < 50:
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    div = detect_rsi_divergence(closes)
    vp = calc_volume_profile(candles)
    if vp["poc"] > 0:
        dist = abs(price - vp["poc"]) / price
        if dist < 0.005:
            if div == "bullish":
                return {"side": "buy", "strategy": "reversal", "confidence": 0.7,
                        "sl": price - 2.0 * atr, "tp": price + 3.0 * atr, "atr": atr}
            if div == "bearish":
                return {"side": "sell", "strategy": "reversal", "confidence": 0.7,
                        "sl": price + 2.0 * atr, "tp": price - 3.0 * atr, "atr": atr}
    if vp["val"] > 0 and price <= vp["val"] and div == "bullish":
        return {"side": "buy", "strategy": "reversal", "confidence": 0.65,
                "sl": price - 2.5 * atr, "tp": vp["poc"], "atr": atr}
    if vp["vah"] > 0 and price >= vp["vah"] and div == "bearish":
        return {"side": "sell", "strategy": "reversal", "confidence": 0.65,
                "sl": price + 2.5 * atr, "tp": vp["poc"], "atr": atr}
    return None


async def strategy_liquidation_hunt(symbol, candles):
    """Fade after a one-sided liquidation cascade.

    Big long-liqs in last 3min -> fade DOWN move with LONG (short squeeze setup).
    Big short-liqs -> fade UP move with SHORT.
    """
    if len(candles) < 30:
        return None
    long_liq, short_liq = liq_cluster_for(symbol)
    threshold = CFG["liq_hunt_threshold_usd"]
    if long_liq < threshold and short_liq < threshold:
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    rsi = calc_rsi(closes, 7)
    # Long-liqs > 2x short-liqs and price already crashed -> fade up
    if long_liq > short_liq * 2 and long_liq >= threshold and rsi < 35:
        conf = min(0.85, 0.55 + long_liq / 5_000_000)
        return {"side": "buy", "strategy": "liq_hunt", "confidence": conf,
                "sl": price - 1.4 * atr, "tp": price + 2.5 * atr, "atr": atr,
                "meta": {"long_liq": long_liq, "short_liq": short_liq}}
    if short_liq > long_liq * 2 and short_liq >= threshold and rsi > 65:
        conf = min(0.85, 0.55 + short_liq / 5_000_000)
        return {"side": "sell", "strategy": "liq_hunt", "confidence": conf,
                "sl": price + 1.4 * atr, "tp": price - 2.5 * atr, "atr": atr,
                "meta": {"long_liq": long_liq, "short_liq": short_liq}}
    return None


async def strategy_news_momentum(symbol, candles):
    """Trade fresh strong news in direction of sentiment with confluence."""
    if len(candles) < 30:
        return None
    items = news_for_symbol(symbol, lookback_sec=30 * 60)
    if not items:
        return None
    # need a clearly directional cluster
    score_sum = sum(i["score"] for i in items)
    if abs(score_sum) < 2:
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    _, _, hist = calc_macd(closes)
    avg_vol = sum(c[5] for c in candles[-20:]) / 20 if len(candles) >= 20 else 1
    vol_ok = candles[-1][5] > avg_vol * 1.2
    if score_sum >= 2 and hist > 0 and vol_ok:
        conf = min(0.85, 0.55 + min(score_sum, 6) * 0.05)
        return {"side": "buy", "strategy": "news_momo", "confidence": conf,
                "sl": price - 1.8 * atr, "tp": price + 3.0 * atr, "atr": atr,
                "meta": {"news_score": score_sum, "news_count": len(items)}}
    if score_sum <= -2 and hist < 0 and vol_ok:
        conf = min(0.85, 0.55 + min(abs(score_sum), 6) * 0.05)
        return {"side": "sell", "strategy": "news_momo", "confidence": conf,
                "sl": price + 1.8 * atr, "tp": price - 3.0 * atr, "atr": atr,
                "meta": {"news_score": score_sum, "news_count": len(items)}}
    return None


async def strategy_oi_divergence(symbol, candles):
    """Open Interest divergence: rising OI + breakout = strong continuation.

    OI up + price up = longs piling in, ride
    OI up + price down = shorts piling in, fade or short
    OI down + move = position unwinding, weaker
    """
    if len(candles) < 30:
        return None
    oi_chg = oi_change(symbol)
    if abs(oi_chg) < CFG["oi_change_threshold"]:
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    if len(closes) < 6:
        return None
    price_chg = (closes[-1] - closes[-6]) / closes[-6] if closes[-6] > 0 else 0
    # Strong OI growth in trend direction = momentum
    if oi_chg > CFG["oi_change_threshold"] and price_chg > 0.005:
        conf = min(0.8, 0.55 + oi_chg * 3)
        return {"side": "buy", "strategy": "oi_div", "confidence": conf,
                "sl": price - 1.8 * atr, "tp": price + 3.2 * atr, "atr": atr,
                "meta": {"oi_change": oi_chg}}
    if oi_chg > CFG["oi_change_threshold"] and price_chg < -0.005:
        # OI grows while price falls -> aggressive shorts, ride down
        conf = min(0.8, 0.55 + oi_chg * 3)
        return {"side": "sell", "strategy": "oi_div", "confidence": conf,
                "sl": price + 1.8 * atr, "tp": price - 3.2 * atr, "atr": atr,
                "meta": {"oi_change": oi_chg}}
    return None


async def strategy_smart_money(symbol, candles):
    """Follow top-trader position bias + taker pressure confluence."""
    if len(candles) < 30:
        return None
    bias = smart_money_bias(symbol)
    if abs(bias) < 0.2:  # no clear edge
        return None
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    atr = calc_atr(highs, lows, closes)
    price = closes[-1]
    ema21 = calc_ema(closes, 21)
    if not ema21:
        return None
    above_ema = closes[-1] > ema21[-1]
    rsi = calc_rsi(closes)
    # Bias up + price above EMA + RSI not overbought
    if bias > 0.25 and above_ema and rsi < 70:
        conf = min(0.85, 0.55 + bias * 0.5)
        return {"side": "buy", "strategy": "smart_money", "confidence": conf,
                "sl": price - 1.8 * atr, "tp": price + 2.8 * atr, "atr": atr,
                "meta": {"bias": bias}}
    if bias < -0.25 and not above_ema and rsi > 30:
        conf = min(0.85, 0.55 + abs(bias) * 0.5)
        return {"side": "sell", "strategy": "smart_money", "confidence": conf,
                "sl": price + 1.8 * atr, "tp": price - 2.8 * atr, "atr": atr,
                "meta": {"bias": bias}}
    return None


ALL_STRATEGIES = [
    strategy_trend, strategy_momentum, strategy_breakout,
    strategy_scalp, strategy_reversal,
    strategy_liquidation_hunt, strategy_news_momentum,
    strategy_oi_divergence, strategy_smart_money,
]


# ===========================================================================
# POSITION SIZING (Bug #5, #16, Feature #2, #7)
# ===========================================================================


async def kelly_size(confidence: float, equity: float, signal: Optional[Dict] = None) -> float:
    """Kelly + session + recovery + sentiment bias for sizing."""
    win_rate = await db_win_rate() if db else ml_win_rate
    if win_rate <= 0 or win_rate >= 1:
        win_rate = 0.5
    avg_r = 1.5
    kelly = win_rate - (1 - win_rate) / avg_r
    kelly = max(0.01, min(kelly, 0.25))
    base_risk = risk_override if risk_override is not None else CFG["risk_per_trade"]
    size_pct = kelly * confidence * base_risk / 0.02
    size_pct *= session_size_mult()
    if drawdown_recovery_remaining > 0:
        size_pct *= CFG["drawdown_recovery_size_mult"]

    # Sentiment-aligned sizing boost / cut
    if signal:
        sym = signal.get("_symbol")
        side = signal.get("side")
        sym_sent = symbol_sentiment.get(sym, 0.0) if sym else 0.0
        global_sent = sentiment_score
        # Aligned sentiment -> boost; misaligned -> cut
        if side == "buy":
            aligned = (sym_sent > 0.1) or (global_sent > 0.2)
            misaligned = (sym_sent < -0.3) or (global_sent < -0.4)
        else:
            aligned = (sym_sent < -0.1) or (global_sent < -0.2)
            misaligned = (sym_sent > 0.3) or (global_sent > 0.4)
        if aligned:
            size_pct *= min(CFG["sentiment_size_boost_max"],
                            1.0 + abs(sym_sent) * 0.25 + abs(global_sent) * 0.15)
        if misaligned:
            size_pct *= max(CFG["sentiment_size_cut_min"], 0.7)

    size_pct = min(size_pct, CFG["max_risk_per_trade"])
    return equity * size_pct


def portfolio_heat(positions: List[Dict]) -> float:
    """Bug #5: use original sl_dist for heat, not current SL (which may be at BE)."""
    if not positions:
        return 0.0
    total_heat = 0.0
    for pos in positions:
        sym = pos.get("symbol", "")
        if sym in active_trades:
            trade = active_trades[sym]
            sl_dist = trade.get("sl_dist_original", 0)
            size = float(pos.get("contracts", 0)) * float(pos.get("contractSize", 1))
            if sl_dist > 0:
                total_heat += sl_dist * size
    equity = peak_equity if peak_equity > 0 else 1.0
    return total_heat / equity


def calc_leverage(confidence: float, atr_pct: float) -> int:
    base = CFG["leverage_min"]
    mx = CFG["leverage_max"]
    lev = base + int((mx - base) * confidence)
    if atr_pct > 0.02:
        lev = max(base, lev - 3)
    elif atr_pct > 0.015:
        lev = max(base, lev - 1)
    return min(max(lev, base), mx)


# ===========================================================================
# TRADE EXECUTION
# ===========================================================================


async def open_trade(symbol, signal, equity):
    global drawdown_recovery_remaining
    side = signal["side"]
    strategy = signal["strategy"]
    confidence = signal["confidence"]
    atr = signal["atr"]
    sl_price = signal["sl"]

    ticker = await fetch_ticker(symbol)
    if not ticker:
        return
    price = float(ticker.get("last", 0))
    if price <= 0:
        return

    sl_dist = abs(price - sl_price)
    if sl_dist <= 0:
        sl_dist = atr * 1.5

    # pass symbol so kelly_size can apply sentiment boost
    signal["_symbol"] = symbol
    risk_amount = await kelly_size(confidence, equity, signal)
    atr_pct = atr / price if price > 0 else 0.01
    leverage = calc_leverage(confidence, atr_pct)

    contracts = risk_amount / sl_dist if sl_dist > 0 else 0
    if contracts <= 0:
        return

    market_info = symbols_info.get(symbol, {})
    min_amt = float(market_info.get("limits", {}).get("amount", {}).get("min", 0.001))
    if contracts < min_amt:
        return

    await set_leverage(symbol, leverage)
    order = await place_smart(symbol, side, contracts, price)
    if not order:
        return

    fill_price = float(order.get("average", price) or price)
    slippage = abs(fill_price - price) / price
    slippage_log.append({"symbol": symbol, "expected": price, "actual": fill_price,
                         "slippage": slippage, "time": time.time()})

    trade_id = await db_open_trade(symbol, side, fill_price, contracts, strategy,
                                   sl_price, sl_dist, slippage)

    mult = 1 if side == "buy" else -1
    tp1_price = fill_price + sl_dist * CFG["tp1_r"] * mult
    tp2_price = signal["tp"]
    notional = fill_price * contracts
    rr = abs(tp2_price - fill_price) / sl_dist if sl_dist > 0 else 0

    active_trades[symbol] = {
        "id": trade_id, "side": side, "entry": fill_price,
        "weighted_entry": fill_price, "size": contracts,
        "sl": sl_price, "sl_dist_original": sl_dist,
        "tp1": tp1_price, "tp2": tp2_price,
        "trail_active": False, "trail_price": None,
        "strategy": strategy, "confidence": confidence, "atr": atr,
        "leverage": leverage, "opened_at": time.time(),
        "tp1_hit": False, "pyramided": False, "notional": notional,
        "ml_prob": signal.get("ml_prob", 0.5),
        "mtf_score": signal.get("mtf_score", 0),
        "op_score": signal.get("op_score", 0.0),
        "funding": signal.get("funding", 0.0),
        "ob_imb": signal.get("ob_imb", 0.0),
        "meta": signal.get("meta", {}),
    }

    if drawdown_recovery_remaining > 0:
        drawdown_recovery_remaining -= 1

    log.info("OPEN %s %s @ %s | SL=%s | Lev=%dx | %s",
             side.upper(), symbol, sf(fill_price), sf(sl_price), leverage, strategy)

    # ----- Rich Telegram message -----
    side_emoji = "\U0001f7e2" if side == "buy" else "\U0001f534"
    arrow = "\u2191" if side == "buy" else "\u2193"
    sym_sent = symbol_sentiment.get(symbol, 0.0)
    smart = smart_money_bias(symbol)
    long_liq, short_liq = liq_cluster_for(symbol)
    oi_chg = oi_change(symbol)
    news_count = len(news_for_symbol(symbol))
    sess = get_session()

    msg = (
        f"{side_emoji} <b>OPEN {side.upper()}</b> {arrow} <code>{symbol}</code>\n"
        f"\u2503 Strategie: <b>{strategy}</b>\n"
        f"\u2503 Konfidenz: <b>{confidence*100:.1f}%</b> | ML: {signal.get('ml_prob',0)*100:.0f}%\n"
        f"\u2503 MTF-Score: {signal.get('mtf_score',0)} | Op-Score: {signal.get('op_score',0):.2f}\n"
        f"\n<b>Order</b>\n"
        f"\u2503 Entry: <code>{sf(fill_price)}</code>\n"
        f"\u2503 SL: <code>{sf(sl_price)}</code>  (\u0394 {sl_dist/price*100:.2f}%)\n"
        f"\u2503 TP1: <code>{sf(tp1_price)}</code>  ({CFG['tp1_pct']*100:.0f}% out @ {CFG['tp1_r']}R)\n"
        f"\u2503 TP2: <code>{sf(tp2_price)}</code>  ({CFG['tp2_r']}R)\n"
        f"\u2503 Risk:Reward: <b>1:{rr:.2f}</b>\n"
        f"\u2503 Leverage: <b>{leverage}x</b>\n"
        f"\u2503 Size: {contracts:.4f} ({notional:.0f} USDT)\n"
        f"\u2503 Risk: {risk_amount:.2f} USDT ({risk_amount/equity*100:.1f}%)\n"
        f"\u2503 Slippage: {slippage*100:.3f}%\n"
        f"\n<b>Marktkontext</b>\n"
        f"\u2503 Funding: {signal.get('funding',0)*100:.4f}%\n"
        f"\u2503 OB-Imbalance: {signal.get('ob_imb',0):+.2f}\n"
        f"\u2503 OI 1h: {oi_chg*100:+.2f}%\n"
        f"\u2503 Smart-Money: {smart:+.2f}\n"
        f"\u2503 Liqs 3min: long {long_liq/1000:.0f}k$ / short {short_liq/1000:.0f}k$\n"
        f"\u2503 Sentiment: Symbol {sym_sent:+.2f} | Markt {sentiment_score:+.2f}\n"
        f"\u2503 News: {news_count} relevant in 60min\n"
        f"\u2503 Session: {sess} | ATR: {atr/price*100:.2f}%\n"
        f"\n<b>Konto</b>\n"
        f"\u2503 Equity: {equity:.2f} USDT | DD: {current_drawdown*100:.1f}%\n"
        f"\u2503 Offen: {len(active_trades)+1}/{CFG['max_concurrent']} | Heute: {daily_pnl:+.2f}"
    )
    await tg_send(msg)


# ===========================================================================
# TRADE MANAGEMENT (Bug #7, #15, Feature #8, #14)
# ===========================================================================


async def manage_positions():
    """Manage open: TP, SL, trailing, pyramid, timeouts."""
    global active_trades
    positions = await fetch_positions_once()
    pos_map = {p["symbol"]: p for p in positions}
    closed_symbols = []

    for symbol, trade in list(active_trades.items()):
        pos = pos_map.get(symbol)
        if not pos or float(pos.get("contracts", 0)) <= 0:
            closed_symbols.append(symbol)
            continue

        price = float(pos.get("markPrice", 0) or pos.get("entryPrice", 0))
        if price <= 0:
            continue
        side = trade["side"]
        entry = trade["weighted_entry"]
        sl_dist = trade["sl_dist_original"]
        atr = trade["atr"]

        # Current R-multiple (Bug #7: based on weighted entry)
        if side == "buy":
            current_r = (price - entry) / sl_dist if sl_dist > 0 else 0
        else:
            current_r = (entry - price) / sl_dist if sl_dist > 0 else 0

        # Hard timeout (Bug #13)
        hours_open = (time.time() - trade["opened_at"]) / 3600
        if hours_open >= CFG["hard_timeout_hours"]:
            await close_position(symbol, trade, price, "hard_timeout")
            closed_symbols.append(symbol)
            continue

        # Max duration with min R (Feature #14)
        if hours_open >= CFG["max_trade_duration_hours"] and current_r < CFG["max_trade_duration_min_r"]:
            await close_position(symbol, trade, price, "duration_timeout")
            closed_symbols.append(symbol)
            continue

        # Stop Loss
        hit_sl = (side == "buy" and price <= trade["sl"]) or (side == "sell" and price >= trade["sl"])
        if hit_sl:
            await close_position(symbol, trade, price, "stoploss")
            closed_symbols.append(symbol)
            continue

        # TP1: partial close
        if not trade["tp1_hit"] and current_r >= CFG["tp1_r"]:
            partial = trade["size"] * CFG["tp1_pct"]
            close_side = "sell" if side == "buy" else "buy"
            await place_order(symbol, close_side, partial, params={"reduceOnly": True})
            trade["tp1_hit"] = True
            trade["sl"] = entry  # Move to breakeven
            log.info("%s: TP1 hit, partial close, SL->BE", symbol)
            await tg_send(f"\u2705 {symbol} TP1 erreicht! SL auf Einstand")

        # Chandelier Trailing (Feature #8)
        if current_r >= CFG["trail_activate_r"]:
            trade["trail_active"] = True
            chandelier_dist = atr * CFG["trail_atr_mult"]
            if side == "buy":
                new_trail = price - chandelier_dist
                if trade["trail_price"] is None or new_trail > trade["trail_price"]:
                    trade["trail_price"] = new_trail
                trade["sl"] = max(trade["sl"], trade["trail_price"])
            else:
                new_trail = price + chandelier_dist
                if trade["trail_price"] is None or new_trail < trade["trail_price"]:
                    trade["trail_price"] = new_trail
                trade["sl"] = min(trade["sl"], trade["trail_price"])

        # Pyramiding (Bug #7, #15 fixes)
        if not trade["pyramided"] and current_r >= CFG["pyramid_r"]:
            add_size = trade["size"] * CFG["pyramid_size_pct"]
            order = await place_order(symbol, side, add_size)
            if order:
                fill_p = float(order.get("average", price) or price)
                old_size = trade["size"]
                new_total = old_size + add_size
                trade["weighted_entry"] = (entry * old_size + fill_p * add_size) / new_total
                trade["size"] = new_total
                trade["pyramided"] = True
                # Bug #15: adjust SL to protect profit
                if side == "buy":
                    trade["sl"] = max(trade["sl"], trade["weighted_entry"] + sl_dist * 0.5)
                else:
                    trade["sl"] = min(trade["sl"], trade["weighted_entry"] - sl_dist * 0.5)
                await db_update_weighted_entry(trade["id"], trade["weighted_entry"])
                log.info("%s: Pyramided +%.4f", symbol, add_size)

        # TP2: full close
        if current_r >= CFG["tp2_r"]:
            await close_position(symbol, trade, price, "tp2")
            closed_symbols.append(symbol)
            continue

    for sym in closed_symbols:
        active_trades.pop(sym, None)


async def close_position(symbol, trade, price, reason):
    """Close position and record results."""
    global consec_losses, consec_wins, daily_pnl, drawdown_recovery_remaining
    side = trade["side"]
    entry = trade["weighted_entry"]
    size = trade["size"]
    sl_dist = trade["sl_dist_original"]
    strategy = trade.get("strategy", "?")

    close_side = "sell" if side == "buy" else "buy"
    remaining = size * (1 - CFG["tp1_pct"]) if trade.get("tp1_hit") else size
    if remaining > 0:
        await place_order(symbol, close_side, remaining, params={"reduceOnly": True})

    if side == "buy":
        pnl = (price - entry) * size
        r_mult = (price - entry) / sl_dist if sl_dist > 0 else 0
    else:
        pnl = (entry - price) * size
        r_mult = (entry - price) / sl_dist if sl_dist > 0 else 0

    daily_pnl += pnl
    if pnl > 0:
        consec_wins += 1
        consec_losses = 0
    else:
        consec_losses += 1
        consec_wins = 0

    # Per-strategy stats
    s = strategy_stats.setdefault(strategy, {"wins": 0, "losses": 0, "pnl": 0.0,
                                              "best_r": 0.0, "worst_r": 0.0})
    s["pnl"] += pnl
    if pnl > 0:
        s["wins"] = s.get("wins", 0) + 1
    else:
        s["losses"] = s.get("losses", 0) + 1
    if r_mult > s.get("best_r", 0):
        s["best_r"] = r_mult
    if r_mult < s.get("worst_r", 0):
        s["worst_r"] = r_mult

    if current_drawdown >= CFG["drawdown_recovery_threshold"]:
        drawdown_recovery_remaining = CFG["drawdown_recovery_trades"]

    await db_close_trade(trade["id"], price, pnl, r_mult)
    await db_save_daily()

    # Slippage on exit
    slippage_log.append({"symbol": symbol, "expected": price, "actual": price,
                         "slippage": 0.0, "time": time.time()})

    pnl_pct = pnl / (entry * size) * 100 if entry * size > 0 else 0
    dur = (time.time() - trade["opened_at"]) / 60
    emoji = "\U0001f7e2" if pnl > 0 else "\U0001f534"
    eq = await fetch_balance()
    daily_eq = (daily_pnl / eq * 100) if eq > 0 else 0

    win_total = s.get("wins", 0)
    loss_total = s.get("losses", 0)
    strat_wr = win_total / max(1, win_total + loss_total) * 100
    check, cross = "\u2705", "\u274c"
    f_pyr = check if trade.get("pyramided") else cross
    f_tp1 = check if trade.get("tp1_hit") else cross
    f_trail = check if trade.get("trail_active") else cross

    msg = (
        f"{emoji} <b>CLOSE</b> <code>{symbol}</code> ({side.upper()})\n"
        f"\u2503 Grund: <b>{reason}</b>\n"
        f"\u2503 Strategie: {strategy}\n"
        f"\n<b>Ergebnis</b>\n"
        f"\u2503 PnL: <b>{pnl:+.2f} USDT</b> ({pnl_pct:+.2f}%)\n"
        f"\u2503 R-Multiple: <b>{r_mult:+.2f}R</b>\n"
        f"\u2503 Entry: {sf(entry)} \u2192 Exit: {sf(price)}\n"
        f"\u2503 Hebel: {trade.get('leverage','?')}x | Dauer: {dur:.0f} min\n"
        f"\u2503 Pyramide: {f_pyr} | TP1: {f_tp1} | Trail: {f_trail}\n"
        f"\n<b>Strategie-Bilanz ({strategy})</b>\n"
        f"\u2503 W/L: {win_total}/{loss_total}  WR: {strat_wr:.0f}%\n"
        f"\u2503 PnL gesamt: {s['pnl']:+.2f} USDT\n"
        f"\u2503 Best/Worst R: {s.get('best_r',0):.2f} / {s.get('worst_r',0):.2f}\n"
        f"\n<b>Konto</b>\n"
        f"\u2503 Equity: {eq:.2f} USDT\n"
        f"\u2503 Heute: {daily_pnl:+.2f} USDT ({daily_eq:+.2f}%)\n"
        f"\u2503 Streak: W{consec_wins} / L{consec_losses}\n"
        f"\u2503 DD: {current_drawdown*100:.1f}% | Offen: {len(active_trades)-1}"
    )
    log.info("CLOSE %s | %s | PnL=%.2f | R=%.2f", symbol, reason, pnl, r_mult)
    await tg_send(msg)


async def close_all(reason="emergency"):
    """Bug #8: close all positions AND DB records."""
    global active_trades
    positions = await fetch_positions_once()
    for pos in positions:
        symbol = pos["symbol"]
        side = pos.get("side", "")
        contracts = float(pos.get("contracts", 0))
        if contracts > 0:
            cs = "sell" if side == "long" else "buy"
            await place_order(symbol, cs, contracts, params={"reduceOnly": True})
    await db_close_all_open()
    active_trades = {}
    log.warning("All positions closed: %s", reason)
    await tg_send(f"\U0001f6a8 ALLE POSITIONEN GESCHLOSSEN\nGrund: {reason}")


# ===========================================================================
# RISK MANAGEMENT (Bug #6, #13, Feature #4)
# ===========================================================================


async def check_risk() -> bool:
    """Pre-trade risk checks. Returns True if trading allowed."""
    global bot_paused, current_drawdown, peak_equity

    if not bot_active or bot_paused:
        return False

    equity = await fetch_balance()
    if equity <= 0:
        return False

    if equity > peak_equity:
        peak_equity = equity
    if peak_equity > 0:
        current_drawdown = (peak_equity - equity) / peak_equity

    # Bug #6: only pause, never auto-resume here
    if current_drawdown >= CFG["max_drawdown"]:
        if not bot_paused:
            bot_paused = True
            log.critical("Max drawdown %.1f%% - PAUSED", current_drawdown * 100)
            await tg_send(f"\U0001f6a8 MAX DRAWDOWN {current_drawdown:.1%} - Bot pausiert!")
            await close_all("max_drawdown")
        return False

    # Daily loss limit (Bug #14: persisted)
    if equity > 0 and daily_pnl < 0:
        daily_loss_pct = abs(daily_pnl) / equity
        if daily_loss_pct >= CFG["daily_loss_limit"]:
            log.warning("Daily loss limit: %.1f%%", daily_loss_pct * 100)
            return False

    # Portfolio heat (Bug #5)
    positions = await fetch_positions_once()
    heat = portfolio_heat(positions)
    if heat > CFG["portfolio_heat_max"]:
        return False

    # Max concurrent
    if len(active_trades) >= CFG["max_concurrent"]:
        return False

    # Anti-liquidation (Feature #4)
    margin = await get_margin_ratio()
    if margin < CFG["margin_ratio_warn"]:
        log.warning("Low margin: %.1f%% - reducing", margin * 100)
        await tg_send(f"\u26a0\ufe0f Niedrige Margin: {margin:.1%}!")
        if positions:
            largest = max(positions, key=lambda p: float(p.get("notional", 0) or 0))
            sym = largest["symbol"]
            c = float(largest.get("contracts", 0))
            reduce = c * CFG["margin_reduce_pct"]
            cs = "sell" if largest.get("side") == "long" else "buy"
            await place_order(sym, cs, reduce, params={"reduceOnly": True})
        return False

    return True


# ===========================================================================
# SIGNAL SCANNER
# ===========================================================================


async def scan_signals():
    """Scan all symbols for trading signals - ranked by opportunity score."""
    if not await check_risk():
        return

    equity = await fetch_balance()
    if equity <= 0:
        return

    # 1) Pre-fetch candles for ranking
    candidates: List[Tuple[str, List, float]] = []
    for symbol in CFG["symbols"]:
        if symbol in active_trades:
            continue
        if symbol in symbol_blacklist:
            continue
        if is_correlated_blocked(symbol):
            continue
        candles = await fetch_ohlcv(symbol, "5m", 200)
        if not candles or len(candles) < 50:
            continue
        score = opportunity_score(symbol, candles)
        candidates.append((symbol, candles, score))

    # 2) Rank by opportunity score, take top N
    candidates.sort(key=lambda x: x[2], reverse=True)
    candidates = candidates[: CFG["rank_top_n"]]

    # 3) Eval strategies on ranked candidates
    for symbol, candles, op_score in candidates:
        # MTF Confluence (skip for news/liq strategies which have own logic)
        mtf_sc, mtf_dir = await mtf_score(symbol)

        # Run strategies
        best_signal = None
        best_conf = 0.0
        for strat_fn in ALL_STRATEGIES:
            try:
                sig = await strat_fn(symbol, candles)
                if not sig:
                    continue
                # MTF confluence required for "regular" strategies
                if strat_fn.__name__ in ("strategy_trend", "strategy_momentum",
                                         "strategy_breakout", "strategy_scalp",
                                         "strategy_reversal"):
                    if mtf_sc < CFG["mtf_min_score"] or mtf_dir == "none":
                        continue
                    if not ((mtf_dir == "long" and sig["side"] == "buy") or
                            (mtf_dir == "short" and sig["side"] == "sell")):
                        continue
                if sig["confidence"] > best_conf:
                    best_signal = sig
                    best_conf = sig["confidence"]
            except Exception as e:
                log.error("Strategy %s on %s: %s", strat_fn.__name__, symbol, e)

        if not best_signal:
            continue

        # ML check
        features = build_features(candles)
        ml_prob, ml_conf = ml_predict(features)
        if ml_conf < CFG["ml_confidence_threshold"]:
            continue
        best_signal["ml_prob"] = ml_prob
        best_signal["ml_conf"] = ml_conf
        best_signal["mtf_score"] = mtf_sc
        best_signal["op_score"] = op_score
        best_signal["confidence"] = (best_signal["confidence"] + ml_conf) / 2

        # Funding rate filter
        funding = await fetch_funding_rate(symbol)
        best_signal["funding"] = funding
        if best_signal["side"] == "buy" and funding > CFG["funding_threshold"]:
            continue
        if best_signal["side"] == "sell" and funding < -CFG["funding_threshold"]:
            continue

        # Volume profile boost
        vp = calc_volume_profile(candles)
        price = candles[-1][4]
        if vp["poc"] > 0 and abs(price - vp["poc"]) / price < 0.01:
            best_signal["confidence"] *= 1.1

        # Order Book Imbalance confirmation (extra confidence)
        ob_imb = await order_book_imbalance(symbol)
        best_signal["ob_imb"] = ob_imb
        if best_signal["side"] == "buy" and ob_imb > 0.15:
            best_signal["confidence"] *= 1.08
        elif best_signal["side"] == "sell" and ob_imb < -0.15:
            best_signal["confidence"] *= 1.08
        elif best_signal["side"] == "buy" and ob_imb < -0.25:
            continue  # heavy sell wall
        elif best_signal["side"] == "sell" and ob_imb > 0.25:
            continue  # heavy buy wall

        # Cooldown after recent close
        last_close = 0
        for s in slippage_log:
            if s.get("symbol") == symbol:
                last_close = max(last_close, s.get("time", 0))
        if time.time() - last_close < cooldown_seconds():
            continue

        await open_trade(symbol, best_signal, equity)
        await asyncio.sleep(0.3)


# ===========================================================================
# GRID BOT (Bug #9 fix: proper execution)
# ===========================================================================


async def grid_execute(symbol, upper, lower, grids=10, size_per_grid=0.0):
    """Bug #9: grid bot that actually places orders."""
    if not exchange:
        return
    step = (upper - lower) / grids
    if size_per_grid <= 0:
        equity = await fetch_balance()
        size_per_grid = (equity * 0.01) / upper

    ticker = await fetch_ticker(symbol)
    if not ticker:
        return
    price = float(ticker.get("last", 0))

    levels = []
    for i in range(grids + 1):
        lp = lower + i * step
        if lp < price:
            o = await place_order(symbol, "buy", size_per_grid, "limit", lp)
            if o:
                levels.append({"price": lp, "side": "buy", "order_id": o["id"], "status": "placed"})
        elif lp > price:
            o = await place_order(symbol, "sell", size_per_grid, "limit", lp)
            if o:
                levels.append({"price": lp, "side": "sell", "order_id": o["id"], "status": "placed"})
    grid_levels[symbol] = levels
    log.info("Grid on %s: %d levels", symbol, len(levels))


async def grid_check():
    """Check and refill grid orders."""
    for symbol, levels in list(grid_levels.items()):
        if not exchange:
            continue
        for level in levels:
            if level["status"] != "placed":
                continue
            try:
                oi = await exchange.fetch_order(level["order_id"], symbol)
                if oi["status"] == "closed":
                    level["status"] = "filled"
                    ns = "sell" if level["side"] == "buy" else "buy"
                    step = abs(levels[1]["price"] - levels[0]["price"]) if len(levels) > 1 else 0
                    np_ = level["price"] + step if ns == "sell" else level["price"] - step
                    amt = float(oi.get("amount", 0))
                    if np_ > 0 and amt > 0:
                        no = await place_order(symbol, ns, amt, "limit", np_)
                        if no:
                            level["side"] = ns
                            level["price"] = np_
                            level["order_id"] = no["id"]
                            level["status"] = "placed"
            except Exception:
                pass


# ===========================================================================
# AUTO-ALERTS (NEW)
# ===========================================================================


async def alert_big_liquidations():
    """Push Telegram alert when liquidations cluster crosses threshold."""
    global last_liq_alert_ts
    now = time.time()
    if now - last_liq_alert_ts < 120:  # max one alert / 2 min
        return
    threshold = CFG["liq_alert_usd"]
    agg = total_liq_window(120)  # last 2 min
    big = [(s, v) for s, v in agg.items() if v >= threshold]
    if not big:
        return
    big.sort(key=lambda x: x[1], reverse=True)
    last_liq_alert_ts = now
    text = "\U0001f6a8 <b>Massive Liquidationen (2min)</b>\n\n"
    for sym, val in big[:6]:
        long_v, short_v = liq_cluster_for(sym, 120)
        dom = "LONG" if long_v > short_v else "SHORT"
        text += f"<b>{sym.split('/')[0]}</b>: {val/1_000_000:.2f}M$ ({dom} dominant)\n"
    text += "\nMoegliche Konter-Setups durch liquidation_hunt-Strategie."
    await tg_send(text)


async def alert_important_news():
    """Push Telegram alert for very strong news items."""
    global last_news_alert_id
    if not news_cache:
        return
    # find strongest |score| not yet alerted, recent (<10min)
    cutoff = time.time() - 600
    candidates = [n for n in news_cache if n["ts"] >= cutoff and abs(n["score"]) >= 3]
    if not candidates:
        return
    candidates.sort(key=lambda n: abs(n["score"]), reverse=True)
    top = candidates[0]
    nid = str(top.get("id", ""))
    if nid == last_news_alert_id:
        return
    last_news_alert_id = nid
    score = top["score"]
    arrow = "\U0001f7e2 BULLISH" if score > 0 else "\U0001f534 BEARISH"
    cur = "/".join(top.get("currencies", [])[:3]) or "Markt"
    text = (
        f"\U0001f4f0 <b>News-Alert</b> {arrow}\n\n"
        f"<b>{cur}</b> ({top['source']})\n"
        f"Score: {score:+d}\n\n"
        f"{top['title'][:300]}\n"
    )
    if top.get("url"):
        text += f"\n{top['url']}"
    await tg_send(text)


async def hourly_summary():
    """Comprehensive hourly status to Telegram."""
    global last_hourly_summary
    if not CFG["tg_hourly_summary"]:
        return
    now = time.time()
    if now - last_hourly_summary < CFG["tg_summary_interval_sec"]:
        return
    last_hourly_summary = now

    eq = await fetch_balance()
    fg = await fetch_fear_greed()
    margin = await get_margin_ratio()
    positions = await fetch_positions_once()
    heat = portfolio_heat(positions)
    uptime = (now - sprint_start) / 3600

    # Top 3 strategies by pnl
    top_strats = sorted(strategy_stats.items(), key=lambda x: x[1].get("pnl", 0), reverse=True)[:3]
    strat_str = " | ".join(f"{n}:{s['pnl']:+.1f}" for n, s in top_strats) or "noch keine"

    # Recent liquidation total (5 min)
    liq_total = sum(total_liq_window(300).values())

    text = (
        f"\u23f0 <b>Stuendliche Zusammenfassung</b>\n\n"
        f"<b>Konto</b>\n"
        f"Equity: {eq:.2f} USDT | Peak: {peak_equity:.2f}\n"
        f"DD: {current_drawdown*100:.2f}% | Heute: {daily_pnl:+.2f}\n"
        f"Heat: {heat*100:.1f}% | Margin: {margin*100:.1f}%\n"
        f"\n<b>Trading</b>\n"
        f"Offen: {len(active_trades)}/{CFG['max_concurrent']}\n"
        f"Streak: W{consec_wins} / L{consec_losses}\n"
        f"Top Strats: {strat_str}\n"
        f"\n<b>Markt</b>\n"
        f"Sentiment: {sentiment_score:+.2f} ({len(news_cache)} News)\n"
        f"Fear &amp; Greed: {fg}/100\n"
        f"Liqs 5min: {liq_total/1_000_000:.2f}M$\n"
        f"Session: {get_session()} | Uptime: {uptime:.1f}h"
    )
    await tg_send(text)


# ===========================================================================
# FEAR & GREED, HEALTH MONITOR (Feature #9)
# ===========================================================================


async def fetch_fear_greed() -> int:
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            url = "https://api.alternative.me/fng/?limit=1"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return int(data["data"][0]["value"])
    except Exception:
        pass
    return 50


async def health_check():
    """Feature #9: ping exchange, auto-reconnect."""
    global last_health_ping
    try:
        if not exchange:
            await init_exchange()
            return
        await exchange.fetch_time()
        last_health_ping = time.time()
    except Exception as e:
        log.error("Health check failed: %s", e)
        await tg_send(f"\u26a0\ufe0f Verbindungsproblem: {e}")
        try:
            await close_exchange()
            await init_exchange()
            log.info("Reconnected")
            await tg_send("\u2705 Verbindung wiederhergestellt")
        except Exception as re:
            log.error("Reconnect failed: %s", re)


# ===========================================================================
# TELEGRAM (Bug #2, #3, #4 fixes: proper async handlers)
# ===========================================================================


async def tg_send(text: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        import aiohttp
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    log.debug("TG send failed: %d", resp.status)
    except Exception as e:
        log.debug("TG send error: %s", e)


async def send_report(report_type="taeglich"):
    equity = await fetch_balance()
    session = get_session()
    uptime = (time.time() - sprint_start) / 3600
    text = (
        f"\U0001f4ca <b>OLYMPUS Report ({report_type})</b>\n\n"
        f"Equity: {sf(equity, '.2f')} USDT\n"
        f"Tages-PnL: {sf(daily_pnl, '.2f')} USDT\n"
        f"Drawdown: {sf(current_drawdown * 100, '.1f')}%\n"
        f"Offene Trades: {len(active_trades)}\n"
        f"Session: {session}\n"
        f"Laufzeit: {uptime:.1f}h\n"
        f"Verlust-Serie: {consec_losses}\n"
        f"Gewinn-Serie: {consec_wins}\n"
    )
    if drawdown_recovery_remaining > 0:
        text += f"Recovery-Modus: noch {drawdown_recovery_remaining} Trades\n"
    await tg_send(text)


async def handle_ask(question: str) -> str:
    """Gemini AI for /ask command."""
    if not GEMINI_KEY:
        return "Kein Gemini API-Key konfiguriert."
    try:
        import aiohttp
        url = (f"https://generativelanguage.googleapis.com/v1beta/"
               f"models/gemini-pro:generateContent?key={GEMINI_KEY}")
        prompt = f"Du bist ein Krypto-Trading-Assistent. Beantworte: {question}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data["candidates"][0]["content"]["parts"][0]["text"]
                return f"API Fehler: {resp.status}"
    except Exception as e:
        return f"Fehler: {e}"


async def setup_telegram():
    """Set up Telegram bot with proper async handlers (Bug #2, #3, #4)."""
    if not TG_TOKEN:
        log.info("No Telegram token - commands disabled")
        return None
    try:
        from telegram import Update
        from telegram.ext import Application, CommandHandler, ContextTypes
    except ImportError:
        log.warning("python-telegram-bot not installed")
        return None

    app = Application.builder().token(TG_TOKEN).build()

    async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "\U0001f3db <b>OLYMPUS Trading Bot</b>\n\n"
            "<b>Status &amp; Info</b>\n"
            "/status - Bot Status\n"
            "/report - Voller Bericht\n"
            "/trades - Offene Trades (detail)\n"
            "/log [n] - Letzte n Closes\n"
            "/perf - Strategie-Performance\n"
            "/risk - Risiko-Dashboard\n"
            "<b>Markt</b>\n"
            "/news [sym] - Aktuelle News\n"
            "/liq [sym] - Liquidationen\n"
            "/top [n] - Top Opportunities\n"
            "/sentiment - Markt-Sentiment\n"
            "/oi sym - Open Interest\n"
            "<b>Steuerung</b>\n"
            "/pause - Pausieren\n"
            "/resume - Fortsetzen\n"
            "/close_all - Alle schliessen\n"
            "/setrisk &lt;pct&gt; - Risk pro Trade\n"
            "/blacklist sym - Symbol blocken\n"
            "/whitelist sym - Symbol freigeben\n"
            "/ask &lt;frage&gt; - KI fragen",
            parse_mode="HTML",
        )

    async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
        eq = await fetch_balance()
        active = "Ja" if bot_active and not bot_paused else "Nein"
        text = (f"\U0001f4ca <b>Status</b>\nAktiv: {active}\n"
                f"Equity: {sf(eq, '.2f')} USDT\n"
                f"Drawdown: {sf(current_drawdown * 100, '.1f')}%\n"
                f"Trades offen: {len(active_trades)}\nSession: {get_session()}")
        await update.message.reply_text(text, parse_mode="HTML")

    # Bug #4 fix: proper async handler
    async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await send_report("manuell")

    async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not active_trades:
            await update.message.reply_text("Keine offenen Trades.")
            return
        text = f"\U0001f4c8 <b>Offene Trades ({len(active_trades)})</b>\n\n"
        for sym, t in active_trades.items():
            ticker = await fetch_ticker(sym)
            cur = float(ticker.get("last", 0) or t["entry"])
            entry = t["weighted_entry"]
            sl_dist = t.get("sl_dist_original", 0)
            mult = 1 if t["side"] == "buy" else -1
            cur_r = ((cur - entry) / sl_dist * mult) if sl_dist > 0 else 0
            unr_pnl = (cur - entry) * t["size"] * mult
            dur = (time.time() - t["opened_at"]) / 60
            arrow = "\u2191" if t["side"] == "buy" else "\u2193"
            text += (
                f"<b>{sym}</b> {arrow} {t['side'].upper()} ({t.get('strategy','?')})\n"
                f"  Entry {sf(entry)} | Now {sf(cur)} | <b>{cur_r:+.2f}R</b>\n"
                f"  PnL: {unr_pnl:+.2f}$ | Lev {t.get('leverage','?')}x | {dur:.0f}min\n"
                f"  SL {sf(t['sl'])} | TP1 {sf(t.get('tp1',0))} | TP2 {sf(t.get('tp2',0))}\n\n"
            )
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
        n = 10
        if context.args:
            try:
                n = max(1, min(50, int(context.args[0])))
            except Exception:
                pass
        if not db:
            await update.message.reply_text("DB nicht verfuegbar.")
            return
        async with db.execute(
            "SELECT symbol,side,strategy,pnl,r_multiple,opened_at,closed_at "
            "FROM trades WHERE status='closed' ORDER BY id DESC LIMIT ?",
            (n,),
        ) as cur:
            rows = await cur.fetchall()
        if not rows:
            await update.message.reply_text("Keine geschlossenen Trades.")
            return
        text = f"\U0001f4dc <b>Letzte {len(rows)} Trades</b>\n\n"
        for r in rows:
            pnl = float(r["pnl"] or 0)
            rm = float(r["r_multiple"] or 0)
            emo = "\u2705" if pnl > 0 else "\u274c"
            text += f"{emo} {r['symbol']} {r['side'][:1].upper()} {r['strategy']}: {pnl:+.2f}$ ({rm:+.2f}R)\n"
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_perf(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not strategy_stats:
            await update.message.reply_text("Noch keine Strategie-Stats.")
            return
        text = "\U0001f4ca <b>Strategie-Performance</b>\n\n"
        rows = sorted(strategy_stats.items(), key=lambda x: x[1].get("pnl", 0), reverse=True)
        for name, s in rows:
            w, l = s.get("wins", 0), s.get("losses", 0)
            wr = w / max(1, w + l) * 100
            text += (f"<b>{name}</b>\n"
                     f"  W/L: {w}/{l}  WR: {wr:.0f}%\n"
                     f"  PnL: {s.get('pnl',0):+.2f}$  Best/Worst R: {s.get('best_r',0):.2f}/{s.get('worst_r',0):.2f}\n\n")
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE):
        eq = await fetch_balance()
        margin = await get_margin_ratio()
        positions = await fetch_positions_once()
        heat = portfolio_heat(positions)
        cur_risk = risk_override if risk_override is not None else CFG["risk_per_trade"]
        text = (
            f"\u26a0\ufe0f <b>Risiko-Dashboard</b>\n\n"
            f"Equity: {eq:.2f} USDT\n"
            f"Peak: {peak_equity:.2f} | DD: {current_drawdown*100:.2f}%\n"
            f"Tages-PnL: {daily_pnl:+.2f} USDT\n"
            f"\n<b>Limits</b>\n"
            f"Risk pro Trade: {cur_risk*100:.1f}% (Max: {CFG['max_risk_per_trade']*100:.1f}%)\n"
            f"Max DD: {CFG['max_drawdown']*100:.0f}% | Tageslimit: {CFG['daily_loss_limit']*100:.0f}%\n"
            f"\n<b>Aktuell</b>\n"
            f"Portfolio Heat: {heat*100:.1f}% / {CFG['portfolio_heat_max']*100:.0f}%\n"
            f"Margin frei: {margin*100:.1f}%\n"
            f"Offen: {len(active_trades)}/{CFG['max_concurrent']}\n"
            f"Streak: W{consec_wins} / L{consec_losses}\n"
            f"Recovery-Modus: {'ja, ' + str(drawdown_recovery_remaining) + ' Trades' if drawdown_recovery_remaining > 0 else 'nein'}\n"
            f"Cooldown: {cooldown_seconds()}s"
        )
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await fetch_news()
        sym_filter = None
        if context.args:
            sym_filter = context.args[0].upper()
        items = news_cache[:15]
        if sym_filter:
            items = [n for n in news_cache if sym_filter in [c.upper() for c in n.get("currencies", [])]][:10]
        if not items:
            await update.message.reply_text("Keine News verfuegbar.")
            return
        text = "\U0001f4f0 <b>News</b>\n\n"
        for n in items:
            score = n["score"]
            arrow = "\U0001f7e2" if score > 0 else ("\U0001f534" if score < 0 else "\u26aa\ufe0f")
            cur_str = "/".join(n.get("currencies", [])[:3])
            text += f"{arrow} <b>{cur_str}</b> ({n['source']})\n  {n['title'][:100]}\n\n"
        await update.message.reply_text(text[:4000], parse_mode="HTML")

    async def cmd_liq(update: Update, context: ContextTypes.DEFAULT_TYPE):
        sym_filter = context.args[0].upper() if context.args else None
        liqs = list(liquidations_log)[-30:]
        if sym_filter:
            liqs = [l for l in liqs if sym_filter in l["symbol"]]
        if not liqs:
            await update.message.reply_text("Keine Liquidationen im Buffer.")
            return
        liqs = sorted(liqs, key=lambda x: x["value_usd"], reverse=True)[:15]
        text = "\U0001f6a8 <b>Top Liquidationen</b>\n\n"
        for l in liqs:
            arrow = "\U0001f534" if l["side"] == "long" else "\U0001f7e2"
            ago = (time.time() - l["ts"]) / 60
            text += f"{arrow} {l['symbol'].split('/')[0]} {l['side'].upper()}: {l['value_usd']/1000:.0f}k$ ({ago:.0f}m)\n"
        # Aggregated last 5min
        agg = total_liq_window(300)
        if agg:
            top = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:5]
            text += "\n<b>5min Aggregate</b>\n"
            for sym, v in top:
                text += f"  {sym.split('/')[0]}: {v/1000:.0f}k$\n"
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
        n = CFG["tg_top_n"]
        if context.args:
            try:
                n = max(3, min(20, int(context.args[0])))
            except Exception:
                pass
        scored: List[Tuple[str, float, float, float]] = []
        for sym in CFG["symbols"]:
            try:
                cs = await fetch_ohlcv(sym, "5m", 100)
                if not cs or len(cs) < 30:
                    continue
                op = opportunity_score(sym, cs)
                sent = symbol_sentiment.get(sym, 0.0)
                smart = smart_money_bias(sym)
                scored.append((sym, op, sent, smart))
            except Exception:
                continue
        scored.sort(key=lambda x: x[1], reverse=True)
        text = f"\U0001f3af <b>Top {n} Opportunities</b>\n\n"
        for sym, op, sent, smart in scored[:n]:
            text += f"<b>{sym.split('/')[0]:<6}</b> Score {op:.2f} | Sent {sent:+.2f} | Smart {smart:+.2f}\n"
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_sentiment(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await fetch_news()
        await refresh_sentiment()
        fg = await fetch_fear_greed()
        text = (
            f"\U0001f9e0 <b>Markt-Sentiment</b>\n\n"
            f"Markt-Score: <b>{sentiment_score:+.2f}</b> ({len(news_cache)} News)\n"
            f"Fear &amp; Greed: <b>{fg}/100</b>\n\n"
            f"<b>Top bullish</b>\n"
        )
        srt = sorted(symbol_sentiment.items(), key=lambda x: x[1], reverse=True)
        for sym, v in srt[:5]:
            if v > 0:
                text += f"  \U0001f7e2 {sym.split('/')[0]}: {v:+.2f}\n"
        text += "\n<b>Top bearish</b>\n"
        for sym, v in srt[-5:]:
            if v < 0:
                text += f"  \U0001f534 {sym.split('/')[0]}: {v:+.2f}\n"
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_oi(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Nutzung: /oi BTC")
            return
        target = context.args[0].upper()
        match = None
        for sym in CFG["symbols"]:
            if _base_of(sym) == target:
                match = sym
                break
        if not match:
            await update.message.reply_text(f"Symbol {target} nicht in Universum.")
            return
        chg = oi_change(match) * 100
        hist = oi_history.get(match)
        latest = hist[-1][1] if hist else 0
        smart = smart_money_bias(match)
        top = ls_ratio.get(match + "_top", 0)
        tak = ls_ratio.get(match + "_taker", 0)
        text = (
            f"\U0001f4c8 <b>{target} Markt-Daten</b>\n\n"
            f"Open Interest: {latest:,.0f}\n"
            f"OI 1h Change: {chg:+.2f}%\n"
            f"Top-Trader L/S: {top:.2f}\n"
            f"Taker B/S 5m: {tak:.2f}\n"
            f"Smart-Money Bias: {smart:+.2f}\n"
        )
        long_liq, short_liq = liq_cluster_for(match)
        text += f"Liqs 3min: long {long_liq/1000:.0f}k$ / short {short_liq/1000:.0f}k$\n"
        text += f"Sentiment: {symbol_sentiment.get(match, 0):+.2f}\n"
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_setrisk(update: Update, context: ContextTypes.DEFAULT_TYPE):
        global risk_override
        if not context.args:
            cur = risk_override if risk_override is not None else CFG["risk_per_trade"]
            await update.message.reply_text(
                f"Aktuelles Risk: {cur*100:.1f}%. Nutzung: /setrisk 5  (= 5%)"
            )
            return
        try:
            pct = float(context.args[0])
            if pct < 0.5 or pct > CFG["max_risk_per_trade"] * 100:
                await update.message.reply_text(
                    f"Risk muss zwischen 0.5% und {CFG['max_risk_per_trade']*100:.0f}% liegen."
                )
                return
            risk_override = pct / 100
            await update.message.reply_text(f"\u2705 Risk auf {pct:.1f}% gesetzt.")
        except Exception:
            await update.message.reply_text("Ungueltige Eingabe.")

    async def cmd_blacklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text(f"Blacklist: {', '.join(symbol_blacklist) or 'leer'}")
            return
        target = context.args[0].upper()
        for sym in CFG["symbols"]:
            if _base_of(sym) == target:
                symbol_blacklist.add(sym)
                await update.message.reply_text(f"\U0001f6ab {sym} blockiert.")
                return
        await update.message.reply_text("Symbol nicht gefunden.")

    async def cmd_whitelist(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            symbol_blacklist.clear()
            await update.message.reply_text("\u2705 Blacklist geleert.")
            return
        target = context.args[0].upper()
        before = len(symbol_blacklist)
        for sym in list(symbol_blacklist):
            if _base_of(sym) == target:
                symbol_blacklist.discard(sym)
        if len(symbol_blacklist) < before:
            await update.message.reply_text(f"\u2705 {target} freigegeben.")
        else:
            await update.message.reply_text("Nicht in Blacklist.")

    async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
        global bot_paused
        bot_paused = True
        await update.message.reply_text("\u23f8 Bot pausiert.")

    # Bug #6 fix: separate resume from drawdown logic
    async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
        global bot_paused, bot_active
        bot_paused = False
        bot_active = True
        await update.message.reply_text("\u25b6\ufe0f Bot fortgesetzt.")

    async def cmd_close_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await close_all("manuell via Telegram")
        await update.message.reply_text("\u2705 Alle Positionen geschlossen.")

    async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
        q = " ".join(context.args) if context.args else ""
        if not q:
            await update.message.reply_text("Bitte Frage: /ask <frage>")
            return
        answer = await handle_ask(q)
        await update.message.reply_text(answer[:4000])

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("trades", cmd_trades))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("close_all", cmd_close_all))
    app.add_handler(CommandHandler("ask", cmd_ask))
    app.add_handler(CommandHandler("log", cmd_log))
    app.add_handler(CommandHandler("perf", cmd_perf))
    app.add_handler(CommandHandler("risk", cmd_risk))
    app.add_handler(CommandHandler("news", cmd_news))
    app.add_handler(CommandHandler("liq", cmd_liq))
    app.add_handler(CommandHandler("top", cmd_top))
    app.add_handler(CommandHandler("sentiment", cmd_sentiment))
    app.add_handler(CommandHandler("oi", cmd_oi))
    app.add_handler(CommandHandler("setrisk", cmd_setrisk))
    app.add_handler(CommandHandler("blacklist", cmd_blacklist))
    app.add_handler(CommandHandler("whitelist", cmd_whitelist))

    # Bug #2, #3: proper async job functions (not lambdas)
    async def job_refresh(context: ContextTypes.DEFAULT_TYPE):
        if exchange:
            await exchange.load_markets()

    async def job_train(context: ContextTypes.DEFAULT_TYPE):
        await train_ml()

    async def job_report(context: ContextTypes.DEFAULT_TYPE):
        await send_report("taeglich")

    async def job_corr(context: ContextTypes.DEFAULT_TYPE):
        await update_correlations()

    if app.job_queue:
        app.job_queue.run_repeating(job_refresh, interval=3600, first=60)
        app.job_queue.run_repeating(job_train, interval=CFG["ml_retrain_hours"] * 3600, first=300)
        app.job_queue.run_repeating(job_report, interval=86400, first=3600)
        app.job_queue.run_repeating(job_corr, interval=1800, first=120)

    return app


# ===========================================================================
# WEB DASHBOARD (Bug #17, #18: token-based auth)
# ===========================================================================

DASH_HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>OLYMPUS</title>
<meta http-equiv="refresh" content="10">
<style>
body{{font-family:monospace;background:#1a1a2e;color:#eee;padding:20px;margin:0}}
h1{{color:#e94560;text-align:center}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:15px}}
.card{{background:#16213e;border-radius:8px;padding:15px;border:1px solid #0f3460}}
.card h3{{color:#e94560;margin-top:0}}
.green{{color:#4caf50}}.red{{color:#f44336}}.yellow{{color:#ffeb3b}}
table{{width:100%;border-collapse:collapse}}
td,th{{padding:5px;text-align:left;border-bottom:1px solid #333}}
.stat{{font-size:1.5em;font-weight:bold}}
</style></head><body>
<h1>OLYMPUS Trading Bot</h1>
<div class="grid">
<div class="card"><h3>Account</h3>
<p>Equity: <span class="stat">{equity:.2f} USDT</span></p>
<p>Drawdown: <span class="{dd_class}">{drawdown:.1f}%</span></p>
<p>Daily PnL: <span class="{pnl_class}">{daily_pnl:.2f} USDT</span></p>
</div>
<div class="card"><h3>Status</h3>
<p>Bot: <span class="{st_class}">{status}</span></p>
<p>Session: {session}</p><p>Uptime: {uptime:.1f}h</p>
<p>Sprint: {sprint_rem:.1f}h verbleibend</p>
</div>
<div class="card"><h3>Statistik</h3>
<p>Offene: {open_trades} | Verluste: {c_losses} | Gewinne: {c_wins}</p>
<p>Win-Rate: {win_rate:.0f}%</p>
</div>
<div class="card"><h3>Positionen</h3>
<table><tr><th>Symbol</th><th>Seite</th><th>Entry</th></tr>
{pos_html}</table></div>
</div>
<p style="text-align:center;color:#666;margin-top:20px">Auto-Refresh 10s</p>
</body></html>"""


async def start_dashboard():
    """Bug #17, #18: dashboard with token auth."""
    try:
        import aiohttp.web as web
    except ImportError:
        log.warning("aiohttp not installed - no dashboard")
        return None

    async def dash_handler(request):
        token = request.query.get("token", "")
        auth_h = request.headers.get("Authorization", "")
        if token != DASH_TOKEN and auth_h != f"Bearer {DASH_TOKEN}":
            return web.Response(text="Unauthorized. Add ?token=YOUR_TOKEN", status=401)

        equity = await fetch_balance()
        pos_html = ""
        for sym, t in active_trades.items():
            pos_html += f"<tr><td>{sym}</td><td>{t['side'].upper()}</td><td>{sf(t['entry'])}</td></tr>\n"
        if not active_trades:
            pos_html = "<tr><td colspan=3>Keine</td></tr>"

        uptime = (time.time() - sprint_start) / 3600
        html = DASH_HTML.format(
            equity=equity, drawdown=current_drawdown * 100, daily_pnl=daily_pnl,
            status="AKTIV" if bot_active and not bot_paused else "PAUSIERT",
            session=get_session(), uptime=uptime,
            sprint_rem=max(0, CFG["sprint_hours"] - uptime),
            open_trades=len(active_trades), c_losses=consec_losses,
            c_wins=consec_wins, win_rate=ml_win_rate * 100,
            pos_html=pos_html,
            dd_class="red" if current_drawdown > 0.1 else "green",
            pnl_class="green" if daily_pnl >= 0 else "red",
            st_class="green" if bot_active and not bot_paused else "red",
        )
        return web.Response(text=html, content_type="text/html")

    async def api_status(request):
        token = request.query.get("token", "")
        if token != DASH_TOKEN:
            return web.Response(text="Unauthorized", status=401)
        return web.json_response({
            "active": bot_active, "paused": bot_paused,
            "drawdown": current_drawdown, "daily_pnl": daily_pnl,
            "open_trades": len(active_trades), "session": get_session(),
        })

    app = web.Application()
    app.router.add_get("/", dash_handler)
    app.router.add_get("/api/status", api_status)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, CFG["dash_host"], CFG["dash_port"])
    await site.start()
    log.info("Dashboard: http://%s:%d/?token=%s", CFG["dash_host"], CFG["dash_port"], DASH_TOKEN)
    return runner


# ===========================================================================
# WEBHOOK (TradingView)
# ===========================================================================


async def start_webhook():
    if not WEBHOOK_SECRET:
        return None
    try:
        import aiohttp.web as web
    except ImportError:
        return None

    async def wh_handler(request):
        try:
            body = await request.json()
        except Exception:
            return web.Response(text="Invalid JSON", status=400)

        secret = body.get("secret", "")
        if not hmac.compare_digest(secret, WEBHOOK_SECRET):
            return web.Response(text="Unauthorized", status=401)

        symbol = body.get("symbol", "")
        action = body.get("action", "")
        log.info("Webhook: %s %s", action, symbol)

        if action == "close" and symbol in active_trades:
            trade = active_trades[symbol]
            ticker = await fetch_ticker(symbol)
            price = float(ticker.get("last", 0)) if ticker else 0
            if price > 0:
                await close_position(symbol, trade, price, "webhook")
                active_trades.pop(symbol, None)
        elif action in ("buy", "sell") and symbol not in active_trades:
            candles = await fetch_ohlcv(symbol, "5m", 100)
            if candles and len(candles) > 20:
                closes = [c[4] for c in candles]
                highs = [c[2] for c in candles]
                lows = [c[3] for c in candles]
                atr = calc_atr(highs, lows, closes)
                price = closes[-1]
                sl = price - 2 * atr if action == "buy" else price + 2 * atr
                tp = price + 3 * atr if action == "buy" else price - 3 * atr
                sig = {"side": action, "strategy": "webhook", "confidence": 0.7,
                       "sl": sl, "tp": tp, "atr": atr}
                equity = await fetch_balance()
                await open_trade(symbol, sig, equity)
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_post("/webhook", wh_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", CFG["webhook_port"])
    await site.start()
    log.info("Webhook on port %d", CFG["webhook_port"])
    return runner


# ===========================================================================
# EQUITY & DAILY (Feature #15)
# ===========================================================================


async def equity_snapshot():
    global peak_equity
    equity = await fetch_balance()
    if equity > 0:
        if equity > peak_equity:
            peak_equity = equity
        dd = (peak_equity - equity) / peak_equity if peak_equity > 0 else 0
        await db_save_equity(equity, dd)
        equity_history.append((time.time(), equity))
        cutoff = time.time() - CFG["sprint_hours"] * 3600
        while equity_history and equity_history[0][0] < cutoff:
            equity_history.pop(0)


async def check_daily_reset():
    global daily_pnl, daily_pnl_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if daily_pnl_date != today:
        if daily_pnl_date:
            await db_save_daily()
        daily_pnl = 0.0
        daily_pnl_date = today


# ===========================================================================
# MAIN LOOP
# ===========================================================================


async def main_loop():
    global bot_active, shutdown_event, peak_equity

    shutdown_event = asyncio.Event()
    log.info("OLYMPUS starting...")

    await db_init()
    await init_exchange()
    await db_load_daily()

    saved_peak = await db_load_state("peak_equity")
    if saved_peak:
        peak_equity = float(saved_peak)

    await warmup_ml()
    await update_correlations()

    # Initial fetch of news / OI / LSR / sentiment so bot has data immediately
    await fetch_news()
    await refresh_sentiment()
    await fetch_oi_history_binance()
    await fetch_long_short_ratio()
    await fetch_top_trader_ratio()
    await fetch_taker_ratio()
    # Start liquidation websocket (background, runs forever)
    await fetch_liquidations()

    dash_runner = await start_dashboard()
    wh_runner = await start_webhook()
    tg_app = await setup_telegram()

    if tg_app:
        asyncio.create_task(_run_telegram(tg_app))

    equity = await fetch_balance()
    if equity > 0 and peak_equity == 0:
        peak_equity = equity

    fg_init = await fetch_fear_greed()
    msg = (
        f"\U0001f3db <b>OLYMPUS gestartet</b>\n\n"
        f"Equity: {sf(equity, '.2f')} USDT\n"
        f"Sprint: {CFG['sprint_hours']}h\n"
        f"Symbole: {len(CFG['symbols'])} | Max Positionen: {CFG['max_concurrent']}\n"
        f"Strategien: {len(ALL_STRATEGIES)}\n"
        f"Risk/Trade: {CFG['risk_per_trade']*100:.1f}% (Max {CFG['max_risk_per_trade']*100:.0f}%)\n"
        f"Leverage: {CFG['leverage_min']}-{CFG['leverage_max']}x\n"
        f"Fear &amp; Greed: {fg_init}/100 | News: {len(news_cache)}"
    )
    await tg_send(msg)

    cycle_count = 0
    health_iv = max(1, int(CFG["health_ping_sec"] / CFG["cycle_sec"]))
    equity_iv = max(1, int(3600 / CFG["cycle_sec"]))
    news_iv = max(1, int(CFG["news_check_sec"] / CFG["cycle_sec"]))
    oi_iv = max(1, int(CFG["oi_check_sec"] / CFG["cycle_sec"]))
    lsr_iv = max(1, int(CFG["lsr_check_sec"] / CFG["cycle_sec"]))
    summary_iv = max(1, int(CFG["tg_summary_interval_sec"] / CFG["cycle_sec"]))
    alert_iv = max(1, int(60 / CFG["cycle_sec"]))  # alerts every ~minute

    try:
        while not shutdown_event.is_set():
            cycle_start = time.time()
            cycle_count += 1

            uptime_h = (time.time() - sprint_start) / 3600
            if uptime_h >= CFG["sprint_hours"]:
                log.info("Sprint ended")
                await send_report("Sprint-Ende")
                await close_all("Sprint beendet")
                break

            try:
                await check_daily_reset()

                if cycle_count % health_iv == 0:
                    await health_check()

                if cycle_count % equity_iv == 0:
                    await equity_snapshot()
                    await db_save_state("peak_equity", peak_equity)

                # Periodic data refresh
                if cycle_count % news_iv == 0:
                    await fetch_news()
                    await refresh_sentiment()

                if cycle_count % oi_iv == 0:
                    await fetch_oi_history_binance()

                if cycle_count % lsr_iv == 0:
                    await fetch_long_short_ratio()
                    await fetch_top_trader_ratio()
                    await fetch_taker_ratio()

                # Auto-alerts
                if cycle_count % alert_iv == 0:
                    await alert_big_liquidations()
                    await alert_important_news()

                if cycle_count % summary_iv == 0:
                    await hourly_summary()

                if active_trades:
                    await manage_positions()

                if grid_levels:
                    await grid_check()

                if bot_active and not bot_paused:
                    await scan_signals()

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("Cycle error: %s", e, exc_info=True)

            elapsed = time.time() - cycle_start
            wait = max(0, CFG["cycle_sec"] - elapsed)
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=wait)
                break
            except asyncio.TimeoutError:
                pass

    finally:
        log.info("Shutting down...")
        await send_report("Shutdown")
        await db_save_daily()
        await db_save_state("peak_equity", peak_equity)
        if db:
            await db_close()
        await close_exchange()
        if dash_runner:
            await dash_runner.cleanup()
        if wh_runner:
            await wh_runner.cleanup()
        log.info("OLYMPUS stopped.")


async def _run_telegram(app):
    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        while not shutdown_event.is_set():
            await asyncio.sleep(1)
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
    except Exception as e:
        log.error("Telegram error: %s", e)


# ===========================================================================
# ENTRY POINT
# ===========================================================================


def _signal_handler(sig, frame):
    log.info("Signal %s received", sig)
    if shutdown_event:
        shutdown_event.set()


def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    log.info("=" * 60)
    log.info("OLYMPUS Crypto Trading Bot v2.0")
    log.info("=" * 60)

    if not API_KEY or not API_SECRET:
        log.error("BITGET_API_KEY and BITGET_API_SECRET required in .env")
        sys.exit(1)

    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        log.info("Keyboard interrupt")
    except Exception as e:
        log.critical("Fatal: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
