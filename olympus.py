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
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "XRP/USDT:USDT", "DOGE/USDT:USDT", "ARB/USDT:USDT",
        "AVAX/USDT:USDT", "LINK/USDT:USDT", "OP/USDT:USDT",
        "MATIC/USDT:USDT", "ADA/USDT:USDT", "DOT/USDT:USDT",
        "NEAR/USDT:USDT", "APT/USDT:USDT", "SUI/USDT:USDT",
        "WIF/USDT:USDT", "PEPE/USDT:USDT", "FET/USDT:USDT",
    ],
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
last_closed_at: Dict[str, float] = {}


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
    try:
        params = params or {}
        order = await exchange.create_order(symbol, order_type, side, amount, price, params)
        return order
    except Exception as e:
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
# MULTI-TIMEFRAME CONFLUENCE (Feature #1)
# ===========================================================================


async def mtf_score(symbol: str) -> Tuple[float, str]:
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
        return bull, "long"
    if bear >= CFG["mtf_min_score"] and bear > bull:
        return bear, "short"
    return 0.0, "none"


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
            future_window = candles[i + 1:i + 11]
            if not future_window:
                continue
            future_high = max(c[2] for c in future_window)
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


ALL_STRATEGIES = [strategy_trend, strategy_momentum, strategy_breakout, strategy_scalp, strategy_reversal]


# ===========================================================================
# POSITION SIZING (Bug #5, #16, Feature #2, #7)
# ===========================================================================


async def kelly_size(confidence: float, equity: float) -> float:
    """Kelly with actual win rate (Bug #16), session adjust (Feature #2), recovery (Feature #7)."""
    win_rate = await db_win_rate() if db else ml_win_rate
    if win_rate <= 0 or win_rate >= 1:
        win_rate = 0.5
    avg_r = 1.5
    kelly = win_rate - (1 - win_rate) / avg_r
    if kelly <= 0:
        log.debug("kelly_size: negative edge (kelly=%.4f), using minimum size", kelly)
        kelly = 0.01
    else:
        kelly = min(kelly, 0.25)
    size_pct = kelly * confidence * CFG["risk_per_trade"] / 0.02
    size_pct *= session_size_mult()
    if drawdown_recovery_remaining > 0:
        size_pct *= CFG["drawdown_recovery_size_mult"]
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

    risk_amount = await kelly_size(confidence, equity)
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

    # Feature #5: slippage tracking
    fill_price = float(order.get("average", price) or price)
    slippage = abs(fill_price - price) / price
    slippage_log.append({"symbol": symbol, "expected": price, "actual": fill_price,
                         "slippage": slippage, "time": time.time()})

    trade_id = await db_open_trade(symbol, side, fill_price, contracts, strategy,
                                   sl_price, sl_dist, slippage)

    mult = 1 if side == "buy" else -1
    active_trades[symbol] = {
        "id": trade_id, "side": side, "entry": fill_price,
        "weighted_entry": fill_price, "size": contracts,
        "sl": sl_price, "sl_dist_original": sl_dist,
        "tp1": fill_price + sl_dist * CFG["tp1_r"] * mult,
        "tp2": signal["tp"], "trail_active": False, "trail_price": None,
        "strategy": strategy, "confidence": confidence, "atr": atr,
        "leverage": leverage, "opened_at": time.time(),
        "tp1_hit": False, "pyramided": False,
    }

    if drawdown_recovery_remaining > 0:
        drawdown_recovery_remaining -= 1

    log.info("OPEN %s %s @ %s | SL=%s | Lev=%dx | %s",
             side.upper(), symbol, sf(fill_price), sf(sl_price), leverage, strategy)
    msg = (f"\U0001f680 OPEN {side.upper()} {symbol}\n"
           f"Preis: {sf(fill_price)}\nSL: {sf(sl_price)}\n"
           f"Leverage: {leverage}x\nStrategie: {strategy}")
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
            trade["tp1_price"] = price
            trade["tp1_filled_size"] = partial
            trade["size"] -= partial  # Reduce size by partial amount
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

    close_side = "sell" if side == "buy" else "buy"
    if size > 0:
        await place_order(symbol, close_side, size, params={"reduceOnly": True})

    # Calculate PnL including TP1 partial if it was hit
    if side == "buy":
        remaining_pnl = (price - entry) * size
    else:
        remaining_pnl = (entry - price) * size

    tp1_pnl = 0.0
    if trade.get("tp1_hit") and trade.get("tp1_price") and trade.get("tp1_filled_size"):
        tp1_price = trade["tp1_price"]
        tp1_size = trade["tp1_filled_size"]
        if side == "buy":
            tp1_pnl = (tp1_price - entry) * tp1_size
        else:
            tp1_pnl = (entry - tp1_price) * tp1_size

    pnl = remaining_pnl + tp1_pnl
    if side == "buy":
        r_mult = (price - entry) / sl_dist if sl_dist > 0 else 0
    else:
        r_mult = (entry - price) / sl_dist if sl_dist > 0 else 0

    daily_pnl += pnl
    if pnl > 0:
        consec_wins += 1
        consec_losses = 0
    elif pnl < 0:
        consec_losses += 1
        consec_wins = 0
    # pnl == 0 (break-even): don't touch streaks

    if current_drawdown >= CFG["drawdown_recovery_threshold"] and drawdown_recovery_remaining <= 0:
        drawdown_recovery_remaining = CFG["drawdown_recovery_trades"]

    # Track last close time for cooldown
    last_closed_at[symbol] = time.time()

    await db_close_trade(trade["id"], price, pnl, r_mult)
    await db_save_daily()

    emoji = "\U0001f4b0" if pnl > 0 else "\U0001f534"
    log.info("CLOSE %s | %s | PnL=%.2f | R=%.2f", symbol, reason, pnl, r_mult)
    dur = (time.time() - trade["opened_at"]) / 60
    msg = (f"{emoji} CLOSE {symbol}\nGrund: {reason}\n"
           f"PnL: {pnl:.2f} USDT\nR: {r_mult:.2f}\nDauer: {dur:.0f} min")
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
    """Scan all symbols for trading signals."""
    if not await check_risk():
        return

    equity = await fetch_balance()
    if equity <= 0:
        return

    for symbol in CFG["symbols"]:
        if symbol in active_trades:
            continue
        if is_correlated_blocked(symbol):
            continue

        candles = await fetch_ohlcv(symbol, "5m", 200)
        if not candles or len(candles) < 50:
            continue

        # Feature #1: MTF confluence
        mtf_sc, mtf_dir = await mtf_score(symbol)
        if mtf_sc < CFG["mtf_min_score"] or mtf_dir == "none":
            continue

        # Run strategies
        best_signal = None
        best_conf = 0.0
        for strat_fn in ALL_STRATEGIES:
            try:
                sig = await strat_fn(symbol, candles)
                if sig and sig["confidence"] > best_conf:
                    if ((mtf_dir == "long" and sig["side"] == "buy") or
                            (mtf_dir == "short" and sig["side"] == "sell")):
                        best_signal = sig
                        best_conf = sig["confidence"]
            except Exception as e:
                log.error("Strategy %s error on %s: %s", strat_fn.__name__, symbol, e)

        if not best_signal:
            continue

        # ML check
        features = build_features(candles)
        ml_prob, ml_conf = ml_predict(features)
        if ml_conf < CFG["ml_confidence_threshold"]:
            continue
        best_signal["confidence"] = (best_signal["confidence"] + ml_conf) / 2

        # Feature #3: funding rate
        funding = await fetch_funding_rate(symbol)
        if best_signal["side"] == "buy" and funding > CFG["funding_threshold"]:
            continue
        if best_signal["side"] == "sell" and funding < -CFG["funding_threshold"]:
            continue

        # Feature #11: volume profile boost
        vp = calc_volume_profile(candles)
        price = candles[-1][4]
        if vp["poc"] > 0 and abs(price - vp["poc"]) / price < 0.01:
            best_signal["confidence"] *= 1.1

        # Feature #13: cooldown
        last_close = last_closed_at.get(symbol, 0)
        if time.time() - last_close < cooldown_seconds():
            continue

        await open_trade(symbol, best_signal, equity)
        await asyncio.sleep(0.5)


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
            "\U0001f3db OLYMPUS Trading Bot aktiv!\n"
            "/status - Bot Status\n/report - Bericht\n"
            "/trades - Offene Trades\n/pause - Pausieren\n"
            "/resume - Fortsetzen\n/close_all - Alle schliessen\n"
            "/ask <frage> - KI fragen"
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
        text = "\U0001f4c8 <b>Offene Trades:</b>\n\n"
        for sym, t in active_trades.items():
            text += f"{sym} {t['side'].upper()}\nEntry: {sf(t['entry'])}\nSL: {sf(t['sl'])}\n\n"
        await update.message.reply_text(text, parse_mode="HTML")

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

    dash_runner = await start_dashboard()
    wh_runner = await start_webhook()
    tg_app = await setup_telegram()

    if tg_app:
        asyncio.create_task(_run_telegram(tg_app))

    equity = await fetch_balance()
    if equity > 0 and peak_equity == 0:
        peak_equity = equity

    msg = (f"\U0001f3db OLYMPUS gestartet!\n"
           f"Equity: {sf(equity, '.2f')} USDT\n"
           f"Sprint: {CFG['sprint_hours']}h\n"
           f"Symbole: {len(CFG['symbols'])}\n"
           f"Max Positionen: {CFG['max_concurrent']}")
    await tg_send(msg)

    cycle_count = 0
    health_iv = max(1, int(CFG["health_ping_sec"] / CFG["cycle_sec"]))
    equity_iv = max(1, int(3600 / CFG["cycle_sec"]))

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
