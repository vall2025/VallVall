# =============================================================================
# NSE F&O Momentum Scanner v2 — MRTQ Enhanced
# =============================================================================
#
#  MRTQ v2: Momentum Rank with Trend Quality — Enhanced
#  ──────────────────────────────────────────────────────────────────────────
#  Objective: Identify Top Gainer Bullish / Top Loser Bearish F&O stocks
#  at 11:00–11:45 AM that continue trending strongly until EOD.
#
#  WHAT CHANGED FROM MRTQ v1
#  ──────────────────────────────────────────────────────────────────────────
#  v1 Condition               v2 Improvement
#  ─────────────────────────────────────────────────────────────────────────
#  1. Move % from Prev Close  → KEPT (primary signal, unchanged)
#  2. Candle Direction %      → UPGRADED to Candle Body Strength Score
#                               (quality of candles, not just green/red count)
#  3. Trend Persistence %     → UPGRADED to Trend Efficiency Ratio (TER)
#                               (measures how straight the move is, not just
#                               how many closes are in trend direction)
#  4. Relative Volume         → UPGRADED to Same-Window RVOL
#                               (compares same time-window vs historical,
#                               not time-fraction of daily volume)
#
#  NEW CONDITIONS ADDED
#  ──────────────────────────────────────────────────────────────────────────
#  5. Momentum State          → Is buying/selling pressure sustaining?
#                               Rejects stocks that spiked early and stalled.
#  6. Shadow Quality          → Are opposing wicks rejecting the trend?
#                               Long upper wicks in bullish = selling at highs.
#                               Long lower wicks in bearish = buying at lows.
#                               Both are early EOD-reversal signals.
#
#  NEW RANKING METHOD
#  ──────────────────────────────────────────────────────────────────────────
#  v1: Ranked by Move% only
#  v2: Ranked by EOD Continuation Score (0–100 composite) then Move%
#      Score = Move%(30) + TER(25) + RVOL(20) + BodyStr(15) + Momentum(10)
#      This surfaces the stock with the highest EOD continuation probability,
#      not just the biggest early mover.
#
#  DATA FETCHING (from reference EOD_Trend_Scanner_v8)
#  ──────────────────────────────────────────────────────────────────────────
#  Uses TradeMaster.TradeSync TradeHub — same API as your existing scripts.
#  Fetches 1-min data (resolution="1"), resamples to 15-min anchored 9:15AM.
#  One wide API call per stock, reused for all metrics.
#  Same sliding-window rate limiter + retry-with-backoff infrastructure.
#  History reduced to 15 calendar days (no EMA warmup needed) → faster scan.
#
#  CUTOFF RULE (unchanged)
#  ──────────────────────────────────────────────────────────────────────────
#  to_dt = cutoff_dt is passed directly to the API — no candle after the
#  selected cutoff is ever used. Applies identically to Live and Historical.
#
# =============================================================================

import os
os.environ['STREAMLIT_SERVER_FILEWATCHERTYPE'] = 'none'

import warnings
warnings.filterwarnings('ignore')

import streamlit as st
try:
    st.set_option('server.fileWatcherType', 'none')
except Exception:
    pass

from TradeMaster.TradeSync import TradeHub, Exchange
import pytz
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time as dt_time
import time
import json
import threading
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================================================================
# CONSTANTS
# =============================================================================
CREDS_FILE   = os.path.join(os.path.expanduser('~'), 'alice_creds.json')
RR_RATIO     = 2.0
DEBUG        = False

# ── Candle timeframe ─────────────────────────────────────────────────────────
CANDLE_MIN   = 15        # 15-min candles, anchored at 9:15 AM
MIN_CANDLES  = 4         # minimum candles needed for any metric

# ── MRTQ v2 qualifying thresholds (all conditions must pass) ─────────────────
MIN_MOVE_PCT   = 0.5     # % move from prev close (Condition 1)
MIN_GREEN_PCT  = 0.55    # min fraction of green/red candles (Condition 2)
MIN_BODY_STR   = 0.35    # min avg body-to-range ratio of directional candles (Condition 2 upgrade)
MIN_TER        = 0.45    # min Trend Efficiency Ratio (Condition 3 upgrade)
MIN_RVOL       = 1.2     # min same-window RVOL (Condition 4 upgrade)
MIN_MOMENTUM   = 0.20    # min momentum ratio — second half >= 20% of first half (Condition 5)
MAX_SHADOW_RATIO = 1.20  # max opposing shadow / avg body ratio (Condition 6)

# ── EOD Score weights (must sum to 100) ──────────────────────────────────────
# Weights redistributed to:
#   (a) give quality metrics more influence than raw price move
#   (b) incorporate three new components: PBQ, LRS, TMS
#   (c) add Shadow Quality to the score (was gate-only before)
# ── Score weights (sum = 100) ─────────────────────────────────────────────────
# Calibrated from reference data: extreme RVOL and extreme Move% predict
# REVERSAL, not continuation. Moderate RVOL + building momentum = continuation.
W_MOVE    = 14   # ATR-adjusted move (not raw %) — overextension is now penalised
W_TER     = 18   # trend efficiency — unchanged, key quality predictor
W_RVOL    = 12   # continuation-optimised RVOL — extreme values now penalised
W_BODY    =  8   # candle body strength
W_MOMEN   = 10   # momentum state
W_SHADOW  =  4   # shadow quality
W_PBQ     =  8   # pullback quality — shallow pullbacks = continuation
W_LRS     =  4   # linear regression slope × R²
W_TMS     =  4   # trend maturity (was 2 — increased; overextension matters more)
W_PACCEL  = 10   # NEW: price acceleration in recent candles vs early candles
W_RELSTR  =  8   # NEW: relative strength rank vs scan universe (post-scan)
# Sum: 14+18+12+8+10+4+8+4+4+10+8 = 100

# ── History / RVOL ───────────────────────────────────────────────────────────
FETCH_LOOKBACK_DAYS = 15   # MUST be ≥15 for Alice Blue to return same-day intraday data.
                           # Alice Blue's historical API silently omits today's data when
                           # the fetch window is < ~15 calendar days. This is a known API
                           # quirk — the original v2 used 15 for exactly this reason.
                           # Do NOT reduce below 12 or ~35 stocks will show 0 today-candles.
RVOL_LOOKBACK       = 5    # trading days for same-window volume baseline

# ── API reliability ───────────────────────────────────────────────────────────
MAX_WORKERS       = 8      # 8 workers — safe with direct _api_call (no pool saturation)
REQUESTS_PER_SEC  = 8      # 8 RPS — direct calls handle this safely
MAX_FETCH_RETRIES = 2      # 2 retries — fast-fail on permanent errors
RETRY_BACKOFF     = [0.5, 1.5]   # shorter backoff = faster scan

# Try NSE first, then BSE; also expand common symbol suffix forms.
EXCHANGE_ORDER = [Exchange.NSE, Exchange.BSE]
SYMBOL_SUFFIXES = {
    # '-EQ' is the standard Alice Blue format for NSE equity stocks.
    # It goes FIRST so we find the correct instrument in one API call
    # instead of wasting a failed attempt on the bare name first.
    Exchange.NSE: ['-EQ', '', '-BE', '.NS'],
    Exchange.BSE: ['-EQ', '', '-BE', '.BSE'],
}

# ── Symbol name map: NSE official → Alice Blue internal name ─────────────────
# Some symbols in NSE's F&O list have different names inside Alice Blue.
# This map is checked FIRST, before any variant generation or API call.
# Add entries here whenever you encounter a symbol that consistently fails.
# ── Symbol name map ──────────────────────────────────────────────────────────
# Maps NSE official symbol → Alice Blue internal name.
# Each entry is a list so multiple candidates are tried in order.
# Add entries here whenever you find a symbol that consistently fails.
# ── SYMBOL_MAP ────────────────────────────────────────────────────────────────
# Maps NSE official symbol → list of Alice Blue name candidates (tried in order).
# RULE: NSE official name is ALWAYS listed FIRST (most likely with -EQ to work).
#       Alternative names come after as fallbacks only.
# To fix a failing stock: expand its list with the exact Alice Blue scrip name.
SYMBOL_MAP = {
    # ── Special characters ────────────────────────────────────────────────────
    'M&M'         : ['M&M',        'MM',           'MAHINDRA'],
    'GVT&D'       : ['GVT&D',      'GVTD',         'GVT',        'GVTANDD'],

    # ── Hyphenated symbols ────────────────────────────────────────────────────
    'BAJAJ-AUTO'  : ['BAJAJ-AUTO', 'BAJAJAUTO',    'BAJAJ'],
    'NAM-INDIA'   : ['NAM-INDIA',  'NAMINDIA',     'NIPPONIND',  'RELIANCENI'],

    # ── Renamed / rebranded stocks ────────────────────────────────────────────
    # Zomato rebranded to Eternal 2025 — try both names
    'ETERNAL'     : ['ETERNAL',    'ZOMATO',       'ETERNALLTD'],
    # Avenue Supermarts listed as DMART on NSE
    'DMART'       : ['DMART',      'AVENUSUPER',   'DMARTLTD'],
    # FSN E-Commerce (Nykaa) — try both
    'NYKAA'       : ['NYKAA',      'FSNECOMM',     'FSNE',       'FSN'],

    # ── Tata Motors DVR ───────────────────────────────────────────────────────
    'TMPV'        : ['TATAMTRDVR', 'TATAMOTDVR',   'TATAMTRDV',  'TATADVR'],

    # ── LTI Mindtree (stock symbol is LTIM on NSE; LTM is wrong) ─────────────
    'LTM'         : ['LTIM',       'LTM',          'LTIMINDTRE'],

    # ── Motilal Oswal — NSE symbol MOTILALOFS; try it first ──────────────────
    'MOTILALOFS'  : ['MOTILALOFS', 'MOTILALOFIN',  'MOFSL',      'MOTILAL'],

    # ── Vishal Mega Mart ──────────────────────────────────────────────────────
    'VMM'         : ['VMM',        'VISHAL',       'VISHALMM',   'VISHALMEGAR'],

    # ── Jio Financial Services ────────────────────────────────────────────────
    'JIOFIN'      : ['JIOFIN',     'JIOFINANCE',   'JIOFINSVC',  'JIOFINSERV'],

    # ── Hitachi Energy India (formerly ABB Power Products) ───────────────────
    'POWERINDIA'  : ['POWERINDIA', 'HITACHIENER',  'HITACHIENE', 'ABBPOWER'],

    # ── Amber Enterprises ────────────────────────────────────────────────────
    'AMBER'       : ['AMBER',      'AMBERENTER',   'AMBERENT',   'AMBERENTR'],

    # ── Blue Star ─────────────────────────────────────────────────────────────
    'BLUESTARCO'  : ['BLUESTARCO', 'BLUESTAR',     'BLUESTAREN'],

    # ── Godfrey Phillips ──────────────────────────────────────────────────────
    'GODFRYPHLP'  : ['GODFRYPHLP', 'GODFREYPHI',   'GODFREY',    'GODFREYPHIL'],

    # ── Mazagon Dock Shipbuilders ─────────────────────────────────────────────
    'MAZDOCK'     : ['MAZDOCK',    'MAZAGONDCK',   'MAZAGONDOCK','MAZGONDOCK'],

    # ── Cochin Shipyard ───────────────────────────────────────────────────────
    'COCHINSHIP'  : ['COCHINSHIP', 'COCHINSHPY',   'COCHIN',     'CSHL'],

    # ── Force Motors ─────────────────────────────────────────────────────────
    'FORCEMOT'    : ['FORCEMOT',   'FORCEMOTOR',   'FORCEMOTORS'],

    # ── PG Electroplast ───────────────────────────────────────────────────────
    'PGEL'        : ['PGEL',       'PGELECTRO',    'PGELECTROPL'],

    # ── Kaynes Technology ─────────────────────────────────────────────────────
    'KAYNES'      : ['KAYNES',     'KAYNESTEC',    'KAYNESTECH', 'KAYNESIND'],

    # ── Radico Khaitan ───────────────────────────────────────────────────────
    'RADICO'      : ['RADICO',     'RADICOKH',     'RADICOKHAI'],

    # ── Waaree Energies ───────────────────────────────────────────────────────
    'WAAREEENER'  : ['WAAREEENER', 'WAAREE',       'WAAREEENRG', 'WAAREEEN'],

    # ── Patanjali Foods ───────────────────────────────────────────────────────
    'PATANJALI'   : ['PATANJALI',  'PATANJALIF',   'PATFOODS',   'PATANJALIF'],

    # ── Nuvama Wealth (formerly Edelweiss Wealth) ─────────────────────────────
    'NUVAMA'      : ['NUVAMA',     'NUVAMAWEALTH', 'EDELWEISS',  'NUVAMAASST'],

    # ── Rail Vikas Nigam ──────────────────────────────────────────────────────
    'RVNL'        : ['RVNL',       'RAILVIKAS',    'RVNLLTD'],

    # ── Bharat Dynamics ───────────────────────────────────────────────────────
    'BDL'         : ['BDL',        'BHARATDYN',    'BDLLTD'],

    # ── Bajaj Holdings ────────────────────────────────────────────────────────
    'BAJAJHLDNG'  : ['BAJAJHLDNG', 'BAJAJHOLD',    'BAJAJHOLDI'],
    # ── Oracle Financial Services Software ───────────────────────────────────
    'OFSS'        : ['OFSS',       'ORACLEFSSS',   'ORACLEFIN'],

    # ── Standard entries (try NSE name with -EQ — usually works) ─────────────
    'LICI'        : ['LICI',       'LICIND',       'LICICORPLTD'],
    'ADANIGREEN'  : ['ADANIGREEN', 'ADANIGRN'],
    'ADANIPOWER'  : ['ADANIPOWER', 'ADANIPOW'],
}

os.environ["TZ"] = "Asia/Kolkata"
try:
    time.tzset()
except AttributeError:
    pass

# =============================================================================
# MARKET CALENDAR
# =============================================================================
_HOLIDAYS = {
    (2025,2,26),(2025,3,14),(2025,3,31),(2025,4,10),(2025,4,14),
    (2025,4,18),(2025,5,1),(2025,8,15),(2025,8,27),(2025,10,2),
    (2025,10,24),(2025,10,28),(2025,11,5),(2025,12,25),
    (2026,1,26),(2026,2,26),(2026,3,20),(2026,4,2),(2026,4,3),
    (2026,4,14),(2026,5,1),(2026,8,15),(2026,8,27),(2026,9,16),
    (2026,10,2),(2026,10,20),(2026,10,21),(2026,11,5),(2026,12,25),
}

def is_trading_day(d):
    return d.weekday() < 5 and (d.year, d.month, d.day) not in _HOLIDAYS

def last_trading_day(d=None):
    tz = pytz.timezone('Asia/Kolkata')
    if d is None:
        d = datetime.now(tz).date()
    c = d
    for _ in range(20):
        if is_trading_day(c):
            return c
        c -= timedelta(days=1)
    return d

def prev_trading_days(d, n):
    result, c = [], d - timedelta(days=1)
    while len(result) < n:
        if is_trading_day(c):
            result.append(c)
        c -= timedelta(days=1)
    return result

# =============================================================================
# RATE LIMITER — sliding window (same as reference EOD_Trend_Scanner_v8)
# =============================================================================
_rate_lock = threading.Lock()
_req_times = collections.deque()

def _throttle():
    while True:
        with _rate_lock:
            now = time.time()
            while _req_times and now - _req_times[0] > 1.0:
                _req_times.popleft()
            if len(_req_times) < REQUESTS_PER_SEC:
                _req_times.append(now)
                return
            wait = 1.0 - (now - _req_times[0])
        if wait > 0:
            time.sleep(wait)

# =============================================================================
# TIMEOUT EXECUTOR  (Fix 2: every broker call gets a hard wall-clock timeout)
# Previously a single hung call blocked ALL workers via the api_lock indefinitely.
# =============================================================================
# API CALL WRAPPER  — direct call, NO thread pool, NO saturation risk
# =============================================================================
# Root cause of 182/210 failures:
#   _API_EXEC = ThreadPoolExecutor(max_workers=30)
#   10 scan workers × 6 variants × 2 exchanges = up to 60 concurrent tasks.
#   Pool saturates after 30.  Remaining tasks queue.  fut.result(timeout=12)
#   fires and the scan worker moves on — but the task KEEPS RUNNING in the
#   pool thread.  Within ~3 scan rounds ALL 30 pool threads are stuck on
#   abandoned tasks.  Every subsequent API call then times out instantly.
#
# Fix: call the API directly — no inner executor, no saturation.
#   The rate limiter (_throttle) already controls frequency correctly.
#   Alice Blue's SDK has its own TCP timeout built in.
def _api_call(fn):
    """Execute fn() directly. Returns (result, error_msg)."""
    try:
        return fn(), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _is_retryable(err: str) -> bool:
    """
    Classify whether an error is worth retrying.

    Non-retryable (fail fast — retrying wastes seconds per stock):
        instrument not found, missing columns, unrecognised response type

    Retryable (transient — try again with backoff):
        timeout, network reset, empty response, generic API errors
    """
    if not err:
        return True
    e = err.lower()
    if 'instrument lookup failed' in e:   return False   # symbol not in broker DB
    if 'missing columns' in e:            return False   # permanent schema issue
    if 'unrecognised response type' in e: return False   # permanent API contract issue
    return True   # timeout / empty / network → worth retrying


# =============================================================================
# INSTRUMENT MASTER PRE-FETCH
# =============================================================================
# Root cause of remaining fetch failures:
#   trade.get_instrument(exchange, symbol) requires the EXACT Alice Blue symbol
#   name. 66 stocks fail because their Alice Blue name differs from the NSE
#   official symbol (e.g. company renamed, suffix required, different format).
#
# Fix:  Download Alice Blue's complete instrument list ONCE before the scan.
#       Then every stock lookup is an O(1) local dict search — no per-stock
#       API call needed. We normalize all keys so symbol-format mismatches
#       are handled automatically.
# =============================================================================

def _normalise_sym(s: str) -> str:
    """Normalise a symbol string for fuzzy matching."""
    s = str(s).strip().upper()
    for sfx in ('-EQ', '-BE', '-SM', '-IL', '-BL', '.NS', '.BSE', '-N', '-B'):
        if s.endswith(sfx):
            s = s[:-len(sfx)]
            break
    return s.replace(' ', '').replace('-', '').replace('&', 'AND').replace('.', '')


def _store_master_entry(master, exch, inst):
    """
    Add one instrument to the master dict under every key variant.
    Called from prefetch_instrument_master() for each instrument record.
    """
    raw_sym = None
    for attr in ('symbol', 'Symbol', 'tradingsymbol',
                 'scripShortName', 'name', 'Name', 'scrip'):
        if hasattr(inst, attr):
            raw_sym = getattr(inst, attr)
        elif isinstance(inst, dict):
            raw_sym = inst.get(attr)
        if raw_sym:
            break
    if not raw_sym:
        return

    raw_sym = str(raw_sym).strip().upper()

    # Store under exact name
    master[(exch.name, raw_sym)] = (exch, inst)

    # Store under normalised base (strips -EQ, -BE, hyphens, spaces, &→AND)
    norm = _normalise_sym(raw_sym)
    if (exch.name, norm) not in master:
        master[(exch.name, norm)] = (exch, inst)

    # Extra: if name has -EQ, also store without it explicitly
    for sfx in ('-EQ', '-BE', '-SM', '-BL'):
        if raw_sym.endswith(sfx):
            bare = raw_sym[:-len(sfx)]
            if (exch.name, bare) not in master:
                master[(exch.name, bare)] = (exch, inst)


def prefetch_instrument_master(trade, cache, cache_lock, timeout_sec=90):
    """
    Download ALL NSE/BSE instruments from Alice Blue and store them in
    cache['__MASTER__'] for O(1) lookups during the scan.

    Tries every known TradeHub / Alice Blue API method for fetching the
    scrip master — different library versions expose different names.

    Returns the number of instruments loaded (0 = none worked, fallback
    to per-symbol get_instrument() calls — this is what shows the
    "Master unavailable" warning in the UI).
    """
    master = {}

    for exch in [Exchange.NSE, Exchange.BSE]:
        result = None

        # ── Try every known method / attribute name ───────────────────────
        candidates = [
            # Method calls (most common)
            lambda e=exch: trade.get_all_instruments(exchange=e),
            lambda e=exch: trade.get_all_instruments(e),
            lambda e=exch: trade.get_master_contract(exchange=e),
            lambda e=exch: trade.get_master_contract(e),
            lambda e=exch: trade.get_instrument_list(exchange=e),
            lambda e=exch: trade.get_instrument_list(e),
            lambda e=exch: trade.get_all_scrip(exchange=e),
            lambda e=exch: trade.scripmaster(exchange=e),
        ]
        # Attribute access (no call)
        attr_names = ('master', 'scripmaster', 'master_contract',
                      'instruments', 'all_instruments')

        for fn in candidates:
            if result:
                break
            try:
                result = fn()
                if not result:
                    result = None
            except Exception:
                result = None
            except Exception:
                result = None

        if not result:
            for attr in attr_names:
                try:
                    val = getattr(trade, attr, None)
                    if callable(val):
                        val = val()
                    if val:
                        result = val
                        break
                except Exception:
                    pass

        if not result:
            continue   # this exchange not available — try next

        instruments = result if isinstance(result, (list, tuple)) else [result]
        for inst in instruments:
            _store_master_entry(master, exch, inst)

    with cache_lock:
        cache['__MASTER__'] = master

    return len(master)


def _lookup_in_master(master, base_variants):
    """
    Search the pre-fetched master for any of the provided base variants.
    Tries exact names, normalised keys, and truncated prefix matches.
    Returns (exchange_enum, instrument_object) or (None, None).
    """
    if not master:
        return None, None

    for exch_name in ('NSE', 'BSE'):
        for b in base_variants:
            b_up = b.upper()
            # 1. Exact match with standard Alice Blue suffixes
            for suffix in ('-EQ', '', '-BE', '-SM', '-BL'):
                key = (exch_name, b_up + suffix)
                if key in master:
                    return master[key]
            # 2. Normalised match (handles &, hyphens, spaces, suffix stripping)
            norm = _normalise_sym(b_up)
            key  = (exch_name, norm)
            if key in master:
                return master[key]

    return None, None



# =============================================================================
# DATA FETCH — 1-min resolution (same methodology as reference)
# =============================================================================
def _fetch_1min_attempt(sym, from_dt, to_dt, trade, cache, cache_lock):
    # NOTE: api_lock removed — it was serialising all 5 workers to 1 at a time.
    # The rate limiter (_throttle) already handles concurrency correctly.
    """Single fetch attempt. Returns (df, error_string)."""
    try:
        # Build a set of candidate base symbols (handle punctuation, hyphens, ampersand, common variants)
        orig = (sym or '').strip()
        if not orig:
            return None, "instrument lookup failed: empty symbol"

        up = orig.upper()
        # common base (strip typical NSE/BSE suffixes if present)
        base = up.replace('.NS', '').replace('.BSE', '')

        # ── Build candidate base names from SYMBOL_MAP + variants ─────────────
        # SYMBOL_MAP values are now lists of candidates (ordered best-first).
        # For each candidate we also generate derived variants.
        mapped_list = SYMBOL_MAP.get(orig.upper())
        if mapped_list:
            candidates = [c.strip().upper() for c in mapped_list if c]
        else:
            candidates = [base]
        # Always include the original symbol as a final fallback
        candidates.append(orig.upper())

        base_variants = set()
        for cand in candidates:
            base_variants.add(cand)
            base_variants.add(cand.replace('-', ''))       # BAJAJ-AUTO → BAJAJAUTO
            base_variants.add(cand.replace('&', 'AND'))    # M&M → MANDM
            base_variants.add(cand.replace('&', ''))       # GVT&D → GVTD
            base_variants.add(cand.replace(' ', ''))       # trailing spaces
            base_variants.add(cand.replace('.', ''))       # dots
            # Truncation variants — Alice Blue stores some symbols with fewer chars
            clean = cand.replace('-','').replace('&','').replace(' ','').replace('.','')
            for n in (10, 9, 8):
                if len(clean) > n:
                    base_variants.add(clean[:n])
        base_variants = {v for v in base_variants if v}

        # Use first candidate as 'base' for cache-key labelling
        base = candidates[0]

        inst = None
        lookup_error = None
        attempts = []

        # ── FAST PATH 1: per-symbol cache (no API call) ──────────────────────
        with cache_lock:
            for exch in EXCHANGE_ORDER:
                for suffix in SYMBOL_SUFFIXES[exch]:
                    for b in base_variants:
                        ck = f"{exch.name}:{b + suffix}"
                        if ck in cache and ck != '__MASTER__':
                            inst = cache[ck]
                            break
                    if inst is not None:
                        break
                if inst is not None:
                    break

        # ── FAST PATH 2: pre-fetched instrument master (O(1) dict lookup) ──────
        # Resolves symbol-name mismatches without any extra API call.
        # Alice Blue may store "M&M-EQ" while our list has "M&M" — master handles it.
        if inst is None:
            master = cache.get('__MASTER__', {})
            if master:
                _exch, inst = _lookup_in_master(master, base_variants)
                if inst is not None:
                    with cache_lock:
                        cache[f"{_exch.name}:{base}"] = inst

        # ── SLOW PATH A: per-symbol get_instrument() API call ─────────────────
        # Only reached when master dict is unavailable (older TradeHub builds).
        if inst is None:
            for exch in EXCHANGE_ORDER:
                for suffix in SYMBOL_SUFFIXES[exch]:
                    for b in base_variants:
                        cand_sym = b + suffix
                        cache_key = f"{exch.name}:{cand_sym}"
                        attempts.append(cache_key)
                        _throttle()
                        inst, _lerr = _api_call(
                            lambda _e=exch, _s=cand_sym: trade.get_instrument(exchange=_e, symbol=_s)
                        )
                        if _lerr:
                            lookup_error = _lerr
                            inst = None
                        if isinstance(inst, dict) and inst.get('stat') == 'Not_ok':
                            lookup_error = inst.get('emsg', 'Not_ok')
                            inst = None
                        if inst:
                            with cache_lock:
                                cache[cache_key] = inst
                            break
                    if inst is not None:
                        break
                if inst is not None:
                    break

        # ── SLOW PATH B: search_instruments() fuzzy fallback ──────────────────
        # Final resort: Alice Blue's search API does partial/fuzzy matching and
        # can find stocks that exact get_instrument() calls miss.
        if inst is None:
            for exch in EXCHANGE_ORDER:
                try:
                    _throttle()
                    results, _serr = _api_call(
                        lambda _e=exch, _b=base: trade.search_instruments(
                            exchange=_e, symbol=_b
                        )
                    )
                    if not _serr and results and isinstance(results, list):
                        best = None
                        best_score = -1
                        for r in results:
                            rsym = str(getattr(r, 'symbol', '') or
                                       (r.get('symbol', '') if isinstance(r, dict) else '')).upper()
                            norm_r    = _normalise_sym(rsym)
                            norm_base = _normalise_sym(base)
                            if norm_r == norm_base:
                                best = r; best_score = 2; break
                            if norm_base in norm_r and 1 > best_score:
                                best = r; best_score = 1
                        if best:
                            inst = best
                            with cache_lock:
                                cache[f"{exch.name}:{base}"] = inst
                            break
                except Exception:
                    pass  # search_instruments not supported — silent skip

        if inst is None:
            err_text = lookup_error or 'not found via master, get_instrument, or search_instruments'
            return None, f"instrument lookup failed for '{sym}' (tried: {attempts[:6]}; {err_text})"

        _throttle()
        result, _ferr = _api_call(
            lambda: trade.get_HistoricalData(
                instrument=inst,
                resolution="1",
                from_datetime=from_dt,
                to_datetime=to_dt,
                indices=False,
            )
        )
        if _ferr:
            return None, f"API call error: {_ferr}"

        # ── Normalise response ───────────────────────────────────────────────
        df = None
        if isinstance(result, list) and result:
            df = pd.DataFrame(result)
        elif isinstance(result, list) and not result:
            return None, "API returned empty list"
        elif isinstance(result, dict) and result.get('stat') == 'Ok':
            df = pd.DataFrame(result.get('data', []))
        elif isinstance(result, pd.DataFrame):
            df = result.copy()
        else:
            try:
                df = pd.DataFrame(result)
            except Exception:
                return None, f"unrecognised response type: {type(result).__name__}"

        if df is None or df.empty:
            return None, "empty dataframe from API"

        # ── Column rename ────────────────────────────────────────────────────
        col_map = {}
        for col in df.columns:
            cl = col.lower().strip()
            if   cl == 'datetime':           col_map[col] = 'datetime'
            elif cl in ('open','o'):         col_map[col] = 'Open'
            elif cl in ('high','h'):         col_map[col] = 'High'
            elif cl in ('low','l'):          col_map[col] = 'Low'
            elif cl in ('close','c'):        col_map[col] = 'Close'
            elif cl in ('volume','vol','v'): col_map[col] = 'Volume'
        df = df.rename(columns=col_map)

        required = ['Open','High','Low','Close','Volume']
        if not all(r in df.columns for r in required):
            return None, f"missing columns, got: {list(df.columns)}"

        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime')

        # ── Timezone handling ─────────────────────────────────────────────────
        tz_str = 'Asia/Kolkata'
        try:
            if df.index.tzinfo is None:
                df.index = pd.to_datetime(df.index).tz_localize(tz_str)
            else:
                df.index = df.index.tz_convert(tz_str)
        except Exception:
            try:
                df.index = pd.to_datetime(df.index).tz_localize(tz_str)
            except Exception as e:
                return None, f"datetime index error: {e}"

        for col in required:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df[required].sort_index()
        df = df[~df.index.duplicated(keep='last')]
        df = df.dropna(subset=['Open','High','Low','Close'])

        if df.empty:
            return None, "all rows dropped after cleaning"
        if len(df) < 5:
            return None, f"too few 1-min bars: {len(df)}"

        return df, None

    except Exception as e:
        if DEBUG:
            import traceback
            print(f"[FETCH ERR] {sym}: {e}\n{traceback.format_exc()}")
        return None, f"unexpected error: {e}"


def fetch_1min(sym, from_dt, to_dt, trade, cache, cache_lock):
    """Fetch with retry-and-backoff (smart: non-retryable errors fail fast)."""
    last_err = "unknown"
    for attempt in range(MAX_FETCH_RETRIES):
        df, err = _fetch_1min_attempt(sym, from_dt, to_dt, trade, cache, cache_lock)
        if df is not None:
            return df, None
        last_err = err or "unknown"
        if not _is_retryable(last_err):
            break   # instrument not found / schema issue — retrying is pointless
        if attempt < MAX_FETCH_RETRIES - 1:
            time.sleep(RETRY_BACKOFF[attempt])
    return None, f"{last_err} (after {attempt+1} attempts)"


def to_Nmin(df, n_min):
    """Resample 1-min DataFrame to n-min, anchored at 9:15 AM."""
    if df is None or df.empty:
        return pd.DataFrame()
    try:
        tz_str = 'Asia/Kolkata'
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(tz_str)
        df = df[(df.index.time >= dt_time(9,15)) &
                (df.index.time <= dt_time(15,30))].copy()
        if df.empty:
            return pd.DataFrame()
        out = df[['Open','High','Low','Close','Volume']].resample(
            f'{n_min}min',
            origin='start_day',
            offset='9h15min',
            label='left',
            closed='left'
        ).agg({'Open':'first','High':'max','Low':'min',
               'Close':'last','Volume':'sum'})
        return out[out['Close'].notna()].copy()
    except Exception:
        return pd.DataFrame()

# =============================================================================
# MRTQ v2 — METRIC COMPUTATION
# =============================================================================
def compute_mrtq_metrics(df15_today: pd.DataFrame) -> dict:
    """
    Compute all six MRTQ v2 metrics from today's 15-min candles up to cutoff.

    Parameters
    ----------
    df15_today : DataFrame with columns Open/High/Low/Close/Volume,
                 already filtered to the cutoff window.

    Returns
    -------
    dict of all raw metric values, or None if data insufficient.
    """
    n = len(df15_today)
    if n < MIN_CANDLES:
        return None

    opens  = df15_today['Open'].values.astype(float)
    highs  = df15_today['High'].values.astype(float)
    lows   = df15_today['Low'].values.astype(float)
    closes = df15_today['Close'].values.astype(float)

    ref_price = closes[-1]  # for % floors
    floor     = max(ref_price * 0.00001, 1e-6)

    # ── CONDITION 2a: Candle direction count ──────────────────────────────────
    green_mask  = closes > opens
    red_mask    = closes < opens
    green_count = int(green_mask.sum())
    red_count   = int(red_mask.sum())
    green_pct   = green_count / n
    red_pct     = red_count   / n

    # ── CONDITION 2b: Body Strength ───────────────────────────────────────────
    # Measures the QUALITY of directional candles, not just their count.
    # A green candle where body = 90% of range signals strong conviction.
    # A green candle where body = 5% of range (near-doji) signals indecision.
    bodies   = np.abs(closes - opens)
    ranges_  = np.maximum(highs - lows, floor)
    body_ratios = bodies / ranges_

    bull_body_str = float(body_ratios[green_mask].mean()) if green_count > 0 else 0.0
    bear_body_str = float(body_ratios[red_mask].mean())   if red_count  > 0 else 0.0

    # ── CONDITION 3: Trend Efficiency Ratio (TER) ─────────────────────────────
    # Measures how straight-line the move is.
    # net_move / total_path  →  1.0 = perfect straight line, 0 = choppy oscillation
    # A stock moving lower-left to upper-right cleanly has TER close to 1.
    # A choppy, oscillating stock has TER close to 0 even if it ends up.
    close_diffs = np.diff(closes)
    path_length = float(np.sum(np.abs(close_diffs)))

    net_bull = float(closes[-1] - closes[0])   # positive = overall up
    net_bear = float(closes[0]  - closes[-1])  # positive = overall down

    ter_bull = net_bull / path_length if path_length > floor else 0.0
    ter_bear = net_bear / path_length if path_length > floor else 0.0
    # Note: TER is signed. For bullish we need ter_bull > 0 (and ideally >= MIN_TER).

    # ── CONDITION 5: Momentum State ───────────────────────────────────────────
    # Compares price movement in the SECOND half of the session window vs FIRST half.
    # If second_half_move >= threshold × first_half_move, momentum is sustained.
    # Filters stocks that spiked at open and are now stalling / pulling back.
    if n >= 6:
        half = n // 2
        first_net_bull = closes[half - 1] - closes[0]
        last_net_bull  = closes[-1]       - closes[half]

        first_net_bear = closes[0]    - closes[half - 1]
        last_net_bear  = closes[half] - closes[-1]

        denom_bull = abs(first_net_bull) if abs(first_net_bull) > floor else floor
        denom_bear = abs(first_net_bear) if abs(first_net_bear) > floor else floor

        momentum_bull = last_net_bull / denom_bull
        momentum_bear = last_net_bear / denom_bear
    else:
        # Not enough candles to split — pass through with neutral value
        momentum_bull = 1.0
        momentum_bear = 1.0

    # ── CONDITION 6: Shadow Quality ───────────────────────────────────────────
    # For bullish: upper wicks (high - max(open,close)) reflect selling pressure.
    # Large upper wicks mean the market is rejecting higher prices — bearish sign
    # within an otherwise bullish candle set.
    # For bearish: lower wicks reflect buying support — a bearish reversal signal.
    upper_shadows = highs - np.maximum(opens, closes)
    lower_shadows = np.minimum(opens, closes) - lows

    avg_body         = float(bodies.mean()) if len(bodies) > 0 else floor
    avg_body         = max(avg_body, floor)
    avg_upper_shadow = float(upper_shadows.mean())
    avg_lower_shadow = float(lower_shadows.mean())

    # Shadow ratio: shadow / body. > 1 = shadow larger than body (rejection signal)
    bull_shadow_ratio = avg_upper_shadow / avg_body  # ideally low for bullish
    bear_shadow_ratio = avg_lower_shadow / avg_body  # ideally low for bearish

    # ── NEW: Pullback Quality Score ─────────────────────────────────────────
    # Measures how CLEAN the intraday trend is by examining pullback depth and
    # frequency.  Shallow, infrequent pullbacks → high EOD continuation.
    # Deep or frequent pullbacks → sellers still active → reversal risk.
    closes_arr = df15_today['Close'].values.astype(float)
    diffs      = np.diff(closes_arr)                          # close-to-close changes

    # Bullish PBQ: penalise downward moves (bounces against the bull trend)
    if closes_arr[-1] > closes_arr[0]:
        total_up_bull  = closes_arr[-1] - closes_arr[0]
        down_moves     = np.maximum(-diffs, 0.0)              # magnitude of each dip
        avg_pb_bull    = float(np.mean(down_moves)) / total_up_bull
        max_pb_bull    = float(np.max(down_moves))  / total_up_bull
        pb_freq_bull   = float(np.sum(down_moves > 0)) / len(diffs)
        pbq_bull       = max(0.0, 1.0 - 0.4*min(avg_pb_bull, 1.0)
                                      - 0.4*min(max_pb_bull, 1.0)
                                      - 0.2*pb_freq_bull)
    else:
        pbq_bull = 0.0   # not a bullish move

    # Bearish PBQ: penalise upward bounces (against the bear trend)
    if closes_arr[-1] < closes_arr[0]:
        total_dn_bear  = closes_arr[0] - closes_arr[-1]
        up_moves       = np.maximum(diffs, 0.0)               # magnitude of each bounce
        avg_pb_bear    = float(np.mean(up_moves)) / total_dn_bear
        max_pb_bear    = float(np.max(up_moves))  / total_dn_bear
        pb_freq_bear   = float(np.sum(up_moves > 0)) / len(diffs)
        pbq_bear       = max(0.0, 1.0 - 0.4*min(avg_pb_bear, 1.0)
                                      - 0.4*min(max_pb_bear, 1.0)
                                      - 0.2*pb_freq_bear)
    else:
        pbq_bear = 0.0

    # ── NEW: Linear Regression Slope Score ──────────────────────────────────
    # Measures HOW STEEPLY the trend is moving, combined with how well the
    # price action fits a straight line (R²).
    # Score = (slope% per candle × R²) / reference  — linear, no reshaping.
    # Reference: 0.25% per candle is a strong institutional trend.
    _x         = np.arange(n, dtype=float)
    slope, icp = np.polyfit(_x, closes_arr, 1)
    avg_px     = float(closes_arr.mean())
    slope_pct  = slope / avg_px if avg_px > 0 else 0.0        # % per candle

    y_pred  = slope * _x + icp
    ss_res  = float(np.sum((closes_arr - y_pred) ** 2))
    ss_tot  = float(np.sum((closes_arr - closes_arr.mean()) ** 2))
    r2      = max(0.0, 1.0 - ss_res / ss_tot) if ss_tot > 1e-12 else 0.0

    REF_SLOPE = 0.0025   # 0.25% per 15-min candle = strong trend reference
    lrs_bull  = min(max( slope_pct, 0.0) / REF_SLOPE, 1.0) * r2   # bull: rising
    lrs_bear  = min(max(-slope_pct, 0.0) / REF_SLOPE, 1.0) * r2   # bear: falling

    return {
        'n_candles'       : n,
        # Condition 2
        'green_count'     : green_count,
        'red_count'       : red_count,
        'green_pct'       : round(green_pct, 3),
        'red_pct'         : round(red_pct, 3),
        'bull_body_str'   : round(bull_body_str, 3),
        'bear_body_str'   : round(bear_body_str, 3),
        # Condition 3
        'ter_bull'        : round(ter_bull, 3),
        'ter_bear'        : round(ter_bear, 3),
        # Condition 5
        'momentum_bull'   : round(momentum_bull, 3),
        'momentum_bear'   : round(momentum_bear, 3),
        # Condition 6
        'bull_shadow_ratio': round(bull_shadow_ratio, 3),
        'bear_shadow_ratio': round(bear_shadow_ratio, 3),
        # NEW: Pullback Quality (0-1, higher = cleaner trend)
        'pbq_bull'        : round(pbq_bull, 3),
        'pbq_bear'        : round(pbq_bear, 3),
        # NEW: Linear Regression Slope × R² (0-1)
        'lrs_bull'        : round(lrs_bull, 3),
        'lrs_bear'        : round(lrs_bear, 3),
    }

# =============================================================================
# MRTQ v2 — QUALIFICATION GATES
# =============================================================================
def qualify_bull(m: dict, move_pct: float, rvol) -> tuple:
    """
    Bullish qualification: all 6 conditions must pass.
    Returns (qualified: bool, fail_reasons: list[str]).

    Condition 1: Move % >= MIN_MOVE_PCT
    Condition 2: Green % >= MIN_GREEN_PCT  AND  Body Strength >= MIN_BODY_STR
    Condition 3: TER (bull direction) >= MIN_TER
    Condition 4: Same-Window RVOL >= MIN_RVOL
    Condition 5: Momentum ratio >= MIN_MOMENTUM
    Condition 6: Upper shadow ratio <= MAX_SHADOW_RATIO
    """
    reasons = []

    # 1. Move %
    if move_pct < MIN_MOVE_PCT:
        reasons.append(
            f"Move {move_pct:+.2f}% < {MIN_MOVE_PCT}% (not a strong enough early mover)"
        )

    # 2a. Candle direction majority
    if m['green_pct'] < MIN_GREEN_PCT:
        reasons.append(
            f"Green {m['green_count']}/{m['n_candles']} "
            f"({m['green_pct']*100:.0f}% < {MIN_GREEN_PCT*100:.0f}%)"
        )

    # 2b. Body strength
    if m['bull_body_str'] < MIN_BODY_STR:
        reasons.append(
            f"Body strength {m['bull_body_str']:.2f} < {MIN_BODY_STR} "
            f"(green candles too small/doji-like)"
        )

    # 3. Trend Efficiency
    if m['ter_bull'] < MIN_TER:
        reasons.append(
            f"TER {m['ter_bull']:.2f} < {MIN_TER} "
            f"(choppy, not a clean directional move)"
        )

    # 4. RVOL
    if rvol is not None and rvol < MIN_RVOL:
        reasons.append(
            f"RVOL {rvol:.2f}× < {MIN_RVOL}× (volume not confirming the move)"
        )

    # 5. Momentum
    if m['momentum_bull'] < MIN_MOMENTUM:
        reasons.append(
            f"Momentum {m['momentum_bull']:.2f} < {MIN_MOMENTUM} "
            f"(buying pressure decelerating)"
        )

    # 6. Shadow quality
    if m['bull_shadow_ratio'] > MAX_SHADOW_RATIO:
        reasons.append(
            f"Upper shadow ratio {m['bull_shadow_ratio']:.2f} > {MAX_SHADOW_RATIO} "
            f"(sellers rejecting highs — EOD reversal risk)"
        )

    return len(reasons) == 0, reasons


def qualify_bear(m: dict, move_pct: float, rvol) -> tuple:
    """Symmetric bearish qualification."""
    reasons = []

    if move_pct > -MIN_MOVE_PCT:
        reasons.append(
            f"Move {move_pct:+.2f}% > -{MIN_MOVE_PCT}% (not a strong enough loser)"
        )

    if m['red_pct'] < MIN_GREEN_PCT:
        reasons.append(
            f"Red {m['red_count']}/{m['n_candles']} "
            f"({m['red_pct']*100:.0f}% < {MIN_GREEN_PCT*100:.0f}%)"
        )

    if m['bear_body_str'] < MIN_BODY_STR:
        reasons.append(
            f"Body strength {m['bear_body_str']:.2f} < {MIN_BODY_STR} "
            f"(red candles too small/doji-like)"
        )

    if m['ter_bear'] < MIN_TER:
        reasons.append(
            f"TER {m['ter_bear']:.2f} < {MIN_TER} "
            f"(choppy, not a clean directional move)"
        )

    if rvol is not None and rvol < MIN_RVOL:
        reasons.append(
            f"RVOL {rvol:.2f}× < {MIN_RVOL}× (volume not confirming the move)"
        )

    if m['momentum_bear'] < MIN_MOMENTUM:
        reasons.append(
            f"Momentum {m['momentum_bear']:.2f} < {MIN_MOMENTUM} "
            f"(selling pressure decelerating)"
        )

    if m['bear_shadow_ratio'] > MAX_SHADOW_RATIO:
        reasons.append(
            f"Lower shadow ratio {m['bear_shadow_ratio']:.2f} > {MAX_SHADOW_RATIO} "
            f"(buyers supporting lows — EOD reversal risk)"
        )

    return len(reasons) == 0, reasons

# =============================================================================
# MRTQ v2+ — EOD CONTINUATION SCORE  (0–100)
# Transparent linear normalization. No nonlinear reshaping. No saturation caps
# that artificially equalise stocks — every increment always increases the score.
# =============================================================================
# _clamp is kept as a utility but is NOT used for score shaping —
# only to keep component contributions within [0, 1] for legibility.

# ── Evidence-based scoring helpers ───────────────────────────────────────────
def _rvol_cont(rv: float) -> float:
    """RVOL Continuation Score (0-1). Penalises extreme RVOL (>6×) which signals
    exhaustion events (gap/news) rather than sustainable institutional flow.
    Sweet spot: 1.5-3.0× = genuine institutional participation."""
    if rv < 1.0:  return 0.0
    if rv < 1.5:  return (rv - 1.0) / 0.5 * 0.35
    if rv <= 3.0: return 0.35 + (rv - 1.5) / 1.5 * 0.65
    if rv <= 5.0: return max(0.40, 1.0 - (rv - 3.0) / 4.0)
    return max(0.0, 0.40 - (rv - 5.0) / 15.0)


def _atr_move(move_pct: float, atr_pct: float) -> float:
    """ATR-Normalised Move Score (0-1). Measures fraction of typical daily range
    already consumed. Sweet spot 25-65% (room to run). Above 80% = exhaustion.
    MOTILALOFS -10.55% on 2% ATR = 2.6× → score≈0. GVT&D -3% on 4% ATR = 0.75 → score≈0.7."""
    if atr_pct <= 0: atr_pct = max(abs(move_pct)*1.5, 1.0)
    ratio = abs(move_pct) / atr_pct
    if ratio < 0.20: return ratio / 0.20 * 0.35
    if ratio <= 0.65: return 0.35 + (ratio - 0.20) / 0.45 * 0.65
    return max(0.0, 1.0 - (ratio - 0.65) / 0.55)


def _price_accel(closes, direction: str) -> float:
    """Price Acceleration Score (0-1). Compares directional momentum in last
    third of candles vs first third. Accelerating = institutions still entering.
    Decelerating = move losing steam."""
    n = len(closes)
    if n < 4: return 0.5
    n3 = max(2, n // 3)
    diffs = np.diff(closes)
    if direction == 'bull':
        early = float(np.mean(np.maximum(diffs[:n3], 0)))
        late  = float(np.mean(np.maximum(diffs[-n3:], 0)))
    else:
        early = float(np.mean(np.maximum(-diffs[:n3], 0)))
        late  = float(np.mean(np.maximum(-diffs[-n3:], 0)))
    if early < 1e-6: return 0.5
    return float(min(1.0, max(0.0, 0.3 + (late / early) * 0.5)))


def _compute_atr_pct(df15, scan_date) -> float:
    """Average daily ATR% from last 5 historical trading days."""
    try:
        hist = sorted(d for d in set(df15.index.date) if d < scan_date)[-5:]
        ranges = []
        for hd in hist:
            dh = df15[df15.index.date == hd]
            if len(dh) >= 4:
                ref = float(dh['Open'].iloc[0])
                if ref > 0:
                    ranges.append((float(dh['High'].max()) - float(dh['Low'].min())) / ref * 100.0)
        return float(np.mean(ranges)) if ranges else 2.0
    except Exception:
        return 2.0


def _clamp(val, lo=0.0, hi=1.0):
    return max(lo, min(hi, float(val)))


def eod_score_bull(m: dict, move_pct: float, rvol) -> float:
    """
    MRTQ v2+ EOD Continuation Score — BULL (0-100).

    Evidence-based scoring calibrated from reference stock analysis.
    Key changes vs previous version:
      move_n  → ATR-normalised: penalises overextended moves (>80% ATR consumed)
      rvol_n  → continuation-optimised: extreme RVOL (>6×) penalised (exhaustion)
      paccel  → NEW: price acceleration in recent vs early candles
      relstr  → NEW: relative strength rank vs scan universe (set post-scan)
    """
    rv = float(rvol) if rvol is not None else 1.0

    move_n   = _atr_move(abs(move_pct), m.get('atr_pct', 2.0))   # ATR-adjusted
    ter_n    = _clamp(m['ter_bull'])
    rvol_n   = _rvol_cont(rv)                                      # continuation-optimised
    body_n   = _clamp(m['bull_body_str'])
    mom_n    = _clamp(m['momentum_bull'] / 2.0)
    shadow_n = _clamp(1.0 - m['bull_shadow_ratio'])
    pbq_n    = _clamp(m.get('pbq_bull', 0.5))
    lrs_n    = _clamp(m.get('lrs_bull', 0.0))
    tms_n    = _clamp(m.get('tms', 0.5))
    paccel_n = _clamp(m.get('price_accel_bull', 0.5))             # NEW
    relstr_n = _clamp(m.get('relative_strength', 0.5))            # NEW (post-scan)

    return round(_clamp(
        move_n   * W_MOVE   +
        ter_n    * W_TER    +
        rvol_n   * W_RVOL   +
        body_n   * W_BODY   +
        mom_n    * W_MOMEN  +
        shadow_n * W_SHADOW +
        pbq_n    * W_PBQ    +
        lrs_n    * W_LRS    +
        tms_n    * W_TMS    +
        paccel_n * W_PACCEL +
        relstr_n * W_RELSTR,
        0, 100
    ), 1)


def eod_score_bear(m: dict, move_pct: float, rvol) -> float:
    """MRTQ v2+ EOD Continuation Score — BEAR (symmetric, 0-100)."""
    rv = float(rvol) if rvol is not None else 1.0

    move_n   = _atr_move(abs(move_pct), m.get('atr_pct', 2.0))
    ter_n    = _clamp(m['ter_bear'])
    rvol_n   = _rvol_cont(rv)
    body_n   = _clamp(m['bear_body_str'])
    mom_n    = _clamp(m['momentum_bear'] / 2.0)
    shadow_n = _clamp(1.0 - m['bear_shadow_ratio'])
    pbq_n    = _clamp(m.get('pbq_bear', 0.5))
    lrs_n    = _clamp(m.get('lrs_bear', 0.0))
    tms_n    = _clamp(m.get('tms', 0.5))
    paccel_n = _clamp(m.get('price_accel_bear', 0.5))
    relstr_n = _clamp(m.get('relative_strength', 0.5))

    return round(_clamp(
        move_n   * W_MOVE   +
        ter_n    * W_TER    +
        rvol_n   * W_RVOL   +
        body_n   * W_BODY   +
        mom_n    * W_MOMEN  +
        shadow_n * W_SHADOW +
        pbq_n    * W_PBQ    +
        lrs_n    * W_LRS    +
        tms_n    * W_TMS    +
        paccel_n * W_PACCEL +
        relstr_n * W_RELSTR,
        0, 100
    ), 1)


# =============================================================================
# SCAN ONE STOCK
# =============================================================================
def scan_one(sym, scan_date, cutoff_dt, trade, cache, cache_lock):
    """
    Full MRTQ v2 pipeline for one stock.
    Returns (result_dict | None, status_string).
    """
    tz = pytz.timezone('Asia/Kolkata')
    try:
        # ── 1. FETCH  (one wide call, reused for all metrics) ────────────────
        from_dt = tz.localize(datetime.combine(
            scan_date - timedelta(days=FETCH_LOOKBACK_DAYS), dt_time(9, 14)
        ))
        df1, err = fetch_1min(sym, from_dt, cutoff_dt, trade, cache, cache_lock)
        if df1 is None:
            return None, f"FAILED: {err}"

        # ── 2. RESAMPLE 1-MIN → 15-MIN ────────────────────────────────────────
        df15 = to_Nmin(df1, CANDLE_MIN)
        if df15.empty:
            return None, "FAILED: empty after resample to 15-min"

        # ── 3. PREVIOUS DAY CLOSE ─────────────────────────────────────────────
        # Try up to 5 previous trading days — uses the first one with data.
        # Handles stocks with data gaps (newly listed, circuit halt, etc.)
        prev_close = None
        prev_close = None
        for _pd in prev_trading_days(scan_date, 5):
            _df_prev = df1[df1.index.date == _pd]
            if not _df_prev.empty:
                _pc = float(_df_prev['Close'].iloc[-1])
                if _pc > 0:
                    prev_close = _pc
                    prev_close = _pc
                    break
        if prev_close is None:
            return None, "FAILED: no valid prev close in last 5 trading days"

        if prev_close <= 0:
            return None, "FAILED: prev close invalid"

        # ── 4. TODAY'S 15-MIN CANDLES ─────────────────────────────────────────
        df15_today = df15[df15.index.date == scan_date].copy()

        # ── STRICT CUTOFF FILTER ─────────────────────────────────────────────
        # CRITICAL FIX: Alice Blue's API often ignores to_datetime and returns
        # ALL intraday data up to the current time — not just up to the cutoff.
        # Without this filter:
        #   Run at 11:03 AM → uses 8 candles (9:15–11:00)  ← correct
        #   Run at  2:30 PM → uses 24 candles (9:15–14:30) ← WRONG
        # This is why selecting "11:00 AM cutoff" gave different results
        # depending on what time you ran the script.
        #
        # Fix: after resampling, drop any 15-min candle whose FULL period has
        # not yet completed before the selected cutoff time.
        # A candle at time T covers T → T+CANDLE_MIN.
        # Include it when T <= cutoff_dt (i.e. it has STARTED by cutoff).
        # "11:00 AM (8 candles)" = 8 candles that START by 11:00 AM.
        # e.g. cutoff 11:00 AM:
        #   candle at 10:45 → ends 11:00 → INCLUDED  ✅
        #   candle at 11:00 → ends 11:15 → EXCLUDED  ❌
        _idx = df15_today.index
        if _idx.tzinfo is None:
            _idx = _idx.tz_localize('Asia/Kolkata')
        else:
            _idx = _idx.tz_convert('Asia/Kolkata')
        df15_today = df15_today[
            # Strict filter: only fully-complete candles.
            # A candle at T covers T → T+15 min; include only when T+15 ≤ cutoff_dt.
            # Because cutoff_dt is +15 min from display label (e.g. "11:00 AM" label
            # → cutoff_dt = 11:15), the 11:00 candle (ends 11:15 ≤ 11:15) IS included
            # and is ALWAYS fully formed before being used.
            # This makes Live mode == Historical mode for same date and cutoff.
            (_idx + pd.Timedelta(minutes=CANDLE_MIN)) <= cutoff_dt
        ]
        del _idx   # clean up
        if len(df15_today) < MIN_CANDLES:
            return None, (
                f"FAILED: only {len(df15_today)} × {CANDLE_MIN}-min candles today "
                f"(need ≥{MIN_CANDLES})"
            )

        curr_price     = float(df15_today['Close'].iloc[-1])
        total_move_pct = (curr_price - prev_close) / prev_close * 100

        # ── 5. SAME-WINDOW RVOL (Condition 4 upgrade) ────────────────────────
        # Compare today's cumulative volume to the SAME N candles on past 5 days.
        # Avoids the time-of-day volume bias in v1's time-fraction approach.
        n_today   = len(df15_today)
        today_vol = float(df15_today['Volume'].sum())

        hist_vols = []
        for hd in prev_trading_days(scan_date, RVOL_LOOKBACK):
            dh = df15[df15.index.date == hd]
            if len(dh) >= n_today:
                # Use same number of candles as today (apples-to-apples)
                same_window_vol = float(dh.iloc[:n_today]['Volume'].sum())
                if same_window_vol > 0:
                    hist_vols.append(same_window_vol)

        if hist_vols:
            avg_hist_vol = float(np.mean(hist_vols))
            rvol = today_vol / avg_hist_vol if avg_hist_vol > 0 else None
        else:
            rvol = None

        # ── 6. COMPUTE MRTQ v2 METRICS ───────────────────────────────────────
        m = compute_mrtq_metrics(df15_today)
        if m is None:
            return None, "FAILED: insufficient candles for metric computation"

        # ── 6b. TREND MATURITY SCORE (needs historical df15) ────────────────
        # Compares today's move to the stock's own typical move in the same
        # time window over the last 5 trading days.
        # Rationale: a stock moving 3× its historical average by 11 AM is
        # statistically more likely to consolidate or reverse by EOD.
        # Higher TMS = NOT overextended = more room to continue.
        _hist_moves = []
        for _hd in prev_trading_days(scan_date, RVOL_LOOKBACK):
            _dh = df15[df15.index.date == _hd]
            if len(_dh) >= n_today:
                _w = _dh.iloc[:n_today]
                _open = float(_w['Open'].iloc[0])
                if _open > 0:
                    _hist_move = abs(float(_w['Close'].iloc[-1]) - _open) / _open * 100
                    _hist_moves.append(_hist_move)
        if _hist_moves:
            _avg_hist = max(float(np.mean(_hist_moves)), 0.1)  # floor avoids div/0
            _maturity_ratio = abs(total_move_pct) / _avg_hist
            # Linear: ratio≤1 → TMS=1.0 | ratio=2 → TMS=0.5 | ratio≥3 → TMS=0.0
            _tms = max(0.0, 1.0 - max(0.0, _maturity_ratio - 1.0) / 2.0)
        else:
            _tms = 0.5   # neutral if no history

        m['tms'] = round(_tms, 3)   # inject into metrics dict for scoring

        # ── 6c. ATR% + PRICE ACCELERATION ───────────────────────────────────────
        # ATR%: typical daily range from 5 prior days — used to judge whether
        # today's move is moderate (room to run) or extreme (exhaustion risk).
        # price_accel: is momentum building or fading in recent candles?
        m['atr_pct']         = _compute_atr_pct(df15, scan_date)
        closes_arr           = df15_today['Close'].values.astype(float)
        m['price_accel_bull'] = round(_price_accel(closes_arr, 'bull'), 3)
        m['price_accel_bear'] = round(_price_accel(closes_arr, 'bear'), 3)
        m['relative_strength'] = 0.5   # placeholder; updated post-scan by run_scan()


        # ── 7. QUALIFY ────────────────────────────────────────────────────────
        bull_ok, bull_reasons = qualify_bull(m, total_move_pct, rvol)
        bear_ok, bear_reasons = qualify_bear(m, total_move_pct, rvol)

        if bull_ok:
            qualified = 'BULL'
        elif bear_ok:
            qualified = 'BEAR'
        else:
            qualified = None

        # ── 8. EOD CONTINUATION SCORE ─────────────────────────────────────────
        if qualified == 'BULL':
            score = eod_score_bull(m, total_move_pct, rvol)
        elif qualified == 'BEAR':
            score = eod_score_bear(m, total_move_pct, rvol)
        else:
            # Compute for diagnostics (show score even for non-qualifiers)
            if total_move_pct >= 0:
                score = eod_score_bull(m, total_move_pct, rvol)
            else:
                score = eod_score_bear(m, total_move_pct, rvol)

        # ── 9. ENTRY / SL / TARGET (last candle = entry candle at cutoff) ─────
        entry_cdl = df15_today.iloc[-1]
        ec_high   = float(entry_cdl['High'])
        ec_low    = float(entry_cdl['Low'])

        entry = sl = target = risk = None
        if qualified == 'BULL':
            entry = round(ec_high, 2)
            sl    = round(ec_low,  2)
        elif qualified == 'BEAR':
            entry = round(ec_low,  2)
            sl    = round(ec_high, 2)

        if entry is not None and sl is not None:
            risk = abs(entry - sl)
            if risk < entry * 0.001:
                risk = entry * 0.003
            if qualified == 'BULL':
                target = round(entry + RR_RATIO * risk, 2)
            else:
                target = round(entry - RR_RATIO * risk, 2)

        # ── 10. CANDLE DISPLAY STRING ─────────────────────────────────────────
        candle_str = ''
        for i in range(len(df15_today)):
            cdl = df15_today.iloc[i]
            o, c = float(cdl['Open']), float(cdl['Close'])
            t    = df15_today.index[i].strftime('%H:%M')
            candle_str += f"{'🟢' if c >= o else '🔴'}{t} "

        n_bull_cond = 6 - len(bull_reasons)
        n_bear_cond = 6 - len(bear_reasons)
        tag = (
            f"QUALIFIED {qualified} (Score:{score})" if qualified
            else f"no qualify (bull:{n_bull_cond}/6, bear:{n_bear_cond}/6)"
        )

        result = {
            'Symbol'         : sym,
            'Qualified'      : qualified,
            'Signal'         : ('🟢 BUY'   if qualified == 'BULL' else
                                '🔴 SHORT' if qualified == 'BEAR' else '⚪ —'),
            'EODScore'       : score,
            'TotalMove%'     : round(total_move_pct, 2),
            'CurrPrice'      : round(curr_price, 2),
            'PrevClose'      : round(prev_close, 2),
            # Condition 2
            'GreenCount'     : m['green_count'],
            'RedCount'       : m['red_count'],
            'BullBodyStr'    : round(m['bull_body_str'], 2),
            'BearBodyStr'    : round(m['bear_body_str'], 2),
            # Condition 3
            'TER_Bull'       : round(m['ter_bull'], 2),
            'TER_Bear'       : round(m['ter_bear'], 2),
            # Condition 4
            'VolRatio'       : round(rvol, 2) if rvol is not None else None,
            # Condition 5
            'MomBull'        : round(m['momentum_bull'], 2),
            'MomBear'        : round(m['momentum_bear'], 2),
            # Condition 6
            'ShadowBull'     : round(m['bull_shadow_ratio'], 2),
            'ShadowBear'     : round(m['bear_shadow_ratio'], 2),
            # Candles
            'NCandles'       : m['n_candles'],
            'Candles'        : candle_str.strip(),
            # Entry
            'Entry'          : entry,
            'SL'             : sl,
            'Target'         : target,
            'Risk'           : round(risk, 2) if risk is not None else None,
            'EntryCandle'    : f"H:{round(ec_high,2)} / L:{round(ec_low,2)}",
            # New MRTQ v2+ metrics (for display and transparency)
            'PBQ_Bull'       : round(m.get('pbq_bull', 0.5), 2),
            'PBQ_Bear'       : round(m.get('pbq_bear', 0.5), 2),
            'LRS_Bull'       : round(m.get('lrs_bull', 0.0), 2),
            'LRS_Bear'       : round(m.get('lrs_bear', 0.0), 2),
            'TMS'            : round(m.get('tms', 0.5), 2),
            # Diagnostics
            'BullReasons'    : bull_reasons,
            'BearReasons'    : bear_reasons,
            'BullScore'      : n_bull_cond,
            'BearScore'      : n_bear_cond,
        }

        return result, f"OK ({total_move_pct:+.2f}%, {tag})"

    except Exception as e:
        if DEBUG:
            import traceback
            print(f"[SCAN] {sym}: {e}\n{traceback.format_exc()}")
        return None, f"FAILED: unexpected error: {e}"

# =============================================================================
# PARALLEL FULL SCAN
# =============================================================================
def run_scan(stocks, scan_date, cutoff_h, cutoff_m, trade, status_cb=None):
    tz        = pytz.timezone('Asia/Kolkata')
    cutoff_dt = tz.localize(datetime.combine(
        scan_date, dt_time(cutoff_h, cutoff_m)
    ))

    # ── PERSISTENT INSTRUMENT CACHE ──────────────────────────────────────────
    # cache is stored in st.session_state so it SURVIVES across re-runs.
    # Root cause of 142→45 inconsistency: cache was re-created empty on each run.
    # First run now populates the cache. Every subsequent run in the same
    # browser session reuses all resolved instruments instantly — zero API calls
    # needed for instrument lookup on the second scan.
    import streamlit as _st
    if 'inst_cache' not in _st.session_state:
        _st.session_state['inst_cache'] = {}
    cache = _st.session_state['inst_cache']   # shared, persistent reference

    cache_lock = threading.Lock()
    done_lock  = threading.Lock()
    all_res    = []
    diag       = {}
    done_ctr   = [0]
    total      = len(stocks)

    # ── PRE-FETCH INSTRUMENT MASTER ───────────────────────────────────────────
    # Only attempt master download if it hasn't been done yet this session
    # (or if the last attempt returned 0 instruments).
    master_already_loaded = bool(cache.get('__MASTER__'))
    if not master_already_loaded:
        if status_cb:
            status_cb(0, total, "⏳ Loading Alice Blue instrument master…")
        n_master = prefetch_instrument_master(trade, cache, cache_lock)
        if status_cb:
            label = (f"✅ Instrument master: {n_master} instruments loaded"
                     if n_master > 0
                     else "⚠️ Master unavailable — using cached per-symbol lookup")
            status_cb(0, total, label)
    else:
        n_master = len(cache.get('__MASTER__', {}))
        if status_cb:
            n_cached = sum(1 for k in cache if k != '__MASTER__')
            status_cb(0, total,
                      f"✅ Using session cache: {n_cached} instruments + "
                      f"{n_master} master entries")

    def _proc(sym):
        r, status = scan_one(sym, scan_date, cutoff_dt, trade, cache, cache_lock)
        return sym, r, status

    workers = MAX_WORKERS   # full parallelism — per-call timeout guards each request

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_proc, s): s for s in stocks}
        for f in as_completed(futs):
            sym_done = futs[f]
            try:
                sym_done, r, status = f.result()
                diag[sym_done] = status
                if r:
                    all_res.append(r)
            except Exception as e:
                diag[sym_done] = f"FAILED: thread error: {e}"
            finally:
                with done_lock:
                    done_ctr[0] += 1
                if status_cb:
                    status_cb(done_ctr[0], total, sym_done)


    # ── Relative Strength Rank (post-scan) ───────────────────────────────────
    # After all stocks are collected, rank each stock's move vs the full universe.
    # Evidence: extreme movers (top 5%) often reverse; stocks at 50th-80th
    # percentile have the best EOD continuation probability.
    if all_res:
        all_abs_moves = [abs(r.get('TotalMove%', 0)) for r in all_res]
        n_total = len(all_abs_moves)
        for r in all_res:
            this_move = abs(r.get('TotalMove%', 0))
            rank_pct  = sum(1 for mv in all_abs_moves if mv <= this_move) / n_total
            # Optimal: 40th-80th percentile (strong but not overextended)
            if rank_pct < 0.25:
                relstr = rank_pct / 0.25 * 0.30
            elif rank_pct <= 0.80:
                relstr = 0.30 + (rank_pct - 0.25) / 0.55 * 0.70
            else:
                relstr = max(0.0, 1.0 - (rank_pct - 0.80) / 0.20)
            r['relative_strength'] = round(relstr, 3)
            # Recompute EOD score with relative_strength now populated
            direction = r.get('direction', r.get('Direction', 'bull'))
            mv  = r.get('TotalMove%', 0)
            rv  = r.get('rvol', r.get('RVOL', 1.0))
            try:
                if direction == 'bull':
                    r['EODScore'] = eod_score_bull(r, mv, rv)
                else:
                    r['EODScore'] = eod_score_bear(r, mv, rv)
            except Exception:
                pass   # keep original score if recompute fails

    # ── Sort: primary by EOD Score (higher=better), secondary by Move% ────────
    top_bull = sorted(
        [r for r in all_res if r['Qualified'] == 'BULL'],
        key=lambda r: (r['EODScore'], r['TotalMove%']), reverse=True
    )
    top_bear = sorted(
        [r for r in all_res if r['Qualified'] == 'BEAR'],
        key=lambda r: (r['EODScore'], -r['TotalMove%']), reverse=True
    )

    return top_bull, top_bear, all_res, diag

# =============================================================================
# BACKTEST VERIFICATION  (same logic as reference)
# =============================================================================
def verify_backtest(bull, bear, scan_date, trade, cache, cache_lock):
    tz = pytz.timezone('Asia/Kolkata')
    fd = tz.localize(datetime.combine(scan_date - timedelta(days=2), dt_time(9, 0)))
    td = tz.localize(datetime.combine(scan_date,                     dt_time(15, 35)))

    def eod_close(sym):
        df1, _ = fetch_1min(sym, fd, td, trade, cache, cache_lock)
        if df1 is not None and not df1.empty:
            df1 = df1[df1.index.date == scan_date]
        if df1 is None or df1.empty:
            return None
        return float(df1['Close'].iloc[-1])

    stats = dict(bw=0, bl=0, sw=0, sl=0, total=0)

    for r in bull:
        if r['Entry'] is None:
            r['EOD'] = None; r['Result'] = '—'; continue
        eod = eod_close(r['Symbol'])
        if eod is not None:
            r['EOD'] = round(eod, 2)
            chg = (eod - r['Entry']) / r['Entry'] * 100
            if eod >= r['Target']:
                r['Result'] = '✅ Target Hit'; stats['bw'] += 1
            elif eod <= r['SL']:
                r['Result'] = '❌ SL Hit';     stats['bl'] += 1
            elif eod > r['Entry']:
                r['Result'] = f'🟡 +{chg:.1f}% (3PM)'; stats['bw'] += 1
            else:
                r['Result'] = f'❌ {chg:.1f}% (3PM)';  stats['bl'] += 1
        else:
            r['EOD'] = None; r['Result'] = '❓ No Data'
        stats['total'] += 1

    for r in bear:
        if r['Entry'] is None:
            r['EOD'] = None; r['Result'] = '—'; continue
        eod = eod_close(r['Symbol'])
        if eod is not None:
            r['EOD'] = round(eod, 2)
            chg = (r['Entry'] - eod) / r['Entry'] * 100
            if eod <= r['Target']:
                r['Result'] = '✅ Target Hit'; stats['sw'] += 1
            elif eod >= r['SL']:
                r['Result'] = '❌ SL Hit';     stats['sl'] += 1
            elif eod < r['Entry']:
                r['Result'] = f'🟡 +{chg:.1f}% (3PM)'; stats['sw'] += 1
            else:
                r['Result'] = f'❌ {chg:.1f}% (3PM)';  stats['sl'] += 1
        else:
            r['EOD'] = None; r['Result'] = '❓ No Data'
        stats['total'] += 1

    w = stats['bw'] + stats['sw']
    t = stats['total']
    stats['win_rate'] = round(w / t * 100, 1) if t > 0 else 0.0
    return bull, bear, stats

# =============================================================================
# HTML RESULT TABLE
# =============================================================================
def build_table(results, is_bull, is_backtest=False, show_diag=False):
    if not results:
        return "<p style='padding:12px 0;'>No stocks in this list.</p>"

    hdr_bg = '#1B5E20' if is_bull else '#B71C1C'
    row_bg = '#F1F8E9' if is_bull else '#FCE4EC'
    row_fg = '#1B5E20' if is_bull else '#880E4F'

    def th(txt):
        return (f'<th style="padding:8px 10px;border:1px solid #999;'
                f'background:{hdr_bg};color:#fff;white-space:nowrap;">{txt}</th>')

    def td(val, extra=''):
        return f'<td style="padding:8px 10px;border:1px solid #ddd;{extra}">{val}</td>'

    bt_th = (th('EOD Close') + th('Result')) if is_backtest else ''
    diag_th = (th('Bull/6') + th('Bear/6') + th('Fail Reasons')) if show_diag else ''

    header = (
        th('#') + th('Symbol') + th('Signal') +
        th('EOD Score') + th('Move %') + th('Price ₹') +
        th('RVOL') + th('Green/Red') + th('Body Str') +
        th('TER') + th('Momentum') + th('Shadow') +
        th('Entry ₹') + th('SL ₹') + th('Target ₹') +
        th('Candles') +
        diag_th + bt_th
    )

    rows = []
    for i, r in enumerate(results, 1):
        sym = r['Symbol'].replace('.NS','')

        # Score colour: green ≥ 65, amber ≥ 45, red < 45
        sc = r['EODScore']
        sc_col = '#00C853' if sc >= 65 else ('#FF8F00' if sc >= 45 else '#D50000')
        sc_str = f'<b style="color:{sc_col}">{sc}</b>'

        tm     = r['TotalMove%']
        tm_col = '#00C853' if tm > 0 else '#D50000'
        tm_str = f'<b style="color:{tm_col}">{tm:+.2f}%</b>'

        vr = r.get('VolRatio')
        if vr is None:
            vr_str = '—'
        else:
            vr_col = '#00C853' if vr >= 2.0 else ('#FF8F00' if vr >= MIN_RVOL else '#D50000')
            vr_str = f'<b style="color:{vr_col}">{vr:.2f}×</b>'

        gr_str = f"{r['GreenCount']}🟢 / {r['RedCount']}🔴"

        bs = r['BullBodyStr'] if is_bull else r['BearBodyStr']
        bs_col = '#00C853' if bs >= 0.55 else ('#FF8F00' if bs >= MIN_BODY_STR else '#D50000')
        bs_str = f'<span style="color:{bs_col}">{bs:.2f}</span>'

        ter = r['TER_Bull'] if is_bull else r['TER_Bear']
        ter_col = '#00C853' if ter >= 0.65 else ('#FF8F00' if ter >= MIN_TER else '#D50000')
        ter_str = f'<span style="color:{ter_col}">{ter:.2f}</span>'

        mom = r['MomBull'] if is_bull else r['MomBear']
        mom_col = '#00C853' if mom >= 0.8 else ('#FF8F00' if mom >= MIN_MOMENTUM else '#D50000')
        mom_str = f'<span style="color:{mom_col}">{mom:.2f}</span>'

        shd = r['ShadowBull'] if is_bull else r['ShadowBear']
        shd_col = '#00C853' if shd <= 0.5 else ('#FF8F00' if shd <= MAX_SHADOW_RATIO else '#D50000')
        shd_str = f'<span style="color:{shd_col}">{shd:.2f}</span>'

        entry_str  = f'₹{r["Entry"]}'  if r.get("Entry")  else '—'
        sl_str     = f'₹{r["SL"]}'     if r.get("SL")     else '—'
        target_str = f'₹{r["Target"]}' if r.get("Target") else '—'

        bt_td = ''
        if is_backtest:
            eod = r.get('EOD', '—')
            res = r.get('Result', '—')
            rc  = ('#00C853' if '✅' in res else '#D50000' if '❌' in res else '#FF8F00')
            bt_td = (
                td(eod if eod else '—') +
                td(res, f'color:{rc};font-weight:bold;')
            )

        diag_td = ''
        if show_diag:
            bc  = r.get('BullScore', 0)
            brc = r.get('BearScore', 0)
            bcol = '#00C853' if bc == 6 else ('#FF8F00' if bc >= 4 else '#999')
            brcol= '#00C853' if brc== 6 else ('#FF8F00' if brc>= 4 else '#999')
            # Show the closer side's reasons
            if bc >= brc:
                reasons = r.get('BullReasons', []) or ['✅ all 6 pass']
                side = 'Bull'
            else:
                reasons = r.get('BearReasons', []) or ['✅ all 6 pass']
                side = 'Bear'
            reasons_str = '<br>'.join(reasons)
            diag_td = (
                td(f'<b style="color:{bcol}">{bc}/6</b>') +
                td(f'<b style="color:{brcol}">{brc}/6</b>') +
                td(f'<i style="font-size:10px;">{side}: {reasons_str}</i>',
                   'max-width:280px;')
            )

        rows.append(
            f'<tr style="background:{row_bg};color:{row_fg};">'
            + td(i,               'font-weight:900;')
            + td(sym,             'font-weight:900;font-size:14px;')
            + td(r['Signal'])
            + td(sc_str,          'font-size:13px;')
            + td(tm_str,          'font-size:13px;')
            + td(f'₹{r["CurrPrice"]}')
            + td(vr_str)
            + td(gr_str)
            + td(bs_str)
            + td(ter_str)
            + td(mom_str)
            + td(shd_str)
            + td(entry_str)
            + td(sl_str,          'color:#D50000;font-weight:bold;')
            + td(target_str,      'color:#00C853;font-weight:bold;')
            + td(f'<span style="font-size:11px;">{r.get("Candles","")}</span>')
            + diag_td + bt_td
            + '</tr>'
        )

    table = (
        '<div style="overflow-x:auto;">'
        '<table style="border-collapse:collapse;width:100%;font-size:12px;">'
        f'<thead><tr>{header}</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table></div>'
    )
    return table

# =============================================================================
# STOCK UNIVERSE
# =============================================================================
# NSE F&O stocks with active options chain (source: NSE website, July 2026)
DEFAULT_STOCKS = [
    "360ONE","ABB","APLAPOLLO","AUBANK","ADANIENSOL","ADANIENT",
    "ADANIGREEN","ADANIPORTS","ADANIPOWER","ABCAPITAL","ALKEM",
    "AMBER","AMBUJACEM","ANGELONE","APOLLOHOSP","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUROPHARMA","DMART","AXISBANK","BSE",
    "BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BAJAJHLDNG",
    "BANDHANBNK","BANKBARODA","BANKINDIA","BDL","BEL",
    "BHARATFORG","BHEL","BPCL","BHARTIARTL","BIOCON",
    "BLUESTARCO","BOSCHLTD","BRITANNIA","CGPOWER","CANBK",
    "CDSL","CHOLAFIN","CIPLA","COALINDIA","COCHINSHIP",
    "COFORGE","COLPAL","CAMS","CONCOR","CROMPTON",
    "CUMMINSIND","DLF","DABUR","DALBHARAT","DELHIVERY",
    "DIVISLAB","DIXON","DRREDDY","ETERNAL","EICHERMOT",
    "EXIDEIND","FORCEMOT","NYKAA","FORTIS","GAIL",
    "GVT&D","GMRAIRPORT","GLENMARK","GODFRYPHLP","GODREJCP",
    "GODREJPROP","GRASIM","HCLTECH","HDFCAMC","HDFCBANK",
    "HDFCLIFE","HAVELLS","HEROMOTOCO","HINDALCO","HAL",
    "HINDPETRO","HINDUNILVR","HINDZINC","POWERINDIA","HYUNDAI",
    "ICICIBANK","ICICIGI","ICICIPRULI","IDFCFIRSTB","ITC",
    "INDIANB","IEX","IOC","IRFC","IREDA",
    "INDUSTOWER","INDUSINDBK","NAUKRI","INFY","INOXWIND",
    "INDIGO","JINDALSTEL","JSWENERGY","JSWSTEEL","JIOFIN",
    "JUBLFOOD","KEI","KPITTECH","KALYANKJIL","KAYNES",
    "KFINTECH","KOTAKBANK","LTF","LICHSGFIN","LTM",
    "LT","LAURUSLABS","LICI","LODHA","LUPIN",
    "M&M","MANAPPURAM","MANKIND","MARICO","MARUTI",
    "MFSL","MAXHEALTH","MAZDOCK","MOTILALOFS","MPHASIS",
    "MCX","MUTHOOTFIN","NBCC","NHPC","NMDC",
    "NTPC","NATIONALUM","NESTLEIND","NAM-INDIA","NUVAMA",
    "OBEROIRLTY","ONGC","OIL","PAYTM","OFSS",
    "POLICYBZR","PGEL","PIIND","PNBHOUSING","PAGEIND",
    "PATANJALI","PERSISTENT","PETRONET","PIDILITIND","POLYCAB",
    "PFC","POWERGRID","PREMIERENE","PRESTIGE","PNB",
    "RBLBANK","RECLTD","RADICO","RVNL","RELIANCE",
    "SBICARD","SBILIFE","SHREECEM","SRF","MOTHERSON",
    "SHRIRAMFIN","SIEMENS","SOLARINDS","SONACOMS","SBIN",
    "SAIL","SUNPHARMA","SUPREMEIND","SUZLON","SWIGGY",
    "TATACONSUM","TVSMOTOR","TCS","TATAELXSI","TMPV",
    "TATAPOWER","TATASTEEL","TECHM","FEDERALBNK","INDHOTEL",
    "PHOENIXLTD","TITAN","TORNTPHARM","TRENT","TIINDIA",
    "UNOMINDA","UPL","ULTRACEMCO","UNIONBANK","UNITDSPR",
    "VBL","VEDL","VMM","IDEA","VOLTAS",
    "WAAREEENER","WIPRO","YESBANK","ZYDUSLIFE",
]

# =============================================================================
# STREAMLIT UI
# =============================================================================
def main():
    st.set_page_config(
        page_title="NSE F&O Momentum Scanner v2",
        page_icon="📈",
        layout="wide",
    )

    # ── Session state ─────────────────────────────────────────────────────────
    for k, v in [('connected', False), ('trade', None),
                 ('results', None), ('bt_stats', None)]:
        st.session_state.setdefault(k, v)

    # Auto-load saved credentials — MUST run before setdefault('uid','')
    # Bug fix: setdefault is a no-op if the key already exists.
    # If we setdefault('uid','') first, the saved value never loads.
    if 'uid' not in st.session_state:
        loaded_uid, loaded_auth, loaded_skey = '', '', ''
        if os.path.exists(CREDS_FILE):
            try:
                with open(CREDS_FILE) as f:
                    c = json.load(f)
                loaded_uid  = c.get('user_id',   '')
                loaded_auth = c.get('auth_code',  '')
                loaded_skey = c.get('secret_key', '')
            except Exception:
                pass
        st.session_state['uid']  = loaded_uid
        st.session_state['auth'] = loaded_auth
        st.session_state['skey'] = loaded_skey

    st.title("📈 NSE F&O Momentum Scanner v2")
    st.caption("MRTQ Enhanced — Top Gainer Bullish / Top Loser Bearish | EOD Trend Continuation")

    # ── LOGIN ─────────────────────────────────────────────────────────────────
    with st.expander("🔐 Alice Blue Login", expanded=not st.session_state['connected']):
        c1, c2, c3 = st.columns(3)
        uid  = c1.text_input("User ID",    value=st.session_state['uid'])
        auth = c2.text_input("Auth Code",  value=st.session_state['auth'])
        skey = c3.text_input("Secret Key", value=st.session_state['skey'], type="password")

        b1, _, b2, b3 = st.columns([3, 2, 1, 1])
        ph = st.empty()

        if b1.button("🔌 Connect", width='stretch'):
            if not (uid and auth and skey):
                ph.error("All three fields are required.")
            else:
                ok = False
                for fn in [
                    lambda: TradeHub(user_id=uid, auth_code=auth,  secret_key=skey),
                    lambda: TradeHub(user_id=uid, auth_code=skey,  secret_key=auth),
                    lambda: TradeHub(uid, auth, skey),
                ]:
                    try:
                        t = fn()
                        s = t.get_session_id()
                        if s and 'Not_ok' not in str(s):
                            st.session_state.update(
                                trade=t, connected=True, uid=uid, auth=auth, skey=skey
                            )
                            ph.success("✅ Connected to Alice Blue!")
                            try:
                                with open(CREDS_FILE, 'w') as f:
                                    json.dump(dict(user_id=uid, auth_code=auth,
                                                   secret_key=skey), f)
                            except Exception:
                                pass
                            ok = True
                            break
                    except Exception:
                        continue
                if not ok:
                    ph.error("❌ Authentication failed. Check your credentials.")

        if b2.button("💾 Save", width='stretch'):
            try:
                with open(CREDS_FILE, 'w') as f:
                    json.dump(dict(user_id=uid, auth_code=auth, secret_key=skey), f)
                st.success("Saved!")
            except Exception as e:
                st.error(str(e))

        if b3.button("🗑️", width='stretch'):
            try:
                os.remove(CREDS_FILE); st.info("Credentials cleared.")
            except Exception:
                pass

        if st.session_state['connected']:
            ph.success("✅ Alice Blue: Connected")

    # ── STRATEGY EXPLANATION ──────────────────────────────────────────────────
    with st.expander("📖 MRTQ v2 Strategy — What Changed and Why", expanded=False):
        st.markdown(f"""
**MRTQ v2 keeps the same 4-condition structure but makes each condition smarter,
then adds 2 new conditions and a composite EOD Score for better ranking.**

---

| # | v1 Condition | v2 Improvement | Why It's Better |
|---|---|---|---|
| 1 | Move % from Prev Close | **KEPT** (unchanged) | Primary signal — biggest movers at 11AM stay strong |
| 2 | Candle Direction % | **→ Body Strength Score** | Counts candle QUALITY (body/range ratio), not just colour. A 10%-body green candle is near-doji (indecision). A 90%-body green candle is strong conviction. |
| 3 | Trend Persistence % | **→ Trend Efficiency Ratio** | Measures how STRAIGHT the move is: net_move ÷ total_path. TER=1.0 = perfect straight line. Choppy back-and-forth scores low even if mostly green. |
| 4 | Relative Volume | **→ Same-Window RVOL** | Compares today's first-N-candles volume to the SAME N candles from past {RVOL_LOOKBACK} days. Eliminates morning-rush volume bias in the old time-fraction method. |
| 5 | *(new)* | **Momentum State** | Second-half move ≥ {MIN_MOMENTUM}× first-half move. Rejects stocks that spiked at open and are now stalling — they rarely continue to EOD. |
| 6 | *(new)* | **Shadow Quality** | Rejects stocks where opposing wicks exceed {MAX_SHADOW_RATIO}× avg body. Long upper wicks in bullish stocks = sellers rejecting highs = likely EOD reversal. |

---

**EOD Continuation Score (0–100) — ranking formula:**

| Component | Weight | Rationale |
|---|---|---|
| Move % magnitude | {W_MOVE} pts | Strong early movers tend to stay top gainers/losers |
| Trend Efficiency Ratio | {W_TER} pts | Clean trends continue; choppy moves reverse |
| Same-Window RVOL | {W_RVOL} pts | Institutional volume is the engine that sustains trends |
| Candle Body Strength | {W_BODY} pts | Conviction candles signal committed buyers/sellers |
| Momentum State | {W_MOMEN} pts | Sustained pressure = EOD continuation |

**Stocks are ranked by EOD Score, not just Move%.**
Two stocks with the same Move% will be ranked differently if one has cleaner
trend quality — giving you the highest-probability trade at Rank 1.
        """)

    # ── SETTINGS ─────────────────────────────────────────────────────────────
    st.divider()
    st.markdown("### ⚙️ Settings")
    c1, c2, c3 = st.columns(3)

    with c1:
        mode    = st.radio("Mode", ["🔴 Live", "📅 Historical"], horizontal=True)
        is_live = mode == "🔴 Live"

    scan_date_input = None
    with c2:
        if not is_live:
            scan_date_input = st.date_input(
                "Historical Date", value=last_trading_day()
            )
        else:
            tz  = pytz.timezone('Asia/Kolkata')
            now = datetime.now(tz)
            st.info(f"🕐 **{now.strftime('%H:%M:%S')}**  |  {now.strftime('%d-%b-%Y')}")
            if not is_trading_day(now.date()):
                st.warning("📅 Market holiday today.")
            elif now.time() < dt_time(10, 30):
                st.warning("⏰ Wait until at least 10:30 AM before scanning.")
            else:
                st.success("✅ Ready to scan!")

    with c3:
        cutoff_opt = st.radio(
            "Data Cutoff",
            ["11:00 AM  (8 candles)", "11:30 AM  (10 candles)"],
            horizontal=True,
            help=(
                "Only 15-min candles completed before this time are used. "
                "Entry candle = the last candle at cutoff. No look-ahead."
            ),
        )
        # Internal cutoff = displayed time + 15 min.
        # This ensures the last named candle is FULLY complete before inclusion.
        # "11:00 AM (8 candles)" → internal cutoff = 11:15 AM
        #   → the 11:00–11:15 candle is only included after 11:15 AM (fully formed)
        # "11:30 AM (10 candles)" → internal cutoff = 11:45 AM
        # Both Live and Historical now use the same set of complete candles.
        if "11:00" in cutoff_opt:
            cutoff_h, cutoff_m = 11, 15   # internal: 11:15 AM
        else:
            cutoff_h, cutoff_m = 11, 45   # internal: 11:45 AM

    # ── LIVE MODE SETTLEMENT WARNING ─────────────────────────────────────────
    # Shown after both c2 and c3 so cutoff_h/cutoff_m are already defined.
    if is_live:
        try:
            _tz  = pytz.timezone("Asia/Kolkata")
            _now = datetime.now(_tz)
            _internal = dt_time(cutoff_h, cutoff_m)
            if _now.time() < _internal and is_trading_day(_now.date()):
                _disp_m = (cutoff_m - 15) % 60
                _disp_h = cutoff_h if cutoff_m >= 15 else cutoff_h - 1
                st.warning(
                    f"⏰ **Run after {_disp_h:02d}:{_disp_m:02d} AM** for the selected cutoff. "
                    f"The last candle is still forming — "
                    f"scanning now may give fewer candles than Historical mode. "
                    f"Wait until **{cutoff_h:02d}:{cutoff_m:02d} AM** for Live = Historical."
                )
        except Exception:
            pass

    # ── STOCK LIST ────────────────────────────────────────────────────────────
    st.markdown("### 📋 Stocks to Scan")
    stocks_txt = st.text_area(
        "Stocks (one per line, NSE symbols)",
        value="\n".join(DEFAULT_STOCKS),
        height=80,
    )
    stocks = [s.strip().upper() for s in stocks_txt.split('\n') if s.strip()]
    st.caption(
        f"**{len(stocks)}** stocks in scan universe. "
        f"Each stock fetches {FETCH_LOOKBACK_DAYS} calendar days of 1-min data → "
        f"resampled to 15-min for all MRTQ v2 metrics."
    )

    st.divider()
    run_btn   = st.button("▶️ RUN SCAN", width='stretch', type="primary")
    result_ph = st.empty()

    # ── RUN SCAN ──────────────────────────────────────────────────────────────
    if run_btn:
        if not st.session_state['connected'] or not st.session_state['trade']:
            st.error("❌ Connect to Alice Blue first (expand the Login section above).")
        else:
            trade       = st.session_state['trade']
            holiday_msg = None

            if is_live:
                today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
                if is_trading_day(today):
                    scan_date = today
                else:
                    scan_date   = last_trading_day(today)
                    holiday_msg = (f"📅 Today is a holiday. "
                                   f"Scanning {scan_date.strftime('%d-%b-%Y')} instead.")
            else:
                scan_date = scan_date_input
                if not is_trading_day(scan_date):
                    holiday_msg = (f"⚠️ {scan_date.strftime('%d-%b-%Y')} "
                                   f"may be a holiday — data may be limited.")

            prog = st.progress(0)
            ptxt = st.empty()

            _scan_t0 = [time.time()]   # mutable so the closure can read it

            def on_progress(done, total, sym):
                if done == 0:
                    # Master-load phase (before actual scan starts)
                    prog.progress(0)
                    ptxt.text(str(sym))
                    _scan_t0[0] = time.time()   # reset timer when scan actually begins
                else:
                    pct = int(done / total * 100)
                    prog.progress(pct)
                    elapsed = time.time() - _scan_t0[0]
                    if done > 2 and elapsed > 0:
                        rate     = done / elapsed           # stocks/second
                        remain   = (total - done) / rate    # seconds left
                        eta_str  = (f"{int(remain)}s" if remain < 90
                                    else f"{int(remain/60)}m {int(remain%60)}s")
                        ptxt.text(
                            f"⏳ {done}/{total}  ({pct}%)  "
                            f"← {sym}   "
                            f"· ETA {eta_str}"
                        )
                    else:
                        ptxt.text(f"⏳ {done}/{total}  ({pct}%)  ← {sym}")

            with st.spinner(f"Scanning {len(stocks)} stocks with MRTQ v2…"):
                top_bull, top_bear, all_res, diag = run_scan(
                    stocks, scan_date, cutoff_h, cutoff_m,
                    trade, status_cb=on_progress
                )

            prog.progress(100)
            ptxt.text(
                f"✅ Done — {len(top_bull)} Top Gainer Bullish  |  "
                f"{len(top_bear)} Top Loser Bearish  |  "
                f"{len(all_res)} stocks processed"
            )

            bt_stats = None
            if not is_live and (top_bull or top_bear):
                with st.spinner("Verifying EOD results…"):
                    c2_, l2_ = {}, threading.Lock()
                    top_bull, top_bear, bt_stats = verify_backtest(
                        top_bull, top_bear, scan_date, trade, c2_, l2_
                    )

            st.session_state['results']  = (
                top_bull, top_bear, all_res, diag,
                scan_date, not is_live, holiday_msg, cutoff_opt
            )
            st.session_state['bt_stats'] = bt_stats

    # ── SHOW RESULTS ──────────────────────────────────────────────────────────
    if st.session_state.get('results') is not None:
        (top_bull, top_bear, all_res, diag,
         scan_date, is_hist, holiday_msg, cutoff_opt) = st.session_state['results']
        bt_stats = st.session_state.get('bt_stats')

        if holiday_msg:
            st.warning(holiday_msg)

        with result_ph.container():
            n_fail = sum(1 for v in diag.values() if v.startswith('FAILED'))
            n_ok   = len(diag) - n_fail

            st.markdown(
                f"## Results — {scan_date.strftime('%d %B %Y')}  |  "
                f"Cutoff: {cutoff_opt}"
            )

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("🟢 Top Gainer Bullish", len(top_bull))
            m2.metric("🔴 Top Loser Bearish",  len(top_bear))
            m3.metric("📊 Scanned OK",         n_ok)
            m4.metric("❌ Fetch Failed",       n_fail)

            if n_ok + n_fail > 0 and n_fail / (n_ok + n_fail) > 0.15:
                # Collect failure reasons from diag dict
                fail_reasons = {}
                fail_syms    = []
                for sym, msg in diag.items():
                    if str(msg).startswith("FAILED"):
                        fail_syms.append(sym)
                        stage = str(msg).split(":")[1].strip() if ":" in msg else "unknown"
                        fail_reasons[stage] = fail_reasons.get(stage, 0) + 1
                rstr = "  |  ".join(f"{s}={c}" for s,c in
                                    sorted(fail_reasons.items(), key=lambda x:-x[1]))
                st.warning(
                    f"⚠️ **{n_fail} of {n_ok+n_fail} stocks failed** — "
                    f"Stages: {rstr or 'unknown'}  ·  "
                    f"Re-run immediately to use cached instruments. "
                    f"Same stocks failing every run = add to SYMBOL_MAP."
                )
                if fail_syms:
                    with st.expander(
                        f"❌ {len(fail_syms)} failed symbols — click to see"
                    ):
                        st.code("\n".join(sorted(fail_syms)))

            # ── Backtest summary ───────────────────────────────────────────────
            if bt_stats and bt_stats['total'] > 0:
                st.divider()
                st.markdown("### 🔬 Backtest Verification")
                bc1,bc2,bc3,bc4,bc5,bc6 = st.columns(6)
                bc1.metric("Trades",      bt_stats['total'])
                bc2.metric("🟢 Buy Win",  bt_stats['bw'])
                bc3.metric("❌ Buy Loss", bt_stats['bl'])
                bc4.metric("🟢 Sell Win", bt_stats['sw'])
                bc5.metric("❌ Sell Loss",bt_stats['sl'])
                bc6.metric("🏆 Win Rate", f"{bt_stats['win_rate']}%")

            st.divider()

            # ── TOP GAINER BULLISH ─────────────────────────────────────────────
            st.markdown(f"## 🟢 TOP GAINER BULLISH  ({len(top_bull)})")
            st.caption(
                "All 6 MRTQ v2 conditions pass. "
                "Sorted by EOD Continuation Score → Move % (Rank 1 = highest probability). "
                "Entry: breakout above last candle HIGH. Exit ~3:00 PM."
            )
            if top_bull:
                st.markdown(
                    build_table(top_bull, is_bull=True, is_backtest=is_hist),
                    unsafe_allow_html=True
                )
            else:
                st.info(
                    "No stock passed all 6 bullish conditions today. "
                    "Open **All Scanned Stocks** below — each stock shows its X/6 "
                    "score with exact reasons for the conditions that failed."
                )

            st.divider()

            # ── TOP LOSER BEARISH ──────────────────────────────────────────────
            st.markdown(f"## 🔴 TOP LOSER BEARISH  ({len(top_bear)})")
            st.caption(
                "All 6 MRTQ v2 conditions pass (bearish direction). "
                "Sorted by EOD Continuation Score → Move % (Rank 1 = highest probability). "
                "Entry: breakdown below last candle LOW. Exit ~3:00 PM."
            )
            if top_bear:
                st.markdown(
                    build_table(top_bear, is_bull=False, is_backtest=is_hist),
                    unsafe_allow_html=True
                )
            else:
                st.info(
                    "No stock passed all 6 bearish conditions today. "
                    "Open **All Scanned Stocks** below for the X/6 breakdown."
                )

            st.divider()

            # ── EXECUTION GUIDE ───────────────────────────────────────────────
            st.markdown(f"""
### 📌 Trade Execution

| | BUY (Bullish) | SHORT (Bearish) |
|---|---|---|
| **Entry Candle** | Last 15-min candle at cutoff | Same |
| **Entry** | Breakout above candle **HIGH** | Breakdown below candle **LOW** |
| **Stop Loss** | Candle **LOW** | Candle **HIGH** |
| **Target** | Entry + {RR_RATIO}× Risk | Entry − {RR_RATIO}× Risk |
| **Exit Time** | ~3:00 PM | ~3:00 PM |

> **Rank 1 first.** The EOD Score ranks stocks by continuation probability.
> If you can only take one trade, take Rank 1.
""")

            st.divider()

            # ── ALL SCANNED STOCKS (diagnostics) ─────────────────────────────
            all_sorted = sorted(all_res, key=lambda r: r['EODScore'], reverse=True)
            with st.expander(
                f"📋 All Scanned Stocks — {len(all_res)} "
                f"(X/6 condition breakdown, sorted by EOD Score)",
                expanded=(len(top_bull) + len(top_bear) == 0)
            ):
                st.markdown(
                    build_table(all_sorted, is_bull=True, show_diag=True),
                    unsafe_allow_html=True
                )

            # ── FETCH DIAGNOSTICS ─────────────────────────────────────────────
            with st.expander(
                f"🔧 Fetch Diagnostics — {n_ok} OK, {n_fail} Failed",
                expanded=(n_fail > 0)
            ):
                diag_rows = [{'Symbol': s, 'Status': diag[s]} for s in sorted(diag)]
                df_diag   = pd.DataFrame(diag_rows)
                fails_only = st.checkbox("Show only failures", value=(n_fail > 0))
                if fails_only:
                    df_diag = df_diag[df_diag['Status'].str.startswith('FAILED')]
                st.dataframe(df_diag, width='stretch', height=400)


if __name__ == "__main__":
    main()
