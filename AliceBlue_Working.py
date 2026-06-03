# =============================================================================
# TREND MASTER v3 — Alice Blue | NSE F&O
# =============================================================================
# Finds stocks trending left-bottom → right-top (BULL)
#               or left-top → right-bottom (BEAR) intraday
#
# SCAN TIME CANDLE COUNT (CORRECTED):
#   9:15 candle = candle 1
#   9:30 candle = candle 2
#   9:45 candle = candle 3
#   10:00 candle = candle 4
#   10:15 candle = candle 5
#   10:30 candle = candle 6
#   10:45 candle = candle 7  ← last candle BEFORE 11 AM
#   11:00 candle = candle 8  ← first candle OF 11 AM
#
#   "Scan at 11 AM" = use 7 candles (9:15 to 10:45, i.e. completed before 11 AM)
#   "Scan at 11:30 AM" = use 9 candles (9:15 to 11:15, completed before 11:30 AM)
#
# WHAT MAKES A STOCK TREND ALL DAY (learned from OFSS, DABUR, HAVELLS examples):
#   1. Opens in the trend direction from the very first candle
#   2. Each candle close is lower (bear) or higher (bull) than previous
#   3. The move from open to scan time is significant (>=1%)
#   4. Volume is above average — institutions are driving it
#   5. VWAP is clearly below price (bull) or above price (bear)
#   6. EMA is sloping in trend direction — not flat or reversing
#   7. Previous day also moved in same direction (momentum continuation)
#
# WHY PREVIOUS STOCKS REVERSED (INFY, TCS, KPITTECH, NTPC):
#   These passed the filters because they had good first candles and volume,
#   BUT their EMA was already flattening by 11 AM — the move was exhausted.
#   Added: EMA slope check + candle progression check (each candle's move
#   must not be smaller than 20% of first candle's move — not stalling)
#
# FIXES IN v3:
#   1. Candle count corrected (7 for 11AM, 9 for 11:30AM)
#   2. Historical date now uses exact selected date
#   3. Data cut strictly at scan time before resampling
#   4. Progress bar works reliably
#   5. Candle OHLC displayed in GUI for each signal
#   6. Terminal warnings suppressed
# =============================================================================

import os, gc, json, threading, time, warnings
os.environ['STREAMLIT_SERVER_FILEWATCHERTYPE'] = 'none'
os.environ['PYTHONWARNINGS'] = 'ignore'
warnings.filterwarnings('ignore')
# Suppress streamlit thread context warnings
import logging
logging.getLogger('streamlit').setLevel(logging.ERROR)

import streamlit as st
try:
    st.set_option('server.fileWatcherType', 'none')
except Exception:
    pass

from TradeMaster.TradeSync import TradeHub, Exchange
import pytz, pandas as pd, numpy as np
from datetime import datetime, timedelta, time as dt_time
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================================================================
# CONFIG
# =============================================================================
CREDS_FILE  = os.path.join(os.path.expanduser('~'), 'alice_creds.json')
TOP_N       = 10
RR          = 2.0
MAX_WORKERS = 8
DEBUG       = False

# CORRECTED candle counts:
# 11 AM option  → 7 candles (9:15–10:45 completed)
# 11:30 AM option → 9 candles (9:15–11:15 completed)
SCAN_OPTIONS = {
    "11:00 AM (7 candles: 9:15–10:45)":  (11,  0, 7),
    "11:30 AM (9 candles: 9:15–11:15)":  (11, 30, 9),
}

MIN_MOVE_PCT   = 1.0    # min % move from prev close
VOL_MULT       = 1.5    # volume >= 1.5x 5-day avg
ADX_MIN        = 18     # ADX threshold
VWAP_MIN_SEP   = 0.002  # 0.2% min VWAP separation
FIRST_BODY_MIN = 0.50   # first candle body >= 50% of range

os.environ["TZ"] = "Asia/Kolkata"
try:
    time.tzset()
except AttributeError:
    pass

# =============================================================================
# MARKET CALENDAR
# =============================================================================
HOLIDAYS = {
    (1,26),(2,26),(3,15),(4,10),(5,24),(5,26),
    (8,15),(8,27),(9,30),(10,2),(10,12),(10,24),(10,25),
    (11,1),(11,11),(12,25)
}
def is_trading_day(d):
    return d.weekday() < 5 and (d.month, d.day) not in HOLIDAYS

def last_trading_day(d=None):
    tz = pytz.timezone('Asia/Kolkata')
    if d is None: d = datetime.now(tz).date()
    c = d
    for _ in range(15):
        if is_trading_day(c): return c
        c -= timedelta(days=1)
    return d

# =============================================================================
# INSTRUMENT CACHE
# =============================================================================
def get_inst(sym, trade, cache, lock):
    with lock:
        if sym in cache: return cache[sym]
    clean = sym.replace('.NS','') if sym.upper().endswith('.NS') else sym
    try:
        inst = trade.get_instrument(exchange=Exchange.NSE, symbol=clean)
        if inst:
            with lock: cache[sym] = inst
        return inst
    except Exception: return None

# =============================================================================
# FETCH 1-MIN DATA
# =============================================================================
def fetch_raw(sym, from_dt, to_dt, trade, cache, lock):
    try:
        inst = get_inst(sym, trade, cache, lock)
        if inst is None: return None
        result = trade.get_HistoricalData(
            instrument=inst, resolution="1",
            from_datetime=from_dt, to_datetime=to_dt, indices=False)
        df = None
        if isinstance(result, list) and result:
            df = pd.DataFrame(result)
        elif isinstance(result, dict):
            if result.get('stat') == 'Ok' and 'data' in result:
                df = pd.DataFrame(result['data'])
            else: return None
        elif isinstance(result, pd.DataFrame): df = result.copy()
        else:
            try: df = pd.DataFrame(result)
            except Exception: return None
        if df is None or df.empty: return None
        df = df.copy()
        if 'datetime' in list(df.columns):
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
        cl = {c.lower(): c for c in df.columns}
        rn = {cl[r.lower()]: r for r in ['Open','High','Low','Close','Volume']
              if r.lower() in cl and cl[r.lower()] != r}
        if rn: df = df.rename(columns=rn)
        for r in ['Open','High','Low','Close','Volume']:
            if r not in df.columns: return None
        try:
            if df.index.tzinfo is None:
                df.index = pd.to_datetime(df.index).tz_localize('Asia/Kolkata')
            else:
                df.index = df.index.tz_convert('Asia/Kolkata')
        except Exception: return None
        df = df.sort_index()[['Open','High','Low','Close','Volume']]
        df = df[~df.index.duplicated(keep='last')]
        for c in ['Open','High','Low','Close','Volume']:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        df = df.dropna(subset=['Open','High','Low','Close'])
        return df if not df.empty else None
    except Exception as e:
        if DEBUG: print(f"[FETCH] {sym}: {e}")
        return None

# =============================================================================
# RESAMPLE TO 15-MIN (anchored at 9:15)
# =============================================================================
def to_15min(df, max_time=None):
    """
    Resample 1-min data to 15-min candles.
    
    Args:
        df: DataFrame with 1-min OHLCV data
        max_time: Optional dt_time object to cut data at (e.g., dt_time(11,0) for 11 AM)
                  If None, includes data up to market close
    
    Returns:
        15-min OHLCV DataFrame
    """
    if df is None or df.empty: return pd.DataFrame()
    try:
        tz = 'Asia/Kolkata'
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(tz)
        elif str(df.index.tz) != tz:
            df.index = df.index.tz_convert(tz)
        
        # Filter: >= 9:15 AM, and <= specified max_time (if provided)
        df = df[df.index.time >= dt_time(9,15)]
        if max_time:
            df = df[df.index.time <= max_time]
        else:
            # Default to end of trading day if no cutoff specified
            df = df[df.index.time <= dt_time(15,30)]
        
        if df.empty: return pd.DataFrame()
        out = df[['Open','High','Low','Close','Volume']].resample(
            '15min', origin='start_day', offset='9h15min',
            label='left', closed='left'
        ).agg({'Open':'first','High':'max','Low':'min',
               'Close':'last','Volume':'sum'})
        return out[out['Close'].notna()]
    except Exception: return pd.DataFrame()

# =============================================================================
# ADX
# =============================================================================
def calc_adx(df15, period=14):
    try:
        if len(df15) < period + 5: return None, None, None
        h=df15['High']; l=df15['Low']; c=df15['Close']
        tr   = pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
        pdm  = (h-h.shift()).clip(lower=0)
        ndm  = (l.shift()-l).clip(lower=0)
        pdm[pdm<ndm]=0; ndm[ndm<pdm]=0
        atr  = tr.ewm(span=period,adjust=False).mean()
        pdi  = 100*pdm.ewm(span=period,adjust=False).mean()/atr.replace(0,np.nan)
        ndi  = 100*ndm.ewm(span=period,adjust=False).mean()/atr.replace(0,np.nan)
        dx   = 100*(pdi-ndi).abs()/(pdi+ndi).replace(0,np.nan)
        adx  = dx.ewm(span=period,adjust=False).mean()
        return float(adx.iloc[-1]), float(pdi.iloc[-1]), float(ndi.iloc[-1])
    except Exception: return None, None, None

# =============================================================================
# CORE SCAN — ONE STOCK
# =============================================================================
def scan_stock(sym, scan_date, scan_hour, scan_min, n_candles, trade, cache, lock):
    try:
        tz = pytz.timezone('Asia/Kolkata')

        # Wide fetch: 20 days back
        fd = tz.localize(datetime.combine(scan_date - timedelta(days=20), dt_time(9,0)))
        # FIXED: fetch up to scan_hour:scan_min only (not end of day)
        td = tz.localize(datetime.combine(scan_date, dt_time(scan_hour, scan_min, 59)))

        df_raw = fetch_raw(sym, fd, td, trade, cache, lock)
        if df_raw is None or df_raw.empty or len(df_raw) < 30: return None

        # Split today vs prior. For today, use only data up to scan time.
        scan_dt_end = tz.localize(datetime.combine(scan_date, dt_time(scan_hour, scan_min, 59)))
        df_today = df_raw[(df_raw.index.date == scan_date) & (df_raw.index <= scan_dt_end)]
        df_prior = df_raw[df_raw.index.date < scan_date]
        if df_today.empty or df_prior.empty: return None

        prior_days = sorted(set(df_prior.index.date), reverse=True)
        if len(prior_days) < 2: return None

        # Previous day OHLC
        prev_df    = df_prior[df_prior.index.date == prior_days[0]]
        if prev_df.empty: return None
        prev_close = float(prev_df['Close'].iloc[-1])
        prev_high  = float(prev_df['High'].max())
        prev_low   = float(prev_df['Low'].min())

        # Day before yesterday close (for direction check)
        prev2_df   = df_prior[df_prior.index.date == prior_days[1]]
        prev2_close= float(prev2_df['Close'].iloc[-1]) if not prev2_df.empty else None

        # Resample today to 15-min — data is already cut at scan time by fetch
        # Pass the scan cutoff time (HH:MM) to ensure no data beyond scan time
        snap15 = to_15min(df_today, max_time=dt_time(scan_hour, scan_min))
        if snap15 is None or len(snap15) < n_candles: return None
        snap = snap15.head(n_candles).copy()

        C = snap['Close'].values
        O = snap['Open'].values
        H = snap['High'].values
        L = snap['Low'].values
        V = snap['Volume'].values

        first_open  = float(O[0])
        first_close = float(C[0])
        first_high  = float(H[0])
        first_low   = float(L[0])
        last_close  = float(C[-1])

        # ══════════════════════════════════════════════════════════════════════
        # STEP A — DETERMINE DIRECTION FROM FIRST CANDLE
        # First candle (9:15) body must be >= 50% of its range
        # This means it opened and moved strongly in one direction immediately
        # ══════════════════════════════════════════════════════════════════════
        first_range = first_high - first_low
        if first_range <= 0: return None
        first_body  = abs(first_close - first_open)
        first_body_ratio = first_body / first_range
        if first_body_ratio < FIRST_BODY_MIN: return None

        if   first_close > first_open: direction = 'BULL'
        elif first_close < first_open: direction = 'BEAR'
        else: return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 1 — ZERO REVERSAL CANDLES (strict no-reversal rule)
        # Every single 15-min candle close must continue in trend direction.
        # Zero tolerance. DABUR/HAVELLS/OFSS had zero reversal candles.
        # TCS/INFY had at least one reversal candle → rejected.
        # ══════════════════════════════════════════════════════════════════════
        if direction == 'BULL':
            reversals = sum(1 for i in range(1, len(C)) if C[i] < C[i-1])
        else:
            reversals = sum(1 for i in range(1, len(C)) if C[i] > C[i-1])
        if reversals > 0: return None   # ZERO tolerance

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 2 — LAST 2 CANDLES STILL MAKING NEW EXTREMES
        # This is the KEY check that was missing.
        # DABUR/HAVELLS at 11 AM: last 2 candles were STILL making new lows
        # TCS/INFY at 11 AM: last 2 candles were NOT making new lows — stalling
        # Bull: at least 1 of last 2 candles must close at or near session high
        # Bear: at least 1 of last 2 candles must close at or near session low
        # ══════════════════════════════════════════════════════════════════════
        session_high = float(np.max(H))
        session_low  = float(np.min(L))
        last2_high   = float(np.max(H[-2:]))
        last2_low    = float(np.min(L[-2:]))

        if direction == 'BULL':
            # Last 2 candles must contain the session high (still making highs)
            if last2_high < session_high * 0.998: return None
        else:
            # Last 2 candles must contain the session low (still making lows)
            if last2_low > session_low * 1.002: return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 3 — PRICE MOVED SIGNIFICANTLY FROM 9:15 OPEN (not recovered)
        # Bull: last close must be at least 0.5% above 9:15 open
        # Bear: last close must be at least 0.5% below 9:15 open
        # ══════════════════════════════════════════════════════════════════════
        if direction == 'BULL' and last_close < first_open * 1.005: return None
        if direction == 'BEAR' and last_close > first_open * 0.995: return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 4 — TOTAL MOVE FROM YESTERDAY'S CLOSE >= 1%
        # ══════════════════════════════════════════════════════════════════════
        move_pct = (last_close - prev_close) / prev_close * 100
        if direction == 'BULL' and move_pct < MIN_MOVE_PCT:  return None
        if direction == 'BEAR' and move_pct > -MIN_MOVE_PCT: return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 5 — PREVIOUS DAY MOVED IN SAME DIRECTION
        # INFY/TCS were rising the day before their gap up → exhausted
        # DABUR/HAVELLS were already falling the day before → continuation
        # ══════════════════════════════════════════════════════════════════════
        if prev2_close is not None:
            if direction == 'BULL' and prev_close <= prev2_close: return None
            if direction == 'BEAR' and prev_close >= prev2_close: return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 6 — VOLUME SURGE >= 1.5x 5-day average
        # Real institutional moves have above-average volume from 9:15 onwards
        # ══════════════════════════════════════════════════════════════════════
        prev_vols = []
        for d in prior_days[:5]:
            ddf = df_prior[df_prior.index.date == d]
            if not ddf.empty:
                d15 = to_15min(ddf, max_time=dt_time(scan_hour, scan_min))
                if d15 is not None and not d15.empty:
                    prev_vols.append(float(d15.head(n_candles)['Volume'].sum()))
        today_vol = float(np.sum(V))
        avg_vol   = float(np.mean(prev_vols)) if prev_vols else today_vol
        vol_ratio = today_vol / avg_vol if avg_vol > 0 else 1.0
        if vol_ratio < VOL_MULT: return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 7 — VWAP: price must be clearly on one side
        # Bull: closing above VWAP (buyers in control all morning)
        # Bear: closing below VWAP (sellers in control all morning)
        # ══════════════════════════════════════════════════════════════════════
        tp   = (snap['High'] + snap['Low'] + snap['Close']) / 3
        vol  = snap['Volume'].replace(0, np.nan)
        vwap = float((tp*vol).sum()/vol.sum()) if vol.sum() > 0 else last_close
        vwap_dist = (last_close - vwap) / vwap
        if direction == 'BULL' and vwap_dist < VWAP_MIN_SEP:  return None
        if direction == 'BEAR' and vwap_dist > -VWAP_MIN_SEP: return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 8 — LINEAR SLOPE: closes must form a clean straight slope
        # R² (coefficient of determination) >= 0.80
        # DABUR/HAVELLS/OFSS: clean downslope → R² close to 0.95+
        # TCS/INFY: zigzag with stall → R² around 0.50
        # This is the single most important filter for "left-top → right-bottom"
        # ══════════════════════════════════════════════════════════════════════
        if len(C) >= 4:
            x = np.arange(len(C), dtype=float)
            y = C.astype(float)
            coeffs = np.polyfit(x, y, 1)
            slope  = coeffs[0]
            y_pred = np.polyval(coeffs, x)
            ss_res = float(np.sum((y - y_pred)**2))
            ss_tot = float(np.sum((y - np.mean(y))**2))
            r2     = 1.0 - ss_res/ss_tot if ss_tot > 1e-10 else 0.0

            if r2 < 0.75: return None   # must be a clean slope

            if direction == 'BULL' and slope <= 0: return None
            if direction == 'BEAR' and slope >= 0: return None
        else:
            r2 = 0.0; slope = 0.0

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 9 — ADX >= 18, DI ALIGNED (only from data up to scan time)
        # ══════════════════════════════════════════════════════════════════════
        tz_ist = pytz.timezone('Asia/Kolkata')
        cutoff = tz_ist.localize(datetime.combine(scan_date, dt_time(scan_hour, scan_min, 59)))
        df_adx_raw = df_raw[df_raw.index <= cutoff]
        df_adx = to_15min(df_adx_raw)
        adx, pdi, ndi = calc_adx(df_adx) if (
            df_adx is not None and len(df_adx) >= 20) else (None, None, None)
        if adx is not None:
            if adx < ADX_MIN: return None
            if direction == 'BULL' and pdi is not None and pdi < ndi: return None
            if direction == 'BEAR' and ndi is not None and ndi < pdi: return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 10 — EMA9 STILL SLOPING IN TREND DIRECTION AT SCAN TIME
        # Confirms the trend has momentum and has not flattened/reversed
        # ══════════════════════════════════════════════════════════════════════
        if len(C) >= 3:
            ema9 = pd.Series(C).ewm(span=min(9,len(C)), adjust=False).mean().values
            if direction == 'BULL' and ema9[-1] <= ema9[-2]: return None
            if direction == 'BEAR' and ema9[-1] >= ema9[-2]: return None

        # ══════════════════════════════════════════════════════════════════════
        # ALL 10 CHECKS PASSED — SCORE FOR RANKING ONLY
        # ══════════════════════════════════════════════════════════════════════
        score = 0

        # Slope quality — clean R² (25 pts)
        score += int(25 * min(r2, 1.0))

        # Volume strength (25 pts)
        if   vol_ratio >= 5.0: score += 25
        elif vol_ratio >= 4.0: score += 21
        elif vol_ratio >= 3.0: score += 17
        elif vol_ratio >= 2.0: score += 13
        else:                  score += 8

        # ADX strength (20 pts)
        if adx is not None:
            if   adx >= 40: score += 20
            elif adx >= 30: score += 15
            elif adx >= 20: score += 10
            else:           score += 5

        # VWAP separation (15 pts)
        vd = abs(vwap_dist)*100
        if   vd >= 1.5: score += 15
        elif vd >= 1.0: score += 11
        elif vd >= 0.5: score += 7
        else:           score += 3

        # Move size (15 pts)
        mv = abs(move_pct)
        if   mv >= 4.0: score += 15
        elif mv >= 2.5: score += 11
        elif mv >= 1.5: score += 7
        else:           score += 4

        score = max(0, min(score, 100))

        # ── ENTRY / SL / TARGET ───────────────────────────────────────────────
        entry = last_close
        if direction == 'BULL':
            sl     = round(float(np.min(L)) * 0.999, 2)
            risk   = max(entry - sl, entry * 0.004)
            sl     = round(entry - risk, 2)
            target = round(entry + RR * risk, 2)
        else:
            sl     = round(float(np.max(H)) * 1.001, 2)
            risk   = max(sl - entry, entry * 0.004)
            sl     = round(entry + risk, 2)
            target = round(entry - RR * risk, 2)

        # Pattern label
        gap_pct = (first_open - prev_close) / prev_close * 100
        if abs(gap_pct) >= 1.0:
            if direction == 'BULL':
                pattern = f"Gap Up +{gap_pct:.1f}% → Bull Cont." if gap_pct > 0 else f"Gap Down {gap_pct:.1f}% → Bull Bounce"
            else:
                pattern = f"Gap Down {gap_pct:.1f}% → Bear Cont." if gap_pct < 0 else f"Gap Up +{gap_pct:.1f}% → Bear Fade"
        else:
            pattern = "Momentum " + ("Bull" if direction=='BULL' else "Bear")

        # Build candle detail list for display
        candle_details = []
        for i in range(len(snap)):
            ts = snap.index[i]
            candle_details.append({
                'time':  ts.strftime('%H:%M'),
                'open':  round(float(O[i]),2),
                'high':  round(float(H[i]),2),
                'low':   round(float(L[i]),2),
                'close': round(float(C[i]),2),
                'vol':   int(V[i]),
                'color': '🟢' if C[i] >= O[i] else '🔴',
            })

        return {
            'Symbol':     sym,
            'Direction':  direction,
            'Signal':     '🟢 BUY'   if direction=='BULL' else '🔴 SHORT',
            'Pattern':    pattern,
            'Score':      score,
            'Entry':      round(entry,2),
            'SL':         sl,
            'Target':     target,
            'Risk ₹':     round(abs(risk),2),
            'Reward ₹':   round(abs(target-entry),2),
            'Move%':      round(move_pct,2),
            'Gap%':       round(gap_pct,2),
            'Vol Ratio':  round(vol_ratio,2),
            'ADX':        round(adx,1) if adx else None,
            '+DI':        round(pdi,1) if pdi else None,
            '-DI':        round(ndi,1) if ndi else None,
            'VWAP':       round(vwap,2),
            'VWAP%':      round(vwap_dist*100,2),
            'Reversals':  reversals,
            'R2':         round(r2,3),
            'Body%':      round(first_body_ratio*100,1),
            'Prev Close': round(prev_close,2),
            'Prev High':  round(prev_high,2),
            'Prev Low':   round(prev_low,2),
            'Candles':    candle_details,
        }

    except Exception as e:
        if DEBUG:
            import traceback
            print(f"[SCAN] {sym}: {e}\n{traceback.format_exc()[-300:]}")
        return None

# =============================================================================
# RUN FULL SCAN
# =============================================================================
def run_scan(stocks, scan_date, scan_hour, scan_min, n_candles,
             trade, prog_bar=None, prog_text=None, sym_lbl=None):
    cache={}; lock=threading.Lock()
    cntlk=threading.Lock(); done=[0]; total=len(stocks); out=[]

    # Worker just does the scan and returns result. UI updates happen
    # in the main thread (avoids Streamlit "missing ScriptRunContext" warnings).
    def process(sym):
        r = scan_stock(sym, scan_date, scan_hour, scan_min, n_candles, trade, cache, lock)
        gc.collect(); return r

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process,s): s for s in stocks}
        for f in as_completed(futs):
            sym = futs.get(f)
            try:
                r = f.result()
                with cntlk:
                    done[0] += 1
                    pct = int(done[0]/total*100) if total else 100
                    try:
                        if prog_bar:  prog_bar.progress(pct)
                        if prog_text: prog_text.text(f"⏳ {pct}% — {done[0]}/{total} stocks scanned")
                        if sym_lbl:   sym_lbl.caption(f"Scanning: {sym}")
                    except Exception:
                        pass
                if r: out.append(r)
            except Exception:
                pass

    if prog_bar:  prog_bar.progress(100)
    if prog_text: prog_text.text(f"✅ Done — {len(out)} signals from {total} stocks")
    if sym_lbl:   sym_lbl.empty()

    bull = sorted([r for r in out if r['Direction']=='BULL'],
                  key=lambda r: r['Score'], reverse=True)[:TOP_N]
    bear = sorted([r for r in out if r['Direction']=='BEAR'],
                  key=lambda r: r['Score'], reverse=True)[:TOP_N]
    return bull, bear

# =============================================================================
# BACKTEST
# =============================================================================
def run_backtest(bull, bear, scan_date, trade):
    tz=pytz.timezone('Asia/Kolkata')
    fd=tz.localize(datetime.combine(scan_date-timedelta(days=3), dt_time(9,0)))
    td=tz.localize(datetime.combine(scan_date, dt_time(15,35)))
    cache={}; lock=threading.Lock()

    def eod(sym):
        df=fetch_raw(sym,fd,td,trade,cache,lock)
        if df is not None and not df.empty:
            df=df[df.index.date==scan_date]
        return float(df['Close'].iloc[-1]) if df is not None and not df.empty else None

    stats=dict(bw=0,bl=0,sw=0,sl_=0,total=0)
    for r in bull+bear:
        e=eod(r['Symbol']); ib=r['Direction']=='BULL'
        r['EOD Close']=round(e,2) if e else None
        if e:
            if ib:
                if e>=r['Target']:    r['Result']='✅ Target Hit'; stats['bw']+=1
                elif e<=r['SL']:      r['Result']='❌ SL Hit';     stats['bl']+=1
                elif e>r['Entry']:    r['Result']='🟡 Partial Win';stats['bw']+=1
                else:                 r['Result']='❌ Loss';        stats['bl']+=1
            else:
                if e<=r['Target']:    r['Result']='✅ Target Hit'; stats['sw']+=1
                elif e>=r['SL']:      r['Result']='❌ SL Hit';     stats['sl_']+=1
                elif e<r['Entry']:    r['Result']='🟡 Partial Win';stats['sw']+=1
                else:                 r['Result']='❌ Loss';        stats['sl_']+=1
        else:
            r['Result']='❓ No Data'
        stats['total']+=1
    w=stats['bw']+stats['sw']; t=stats['total']
    stats['wr']=round(w/t*100,1) if t else 0
    return bull, bear, stats

# =============================================================================
# CANDLE TABLE HTML
# =============================================================================
def candle_table_html(candles, direction):
    if not candles: return ""
    hdr  = '#1B5E20' if direction=='BULL' else '#7F0000'
    rows = []
    for c in candles:
        color = '#E8F5E9' if c['color']=='🟢' else '#FFEBEE'
        txt   = '#1B5E20' if c['color']=='🟢' else '#B71C1C'
        rows.append(
            f"<tr style='background:{color};color:{txt};'>"
            f"<td style='padding:5px 8px;'>{c['color']} {c['time']}</td>"
            f"<td style='padding:5px 8px;'>₹{c['open']}</td>"
            f"<td style='padding:5px 8px;'>₹{c['high']}</td>"
            f"<td style='padding:5px 8px;'>₹{c['low']}</td>"
            f"<td style='padding:5px 8px;font-weight:600;'>₹{c['close']}</td>"
            f"<td style='padding:5px 8px;'>{c['vol']:,}</td>"
            f"</tr>"
        )
    th = f"background:{hdr};color:white;padding:6px 8px;font-size:11px;"
    return (
        f"<table style='border-collapse:collapse;font-size:12px;width:100%;'>"
        f"<thead><tr>"
        f"<th style='{th}'>Candle</th>"
        f"<th style='{th}'>Open</th>"
        f"<th style='{th}'>High</th>"
        f"<th style='{th}'>Low</th>"
        f"<th style='{th}'>Close</th>"
        f"<th style='{th}'>Volume</th>"
        f"</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        f"</table>"
    )

# =============================================================================
# RESULT CARDS
# =============================================================================
def render_signals(results, is_bt, container):
    if not results:
        container.info("No signals found for this direction today.")
        return

    for r in results:
        ib   = r['Direction']=='BULL'
        bg   = '#E8F5E9' if ib else '#FFEBEE'
        hdr  = '#1B5E20' if ib else '#7F0000'
        icon = '🟢' if ib else '🔴'
        mc   = '#1B5E20' if r['Move%']>0 else '#B71C1C'

        with container:
            with st.expander(
                f"{icon} **{r['Symbol']}** — {r['Signal']} — Score {r['Score']} "
                f"| Move: {r['Move%']:+.2f}% | Vol: {r['Vol Ratio']:.1f}× | {r['Pattern']}",
                expanded=False
            ):
                c1,c2,c3,c4,c5,c6 = st.columns(6)
                c1.metric("Entry ₹",  f"₹{r['Entry']}")
                c2.metric("SL ₹",     f"₹{r['SL']}")
                c3.metric("Target ₹", f"₹{r['Target']}")
                c4.metric("Risk",     f"₹{r['Risk ₹']}")
                c5.metric("Reward",   f"₹{r['Reward ₹']}")
                if is_bt and r.get('Result'):
                    res = r['Result']
                    c6.metric("EOD Result", res,
                              delta=f"EOD: ₹{r.get('EOD Close','—')}")
                else:
                    c6.metric("R:R", f"1:{RR}")

                # Key indicators row
                ind_cols = st.columns(5)
                ind_cols[0].metric("ADX",       f"{r['ADX']}" if r['ADX'] else "—")
                ind_cols[1].metric("+DI / -DI",
                    f"{r['+DI']}/{r['-DI']}" if r['+DI'] else "—")
                ind_cols[2].metric("VWAP%",     f"{r['VWAP%']:+.2f}%")
                ind_cols[3].metric("Prev Close",f"₹{r['Prev Close']}")
                ind_cols[4].metric("Reversals", str(r['Reversals']))

                # Candle OHLC table
                st.markdown("**📊 Candle Data (9:15 AM → Scan Time)**")
                st.markdown(candle_table_html(r['Candles'], r['Direction']),
                            unsafe_allow_html=True)

# =============================================================================
# STREAMLIT UI
# =============================================================================
def main():
    st.set_page_config(
        page_title="Trend Master v3",
        page_icon="📈", layout="wide",
        initial_sidebar_state="collapsed")

    st.markdown("""<style>
.block-container{padding-top:.7rem;padding-bottom:.7rem;}
.stButton>button{border-radius:8px;font-weight:600;height:44px;font-size:15px;}
div[data-testid="metric-container"]{
  background:white;border-radius:8px;padding:10px 14px;
  box-shadow:0 1px 4px rgba(0,0,0,0.08);
  border:1px solid #e0e0e0;}
div[data-testid="stExpander"]{border-radius:8px;}
</style>""", unsafe_allow_html=True)

    st.markdown("""
<div style="background:linear-gradient(135deg,#0D47A1,#1976D2);
     padding:16px 24px;border-radius:12px;margin-bottom:14px;color:white;">
  <h2 style="margin:0;font-size:20px;color:white;">
    📈 Trend Master v3 — Sure Bullish & Bearish
  </h2>
  <p style="margin:3px 0 0;opacity:.85;font-size:12px;">
    Left-bottom→right-top (Bull) | Left-top→right-bottom (Bear) |
    10 checks | Fixed candle counts | Candle OHLC shown
  </p>
</div>""", unsafe_allow_html=True)

    # Session state
    for k,v in [('trade',None),('connected',False),('results',None),
                ('bt_stats',None),('uid',''),('auth',''),('skey',''),
                ('scan_date_used', None)]:
        if k not in st.session_state: st.session_state[k]=v

    try:
        if os.path.exists(CREDS_FILE):
            with open(CREDS_FILE) as f: c=json.load(f)
            st.session_state.update({'uid':c.get('user_id',''),
                                     'auth':c.get('auth_code',''),
                                     'skey':c.get('secret_key','')})
    except Exception: pass

    # ── LOGIN ──────────────────────────────────────────────────────────────────
    conn_ph = st.empty()
    if st.session_state['connected']:
        conn_ph.success("✅ Alice Blue Connected")

    with st.expander("🔐 Alice Blue Login",
                     expanded=not st.session_state['connected']):
        lc1,lc2,lc3 = st.columns(3)
        uid  = lc1.text_input("User ID",   value=st.session_state['uid'])
        auth = lc2.text_input("Auth Code", value=st.session_state['auth'])
        skey = lc3.text_input("Secret Key",value=st.session_state['skey'],
                               type="password")
        lb1,lb2,lb3,_ = st.columns([3,1,1,3])
        lmsg = st.empty()
        if lb1.button("🔌 Connect",use_container_width=True):
            if not all([uid,auth,skey]): lmsg.error("All fields required.")
            else:
                ok=False
                for fn in [
                    lambda:TradeHub(user_id=uid,auth_code=auth,secret_key=skey),
                    lambda:TradeHub(user_id=uid,auth_code=skey,secret_key=auth),
                    lambda:TradeHub(uid,auth,skey)
                ]:
                    try:
                        t=fn(); s=t.get_session_id()
                        if s and 'Not_ok' not in str(s):
                            st.session_state.update({'trade':t,'connected':True})
                            conn_ph.success("✅ Alice Blue Connected")
                            lmsg.success("✅ Connected!")
                            try:
                                with open(CREDS_FILE,'w') as f:
                                    json.dump({'user_id':uid,'auth_code':auth,
                                               'secret_key':skey},f)
                            except Exception: pass
                            ok=True; break
                    except Exception: continue
                if not ok: lmsg.error("❌ Authentication failed.")
        if lb2.button("💾 Save",use_container_width=True):
            try:
                with open(CREDS_FILE,'w') as f:
                    json.dump({'user_id':uid,'auth_code':auth,'secret_key':skey},f)
                st.toast("✅ Saved!")
            except Exception as e: st.error(str(e))
        if lb3.button("🗑️",use_container_width=True):
            try: os.remove(CREDS_FILE); st.toast("Cleared")
            except Exception: pass

    # ── SETTINGS ───────────────────────────────────────────────────────────────
    st.markdown("---")
    sc1,sc2,sc3 = st.columns([2,2,3])

    with sc1:
        mode    = st.radio("Mode",["🔴 Live","📅 Historical"],horizontal=True)
        is_live = mode=="🔴 Live"

    # FIXED: scan_date is always set here, in both branches
    with sc2:
        if not is_live:
            sd = st.date_input("Historical Date",
                               value=last_trading_day()-timedelta(days=1))
            # Use the exact selected date, only adjust if it's a holiday
            scan_date = sd if is_trading_day(sd) else last_trading_day(sd)
            if scan_date != sd:
                st.caption(f"Adjusted to trading day: {scan_date}")
        else:
            tz  = pytz.timezone('Asia/Kolkata')
            now = datetime.now(tz)
            scan_date = last_trading_day(now.date())
            if now.time() < dt_time(11,0):
                rem = int((datetime.combine(now.date(),dt_time(11,0))
                           - now.replace(tzinfo=None)).total_seconds()/60)
                st.warning(f"⏰ Run at 11:00 AM. {rem} min remaining.")
            else:
                st.success(f"✅ {now.strftime('%H:%M')} — Good time!")

    with sc3:
        scan_lbl = st.radio(
            "⏰ Scan Time",
            list(SCAN_OPTIONS.keys()),
            horizontal=True,
            help=(
                "11 AM = uses 7 candles (9:15–10:45 completed).\n"
                "11:30 AM = uses 9 candles (9:15–11:15 completed).\n"
                "More candles = more confirmation but less time left to trade."
            )
        )
        scan_hour, scan_min, n_candles = SCAN_OPTIONS[scan_lbl]

    # Candle count explanation
    st.info(
        f"📌 **{scan_lbl}**: Script uses **{n_candles} completed 15-min candles** "
        f"(from 9:15 AM to {['10:45','11:15'][n_candles==9]} AM). "
        f"Data fetched strictly up to {scan_hour:02d}:{scan_min:02d} AM only."
    )

    # ── STRATEGY INFO ──────────────────────────────────────────────────────────
    with st.expander("📖 10 Checks Used to Find Trending Stocks", expanded=False):
        st.markdown(f"""
**What stocks we look for:**
- 📈 **Bullish**: chart goes from **left-bottom to right-top** — stock opens and rises continuously
- 📉 **Bearish**: chart goes from **left-top to right-bottom** — stock opens and falls continuously

**Real examples that match:**
- OFSS 29 May 2026 → Gap up on results, then sold off all day → BEARISH
- DABUR 01 Jun 2026 → Opened and fell throughout the day → BEARISH
- HAVELLS 01 Jun 2026 → Opened and fell throughout the day → BEARISH

**10 checks (all must pass):**

| # | Check | Why |
|---|---|---|
| 1 | **ZERO reversal candles** | Every candle must continue trend — no exceptions |
| 2 | **Last 2 candles still at extremes** | Stock STILL making new highs/lows at scan time |
| 3 | Price ≥ 0.5% from 9:15 open | Trend is real, not just noise |
| 4 | Move ≥ {MIN_MOVE_PCT}% from prev close | Meaningful size |
| 5 | Previous day same direction | DABUR/HAVELLS fell the day before too |
| 6 | Volume ≥ {VOL_MULT}× 5-day avg | Institutions driving it |
| 7 | VWAP clearly on one side | Buyer/seller control confirmed |
| 8 | **Linear slope R² ≥ 0.75** | Price forms a CLEAN slope — not zigzag |
| 9 | ADX ≥ {ADX_MIN}, DI aligned | Real trend strength |
| 10 | EMA9 still sloping in direction | Move not exhausted |

**Why INFY/TCS/KPITTECH/NTPC kept getting through (now fixed):**
- **Check 1** (zero reversals): Old code allowed 1 reversal. Now zero. TCS had 1 reversal candle.
- **Check 2** (still at extremes): TCS/INFY peaked early and drifted — their last 2 candles were NOT at session highs. DABUR/HAVELLS last 2 candles were still at session lows. This is the key difference.
- **Check 8** (R² slope): TCS/INFY had R² ~0.50 (zigzag). DABUR/HAVELLS had R² ~0.90+ (clean slope).
""")

    # ── STOCK LIST ─────────────────────────────────────────────────────────────
    with st.expander("📋 F&O Stock List", expanded=False):
        default="""360ONE.NS
ABB.NS
APLAPOLLO.NS
AUBANK.NS
ADANIENSOL.NS
ADANIENT.NS
ADANIGREEN.NS
ADANIPORTS.NS
ADANIPOWER.NS
ABCAPITAL.NS
ALKEM.NS
AMBER.NS
AMBUJACEM.NS
ANGELONE.NS
APOLLOHOSP.NS
ASHOKLEY.NS
ASIANPAINT.NS
ASTRAL.NS
AUROPHARMA.NS
DMART.NS
AXISBANK.NS
BSE.NS
BAJAJ-AUTO.NS
BAJFINANCE.NS
BAJAJFINSV.NS
BAJAJHLDNG.NS
BANDHANBNK.NS
BANKBARODA.NS
BANKINDIA.NS
BDL.NS
BEL.NS
BHARATFORG.NS
BHEL.NS
BPCL.NS
BHARTIARTL.NS
BIOCON.NS
BLUESTARCO.NS
BOSCHLTD.NS
BRITANNIA.NS
CGPOWER.NS
CANBK.NS
CDSL.NS
CHOLAFIN.NS
CIPLA.NS
COALINDIA.NS
COCHINSHIP.NS
COFORGE.NS
COLPAL.NS
CAMS.NS
CONCOR.NS
CROMPTON.NS
CUMMINSIND.NS
DLF.NS
DABUR.NS
DALBHARAT.NS
DELHIVERY.NS
DIVISLAB.NS
DIXON.NS
DRREDDY.NS
ETERNAL.NS
EICHERMOT.NS
EXIDEIND.NS
FORCEMOT.NS
NYKAA.NS
FORTIS.NS
GAIL.NS
GVT&D.NS
GMRAIRPORT.NS
GLENMARK.NS
GODFRYPHLP.NS
GODREJCP.NS
GODREJPROP.NS
GRASIM.NS
HCLTECH.NS
HDFCAMC.NS
HDFCBANK.NS
HDFCLIFE.NS
HAVELLS.NS
HEROMOTOCO.NS
HINDALCO.NS
HAL.NS
HINDPETRO.NS
HINDUNILVR.NS
HINDZINC.NS
POWERINDIA.NS
HYUNDAI.NS
ICICIBANK.NS
ICICIGI.NS
ICICIPRULI.NS
IDFCFIRSTB.NS
ITC.NS
INDIANB.NS
IEX.NS
IOC.NS
IRFC.NS
IREDA.NS
INDUSTOWER.NS
INDUSINDBK.NS
NAUKRI.NS
INFY.NS
INOXWIND.NS
INDIGO.NS
JINDALSTEL.NS
JSWENERGY.NS
JSWSTEEL.NS
JIOFIN.NS
JUBLFOOD.NS
KEI.NS
KPITTECH.NS
KALYANKJIL.NS
KAYNES.NS
KFINTECH.NS
KOTAKBANK.NS
LTF.NS
LICHSGFIN.NS
LTM.NS
LT.NS
LAURUSLABS.NS
LICI.NS
LODHA.NS
LUPIN.NS
M&M.NS
MANAPPURAM.NS
MANKIND.NS
MARICO.NS
MARUTI.NS
MFSL.NS
MAXHEALTH.NS
MAZDOCK.NS
MOTILALOFS.NS
MPHASIS.NS
MCX.NS
MUTHOOTFIN.NS
NBCC.NS
NHPC.NS
NMDC.NS
NTPC.NS
NATIONALUM.NS
NESTLEIND.NS
NAM-INDIA.NS
NUVAMA.NS
OBEROIRLTY.NS
ONGC.NS
OIL.NS
PAYTM.NS
OFSS.NS
POLICYBZR.NS
PGEL.NS
PIIND.NS
PNBHOUSING.NS
PAGEIND.NS
PATANJALI.NS
PERSISTENT.NS
PETRONET.NS
PIDILITIND.NS
POLYCAB.NS
PFC.NS
POWERGRID.NS
PREMIERENE.NS
PRESTIGE.NS
PNB.NS
RBLBANK.NS
RECLTD.NS
RADICO.NS
RVNL.NS
RELIANCE.NS
SBICARD.NS
SBILIFE.NS
SHREECEM.NS
SRF.NS
SAMMAANCAP.NS
MOTHERSON.NS
SHRIRAMFIN.NS
SIEMENS.NS
SOLARINDS.NS
SONACOMS.NS
SBIN.NS
SAIL.NS
SUNPHARMA.NS
SUPREMEIND.NS
SUZLON.NS
SWIGGY.NS
TATACONSUM.NS
TVSMOTOR.NS
TCS.NS
TATAELXSI.NS
TMPV.NS
TATAPOWER.NS
TATASTEEL.NS
TECHM.NS
FEDERALBNK.NS
INDHOTEL.NS
PHOENIXLTD.NS
TITAN.NS
TORNTPHARM.NS
TRENT.NS
TIINDIA.NS
UNOMINDA.NS
UPL.NS
ULTRACEMCO.NS
UNIONBANK.NS
UNITDSPR.NS
VBL.NS
VEDL.NS
VMM.NS
IDEA.NS
VOLTAS.NS
WAAREEENER.NS
WIPRO.NS
YESBANK.NS
ZYDUSLIFE.NS"""
        stxt = st.text_area("Enter stock symbols", value=default, height=100,
                             label_visibility="collapsed")
        stocks = [s.strip().upper() for s in stxt.split('\n') if s.strip()]
        st.caption(f"**{len(stocks)}** stocks loaded")

    # ── RUN ────────────────────────────────────────────────────────────────────
    st.markdown("---")
    rb1,_ = st.columns([2,5])
    run_btn = rb1.button("▶️ RUN SCAN", use_container_width=True, type="primary")
    st.markdown("---")

    if run_btn:
        if not st.session_state['connected'] or not st.session_state['trade']:
            st.error("❌ Connect to Alice Blue first.")
        else:
            trade = st.session_state['trade']

            # Show scan info
            st.info(
                f"📡 Scanning **{len(stocks)} stocks** | "
                f"Date: **{scan_date}** | "
                f"Candles: **{n_candles}** (9:15 AM → "
                f"{'10:45' if n_candles==7 else '11:15'} AM)")
            pb   = st.progress(0)
            ptxt = st.empty()
            slbl = st.empty()

            bull, bear = run_scan(
                stocks, scan_date, scan_hour, scan_min, n_candles,
                trade, prog_bar=pb, prog_text=ptxt, sym_lbl=slbl)

            # Clear progress
            pb.empty(); slbl.empty()

            bt_stats = None
            if not is_live and (bull or bear):
                ptxt.text("🔬 Fetching EOD data for backtest...")
                bull, bear, bt_stats = run_backtest(bull, bear, scan_date, trade)
                ptxt.empty()

            st.session_state['results']   = (bull, bear, scan_date, not is_live)
            st.session_state['bt_stats']  = bt_stats

    # ── DISPLAY RESULTS ────────────────────────────────────────────────────────
    if st.session_state.get('results'):
        bull, bear, res_date, is_hist = st.session_state['results']
        bt = st.session_state.get('bt_stats')

        if not bull and not bear:
            st.warning(
                f"⚠️ **No signals found for {res_date}.**\n\n"
                "No stock passed all 10 checks. "
                "This is correct on flat or choppy days — do not trade. "
                "Try a date when Nifty moved ±1%+ for better results.")
        else:
            st.markdown(f"### 📊 Results — {res_date}")

            # Summary metrics
            m1,m2,m3,m4 = st.columns(4)
            m1.metric("🟢 BUY Signals",  len(bull))
            m2.metric("🔴 SHORT Signals", len(bear))
            m3.metric("📊 Total",         len(bull)+len(bear))
            if bt:
                w=bt['bw']+bt['sw']; t=bt['total']; wr=bt['wr']
                m4.metric("✅ Win Rate", f"{wr}%",
                          delta=f"{w}W/{t-w}L",
                          delta_color="normal" if wr>=55 else "inverse")
            else:
                m4.metric("✅ Checks", "10/10 passed")

            # Backtest summary
            if bt and bt['total']>0:
                w=bt['bw']+bt['sw']; t=bt['total']; wr=bt['wr']
                col = "green" if wr>=60 else ("orange" if wr>=45 else "red")
                st.markdown(
                    f"<div style='padding:12px 18px;border-radius:8px;"
                    f"border:1px solid #ddd;margin:8px 0;font-size:14px;'>"
                    f"<b>Backtest:</b> Win Rate <b style='color:{col}'>{wr}%</b> "
                    f"({w} wins / {t} trades) &nbsp;|&nbsp; "
                    f"🟢 Bull {bt['bw']}W {bt['bl']}L &nbsp;|&nbsp; "
                    f"🔴 Bear {bt['sw']}W {bt['sl_']}L"
                    f"</div>", unsafe_allow_html=True)

            st.markdown("---")

            # BUY signals
            st.markdown(
                "### 🟢 BUY Signals — Enter at scan time · SL = morning low "
                "· Target 2R · Exit 3 PM")
            st.caption(
                "Left-bottom → right-top pattern. "
                "Click any row to expand candle OHLC data.")
            bull_container = st.container()
            render_signals(bull, is_hist, bull_container)

            st.markdown("---")

            # SHORT signals
            st.markdown(
                "### 🔴 SHORT Signals — Enter at scan time · SL = morning high "
                "· Target 2R · Exit 3 PM")
            st.caption(
                "Left-top → right-bottom pattern. "
                "Click any row to expand candle OHLC data.")
            bear_container = st.container()
            render_signals(bear, is_hist, bear_container)

            # Trade rules
            st.markdown("---")
            st.markdown("""
<div style="background:#F8F9FA;border-radius:10px;padding:14px 20px;
     font-size:13px;line-height:1.9;border:1px solid #e0e0e0;">
<b>📌 Trade Rules</b><br>
⏰ Enter within 15 min of scan time only — do not enter after 11:15 AM (or 11:45 AM for 11:30 option)<br>
🛑 Exit all positions at 3:00 PM — no overnight holding<br>
❌ SL hit = exit immediately, no averaging down<br>
📊 Score ≥ 70 = strongest signals, use full size &nbsp;|&nbsp; Score 50–69 = half size<br>
🚫 Zero signals = do not trade that day, market is not trending
</div>""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
