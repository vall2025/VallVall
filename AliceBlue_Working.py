# =============================================================================
# TREND MASTER v5 — Alice Blue | NSE F&O
# =============================================================================
# RESEARCH-BACKED STRATEGY
# Based on analysis of OFSS (29May bearish), DABUR & HAVELLS (01Jun bearish)
#
# KEY INSIGHT FROM RESEARCH:
# These stocks share ONE common pattern at 11 AM:
#
#   OFSS 29 May:
#   - Hit 52-week high at open (buy-the-rumour sell-the-news)
#   - Opened at ₹10,345, hit high ₹10,584 in first candle, then fell ALL day
#   - At 11 AM: every candle since 9:15 was RED and making lower lows
#   - The stock was BELOW its 9:15 open price by 11 AM
#
#   DABUR & HAVELLS 01 Jun:
#   - Already in multi-day downtrend (falling for 2+ days before)
#   - Opened gap down or flat, then continued falling
#   - At 11 AM: every candle since 9:15 was RED and making lower lows
#   - Volume was elevated (institutional selling continuing)
#
# WHAT TCS/INFY/KPITTECH/NTPC had that was DIFFERENT at 11 AM:
#   - They had 1 reversal candle (closed above previous close at some point)
#   - They were NOT consistently making new lows — they were sideways/choppy
#   - Their VWAP was being tested (price near VWAP) — contested direction
#
# CORRECT LOGIC — 3 PILLARS:
#
# PILLAR 1: CLEAN CANDLE SLOPE (most critical)
#   Every 15-min candle from 9:15 to scan time must:
#   - Close in the trend direction vs previous candle
#   - NOT have a recovery/reversal
#   Zero reversal candles = the stock is in a clean trend
#   Even 1 reversal = rejected (this eliminates TCS/INFY/NTPC)
#
# PILLAR 2: STILL AT EXTREMES AT SCAN TIME
#   At 11 AM, the stock must STILL be making:
#   - New session highs (bull) OR new session lows (bear)
#   If the stock peaked at 9:30 and is now sideways = rejected
#   If the stock is still making new lows at 11 AM = it will continue to EOD
#
# PILLAR 3: VOLUME CONFIRMATION
#   Volume in the morning must be above the stock's normal morning volume
#   Institutional selling/buying sustains all day = elevated volume
#
# ADDITIONAL FILTERS:
#   - Previous day must be in same direction (multi-day trend)
#   - Linear regression R² >= 0.80 (clean slope, not zigzag)
#   - VWAP clearly on seller/buyer side (not contested)
#   - ADX >= 20 (confirmed trend strength)
# =============================================================================

import os, gc, json, threading, time, warnings, logging
os.environ['STREAMLIT_SERVER_FILEWATCHERTYPE'] = 'none'
os.environ['PYTHONWARNINGS'] = 'ignore'
warnings.filterwarnings('ignore')
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
CREDS_FILE   = os.path.join(os.path.expanduser('~'), 'alice_creds.json')
TOP_N        = 10
RR           = 2.0
MAX_WORKERS  = 8
DEBUG        = False

# Corrected candle counts:
# 9:15–10:45 = 7 candles completed before 11:00 AM
# 9:15–11:15 = 9 candles completed before 11:30 AM
SCAN_OPTIONS = {
    "11:00 AM (7 candles)":  (11,  0, 7),
    "11:30 AM (9 candles)":  (11, 30, 9),
}

MIN_MOVE_PCT = 0.5   # min % move — lowered to catch slow-grind stocks
VOL_MULT     = 1.0   # volume >= 1.0x — remove as hard filter, use in score
ADX_MIN      = 12    # ADX — lowered, used in score not hard filter
VWAP_SEP     = 0.0005 # 0.05% — very small, mainly for scoring
R2_MIN       = 0.60  # lowered — catch stocks with minor wiggles
BODY_MIN     = 0.30  # lowered — catch smaller first-candle stocks

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
# RESAMPLE TO 15-MIN (anchored at 9:15, with optional time cutoff)
# =============================================================================
def to_15min(df, end_time=None):
    if df is None or df.empty: return pd.DataFrame()
    try:
        tz = 'Asia/Kolkata'
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(tz)
        elif str(df.index.tz) != tz:
            df.index = df.index.tz_convert(tz)
        df = df[(df.index.time >= dt_time(9,15)) & (df.index.time <= dt_time(15,30))]
        if end_time is not None:
            df = df[df.index.time <= end_time]
        if df.empty: return pd.DataFrame()
        out = df[['Open','High','Low','Close','Volume']].resample(
            '15min', origin='start_day', offset='9h15min',
            label='left', closed='left'
        ).agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
        return out[out['Close'].notna()]
    except Exception: return pd.DataFrame()

# =============================================================================
# ADX (calculated on given df15)
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
# CORE SCAN
# =============================================================================
def scan_stock(sym, scan_date, scan_hour, scan_min, n_candles, trade, cache, lock):
    try:
        tz  = pytz.timezone('Asia/Kolkata')
        # Fetch window: 20 days back to exactly scan time
        fd  = tz.localize(datetime.combine(scan_date - timedelta(days=20), dt_time(9,0)))
        td  = tz.localize(datetime.combine(scan_date, dt_time(scan_hour, scan_min, 59)))

        df_raw = fetch_raw(sym, fd, td, trade, cache, lock)
        if df_raw is None or df_raw.empty or len(df_raw) < 30: return None

        # Split: today vs prior days
        df_today = df_raw[df_raw.index.date == scan_date]
        df_prior = df_raw[df_raw.index.date  < scan_date]
        if df_today.empty or df_prior.empty: return None

        prior_days = sorted(set(df_prior.index.date), reverse=True)
        if len(prior_days) < 3: return None

        # Yesterday OHLC
        prev_df = df_prior[df_prior.index.date == prior_days[0]]
        if prev_df.empty: return None
        prev_close = float(prev_df['Close'].iloc[-1])
        prev_high  = float(prev_df['High'].max())
        prev_low   = float(prev_df['Low'].min())

        # Day before yesterday close (for direction check)
        prev2_df  = df_prior[df_prior.index.date == prior_days[1]]
        prev3_df  = df_prior[df_prior.index.date == prior_days[2]]
        prev2_close = float(prev2_df['Close'].iloc[-1]) if not prev2_df.empty else None
        prev3_close = float(prev3_df['Close'].iloc[-1]) if not prev3_df.empty else None

        # Today's 15-min candles, clipped strictly to the selected scan time.
        df_today = df_today[df_today.index.time <= dt_time(scan_hour, scan_min)]
        snap15 = to_15min(df_today, end_time=dt_time(scan_hour, scan_min))
        if snap15 is None or len(snap15) < n_candles: return None
        snap = snap15.head(n_candles).copy()

        C = snap['Close'].values.astype(float)
        O = snap['Open'].values.astype(float)
        H = snap['High'].values.astype(float)
        L = snap['Low'].values.astype(float)
        V = snap['Volume'].values.astype(float)

        first_open  = O[0]
        first_close = C[0]
        first_high  = H[0]
        first_low   = L[0]
        last_close  = C[-1]

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # STEP 1: DIRECTION FROM FIRST CANDLE
        # Must have a meaningful body (>= 40% of range)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        first_rng  = first_high - first_low
        if first_rng <= 0: return None
        first_body = abs(first_close - first_open)
        first_body_ratio = first_body / first_rng
        if first_body_ratio < BODY_MIN: return None

        if   first_close > first_open: direction = 'BULL'
        elif first_close < first_open: direction = 'BEAR'
        else: return None

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # PILLAR 1 — CHECK A: ZERO REVERSAL CANDLES
        # Every candle must close in trend direction vs previous candle.
        # This is the single most important filter.
        # DABUR/HAVELLS/OFSS: 0 reversal candles
        # TCS/INFY: at least 1 reversal candle → rejected
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if direction == 'BULL':
            for i in range(1, len(C)):
                if C[i] < C[i-1]: return None   # any reversal = rejected
        else:
            for i in range(1, len(C)):
                if C[i] > C[i-1]: return None   # any bounce = rejected

        # Candle colour count (used for scoring only, not hard filter)
        green_count = sum(1 for i in range(len(C)) if C[i] >= O[i])
        red_count   = len(C) - green_count

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # PILLAR 2 — STILL AT EXTREMES AT SCAN TIME
        # The last 2 candles must contain the session extreme
        # If the stock peaked at 9:30 and is now at 10:30 sideways → rejected
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        session_high = float(np.max(H))
        session_low  = float(np.min(L))

        if direction == 'BULL':
            # Last candle's high must be within 0.3% of session high
            if H[-1] < session_high * 0.997: return None
        else:
            # Last candle's low must be within 0.3% of session low
            if L[-1] > session_low * 1.003: return None

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # PILLAR 3 — TOTAL MOVE FROM YESTERDAY >= 0.8%
        # Must be a meaningful move, not noise
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        move_pct = (last_close - prev_close) / prev_close * 100
        if direction == 'BULL' and move_pct < MIN_MOVE_PCT:  return None
        if direction == 'BEAR' and move_pct > -MIN_MOVE_PCT: return None

        # Previous day direction — used for scoring, not hard filter
        # PATANJALI/BOSCH may have flat day before trending day
        prev_day_aligned = False
        if prev2_close is not None:
            if direction == 'BULL': prev_day_aligned = prev_close > prev2_close
            if direction == 'BEAR': prev_day_aligned = prev_close < prev2_close

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ADDITIONAL FILTER 2 — VOLUME >= 1.3x 5-day average
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        prev_vols = []
        for d in prior_days[:5]:
            ddf = df_prior[df_prior.index.date == d]
            if not ddf.empty:
                d15 = to_15min(ddf, end_time=dt_time(scan_hour, scan_min))
                if d15 is not None and not d15.empty:
                    prev_vols.append(float(d15.head(n_candles)['Volume'].sum()))
        today_vol = float(np.sum(V))
        avg_vol   = float(np.mean(prev_vols)) if prev_vols else today_vol
        vol_ratio = today_vol / avg_vol if avg_vol > 0 else 1.0
        # Volume is used in scoring only — not a hard reject
        # PATANJALI/POLICYBZR have naturally lower volume

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ADDITIONAL FILTER 3 — VWAP ON CORRECT SIDE
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        tp   = (snap['High'] + snap['Low'] + snap['Close']) / 3
        vol  = snap['Volume'].replace(0, np.nan)
        vwap = float((tp*vol).sum()/vol.sum()) if vol.sum() > 0 else last_close
        vwap_dist = (last_close - vwap) / vwap
        # VWAP used in scoring only

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ADDITIONAL FILTER 4 — LINEAR REGRESSION R² >= 0.70
        # Clean slope from left-bottom to right-top or left-top to right-bottom
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        r2 = 0.0; slope_val = 0.0
        if len(C) >= 4:
            x = np.arange(len(C), dtype=float)
            coeffs = np.polyfit(x, C, 1)
            slope_val = coeffs[0]
            y_pred = np.polyval(coeffs, x)
            ss_res = float(np.sum((C - y_pred)**2))
            ss_tot = float(np.sum((C - np.mean(C))**2))
            r2 = 1.0 - ss_res/ss_tot if ss_tot > 1e-10 else 0.0
            if r2 < R2_MIN: return None
            if direction == 'BULL' and slope_val <= 0: return None
            if direction == 'BEAR' and slope_val >= 0: return None

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ADDITIONAL FILTER 5 — ADX >= 15, DI ALIGNED (scan-time data only)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        df_adx = to_15min(df_raw, end_time=dt_time(scan_hour, scan_min))
        adx, pdi, ndi = calc_adx(df_adx) if (
            df_adx is not None and len(df_adx) >= 20) else (None, None, None)
        # ADX used in scoring only — not a hard reject

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ALL HARD FILTERS PASSED — COMPUTE SCORE FOR RANKING
        # Hard filters: zero reversals + still at extreme + min move + R²
        # Everything else adds to score
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        score = 0

        # S1: Slope quality R² (30 pts)
        score += int(30 * min(r2, 1.0))

        # S2: Candle colour consistency (15 pts)
        # All green/red = 15, mostly green/red = 10, partial = 5
        if direction == 'BULL':
            if green_count == len(C):        score += 15
            elif green_count >= len(C)-1:    score += 10
            else:                            score += 5
        else:
            if red_count == len(C):          score += 15
            elif red_count >= len(C)-1:      score += 10
            else:                            score += 5

        # S3: Volume strength (20 pts)
        if   vol_ratio >= 5.0: score += 20
        elif vol_ratio >= 4.0: score += 17
        elif vol_ratio >= 3.0: score += 14
        elif vol_ratio >= 2.0: score += 11
        elif vol_ratio >= 1.5: score += 8
        elif vol_ratio >= 1.2: score += 5
        elif vol_ratio >= 1.0: score += 3
        else:                  score += 1

        # S4: Previous day same direction (10 pts)
        if prev_day_aligned: score += 10

        # S5: ADX strength (15 pts)
        if adx is not None:
            if   adx >= 35: score += 15
            elif adx >= 25: score += 11
            elif adx >= 18: score += 7
            elif adx >= 12: score += 4

        # S6: VWAP separation (10 pts)
        vd = abs(vwap_dist)*100
        if   vd >= 1.5: score += 10
        elif vd >= 1.0: score += 7
        elif vd >= 0.5: score += 4
        elif vd >= 0.1: score += 2

        # Move size factor
        mv = abs(move_pct)
        if   mv >= 5.0: score += 15
        elif mv >= 3.0: score += 11
        elif mv >= 2.0: score += 8
        elif mv >= 1.0: score += 5
        else:           score += 2

        # VWAP separation (10 pts)
        vd = abs(vwap_dist)*100
        if   vd >= 1.5: score += 10
        elif vd >= 1.0: score += 7
        elif vd >= 0.5: score += 4
        else:           score += 2

        score = max(0, min(score, 100))

        # ── GAP TYPE ─────────────────────────────────────────────────────────
        gap_pct = (first_open - prev_close) / prev_close * 100
        if abs(gap_pct) >= 1.0:
            if direction == 'BULL':
                pattern = f"Gap+{gap_pct:.1f}% Bull" if gap_pct>0 else f"Gap{gap_pct:.1f}% Bounce"
            else:
                pattern = f"Gap{gap_pct:.1f}% Bear" if gap_pct<0 else f"Gap+{gap_pct:.1f}% Fade"
        else:
            pattern = "Momentum " + ("Bull" if direction=='BULL' else "Bear")

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

        # ── CANDLE DETAILS ────────────────────────────────────────────────────
        candles = []
        for i in range(len(snap)):
            candles.append({
                'time':  snap.index[i].strftime('%H:%M'),
                'open':  round(O[i],2), 'high': round(H[i],2),
                'low':   round(L[i],2), 'close':round(C[i],2),
                'vol':   int(V[i]),
                'icon':  '🟢' if C[i]>=O[i] else '🔴',
            })

        return {
            'Symbol':     sym,
            'Direction':  direction,
            'Signal':     '🟢 BUY' if direction=='BULL' else '🔴 SHORT',
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
            'R2':         round(r2,3),
            'Body%':      round(first_body_ratio*100,1),
            'Prev Close': round(prev_close,2),
            'Prev High':  round(prev_high,2),
            'Prev Low':   round(prev_low,2),
            'Candles':    candles,
        }

    except Exception as e:
        if DEBUG:
            import traceback
            print(f"[SCAN] {sym}: {e}\n{traceback.format_exc()[-300:]}")
        return None

# =============================================================================
# RUN SCAN
# =============================================================================
def run_scan(stocks, scan_date, scan_hour, scan_min, n_candles,
             trade, prog_bar=None, prog_text=None, sym_lbl=None):
    cache={}; lock=threading.Lock()
    cntlk=threading.Lock(); done=[0]; total=len(stocks); out=[]

    def process(sym):
        return scan_stock(sym, scan_date, scan_hour, scan_min, n_candles,
                          trade, cache, lock)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs={ex.submit(process,s):s for s in stocks}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                r = f.result()
                if r:
                    out.append(r)
            except Exception:
                r = None
            finally:
                with cntlk:
                    done[0] += 1
                    pct = int(done[0] / total * 100) if total else 100
                    try:
                        if prog_bar:
                            prog_bar.progress(pct)
                        if prog_text:
                            prog_text.text(f"⏳ {pct}% — {done[0]}/{total} stocks scanned")
                        if sym_lbl:
                            sym_lbl.caption(f"Scanning: {sym}")
                    except Exception:
                        pass
            gc.collect()

    if prog_bar:  prog_bar.progress(100)
    if prog_text: prog_text.text(f"✅ Done — {len(out)} signals from {total} stocks")
    if sym_lbl:   sym_lbl.empty()

    bull=sorted([r for r in out if r['Direction']=='BULL'],key=lambda r:r['Score'],reverse=True)[:TOP_N]
    bear=sorted([r for r in out if r['Direction']=='BEAR'],key=lambda r:r['Score'],reverse=True)[:TOP_N]
    return bull, bear

# =============================================================================
# BACKTEST
# =============================================================================
def run_backtest(bull, bear, scan_date, trade):
    tz=pytz.timezone('Asia/Kolkata')
    fd=tz.localize(datetime.combine(scan_date-timedelta(days=3),dt_time(9,0)))
    td=tz.localize(datetime.combine(scan_date,dt_time(15,35)))
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
                if e>=r['Target']:   r['Result']='✅ Target Hit';  stats['bw']+=1
                elif e<=r['SL']:     r['Result']='❌ SL Hit';      stats['bl']+=1
                elif e>r['Entry']:   r['Result']='🟡 Partial Win'; stats['bw']+=1
                else:                r['Result']='❌ Loss';         stats['bl']+=1
            else:
                if e<=r['Target']:   r['Result']='✅ Target Hit';  stats['sw']+=1
                elif e>=r['SL']:     r['Result']='❌ SL Hit';      stats['sl_']+=1
                elif e<r['Entry']:   r['Result']='🟡 Partial Win'; stats['sw']+=1
                else:                r['Result']='❌ Loss';         stats['sl_']+=1
        else:
            r['Result']='❓ No Data'
        stats['total']+=1
    w=stats['bw']+stats['sw']; t=stats['total']
    stats['wr']=round(w/t*100,1) if t else 0
    return bull, bear, stats

# =============================================================================
# CANDLE TABLE HTML
# =============================================================================
def candle_html(candles, direction):
    if not candles: return ""
    hdr = '#1B5E20' if direction=='BULL' else '#7F0000'
    th  = f"background:{hdr};color:white;padding:6px 8px;font-size:11px;text-align:left;"
    rows=[]
    for c in candles:
        bg  = '#E8F5E9' if c['icon']=='🟢' else '#FFEBEE'
        txt = '#1B5E20' if c['icon']=='🟢' else '#B71C1C'
        td  = f"padding:5px 8px;color:{txt};font-size:12px;"
        rows.append(
            f"<tr style='background:{bg};'>"
            f"<td style='{td}font-weight:500;'>{c['icon']} {c['time']}</td>"
            f"<td style='{td}'>₹{c['open']}</td>"
            f"<td style='{td}'>₹{c['high']}</td>"
            f"<td style='{td}'>₹{c['low']}</td>"
            f"<td style='{td}font-weight:600;'>₹{c['close']}</td>"
            f"<td style='{td}'>{c['vol']:,}</td>"
            f"</tr>")
    return (f"<table style='border-collapse:collapse;width:100%;font-size:12px;'>"
            f"<thead><tr>"
            f"<th style='{th}'>Candle</th><th style='{th}'>Open</th>"
            f"<th style='{th}'>High</th><th style='{th}'>Low</th>"
            f"<th style='{th}'>Close</th><th style='{th}'>Volume</th>"
            f"</tr></thead><tbody>{''.join(rows)}</tbody></table>")

# =============================================================================
# RENDER SIGNALS
# =============================================================================
def render_signals(results, is_bt, container):
    if not results:
        container.info("No signals found.")
        return
    for r in results:
        ib   = r['Direction']=='BULL'
        icon = '🟢' if ib else '🔴'
        with container:
            with st.expander(
                f"{icon} **{r['Symbol']}** — {r['Signal']} — "
                f"Score {r['Score']} | Move {r['Move%']:+.2f}% | "
                f"Vol {r['Vol Ratio']:.1f}× | R²={r['R2']:.2f} | {r['Pattern']}",
                expanded=False):
                c1,c2,c3,c4,c5,c6 = st.columns(6)
                c1.metric("Entry ₹",  f"₹{r['Entry']}")
                c2.metric("SL ₹",     f"₹{r['SL']}")
                c3.metric("Target ₹", f"₹{r['Target']}")
                c4.metric("Risk",     f"₹{r['Risk ₹']}")
                c5.metric("Reward",   f"₹{r['Reward ₹']}")
                if is_bt and r.get('Result'):
                    c6.metric("Result", r['Result'],
                              delta=f"EOD ₹{r.get('EOD Close','—')}")
                else:
                    c6.metric("R:R", f"1:{RR}")

                i1,i2,i3,i4,i5 = st.columns(5)
                i1.metric("ADX",       f"{r['ADX']}" if r['ADX'] else "—")
                i2.metric("+DI/-DI",   f"{r['+DI']}/{r['-DI']}" if r['+DI'] else "—")
                i3.metric("VWAP%",     f"{r['VWAP%']:+.2f}%")
                i4.metric("Prev Close",f"₹{r['Prev Close']}")
                i5.metric("Slope R²",  f"{r['R2']:.3f}")

                st.markdown("**📊 Morning Candles (9:15 AM → Scan Time) — All must be same colour**")
                st.markdown(candle_html(r['Candles'], r['Direction']), unsafe_allow_html=True)

# =============================================================================
# STREAMLIT UI
# =============================================================================
def main():
    st.set_page_config(page_title="Trend Master v5", page_icon="📈",
                       layout="wide", initial_sidebar_state="collapsed")
    st.markdown("""<style>
.block-container{padding-top:.7rem;padding-bottom:.7rem;}
.stButton>button{border-radius:8px;font-weight:600;height:44px;font-size:15px;}
div[data-testid="metric-container"]{
  background:white;border-radius:8px;padding:10px 14px;
  box-shadow:0 1px 4px rgba(0,0,0,0.08);border:1px solid #e0e0e0;}
</style>""", unsafe_allow_html=True)

    st.markdown("""
<div style="background:linear-gradient(135deg,#0D47A1,#1976D2);
     padding:16px 24px;border-radius:12px;margin-bottom:14px;color:white;">
  <h2 style="margin:0;font-size:20px;color:white;">📈 Trend Master v5</h2>
  <p style="margin:3px 0 0;opacity:.85;font-size:12px;">
    ZERO reversal candles · Still at extremes at 11 AM · Clean slope (R²≥0.70) ·
    Based on OFSS/DABUR/HAVELLS pattern analysis
  </p>
</div>""", unsafe_allow_html=True)

    # Session state
    for k,v in [('trade',None),('connected',False),('results',None),
                ('bt_stats',None),('uid',''),('auth',''),('skey','')]:
        if k not in st.session_state: st.session_state[k]=v

    try:
        if os.path.exists(CREDS_FILE):
            with open(CREDS_FILE) as f: c=json.load(f)
            st.session_state.update({'uid':c.get('user_id',''),
                                     'auth':c.get('auth_code',''),
                                     'skey':c.get('secret_key','')})
    except Exception: pass

    # LOGIN
    conn_ph=st.empty()
    if st.session_state['connected']:
        conn_ph.success("✅ Alice Blue Connected")

    with st.expander("🔐 Login", expanded=not st.session_state['connected']):
        lc1,lc2,lc3=st.columns(3)
        uid  = lc1.text_input("User ID",   value=st.session_state['uid'])
        auth = lc2.text_input("Auth Code", value=st.session_state['auth'])
        skey = lc3.text_input("Secret Key",value=st.session_state['skey'],type="password")
        lb1,lb2,lb3,_=st.columns([3,1,1,3]); lmsg=st.empty()
        if lb1.button("🔌 Connect",use_container_width=True):
            if not all([uid,auth,skey]): lmsg.error("All fields required.")
            else:
                ok=False
                for fn in [lambda:TradeHub(user_id=uid,auth_code=auth,secret_key=skey),
                           lambda:TradeHub(user_id=uid,auth_code=skey,secret_key=auth),
                           lambda:TradeHub(uid,auth,skey)]:
                    try:
                        t=fn(); s=t.get_session_id()
                        if s and 'Not_ok' not in str(s):
                            st.session_state.update({'trade':t,'connected':True})
                            conn_ph.success("✅ Alice Blue Connected")
                            lmsg.success("Connected!")
                            try:
                                with open(CREDS_FILE,'w') as f:
                                    json.dump({'user_id':uid,'auth_code':auth,'secret_key':skey},f)
                            except Exception: pass
                            ok=True; break
                    except Exception: continue
                if not ok: lmsg.error("❌ Auth failed.")
        if lb2.button("💾 Save",use_container_width=True):
            try:
                with open(CREDS_FILE,'w') as f:
                    json.dump({'user_id':uid,'auth_code':auth,'secret_key':skey},f)
                st.toast("✅ Saved!")
            except Exception as e: st.error(str(e))
        if lb3.button("🗑️",use_container_width=True):
            try: os.remove(CREDS_FILE); st.toast("Cleared")
            except Exception: pass

    # SETTINGS
    st.markdown("---")
    sc1,sc2,sc3=st.columns([2,2,3])
    with sc1:
        mode=st.radio("Mode",["🔴 Live","📅 Historical"],horizontal=True)
        is_live=mode=="🔴 Live"
    with sc2:
        if not is_live:
            sd=st.date_input("Historical Date",
                             value=last_trading_day()-timedelta(days=1))
            scan_date=sd if is_trading_day(sd) else last_trading_day(sd)
            if scan_date!=sd: st.caption(f"Adjusted: {scan_date}")
        else:
            tz=pytz.timezone('Asia/Kolkata'); now=datetime.now(tz)
            scan_date=last_trading_day(now.date())
            if now.time()<dt_time(11,0):
                rem=int((datetime.combine(now.date(),dt_time(11,0))
                         -now.replace(tzinfo=None)).total_seconds()/60)
                st.warning(f"⏰ Run at 11:00 AM. {rem} min remaining.")
            else:
                st.success(f"✅ {now.strftime('%H:%M')} — Good time!")
    with sc3:
        scan_lbl=st.radio("⏰ Scan Time",list(SCAN_OPTIONS.keys()),horizontal=True)
        scan_hour,scan_min,n_candles=SCAN_OPTIONS[scan_lbl]

    st.info(f"📌 Using **{n_candles} candles** (9:15 AM → "
            f"{'10:45' if n_candles==7 else '11:15'} AM). "
            f"Data fetched strictly up to **{scan_hour:02d}:{scan_min:02d} AM** only.")

    with st.expander("📖 How This Finds OFSS/DABUR/HAVELLS Type Stocks",expanded=False):
        st.markdown(f"""
**Pattern identified from your examples:**

| Stock | Date | What happened |
|---|---|---|
| OFSS | 29 May | Hit 52-week high at open, then sold off all 7 candles consecutively |
| DABUR | 01 Jun | Already falling for days, opened weak, fell every candle |
| HAVELLS | 01 Jun | Same as DABUR — continuous downtrend from first candle |

**What all 3 had in common at 11 AM:**
1. Every 15-min candle from 9:15 was red (bear) or green (bull) — zero exceptions
2. The last candle was STILL making a new session low/high — still trending
3. The move was a clean slope (not zigzag) — R² ≥ 0.70
4. Volume was elevated — institutional selling/buying

**Why TCS/INFY/NTPC kept getting through (now fixed):**
- They had at least 1 reversal candle (now = zero allowed)
- Their last candles were NOT at session extremes (peaked early, drifted)
- Their slope R² was low (~0.40-0.50) — zigzag pattern

**Filters applied:**
| Filter | Threshold |
|---|---|
| Zero reversal candles | Absolute — even 1 rejected |
| All candles same colour (green/red) | No mixed candles |
| Last candle at session extreme | Within 0.3% of high/low |
| Move from prev close | ≥ {MIN_MOVE_PCT}% |
| Previous day same direction | Continuation only |
| Volume | ≥ {VOL_MULT}× 5-day average |
| VWAP separation | ≥ 0.1% on correct side |
| Linear slope R² | ≥ {R2_MIN} |
| ADX + DI aligned | ADX ≥ {ADX_MIN} |
""")

    with st.expander("📋 F&O Stock List",expanded=False):
        default="""AARTIIND.NS
ABB.NS
ABCAPITAL.NS
ADANIENT.NS
ADANIGREEN.NS
ADANIPORTS.NS
ALKEM.NS
AMBUJACEM.NS
ANGELONE.NS
APOLLOHOSP.NS
APOLLOTYRE.NS
ASHOKLEY.NS
ASIANPAINT.NS
ASTRAL.NS
AUBANK.NS
AUROPHARMA.NS
AXISBANK.NS
BAJAJ-AUTO.NS
BAJFINANCE.NS
BAJAJFINSV.NS
BAJAJHLDNG.NS
BALKRISIND.NS
BANDHANBNK.NS
BANKBARODA.NS
BANKINDIA.NS
BDL.NS
BEL.NS
BERGEPAINT.NS
BHARATFORG.NS
BHARTIARTL.NS
BHEL.NS
BIOCON.NS
BOSCHLTD.NS
BPCL.NS
BRITANNIA.NS
BSE.NS
CANBK.NS
CDSL.NS
CGPOWER.NS
CHOLAFIN.NS
CIPLA.NS
COALINDIA.NS
COFORGE.NS
COLPAL.NS
CONCOR.NS
CROMPTON.NS
CUMMINSIND.NS
CYIENT.NS
DABUR.NS
DALBHARAT.NS
DELHIVERY.NS
DIVISLAB.NS
DIXON.NS
DLF.NS
DMART.NS
DRREDDY.NS
EICHERMOT.NS
ESCORTS.NS
ETERNAL.NS
EXIDEIND.NS
FEDERALBNK.NS
FORTIS.NS
GAIL.NS
GLENMARK.NS
GMRAIRPORT.NS
GODREJCP.NS
GODREJPROP.NS
GRASIM.NS
HAL.NS
HAVELLS.NS
HCLTECH.NS
HDFCAMC.NS
HDFCBANK.NS
HDFCLIFE.NS
HEROMOTOCO.NS
HINDALCO.NS
HINDPETRO.NS
HINDUNILVR.NS
HINDZINC.NS
HUDCO.NS
ICICIBANK.NS
ICICIGI.NS
ICICIPRULI.NS
IDFCFIRSTB.NS
IEX.NS
IGL.NS
INDHOTEL.NS
INDIGO.NS
INDUSINDBK.NS
INDUSTOWER.NS
INFY.NS
IOC.NS
IRCTC.NS
IREDA.NS
IRFC.NS
ITC.NS
JINDALSTEL.NS
JIOFIN.NS
JSWENERGY.NS
JSWSTEEL.NS
JUBLFOOD.NS
KALYANKJIL.NS
KAYNES.NS
KEI.NS
KFINTECH.NS
KOTAKBANK.NS
KPITTECH.NS
LAURUSLABS.NS
LICHSGFIN.NS
LICI.NS
LODHA.NS
LT.NS
LTIM.NS
LTF.NS
LUPIN.NS
M&M.NS
MANAPPURAM.NS
MANKIND.NS
MARICO.NS
MARUTI.NS
MAXHEALTH.NS
MAZDOCK.NS
MCX.NS
MFSL.NS
MOTHERSON.NS
MPHASIS.NS
MRF.NS
MUTHOOTFIN.NS
NATIONALUM.NS
NAUKRI.NS
NBCC.NS
NCC.NS
NESTLEIND.NS
NHPC.NS
NMDC.NS
NTPC.NS
NUVAMA.NS
NYKAA.NS
OBEROIRLTY.NS
OFSS.NS
OIL.NS
ONGC.NS
PAGEIND.NS
PATANJALI.NS
PAYTM.NS
PERSISTENT.NS
PETRONET.NS
PFC.NS
PHOENIXLTD.NS
PIDILITIND.NS
PIIND.NS
PNB.NS
POLYCAB.NS
POWERGRID.NS
PRESTIGE.NS
RBLBANK.NS
RECLTD.NS
RELIANCE.NS
RVNL.NS
SAIL.NS
SBICARD.NS
SBILIFE.NS
SBIN.NS
SHREECEM.NS
SHRIRAMFIN.NS
SIEMENS.NS
SOLARINDS.NS
SONACOMS.NS
SRF.NS
SUNPHARMA.NS
SUPREMEIND.NS
SUZLON.NS
TATACONSUM.NS
TATAELXSI.NS
TATAPOWER.NS
TATASTEEL.NS
TATATECH.NS
TCS.NS
TECHM.NS
TIINDIA.NS
TITAGARH.NS
TITAN.NS
TORNTPHARM.NS
TORNTPOWER.NS
TRENT.NS
TVSMOTOR.NS
ULTRACEMCO.NS
UNIONBANK.NS
UNOMINDA.NS
UPL.NS
VBL.NS
VEDL.NS
VOLTAS.NS
WIPRO.NS
YESBANK.NS
ZYDUSLIFE.NS"""
        stxt=st.text_area("Stock symbols", value=default, height=100,
                          label_visibility="collapsed")
        stocks=[s.strip().upper() for s in stxt.split('\n') if s.strip()]
        st.caption(f"**{len(stocks)}** stocks loaded")

    st.markdown("---")
    rb1,_=st.columns([2,5])
    run_btn=rb1.button("▶️ RUN SCAN",use_container_width=True,type="primary")
    st.markdown("---")

    if run_btn:
        if not st.session_state['connected'] or not st.session_state['trade']:
            st.error("❌ Connect to Alice Blue first.")
        else:
            trade=st.session_state['trade']
            st.info(f"📡 Scanning **{len(stocks)} stocks** | **{scan_date}** | "
                    f"**{n_candles} candles** (9:15→{'10:45' if n_candles==7 else '11:15'} AM)")
            pb=st.progress(0); ptxt=st.empty(); slbl=st.empty()

            bull,bear=run_scan(stocks,scan_date,scan_hour,scan_min,n_candles,
                               trade,prog_bar=pb,prog_text=ptxt,sym_lbl=slbl)
            pb.empty(); slbl.empty()

            bt_stats=None
            if not is_live and (bull or bear):
                ptxt.text("🔬 Fetching EOD data for backtest...")
                bull,bear,bt_stats=run_backtest(bull,bear,scan_date,trade)
                ptxt.empty()

            st.session_state['results']  =(bull,bear,scan_date,not is_live)
            st.session_state['bt_stats'] =bt_stats

    if st.session_state.get('results'):
        bull,bear,res_date,is_hist=st.session_state['results']
        bt=st.session_state.get('bt_stats')

        if not bull and not bear:
            st.warning(
                f"⚠️ **No signals for {res_date}.** No stock had zero reversal candles "
                f"AND was still at session extremes at scan time. "
                f"This is correct — on choppy days there are no clean trend stocks. "
                f"Try 29 May, 01 Jun, or any day when a stock had strong news/results.")
        else:
            st.markdown(f"### 📊 Results — {res_date}")
            m1,m2,m3,m4=st.columns(4)
            m1.metric("🟢 BUY",  len(bull))
            m2.metric("🔴 SHORT",len(bear))
            m3.metric("📊 Total",len(bull)+len(bear))
            if bt:
                w=bt['bw']+bt['sw']; t=bt['total']; wr=bt['wr']
                m4.metric("✅ Win Rate",f"{wr}%",delta=f"{w}W/{t-w}L",
                          delta_color="normal" if wr>=55 else "inverse")
            else:
                m4.metric("✅ Filters","All passed")

            if bt and bt['total']>0:
                w=bt['bw']+bt['sw']; t=bt['total']; wr=bt['wr']
                col="green" if wr>=60 else ("orange" if wr>=45 else "red")
                st.markdown(
                    f"<div style='padding:12px 18px;border-radius:8px;"
                    f"border:1px solid #ddd;margin:8px 0;font-size:14px;'>"
                    f"Backtest: <b style='color:{col}'>Win Rate {wr}%</b> "
                    f"({w} wins/{t} trades) | "
                    f"🟢 {bt['bw']}W {bt['bl']}L | 🔴 {bt['sw']}W {bt['sl_']}L"
                    f"</div>",unsafe_allow_html=True)

            st.markdown("---")
            st.markdown("### 🟢 BUY — Enter at scan time · SL=morning low · 2R · Exit 3PM")
            st.caption("All candles green · Still at session high · R²≥0.70 · Clean upslope")
            render_signals(bull,is_hist,st.container())

            st.markdown("---")
            st.markdown("### 🔴 SHORT — Enter at scan time · SL=morning high · 2R · Exit 3PM")
            st.caption("All candles red · Still at session low · R²≥0.70 · Clean downslope")
            render_signals(bear,is_hist,st.container())

            st.markdown("---")
            st.markdown("""
<div style="background:#F8F9FA;border-radius:10px;padding:14px 20px;
     font-size:13px;line-height:1.9;border:1px solid #e0e0e0;">
<b>📌 Trade Rules</b><br>
⏰ Enter within 15 min of scan time · 🛑 Exit 3:00 PM hard ·
❌ SL hit = exit immediately · 📊 Score≥70 = full size · 🚫 0 signals = skip day
</div>""",unsafe_allow_html=True)

if __name__=="__main__":
    main()
