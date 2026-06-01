# =============================================================================
# TREND MASTER — Alice Blue | NSE F&O
# =============================================================================
# Finds stocks that trend continuously from 9:15 AM to EOD.
#
# TWO PATTERNS DETECTED:
#
# PATTERN A — GAP & TREND (most reliable)
#   Stock gaps up/down significantly at open.
#   After the gap, it CONTINUES in the same direction all morning.
#   Example: OFSS gapped up on results, then sold off all day → BEARISH
#   Example: Stock gaps down, then bounces all day → BULLISH
#
# PATTERN B — MOMENTUM TREND (no gap)
#   No significant gap at open.
#   But from 9:15 AM every candle moves in same direction with rising volume.
#   Example: Stock flat opens, then climbs every 15 min → BULLISH
#
# HOW DIRECTION IS CONFIRMED (must pass ALL checks):
#
#   CHECK 1 — FIRST CANDLE SETS DIRECTION
#     The very first 15-min candle (9:15-9:30) must be strongly
#     green (bull) or red (bear). Body must be > 60% of range.
#     This tells us institutions entered immediately at open.
#
#   CHECK 2 — CANDLES CONTINUE IN SAME DIRECTION
#     From 9:15 to scan time, closing prices must move in same direction.
#     Bull: each close >= previous close (1 exception allowed)
#     Bear: each close <= previous close (1 exception allowed)
#
#   CHECK 3 — NO PRICE RECOVERY (most important)
#     Bull: current price must be HIGHER than the 9:15 open
#     Bear: current price must be LOWER than the 9:15 open
#     If price recovered back to open level → trend is weak, skip.
#
#   CHECK 4 — VOLUME CONFIRMS DIRECTION
#     Volume in each candle must be above the stock's average.
#     Sustained volume = institutions driving it all day.
#     Volume must be >= 1.5x the 5-day average morning volume.
#
#   CHECK 5 — VWAP CONFIRMS DIRECTION
#     Bull: price must be above VWAP (buyers in control)
#     Bear: price must be below VWAP (sellers in control)
#     If price is near VWAP → contested, no clear trend.
#
#   CHECK 6 — ADX CONFIRMS STRENGTH
#     ADX >= 20: real trend, not choppy sideways movement
#     +DI > -DI for bull, -DI > +DI for bear
#
#   CHECK 7 — TOTAL MOVE SIZE QUALIFIES
#     Bull: price must be up at least 1% from yesterday's close
#     Bear: price must be down at least 1% from yesterday's close
#     Less than 1% = too small to capture meaningful points
#
# ENTRY / SL / TARGET:
#   Entry  = price at scan time (11:00 or 11:30 AM)
#   SL     = lowest low of morning candles (bull) / highest high (bear)
#   Target = Entry ± 2R
#   Hard exit: 3:00 PM
# =============================================================================

import os, gc, json, threading, time, warnings
os.environ['STREAMLIT_SERVER_FILEWATCHERTYPE'] = 'none'
warnings.filterwarnings('ignore')

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

SCAN_OPTIONS = {
    "11:00 AM (6 candles)":  (11,  0, 6),
    "11:30 AM (7 candles)":  (11, 30, 7),
}

# Thresholds
MIN_MOVE_PCT     = 1.0    # minimum % move from yesterday close
VOL_MULT         = 1.5    # volume must be >= 1.5x 5-day avg
ADX_MIN          = 20     # ADX trend strength threshold
FIRST_BODY_MIN   = 0.55   # first candle body must be >= 55% of range
VWAP_MIN_SEP     = 0.002  # price must be >= 0.2% away from VWAP
MAX_REVERSALS    = 1      # max reversal candles allowed

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

def prior_trading_days(from_date, n):
    days, c = [], from_date - timedelta(days=1)
    while len(days) < n:
        if is_trading_day(c): days.append(c)
        c -= timedelta(days=1)
    return days

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
# RESAMPLE TO 15-MIN
# =============================================================================
def to_15min(df, cutoff_time=None):
    """Resample 1-min data to 15-min candles.
    
    Args:
        df: 1-min price data
        cutoff_time: Optional datetime to limit data (e.g., for scan time cutoff).
                     If provided, only data up to this time is included.
    """
    if df is None or df.empty: return pd.DataFrame()
    try:
        tz = 'Asia/Kolkata'
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(tz)
        elif str(df.index.tz) != tz:
            df.index = df.index.tz_convert(tz)
        
        # Filter to market hours 9:15-15:30
        df = df[(df.index.time >= dt_time(9,15)) &
                (df.index.time <= dt_time(15,30))]
        
        # If cutoff_time provided, only include data up to that time
        if cutoff_time is not None:
            if cutoff_time.tzinfo is None:
                cutoff_time = cutoff_time.replace(tzinfo=pytz.timezone(tz))
            df = df[df.index <= cutoff_time]
        
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
        h = df15['High']; l = df15['Low']; c = df15['Close']
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

        # Single wide fetch: 20 days back → scan time
        fd = tz.localize(datetime.combine(scan_date - timedelta(days=20), dt_time(9,0)))
        td = tz.localize(datetime.combine(scan_date, dt_time(scan_hour, scan_min+1)))
        
        # Exact scan datetime for data cutoff
        scan_datetime = tz.localize(datetime.combine(scan_date, dt_time(scan_hour, scan_min)))

        df_raw = fetch_raw(sym, fd, td, trade, cache, lock)
        if df_raw is None or df_raw.empty or len(df_raw) < 30:
            return None

        # Use explicit date conversion with timezone awareness to avoid boundary issues
        df_raw_tz = df_raw.copy()
        if df_raw_tz.index.tzinfo is None:
            df_raw_tz.index = df_raw_tz.index.tz_localize('Asia/Kolkata')
        elif str(df_raw_tz.index.tz) != 'Asia/Kolkata':
            df_raw_tz.index = df_raw_tz.index.tz_convert('Asia/Kolkata')
        
        # Filter by converting index to date in IST timezone
        df_raw_tz['_date'] = df_raw_tz.index.date
        df_today = df_raw_tz[df_raw_tz['_date'] == scan_date].drop('_date', axis=1)
        df_prior = df_raw_tz[df_raw_tz['_date']  < scan_date].drop('_date', axis=1)
        if df_today.empty or df_prior.empty: return None

        prior_days = sorted(set(df_prior.index.date), reverse=True)
        if len(prior_days) < 2: return None

        # ── PREV DAY ──────────────────────────────────────────────────────────
        prev_df    = df_prior[df_prior.index.date == prior_days[0]]
        if prev_df.empty: return None
        prev_close = float(prev_df['Close'].iloc[-1])
        prev_high  = float(prev_df['High'].max())
        prev_low   = float(prev_df['Low'].min())

        # ── TODAY 15-MIN CANDLES ──────────────────────────────────────────────
        # IMPORTANT: Only include candles up to scan time (11:00 AM or 11:30 AM)
        # This ensures we don't use future data that hasn't occurred yet
        snap15 = to_15min(df_today, cutoff_time=scan_datetime)
        if snap15 is None or len(snap15) < n_candles: return None
        snap = snap15.head(n_candles).copy()

        C  = snap['Close'].values
        O  = snap['Open'].values
        H  = snap['High'].values
        L  = snap['Low'].values
        V  = snap['Volume'].values

        first_open  = float(O[0])   # 9:15 AM open price
        first_close = float(C[0])   # 9:15–9:30 AM close
        first_high  = float(H[0])
        first_low   = float(L[0])
        last_close  = float(C[-1])  # price at scan time

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 1 — FIRST CANDLE SETS DIRECTION STRONGLY
        # The 9:15 candle must be a strong directional candle.
        # Body >= 55% of total range = conviction, not indecision.
        # ══════════════════════════════════════════════════════════════════════
        first_range = first_high - first_low
        first_body  = abs(first_close - first_open)

        if first_range <= 0: return None
        first_body_ratio = first_body / first_range

        if first_body_ratio < FIRST_BODY_MIN:
            return None  # weak first candle — doji/spinning top — skip

        # Direction from first candle
        if first_close > first_open:
            direction = 'BULL'
        elif first_close < first_open:
            direction = 'BEAR'
        else:
            return None  # flat first candle

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 2 — CANDLES CONTINUE IN SAME DIRECTION
        # Count how many candles reversed direction.
        # Allow max 1 reversal (small pullback is ok).
        # ══════════════════════════════════════════════════════════════════════
        if direction == 'BULL':
            reversals = sum(1 for i in range(1, len(C)) if C[i] < C[i-1])
        else:
            reversals = sum(1 for i in range(1, len(C)) if C[i] > C[i-1])

        if reversals > MAX_REVERSALS:
            return None  # too choppy

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 3 — PRICE HAS NOT RECOVERED BACK TO OPEN
        # If price recovered to the opening level, trend is over.
        # Bull: last close must still be above first candle open
        # Bear: last close must still be below first candle open
        # Also: move from first open to last close must be >= 0.5%
        # ══════════════════════════════════════════════════════════════════════
        open_to_now_pct = (last_close - first_open) / first_open * 100

        if direction == 'BULL':
            if last_close <= first_open * 1.003:  # must be at least 0.3% above open
                return None
            if open_to_now_pct < 0.3:
                return None
        else:
            if last_close >= first_open * 0.997:  # must be at least 0.3% below open
                return None
            if open_to_now_pct > -0.3:
                return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 4 — TOTAL MOVE FROM YESTERDAY'S CLOSE >= 1%
        # Ensures we capture meaningful points, not tiny moves.
        # ══════════════════════════════════════════════════════════════════════
        move_from_prev = (last_close - prev_close) / prev_close * 100

        if direction == 'BULL' and move_from_prev < MIN_MOVE_PCT:
            return None
        if direction == 'BEAR' and move_from_prev > -MIN_MOVE_PCT:
            return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 5 — VOLUME ABOVE AVERAGE (1.5x)
        # Confirms institutions are driving the move, not just retail.
        # ══════════════════════════════════════════════════════════════════════
        prev_vols = []
        for d in prior_days[:5]:
            ddf = df_prior[df_prior.index.date == d]
            if not ddf.empty:
                d15 = to_15min(ddf)
                if d15 is not None and not d15.empty:
                    prev_vols.append(float(d15.head(n_candles)['Volume'].sum()))

        today_vol = float(np.sum(V))
        avg_vol   = float(np.mean(prev_vols)) if prev_vols else today_vol
        vol_ratio = today_vol / avg_vol if avg_vol > 0 else 1.0

        if vol_ratio < VOL_MULT:
            return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 6 — VWAP CONFIRMS DIRECTION
        # Price must be clearly above (bull) or below (bear) VWAP.
        # Near VWAP = direction contested = skip.
        # ══════════════════════════════════════════════════════════════════════
        tp   = (snap['High'] + snap['Low'] + snap['Close']) / 3
        vol  = snap['Volume'].replace(0, np.nan)
        vwap = float((tp*vol).sum() / vol.sum()) if vol.sum() > 0 else last_close

        vwap_dist = (last_close - vwap) / vwap

        if direction == 'BULL' and vwap_dist < VWAP_MIN_SEP:
            return None
        if direction == 'BEAR' and vwap_dist > -VWAP_MIN_SEP:
            return None

        # ══════════════════════════════════════════════════════════════════════
        # CHECK 7 — ADX CONFIRMS TREND STRENGTH
        # ADX >= 20 = real trend. < 20 = sideways/choppy.
        # Use full day data for ADX (not limited to scan time) for better trend assessment
        # ══════════════════════════════════════════════════════════════════════
        df_all_15 = to_15min(df_raw, cutoff_time=None)
        adx, pdi, ndi = calc_adx(df_all_15) if (
            df_all_15 is not None and len(df_all_15) >= 20) else (None, None, None)

        if adx is not None:
            if adx < ADX_MIN: return None
            if direction == 'BULL' and pdi is not None and pdi < ndi: return None
            if direction == 'BEAR' and ndi is not None and ndi < pdi: return None

        # ══════════════════════════════════════════════════════════════════════
        # ALL 7 CHECKS PASSED — COMPUTE SCORE FOR RANKING
        # ══════════════════════════════════════════════════════════════════════
        score = 0

        # First candle strength (25 pts)
        score += int(25 * min(first_body_ratio / 0.8, 1.0))

        # Candle cleanliness (20 pts)
        score += 20 - (reversals * 10)

        # Volume strength (25 pts)
        if   vol_ratio >= 4.0: score += 25
        elif vol_ratio >= 3.0: score += 20
        elif vol_ratio >= 2.0: score += 15
        elif vol_ratio >= 1.5: score += 10
        else:                  score += 5

        # ADX strength (15 pts)
        if adx is not None:
            if   adx >= 35: score += 15
            elif adx >= 28: score += 11
            elif adx >= 20: score += 7

        # VWAP separation (15 pts)
        vd = abs(vwap_dist) * 100
        if   vd >= 1.5: score += 15
        elif vd >= 1.0: score += 11
        elif vd >= 0.5: score += 7
        else:           score += 3

        score = max(0, min(score, 100))

        # ── PATTERN TYPE ──────────────────────────────────────────────────────
        gap_pct = (first_open - prev_close) / prev_close * 100
        if abs(gap_pct) >= 1.5:
            if (direction == 'BULL' and gap_pct > 0):
                pattern = f"Gap Up +{gap_pct:.1f}% → Bull"
            elif (direction == 'BEAR' and gap_pct < 0):
                pattern = f"Gap Down {gap_pct:.1f}% → Bear"
            elif (direction == 'BEAR' and gap_pct > 0):
                pattern = f"Gap Up +{gap_pct:.1f}% → Fade (Short)"
            else:
                pattern = f"Gap Down {gap_pct:.1f}% → Bounce (Buy)"
        else:
            pattern = "Momentum" + (" Bull" if direction == 'BULL' else " Bear")

        # ── ENTRY / SL / TARGET ───────────────────────────────────────────────
        entry = last_close
        if direction == 'BULL':
            morning_low  = float(np.min(L))
            sl    = round(min(morning_low, prev_low) * 0.999, 2)
            risk  = max(entry - sl, entry * 0.004)
            sl    = round(entry - risk, 2)
            target= round(entry + RR * risk, 2)
        else:
            morning_high = float(np.max(H))
            sl    = round(max(morning_high, prev_high) * 1.001, 2)
            risk  = max(sl - entry, entry * 0.004)
            sl    = round(entry + risk, 2)
            target= round(entry - RR * risk, 2)

        # Candle summary
        candles = []
        for i in range(len(snap)):
            candles.append({
                'time':  snap.index[i].strftime('%H:%M'),
                'open':  round(float(O[i]),2),
                'close': round(float(C[i]),2),
                'vol':   int(V[i]),
                'icon':  '🟢' if C[i]>=O[i] else '🔴',
            })

        return {
            'Symbol':      sym,
            'Direction':   direction,
            'Signal':      '🟢 BUY' if direction=='BULL' else '🔴 SHORT',
            'Pattern':     pattern,
            'Score':       score,
            'Entry':       round(entry,2),
            'SL':          sl,
            'Target':      target,
            'Risk ₹':      round(abs(risk),2),
            'Reward ₹':    round(abs(target-entry),2),
            'Move%':       round(move_from_prev,2),
            'Gap%':        round(gap_pct,2),
            'Vol Ratio':   round(vol_ratio,2),
            'ADX':         round(adx,1) if adx else None,
            'VWAP':        round(vwap,2),
            'VWAP Dist%':  round(vwap_dist*100,2),
            'Reversals':   reversals,
            'Body%':       round(first_body_ratio*100,1),
            'Prev Close':  round(prev_close,2),
            'Prev High':   round(prev_high,2),
            'Prev Low':    round(prev_low,2),
            'Candles':     candles,
        }

    except Exception as e:
        if DEBUG:
            import traceback
            print(f"[SCAN] {sym}: {e}\n{traceback.format_exc()[-200:]}")
        return None

# =============================================================================
# RUN FULL SCAN
# =============================================================================
def run_scan(stocks, scan_date, scan_hour, scan_min, n_candles,
             is_live, trade, prog_bar=None, prog_text=None, sym_lbl=None):
    tz      = pytz.timezone('Asia/Kolkata')
    snap_dt = tz.localize(datetime.combine(scan_date, dt_time(scan_hour, scan_min)))
    if is_live:
        snap_dt = min(snap_dt, datetime.now(tz))

    cache = {}; lock = threading.Lock()
    done = 0; total = len(stocks); out = []

    def process(sym):
        return scan_stock(sym, scan_date, scan_hour, scan_min, n_candles,
                          trade, cache, lock)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process, s): s for s in stocks}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                r = f.result()
                if r: out.append(r)
            except Exception as e:
                if DEBUG: print(f"[THREAD] {sym}: {e}")
            done += 1
            pct = int(done/total*100) if total else 100
            # Update progress from the main Streamlit thread only
            if prog_bar:
                try:
                    prog_bar.progress(pct, text=f"Scanning: {sym}")
                except Exception:
                    pass
            if prog_text:
                try:
                    prog_text.markdown(
                        f"<div style='font-size:13px;color:var(--color-text-secondary);'>"
                        f"⏳ <b>{pct}%</b> &nbsp;·&nbsp; {done}/{total} stocks · Scanning: <b>{sym}</b></div>",
                        unsafe_allow_html=True)
                except Exception:
                    pass
            if sym_lbl:
                try:
                    sym_lbl.markdown(
                        f"<div style='font-size:12px;color:var(--color-text-secondary);'>"
                        f"📊 Currently scanning: <b>{sym}</b> ({done}/{total})</div>", 
                        unsafe_allow_html=True)
                except Exception:
                    pass
            gc.collect()

    if prog_bar:  prog_bar.progress(100)
    if prog_text: prog_text.markdown(
        f"<div style='font-size:14px;font-weight:500;color:var(--color-text-success);'>"
        f"✅ Done — {len(out)} signals from {total} stocks</div>",
        unsafe_allow_html=True)
    if sym_lbl: sym_lbl.empty()

    bull = sorted([r for r in out if r['Direction']=='BULL'],
                  key=lambda r: r['Score'], reverse=True)[:TOP_N]
    bear = sorted([r for r in out if r['Direction']=='BEAR'],
                  key=lambda r: r['Score'], reverse=True)[:TOP_N]
    return bull, bear

# =============================================================================
# BACKTEST
# =============================================================================
def run_backtest(bull, bear, scan_date, trade):
    tz    = pytz.timezone('Asia/Kolkata')
    fd    = tz.localize(datetime.combine(scan_date-timedelta(days=3), dt_time(9,0)))
    td    = tz.localize(datetime.combine(scan_date, dt_time(15,35)))
    cache = {}; lock = threading.Lock()

    def eod(sym):
        df = fetch_raw(sym, fd, td, trade, cache, lock)
        if df is not None and not df.empty:
            df = df[df.index.date == scan_date]
        return float(df['Close'].iloc[-1]) if df is not None and not df.empty else None

    stats = dict(bw=0,bl=0,sw=0,sl_=0,total=0)

    for r in bull+bear:
        e = eod(r['Symbol']); ib = r['Direction']=='BULL'
        r['EOD Close'] = round(e,2) if e else None
        if e:
            if ib:
                if e>=r['Target']:   r['Result']='✅ Target Hit'; stats['bw']+=1
                elif e<=r['SL']:     r['Result']='❌ SL Hit';    stats['bl']+=1
                elif e>r['Entry']:   r['Result']='🟡 Partial';   stats['bw']+=1
                else:                r['Result']='❌ Loss';       stats['bl']+=1
            else:
                if e<=r['Target']:   r['Result']='✅ Target Hit'; stats['sw']+=1
                elif e>=r['SL']:     r['Result']='❌ SL Hit';    stats['sl_']+=1
                elif e<r['Entry']:   r['Result']='🟡 Partial';   stats['sw']+=1
                else:                r['Result']='❌ Loss';       stats['sl_']+=1
        else:
            r['Result']='❓ No Data'
        stats['total']+=1

    w=stats['bw']+stats['sw']; t=stats['total']
    stats['wr']=round(w/t*100,1) if t else 0
    return bull, bear, stats

# =============================================================================
# HTML RESULT TABLE
# =============================================================================
def make_table(results, is_bt=False):
    if not results:
        return ("<div style='padding:20px;text-align:center;"
                "color:var(--color-text-secondary);font-size:14px;'>"
                "No signals found.</div>")

    ib     = results[0]['Direction']=='BULL'
    hdr    = '#1565C0' if ib else '#7F0000'
    ra     = '#EFF6FF' if ib else '#FFF5F5'
    rb     = '#DBEAFE' if ib else '#FEE2E2'
    txt    = '#1E3A5F' if ib else '#5C0505'
    bt_th  = '<th style="{}">EOD ₹</th><th style="{}">Result</th>'.format(
             f'background:{hdr};color:white;padding:9px 10px;text-align:left;white-space:nowrap;font-size:12px;',
             f'background:{hdr};color:white;padding:9px 10px;text-align:left;white-space:nowrap;font-size:12px;') if is_bt else ''

    th = f"background:{hdr};color:white;padding:9px 10px;text-align:left;white-space:nowrap;font-size:12px;"
    td = "padding:9px 10px;border-bottom:0.5px solid rgba(0,0,0,0.07);"

    rows=[]
    for i,r in enumerate(results):
        bg  = ra if i%2==0 else rb
        mc  = '#1B5E20' if r['Move%']>0 else '#B71C1C'
        sc  = '#1565C0' if r['Score']>=70 else ('#E65100' if r['Score']>=50 else '#555')
        adx = f"{r['ADX']:.0f}" if r.get('ADX') else '—'
        cnd = ' '.join(f"{c['icon']}{c['time']}" for c in r.get('Candles',[]))
        rev = r.get('Reversals',0)
        bd  = r.get('Body%','—')
        vd  = r.get('VWAP Dist%','—')
        pat = r.get('Pattern','—')

        bt_td=''
        if is_bt:
            eodv = r.get('EOD Close','—')
            res  = r.get('Result','—')
            rc   = ('#1B5E20' if '✅' in str(res) else
                    '#B71C1C' if '❌' in str(res) else '#E65100')
            bt_td=(f'<td style="{td}font-weight:500;">₹{eodv if eodv else "—"}</td>'
                   f'<td style="{td}color:{rc};font-weight:700;">{res}</td>')

        rows.append(f"""
<tr style="background:{bg};color:{txt};">
  <td style="{td}font-weight:700;font-size:14px;white-space:nowrap;">{r['Symbol']}</td>
  <td style="{td}font-weight:500;white-space:nowrap;">{r['Signal']}</td>
  <td style="{td}font-size:11px;color:#555;white-space:nowrap;">{pat}</td>
  <td style="{td}font-weight:700;color:{sc};">{r['Score']}</td>
  <td style="{td}font-weight:600;">₹{r['Entry']}</td>
  <td style="{td}color:#C62828;font-weight:600;">₹{r['SL']}</td>
  <td style="{td}color:#1B5E20;font-weight:600;">₹{r['Target']}</td>
  <td style="{td}">₹{r['Risk ₹']} / ₹{r['Reward ₹']}</td>
  <td style="{td}color:{mc};font-weight:600;">{r['Move%']:+.2f}%</td>
  <td style="{td}">{r['Vol Ratio']:.1f}×</td>
  <td style="{td}">{adx}</td>
  <td style="{td}font-size:11px;">{bd}% / {vd}%</td>
  <td style="{td}font-size:11px;">{cnd} ({rev}rev)</td>
  {bt_td}
</tr>""")

    return f"""
<div style="overflow-x:auto;border-radius:10px;
     box-shadow:0 2px 10px rgba(0,0,0,0.1);margin-bottom:20px;">
<table style="width:100%;border-collapse:collapse;font-size:13px;">
<thead><tr>
  <th style="{th}">Symbol</th>
  <th style="{th}">Signal</th>
  <th style="{th}">Pattern</th>
  <th style="{th}">Score</th>
  <th style="{th}">Entry ₹</th>
  <th style="{th}">SL ₹</th>
  <th style="{th}">Target ₹</th>
  <th style="{th}">Risk/Reward</th>
  <th style="{th}">Move%</th>
  <th style="{th}">Vol</th>
  <th style="{th}">ADX</th>
  <th style="{th}">Body/VWAP%</th>
  <th style="{th}">Candles</th>
  {bt_th}
</tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>
<p style="font-size:11px;color:#888;padding:6px 12px;margin:0;">
Score = first candle strength + candle cleanliness + volume + ADX + VWAP separation.
All signals passed 7 checks. Higher score = stronger trend.
</p>
</div>"""

# =============================================================================
# STREAMLIT UI
# =============================================================================
def main():
    st.set_page_config(page_title="Trend Master",
                       page_icon="📈",layout="wide",
                       initial_sidebar_state="collapsed")

    st.markdown("""<style>
.block-container{padding-top:.8rem;padding-bottom:.8rem;}
.stButton>button{border-radius:8px;font-weight:600;height:44px;font-size:15px;}
div[data-testid="metric-container"]{
  background:var(--color-background-secondary);
  border-radius:10px;padding:12px 16px;
  box-shadow:none;border:0.5px solid var(--color-border-tertiary);}
</style>""", unsafe_allow_html=True)

    # Header
    st.markdown("""
<div style="background:linear-gradient(135deg,#0D47A1,#1976D2);
     padding:18px 24px;border-radius:12px;margin-bottom:16px;color:white;">
  <h2 style="margin:0;font-size:22px;color:white;">📈 Trend Master — Sure Bullish & Bearish</h2>
  <p style="margin:4px 0 0;opacity:.85;font-size:13px;">
    Run at 11:00 or 11:30 AM · Detects gap-and-trend + momentum patterns · Buy/Short → hold till 3 PM
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

    # ── LOGIN ─────────────────────────────────────────────────────────────────
    conn_ph = st.empty()
    if st.session_state['connected']:
        conn_ph.success("✅ Alice Blue Connected")

    with st.expander("🔐 Alice Blue Login", expanded=not st.session_state['connected']):
        c1,c2,c3 = st.columns(3)
        uid  = c1.text_input("User ID",   value=st.session_state['uid'])
        auth = c2.text_input("Auth Code", value=st.session_state['auth'])
        skey = c3.text_input("Secret Key",value=st.session_state['skey'],type="password")
        b1,b2,b3,_ = st.columns([3,1,1,3])
        msg = st.empty()
        if b1.button("🔌 Connect",use_container_width=True):
            if not all([uid,auth,skey]): msg.error("All fields required.")
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
                            msg.success("✅ Connected!")
                            try:
                                with open(CREDS_FILE,'w') as f:
                                    json.dump({'user_id':uid,'auth_code':auth,'secret_key':skey},f)
                            except Exception: pass
                            ok=True; break
                    except Exception: continue
                if not ok: msg.error("❌ Authentication failed.")
        if b2.button("💾 Save",use_container_width=True):
            try:
                with open(CREDS_FILE,'w') as f:
                    json.dump({'user_id':uid,'auth_code':auth,'secret_key':skey},f)
                st.toast("✅ Saved!")
            except Exception as e: st.error(str(e))
        if b3.button("🗑️ Clear",use_container_width=True):
            try: os.remove(CREDS_FILE); st.toast("Cleared")
            except Exception: pass

    # ── SETTINGS ──────────────────────────────────────────────────────────────
    st.markdown("---")
    r1,r2,r3 = st.columns([2,2,3])

    with r1:
        mode    = st.radio("Mode",["🔴 Live","📅 Historical"],horizontal=True)
        is_live = mode=="🔴 Live"

    scan_date=None
    with r2:
        if not is_live:
            sd = st.date_input("Historical Date",
                               value=last_trading_day()-timedelta(days=1))
            scan_date = sd if is_trading_day(sd) else last_trading_day(sd)
        else:
            tz  = pytz.timezone('Asia/Kolkata')
            now = datetime.now(tz)
            scan_date = last_trading_day(now.date())
            if now.time()<dt_time(11,0):
                rem=int((datetime.combine(now.date(),dt_time(11,0))-now.replace(tzinfo=None)).total_seconds()/60)
                st.warning(f"⏰ Best time: **11:00 AM**. {rem} min remaining.")
            else:
                st.success(f"✅ {now.strftime('%H:%M')} — Good time to scan!")

    with r3:
        scan_lbl = st.radio("⏰ Scan Time",list(SCAN_OPTIONS.keys()),
                            horizontal=True,
                            help="11:30 gives one more candle of confirmation.")
        scan_hour,scan_min,n_candles = SCAN_OPTIONS[scan_lbl]

    # ── HOW IT WORKS ──────────────────────────────────────────────────────────
    with st.expander("📖 How signals are found — 7 checks + 2 patterns",expanded=False):
        st.markdown(f"""
**Two patterns detected:**

| Pattern | Example | Signal |
|---|---|---|
| Gap up + fade | Stock gaps up 5% on results, then sells off from open | SHORT |
| Gap down + bounce | Stock gaps down 4%, then bounces from open | BUY |
| Momentum (no gap) | Stock opens flat, moves in one direction every candle | BUY or SHORT |

**7 checks — all must pass:**

| # | Check | Threshold |
|---|---|---|
| 1 | First candle (9:15) must be strongly directional | Body ≥ {int(FIRST_BODY_MIN*100)}% of candle range |
| 2 | Candles continue in same direction | Max {MAX_REVERSALS} reversal candle allowed |
| 3 | Price has NOT recovered back to 9:15 open | Must be ≥ 0.3% away from first open |
| 4 | Total move from yesterday's close | ≥ {MIN_MOVE_PCT}% |
| 5 | Volume surge | ≥ {VOL_MULT}× 5-day average |
| 6 | VWAP confirms direction | Price ≥ 0.2% away from VWAP |
| 7 | ADX trend strength | ADX ≥ {ADX_MIN}, DI aligned |

**Score** (0–100) ranks signals: first candle strength + cleanliness + volume + ADX + VWAP distance.
**SL** = morning low (bull) / morning high (bear). **Target** = {RR}R. **Exit** = 3:00 PM hard.
""")

    # ── STOCK LIST ────────────────────────────────────────────────────────────
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
BATAINDIA.NS
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
CANFINHOME.NS
CDSL.NS
CESC.NS
CGPOWER.NS
CHAMBLFERT.NS
CHOLAFIN.NS
CIPLA.NS
COALINDIA.NS
COFORGE.NS
COLPAL.NS
CONCOR.NS
COROMANDEL.NS
CROMPTON.NS
CUMMINSIND.NS
CYIENT.NS
DABUR.NS
DALBHARAT.NS
DEEPAKFERT.NS
DEEPAKNTR.NS
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
GRANULES.NS
GRASIM.NS
GSPL.NS
HAL.NS
HAVELLS.NS
HCLTECH.NS
HDFCAMC.NS
HDFCBANK.NS
HDFCLIFE.NS
HEROMOTOCO.NS
HINDALCO.NS
HINDCOPPER.NS
HINDPETRO.NS
HINDUNILVR.NS
HINDZINC.NS
HUDCO.NS
ICICIBANK.NS
ICICIGI.NS
ICICIPRULI.NS
IDFCFIRSTB.NS
IEX.NS
IIFL.NS
IGL.NS
INDHOTEL.NS
INDIAMART.NS
INDIGO.NS
INDUSINDBK.NS
INDUSTOWER.NS
INFY.NS
INOXWIND.NS
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
METROPOLIS.NS
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
PNBHOUSING.NS
POLYCAB.NS
POLICYBZR.NS
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
SYNGENE.NS
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
UNITDSPR.NS
UNOMINDA.NS
UPL.NS
VBL.NS
VEDL.NS
VOLTAS.NS
WIPRO.NS
YESBANK.NS
ZYDUSLIFE.NS"""
        stxt=st.text_area("Stock symbols (one per line)",value=default,height=100,label_visibility="collapsed")
        stocks=[s.strip().upper() for s in stxt.split('\n') if s.strip()]
        st.caption(f"**{len(stocks)}** stocks loaded")

    # ── RUN ───────────────────────────────────────────────────────────────────
    st.markdown("---")
    rc1,_ = st.columns([2,5])
    run_btn = rc1.button("▶️ RUN SCAN",use_container_width=True,type="primary")
    st.markdown("---")

    prog_ph  = st.empty()
    res_area = st.container()

    if run_btn:
        if not st.session_state['connected'] or not st.session_state['trade']:
            st.error("❌ Connect to Alice Blue first.")
        else:
            trade=st.session_state['trade']
            if not is_trading_day(scan_date):
                scan_date=last_trading_day(scan_date)

            with prog_ph.container():
                st.markdown(f"**Scanning {len(stocks)} stocks · {scan_date} · {scan_lbl}**")
                pb   = st.progress(0)
                ptxt = st.empty()
                slbl = st.empty()

            bull,bear = run_scan(
                stocks,scan_date,scan_hour,scan_min,n_candles,
                is_live,trade,prog_bar=pb,prog_text=ptxt,sym_lbl=slbl)

            bt_stats=None
            if not is_live and (bull or bear):
                with prog_ph.container():
                    st.info("🔬 Fetching EOD prices for backtest...")
                bull,bear,bt_stats = run_backtest(bull,bear,scan_date,trade)

            st.session_state['results']  = (bull,bear,scan_date,not is_live)
            st.session_state['bt_stats'] = bt_stats
            prog_ph.empty()

    # ── DISPLAY ───────────────────────────────────────────────────────────────
    if st.session_state.get('results'):
        bull,bear,res_date,is_hist = st.session_state['results']
        bt = st.session_state.get('bt_stats')

        with res_area:
            if not bull and not bear:
                st.markdown(f"""
<div style="background:var(--color-background-warning);
     border-left:4px solid var(--color-border-warning);
     padding:16px 20px;border-radius:8px;font-size:14px;margin-top:8px;">
<b>⚠️ No signals found for {res_date}</b><br><br>
No stock passed all 7 checks today. This means the market was choppy or flat —
no clear trending stocks identified. This is the correct result on such days.
<br><br>
<b>Tip:</b> Try Historical mode on a strong market day (Nifty moved ±1%+) to
see how the strategy performs when trends are present.
</div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"### 📊 Results — {res_date}")
                m1,m2,m3,m4 = st.columns(4)
                m1.metric("🟢 BUY Signals",  len(bull))
                m2.metric("🔴 SHORT Signals", len(bear))
                m3.metric("📊 Total",         len(bull)+len(bear))
                if bt:
                    w=bt['bw']+bt['sw']; t=bt['total']; wr=bt['wr']
                    m4.metric("✅ Win Rate",f"{wr}%",
                              delta=f"{w}W / {t-w}L",
                              delta_color="normal" if wr>=55 else "inverse")
                else:
                    m4.metric("🎯 All checks","7/7 passed")

                if bt and bt['total']>0:
                    w=bt['bw']+bt['sw']; t=bt['total']; wr=bt['wr']
                    col='var(--color-text-success)' if wr>=60 else (
                        'var(--color-text-warning)' if wr>=45 else 'var(--color-text-danger)')
                    st.markdown(f"""
<div style="background:var(--color-background-secondary);border-radius:10px;
     padding:14px 20px;margin:12px 0;border:0.5px solid var(--color-border-tertiary);">
<span style="font-size:20px;font-weight:500;color:{col};">
  Backtest Win Rate: {wr}%
</span>
<span style="font-size:13px;color:var(--color-text-secondary);margin-left:12px;">
  ({w} wins / {t} trades)
</span>
&nbsp;&nbsp;|&nbsp;&nbsp;
🟢 {bt['bw']}W {bt['bl']}L &nbsp;·&nbsp; 🔴 {bt['sw']}W {bt['sl_']}L
</div>""", unsafe_allow_html=True)

                st.markdown("---")

                st.markdown("""
<div style="background:var(--color-background-success);padding:10px 16px;
     border-radius:8px;margin-bottom:8px;
     border-left:4px solid var(--color-border-success);">
<b style="color:var(--color-text-success);">🟢 BUY Signals</b>
<span style="font-size:12px;color:var(--color-text-secondary);margin-left:8px;">
Enter at scan time · SL = morning low · Target = 2R · Exit 3:00 PM
</span></div>""", unsafe_allow_html=True)
                st.markdown(make_table(bull,is_hist),unsafe_allow_html=True)

                st.markdown("""
<div style="background:var(--color-background-danger);padding:10px 16px;
     border-radius:8px;margin-bottom:8px;margin-top:16px;
     border-left:4px solid var(--color-border-danger);">
<b style="color:var(--color-text-danger);">🔴 SHORT Signals</b>
<span style="font-size:12px;color:var(--color-text-secondary);margin-left:8px;">
Enter at scan time · SL = morning high · Target = 2R · Exit 3:00 PM
</span></div>""", unsafe_allow_html=True)
                st.markdown(make_table(bear,is_hist),unsafe_allow_html=True)

                st.markdown("---")
                st.markdown("""
<div style="background:var(--color-background-secondary);border-radius:10px;
     padding:14px 20px;font-size:13px;line-height:1.9;
     border:0.5px solid var(--color-border-tertiary);">
<b>📌 Trade rules</b><br>
⏰ Enter within 15 min of scan time only<br>
🛑 Exit all positions at 3:00 PM — no exceptions<br>
❌ If SL hit — exit immediately, never average down<br>
📊 Score ≥ 70 = strongest signals, use full position size<br>
📊 Score 50–69 = moderate signals, use half size<br>
🚫 Zero signals today = skip trading, protect capital
</div>""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
