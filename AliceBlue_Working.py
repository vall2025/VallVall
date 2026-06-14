# =============================================================================
# EOD TREND SCANNER v5 — Alice Blue | NSE F&O Stocks
# =============================================================================
#
#  WHY v5 EXISTS
#  ─────────────────────────────────────────────────────────────────────────
#  v4 removed ALL filters → every single stock that was even +0.05% up
#  appeared in the BUY list, and every stock -0.05% down appeared in SHORT.
#  That's 100+ stocks in each list — useless for picking real trades.
#
#  Your requirement is "specific TRENDING stocks" — stocks that have moved
#  in a clean, consistent direction since 9:15 AM (like your sample stocks:
#  SOLARINDS, YESBANK, HYUNDAI, M&M etc. on their respective days), not
#  stocks that are just randomly flickering up or down by a few paise.
#
#  WHAT v5 DOES DIFFERENTLY
#  ─────────────────────────────────────────────────────────────────────────
#  Two BUILT-IN checks (fixed in the code, NOT sliders, NOT user-adjustable)
#  decide whether a stock is "trending":
#
#    1. TREND CONSISTENCY (R² ≥ 0.55)
#       Fit a straight line to the 5-min closes from 9:15 AM to cutoff.
#       R² close to 1.0 = price moved in a clean line (trending).
#       R² close to 0   = price moved randomly up/down (choppy/flat).
#       A truly flat or noisy stock will have R² well below 0.55.
#       A stock that opened low and climbed steadily to 11 AM (or opened
#       high and fell steadily) will have R² well above 0.55.
#
#    2. MEANINGFUL MOVE (≥ 0.3%)
#       Total Move % must be at least 0.3% in magnitude. This removes
#       stocks that are technically "positive" or "negative" by 0.02%
#       (pure noise) but still pass the R² check by coincidence on very
#       few candles.
#
#  A stock must pass BOTH checks to appear in the BUY / SHORT lists.
#  These numbers are fixed constants at the top of the file — change them
#  there if you want to loosen/tighten, but there is nothing to configure
#  in the UI.
#
#  Every stock — trending or not — is still visible in the
#  "📋 All Scanned Stocks" expander at the bottom, sorted by Total Move %,
#  along with full diagnostics (why any stock failed to fetch).
#
#  ENTRY / SL / TARGET (your Fibonacci candle strategy — unchanged)
#    Reference candle = 10:45–10:59 AM
#    BUY  : Entry = high of that candle | SL = low  | Target = Entry + 2×Risk
#    SHORT: Entry = low  of that candle | SL = high | Target = Entry − 2×Risk
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc

# =============================================================================
# CONSTANTS — fixed, not exposed in UI
# =============================================================================
CREDS_FILE   = os.path.join(os.path.expanduser('~'), 'alice_creds.json')
RR_RATIO     = 2.0
MAX_WORKERS  = 20
DEBUG        = False

# ── TREND QUALIFICATION (the only "filter" in this script) ─────────────────
TREND_MIN_R2       = 0.55   # consistency of the move (0=random, 1=straight line)
TREND_MIN_MOVE_PCT = 0.3    # minimum % move to be considered a real trend

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
# DATA FETCH  (proven method — DO NOT MODIFY)
# =============================================================================
def fetch_1min(sym, from_dt, to_dt, trade, cache, lock, retry=False):
    """Returns (df, error_reason). df is None on failure."""
    try:
        sym_clean = sym.replace('.NS','').replace('.BSE','').strip()

        inst = None
        with lock:
            if sym in cache:
                inst = cache[sym]

        if inst is None:
            try:
                inst = trade.get_instrument(exchange=Exchange.NSE, symbol=sym_clean)
                if inst:
                    with lock:
                        cache[sym] = inst
            except Exception as e:
                return None, f"instrument lookup error: {e}"

        if inst is None:
            return None, "instrument not found on Alice Blue"

        result = None
        try:
            result = trade.get_HistoricalData(
                instrument=inst,
                resolution="1",
                from_datetime=from_dt,
                to_datetime=to_dt,
                indices=False
            )
        except Exception as e:
            return None, f"API call error: {e}"

        df = None
        if isinstance(result, list) and result:
            df = pd.DataFrame(result)
        elif isinstance(result, list) and not result:
            return None, "API returned empty list (no candles)"
        elif isinstance(result, dict) and result.get('stat') == 'Ok':
            df = pd.DataFrame(result.get('data', []))
        elif isinstance(result, pd.DataFrame):
            df = result.copy()
        else:
            try:
                df = pd.DataFrame(result)
            except Exception:
                return None, f"unrecognised API response type: {type(result).__name__}"

        if df is None or df.empty:
            return None, "empty dataframe from API"

        col_map = {}
        for col in df.columns:
            cl = col.lower().strip()
            if cl == 'datetime':                    col_map[col] = 'datetime'
            elif cl in ('open','o'):                col_map[col] = 'Open'
            elif cl in ('high','h'):                col_map[col] = 'High'
            elif cl in ('low','l'):                 col_map[col] = 'Low'
            elif cl in ('close','c'):               col_map[col] = 'Close'
            elif cl in ('volume','vol','v'):        col_map[col] = 'Volume'
        df = df.rename(columns=col_map)

        required = ['Open','High','Low','Close','Volume']
        if not all(r in df.columns for r in required):
            return None, f"missing columns, got: {list(df.columns)}"

        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime')

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
            return None, "all rows dropped after cleaning (NaN OHLC)"

        if len(df) < 5 and not retry:
            time.sleep(0.3)
            return fetch_1min(sym, from_dt, to_dt, trade, cache, lock, retry=True)

        if len(df) < 5:
            return None, f"too few candles after retry: {len(df)}"

        return df, None

    except Exception as e:
        if DEBUG:
            import traceback
            print(f"[FETCH ERR] {sym}: {e}\n{traceback.format_exc()}")
        return None, f"unexpected error: {e}"

# =============================================================================
# RESAMPLE 1-MIN → 5-MIN  (anchored at 9:15 AM)
# =============================================================================
def to_5min(df):
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
            '5min', origin='start_day', offset='9h15min',
            label='left', closed='left'
        ).agg({'Open':'first','High':'max','Low':'min',
               'Close':'last','Volume':'sum'})
        return out[out['Close'].notna()].copy()
    except Exception:
        return pd.DataFrame()

# =============================================================================
# HELPERS
# =============================================================================
def calc_vwap(df):
    try:
        if df.empty:
            return pd.Series(dtype=float, index=df.index)
        tp  = (df['High'] + df['Low'] + df['Close']) / 3.0
        vol = df['Volume'].clip(lower=0)
        return ((tp * vol).cumsum() / vol.cumsum().replace(0, np.nan)).rename('VWAP')
    except Exception:
        return pd.Series(dtype=float, index=df.index)


def trend_r2_slope(closes):
    """
    Linear regression of closes vs candle-index.
    Returns (r2, slope).  r2 ∈ [0,1] — how close to a straight line.
    slope sign matches direction (positive = uptrend, negative = downtrend).
    """
    n = len(closes)
    if n < 4:
        return 0.0, 0.0
    try:
        x = np.arange(n, dtype=float)
        slope, intercept = np.polyfit(x, closes, 1)
        fitted = slope * x + intercept
        ss_res = float(np.sum((closes - fitted) ** 2))
        ss_tot = float(np.sum((closes - closes.mean()) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        return max(0.0, r2), slope
    except Exception:
        return 0.0, 0.0

# =============================================================================
# SCAN ONE STOCK
# =============================================================================
def scan_one(sym, scan_date, cutoff_dt, trade, cache, lock):
    """
    Returns (result_dict_or_None, status_string).
    result_dict always includes 'IsTrending' (bool) based on the two
    built-in checks (R² and Move%) — the UI splits on this flag.
    """
    tz = pytz.timezone('Asia/Kolkata')
    try:
        from_dt = tz.localize(datetime.combine(
            scan_date - timedelta(days=12), dt_time(9, 14)
        ))
        df1, err = fetch_1min(sym, from_dt, cutoff_dt, trade, cache, lock)
        if df1 is None:
            return None, f"FAILED: {err}"

        day_start = tz.localize(datetime.combine(scan_date, dt_time(9, 15)))
        df_today  = df1[
            (df1.index >= day_start) &
            (df1.index <= cutoff_dt)
        ].copy()

        if df_today.empty:
            return None, "FAILED: no candles for today in [9:15, cutoff]"
        if len(df_today) < 10:
            return None, f"FAILED: only {len(df_today)} candles today (need ≥10)"

        curr_price = float(df_today['Close'].iloc[-1])
        open_price = float(df_today['Open'].iloc[0])

        pdays = prev_trading_days(scan_date, 1)
        if not pdays:
            return None, "FAILED: could not determine previous trading day"
        pday = pdays[0]
        df_prev = df1[df1.index.date == pday]
        if df_prev.empty:
            return None, f"FAILED: no candles found for previous day ({pday})"

        prev_close = float(df_prev['Close'].iloc[-1])
        prev_high  = float(df_prev['High'].max())
        prev_low   = float(df_prev['Low'].min())

        if prev_close <= 0:
            return None, "FAILED: previous close is zero/invalid"

        total_move_pct   = (curr_price - prev_close) / prev_close * 100
        morning_move_pct = (curr_price - open_price) / open_price * 100
        gap_pct          = (open_price - prev_close) / prev_close * 100

        direction = 'BULL' if total_move_pct > 0 else ('BEAR' if total_move_pct < 0 else 'FLAT')

        # ── RVOL — display only ──────────────────────────────────────────
        today_vol = float(df_today['Volume'].sum())
        cutoff_t  = cutoff_dt.time()
        hist_vols = []
        for hd in prev_trading_days(scan_date, 5):
            dh = df1[df1.index.date == hd]
            if not dh.empty:
                win = dh[(dh.index.time >= dt_time(9,15)) &
                         (dh.index.time <= cutoff_t)]
                if not win.empty:
                    hist_vols.append(float(win['Volume'].sum()))
        if hist_vols:
            avg_vol   = float(np.mean(hist_vols))
            vol_ratio = today_vol / avg_vol if avg_vol > 0 else None
        else:
            vol_ratio = None

        # ── TREND QUALITY — R² + slope direction ─────────────────────────
        df5_today = to_5min(df_today)
        r2, slope = 0.0, 0.0
        vwap_val  = None
        vwap_side = '—'

        if not df5_today.empty and len(df5_today) >= 4:
            closes = df5_today['Close'].values.astype(float)
            r2, slope = trend_r2_slope(closes)
            vwap_series = calc_vwap(df5_today)
            if not vwap_series.empty:
                vwap_val = float(vwap_series.iloc[-1])
                if direction == 'BULL':
                    vwap_side = '✅ Above' if curr_price > vwap_val else '⚠️ Below'
                elif direction == 'BEAR':
                    vwap_side = '✅ Below' if curr_price < vwap_val else '⚠️ Above'

        # ── IS THIS A "TRENDING" STOCK? (the only filter) ────────────────
        slope_matches_dir = (
            (direction == 'BULL' and slope > 0) or
            (direction == 'BEAR' and slope < 0)
        )
        is_trending = (
            direction in ('BULL', 'BEAR') and
            r2 >= TREND_MIN_R2 and
            abs(total_move_pct) >= TREND_MIN_MOVE_PCT and
            slope_matches_dir
        )

        # ── ENTRY / SL / TARGET (10:45–10:59 candle) ─────────────────────
        entry_candles = pd.DataFrame()
        if not df5_today.empty:
            entry_candles = df5_today[
                (df5_today.index.time >= dt_time(10, 45)) &
                (df5_today.index.time <  dt_time(11,  0))
            ]
            if entry_candles.empty:
                entry_candles = df5_today.iloc[[-1]]

        entry = sl = target = risk = None
        ec_str = '—'
        if not entry_candles.empty:
            ec_high = float(entry_candles['High'].max())
            ec_low  = float(entry_candles['Low'].min())
            ec_str  = f"H:{round(ec_high,2)} / L:{round(ec_low,2)}"

            if direction == 'BULL':
                entry = round(ec_high, 2)
                sl    = round(ec_low,  2)
            elif direction == 'BEAR':
                entry = round(ec_low,  2)
                sl    = round(ec_high, 2)

            if entry is not None and sl is not None:
                risk = abs(entry - sl)
                if risk < entry * 0.001:
                    risk = entry * 0.003
                if direction == 'BULL':
                    target = round(entry + RR_RATIO * risk, 2)
                else:
                    target = round(entry - RR_RATIO * risk, 2)

        candle_str = ''
        if not df5_today.empty:
            last6 = df5_today.tail(6)
            parts = []
            for i in range(len(last6)):
                o = float(last6['Open'].iloc[i])
                c = float(last6['Close'].iloc[i])
                t = last6.index[i].strftime('%H:%M')
                parts.append(f"{'🟢' if c>=o else '🔴'}{t}")
            candle_str = ' '.join(parts)

        result = {
            'Symbol':        sym,
            'Direction':     direction,
            'IsTrending':    is_trending,
            'Signal':        ('🟢 BUY'   if direction == 'BULL' else
                              '🔴 SHORT' if direction == 'BEAR' else '⚪ FLAT'),
            'TotalMove%':    round(total_move_pct,   2),
            'MorningMove%':  round(morning_move_pct, 2),
            'Gap%':          round(gap_pct,           2),
            'CurrPrice':     round(curr_price,        2),
            'PrevClose':     round(prev_close,        2),
            'VolRatio':      round(vol_ratio, 2) if vol_ratio is not None else None,
            'TrendR2':       round(r2,                2),
            'VWAP':          round(vwap_val, 2) if vwap_val else None,
            'VWAPSide':      vwap_side,
            'Entry':         entry,
            'SL':            sl,
            'Target':        target,
            'Risk':          round(risk, 2) if risk is not None else None,
            'EntryCandle':   ec_str,
            'Candles':       candle_str,
            'PrevHigh':      round(prev_high, 2),
            'PrevLow':       round(prev_low,  2),
        }
        tag = "TRENDING" if is_trending else "weak/choppy"
        return result, f"OK ({direction}, {total_move_pct:+.2f}%, R²={r2:.2f}, {tag})"

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

    cache     = {}
    lock      = threading.Lock()
    done_lock = threading.Lock()
    all_res   = []
    diag      = {}
    done_ctr  = [0]
    total     = len(stocks)

    def _proc(sym):
        r, status = scan_one(sym, scan_date, cutoff_dt, trade, cache, lock)
        gc.collect()
        return sym, r, status

    workers = min(MAX_WORKERS, max(4, (os.cpu_count() or 4) * 3))

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

    # Trending-only lists (the MAIN output)
    trend_bull = sorted(
        [r for r in all_res if r['Direction'] == 'BULL' and r['IsTrending']],
        key=lambda r: r['TotalMove%'], reverse=True
    )
    trend_bear = sorted(
        [r for r in all_res if r['Direction'] == 'BEAR' and r['IsTrending']],
        key=lambda r: r['TotalMove%']
    )

    # Full reference lists (everything, for the expander)
    all_bull = sorted(
        [r for r in all_res if r['Direction'] == 'BULL'],
        key=lambda r: r['TotalMove%'], reverse=True
    )
    all_bear = sorted(
        [r for r in all_res if r['Direction'] == 'BEAR'],
        key=lambda r: r['TotalMove%']
    )

    return trend_bull, trend_bear, all_bull, all_bear, diag

# =============================================================================
# BACKTEST VERIFICATION
# =============================================================================
def verify_backtest(bull, bear, scan_date, trade, cache, lock):
    tz = pytz.timezone('Asia/Kolkata')
    fd = tz.localize(datetime.combine(scan_date - timedelta(days=2), dt_time(9, 0)))
    td = tz.localize(datetime.combine(scan_date,                     dt_time(15, 35)))

    def eod_close(sym):
        df1, _ = fetch_1min(sym, fd, td, trade, cache, lock)
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
def build_table(results, is_bull, is_backtest=False, show_trend_badge=False):
    if not results:
        return "<p style='color:var(--muted);padding:12px 0;'>No stocks in this list.</p>"

    hdr_bg = '#1B5E20' if is_bull else '#B71C1C'
    row_bg = '#F1F8E9' if is_bull else '#FCE4EC'
    row_fg = '#1B5E20' if is_bull else '#880E4F'

    bt_th = (
        '<th style="padding:9px 12px;border:1px solid #999;">EOD Close</th>'
        '<th style="padding:9px 12px;border:1px solid #999;">Result</th>'
    ) if is_backtest else ''

    trend_th = '<th style="padding:9px 12px;border:1px solid #999;">Trending?</th>' if show_trend_badge else ''

    def th(txt):
        return f'<th style="padding:9px 12px;border:1px solid #999;">{txt}</th>'

    header = (
        th('#') + th('Symbol') + th('Signal') +
        th('Total Move%') + th('Morning Move%') + th('Gap%') +
        th('Price ₹') + th('Vol Ratio') + th('Trend R²') +
        th('VWAP Side') +
        th('Entry ₹') + th('SL ₹') + th('Target ₹') +
        th('Last 6 Candles') +
        trend_th + bt_th
    )

    rows = []
    for i, r in enumerate(results, 1):
        sym   = r['Symbol'].replace('.NS','')
        vwap_s = r.get('VWAPSide', '—')

        r2    = r.get('TrendR2', 0)
        r2col = '#00C853' if r2 >= 0.7 else ('#FF8F00' if r2 >= 0.5 else '#D50000')
        r2bar = (f'<div style="display:flex;align-items:center;gap:6px">'
                 f'<div style="height:6px;width:{int(r2*60)}px;background:{r2col};'
                 f'border-radius:3px;min-width:4px"></div>'
                 f'<span>{r2:.2f}</span></div>')

        vr = r.get('VolRatio')
        if vr is None:
            vr_str = '—'
        else:
            vrcol = '#00C853' if vr >= 2.0 else ('#FF8F00' if vr >= 1.2 else '#888')
            vr_str = f'<b style="color:{vrcol}">{vr:.2f}×</b>'

        trend_td = ''
        if show_trend_badge:
            if r.get('IsTrending'):
                trend_td = '<td style="padding:9px 12px;border:1px solid #ddd;color:#00C853;font-weight:bold;">✅ Yes</td>'
            else:
                trend_td = '<td style="padding:9px 12px;border:1px solid #ddd;color:#999;">— No</td>'

        bt_td = ''
        if is_backtest:
            eod = r.get('EOD','—')
            res = r.get('Result','—')
            rc  = ('#00C853' if '✅' in res else '#D50000' if '❌' in res else '#FF8F00')
            bt_td = (
                f'<td style="padding:9px 12px;border:1px solid #ddd;">{eod if eod else "—"}</td>'
                f'<td style="padding:9px 12px;border:1px solid #ddd;color:{rc};font-weight:bold;">{res}</td>'
            )

        def td(val, extra=''):
            return f'<td style="padding:9px 12px;border:1px solid #ddd;{extra}">{val}</td>'

        tm = r['TotalMove%']; mm = r['MorningMove%']; gp = r['Gap%']
        tm_col = '#00C853' if tm > 0 else '#D50000'
        mm_col = '#00C853' if mm > 0 else '#D50000'
        gp_col = '#00C853' if gp > 0 else '#D50000'

        entry_str  = f'₹{r["Entry"]}'  if r["Entry"]  is not None else '—'
        sl_str     = f'₹{r["SL"]}'     if r["SL"]     is not None else '—'
        target_str = f'₹{r["Target"]}' if r["Target"] is not None else '—'

        rows.append(
            f'<tr style="background:{row_bg};color:{row_fg};">'
            + td(i,          'font-weight:900;')
            + td(sym,        'font-weight:900;font-size:14px;')
            + td(r['Signal'])
            + td(f'<b style="color:{tm_col}">{tm:+.2f}%</b>', 'font-size:13px;')
            + td(f'<span style="color:{mm_col}">{mm:+.2f}%</span>', 'font-size:13px;')
            + td(f'<span style="color:{gp_col}">{gp:+.2f}%</span>', 'font-size:12px;')
            + td(f'₹{r["CurrPrice"]}')
            + td(vr_str)
            + td(r2bar)
            + td(vwap_s,     'font-size:12px;')
            + td(f'<b>{entry_str}</b>',  'font-weight:bold;')
            + td(sl_str,    'color:#D50000;font-weight:bold;')
            + td(target_str,'color:#00796B;font-weight:bold;')
            + td(r.get('Candles',''), 'font-size:11px;white-space:nowrap;')
            + trend_td
            + bt_td
            + '</tr>'
        )

    return f"""
<div style="overflow-x:auto;margin-bottom:20px;">
<table style="width:100%;border-collapse:collapse;font-size:12px;">
<thead>
  <tr style="background:{hdr_bg};color:white;font-size:12px;">{header}</tr>
</thead>
<tbody>{''.join(rows)}</tbody>
</table>
</div>"""

# =============================================================================
# STREAMLIT UI
# =============================================================================
def main():
    st.set_page_config(page_title="EOD Trend Scanner", layout="wide", page_icon="📈")
    st.title("📈 EOD Trend Scanner")
    st.caption(
        "Run at 11:00 AM → only stocks with a clean, consistent trend since "
        "9:15 AM appear in BUY / SHORT. No sliders to configure."
    )

    for k, v in dict(trade=None, connected=False,
                     results=None, bt_stats=None,
                     uid='', auth='', skey='').items():
        if k not in st.session_state:
            st.session_state[k] = v

    try:
        if os.path.exists(CREDS_FILE):
            with open(CREDS_FILE) as f:
                c = json.load(f)
            st.session_state['uid']  = c.get('user_id','')
            st.session_state['auth'] = c.get('auth_code','')
            st.session_state['skey'] = c.get('secret_key','')
    except Exception:
        pass

    # ── LOGIN ─────────────────────────────────────────────────────────────
    with st.expander("🔐 Alice Blue Login", expanded=not st.session_state['connected']):
        c1, c2, c3 = st.columns(3)
        uid  = c1.text_input("User ID",    value=st.session_state['uid'])
        auth = c2.text_input("Auth Code",  value=st.session_state['auth'])
        skey = c3.text_input("Secret Key", value=st.session_state['skey'], type="password")

        b1, _, b2, b3 = st.columns([3,2,1,1])
        ph = st.empty()

        if b1.button("🔌 Connect", use_container_width=True):
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
                            st.session_state.update(trade=t, connected=True)
                            ph.success("✅ Connected!")
                            try:
                                with open(CREDS_FILE,'w') as f:
                                    json.dump(dict(user_id=uid,auth_code=auth,secret_key=skey),f)
                            except Exception:
                                pass
                            ok = True
                            break
                    except Exception:
                        continue
                if not ok:
                    ph.error("❌ Authentication failed.")

        if b2.button("💾 Save", use_container_width=True):
            try:
                with open(CREDS_FILE,'w') as f:
                    json.dump(dict(user_id=uid,auth_code=auth,secret_key=skey),f)
                st.success("Saved!")
            except Exception as e:
                st.error(str(e))

        if b3.button("🗑️", use_container_width=True):
            try:
                os.remove(CREDS_FILE); st.info("Cleared.")
            except Exception:
                pass

        if st.session_state['connected']:
            ph.success("✅ Alice Blue: Connected")

    # ── HOW IT WORKS ──────────────────────────────────────────────────────
    with st.expander("📖 How 'trending' is decided (no sliders — fixed logic)", expanded=False):
        st.markdown(f"""
A stock appears in **BUY** or **SHORT** only if **ALL** of these are true:

| Check | Rule | Why |
|---|---|---|
| **Direction** | Total Move % from yesterday's close is positive (BUY) or negative (SHORT) | Basic direction |
| **Consistency** | Trend R² ≥ **{TREND_MIN_R2}** | The 9:15→cutoff price move is a fairly straight line, not random zig-zag |
| **Magnitude** | \\|Total Move %\\| ≥ **{TREND_MIN_MOVE_PCT}%** | The move is real, not 0.05% noise |
| **Slope direction** | Regression slope matches the move direction | Confirms the trend is still developing the same way at cutoff |

**Total Move % = (Price at cutoff − Yesterday's Close) ÷ Yesterday's Close × 100**

Stocks that fail any check still appear in the **"All Scanned Stocks"** expander at the
bottom (sorted by Move %) so you can see everything — but only the trend-qualified
stocks are in the main BUY/SHORT lists.

**Entry / SL / Target** — based on the 10:45–10:59 AM candle:
- BUY: Entry = candle HIGH, SL = candle LOW, Target = Entry + 2×Risk
- SHORT: Entry = candle LOW, SL = candle HIGH, Target = Entry − 2×Risk
""")

    st.divider()

    # ── CONFIG — only Mode + Cutoff ─────────────────────────────────────
    st.markdown("### ⚙️ Settings")
    c1, c2, c3 = st.columns(3)

    with c1:
        mode    = st.radio("Mode", ["🔴 Live", "📅 Historical"], horizontal=True)
        is_live = mode == "🔴 Live"

    scan_date_input = None
    with c2:
        if not is_live:
            scan_date_input = st.date_input("Historical Date", value=last_trading_day())
        else:
            tz  = pytz.timezone('Asia/Kolkata')
            now = datetime.now(tz)
            st.info(f"🕐 **{now.strftime('%H:%M:%S')}**  |  {now.strftime('%d-%b-%Y')}")
            if not is_trading_day(now.date()):
                st.warning("📅 Market holiday today.")
            elif now.time() < dt_time(10, 30):
                st.warning("⏰ Wait till 10:30 AM before scanning.")
            else:
                st.success("✅ Ready to scan!")

    with c3:
        cutoff_opt = st.radio(
            "Data Cutoff", ["11:00 AM", "11:30 AM"], horizontal=True,
            help="Data fetched ONLY up to this time, even if you run later."
        )
        cutoff_h = 11
        cutoff_m = 0 if cutoff_opt == "11:00 AM" else 30

    # ── STOCK LIST ────────────────────────────────────────────────────────
    st.markdown("### 📋 Stocks to Scan")
    fo_stocks = [
        "360ONE","ABB","APLAPOLLO","AUBANK","ADANIENSOL","ADANIENT",
        "ADANIGREEN","ADANIPORTS","ABCAPITAL","ALKEM","AMBER","AMBUJACEM",
        "ANGELONE","APOLLOHOSP","ASHOKLEY","ASIANPAINT","ASTRAL",
        "AUROPHARMA","DMART","AXISBANK","BSE","BAJAJ-AUTO","BAJFINANCE",
        "BAJAJFINSV","BANDHANBNK","BANKBARODA","BANKINDIA","BDL","BEL",
        "BHARATFORG","BHEL","BPCL","BHARTIARTL","BIOCON","BLUESTARCO",
        "BOSCHLTD","BRITANNIA","CGPOWER","CANBK","CDSL","CHOLAFIN",
        "CIPLA","COALINDIA","COFORGE","COLPAL","CAMS","CONCOR",
        "CROMPTON","CUMMINSIND","CYIENT","DLF","DABUR","DALBHARAT",
        "DELHIVERY","DIVISLAB","DIXON","DRREDDY","ETERNAL","EICHERMOT",
        "EXIDEIND","NYKAA","FORTIS","GAIL","GMRAIRPORT","GLENMARK",
        "GODREJCP","GODREJPROP","GRASIM","HCLTECH","HDFCAMC","HDFCBANK",
        "HDFCLIFE","HFCL","HAVELLS","HEROMOTOCO","HINDALCO","HAL",
        "HINDPETRO","HINDUNILVR","HINDZINC","POWERINDIA","HUDCO",
        "ICICIBANK","ICICIGI","ICICIPRULI","IDFCFIRSTB","IIFL","ITC",
        "INDIANB","IEX","IOC","IRCTC","IRFC","IREDA","IGL",
        "INDUSTOWER","INDUSINDBK","NAUKRI","INFY","INOXWIND","INDIGO",
        "JINDALSTEL","JSWENERGY","JSWSTEEL","JIOFIN","JUBLFOOD","KEI",
        "KPITTECH","KALYANKJIL","KAYNES","KFINTECH","KOTAKBANK","LTF",
        "LICHSGFIN","LTIM","LT","LAURUSLABS","LICI","LODHA","LUPIN",
        "M&M","MANAPPURAM","MANKIND","MARICO","MARUTI","MFSL",
        "MAXHEALTH","MAZDOCK","MPHASIS","MCX","MUTHOOTFIN","NBCC",
        "NCC","NHPC","NMDC","NTPC","NATIONALUM","NESTLEIND","NUVAMA",
        "OBEROIRLTY","ONGC","OIL","PAYTM","OFSS","POLICYBZR","PGEL",
        "PIIND","PNBHOUSING","PAGEIND","PATANJALI","PERSISTENT",
        "PETRONET","PIDILITIND","PPLPHARMA","POLYCAB","PFC","POWERGRID",
        "PRESTIGE","PNB","RBLBANK","RECLTD","RVNL","RELIANCE",
        "SBICARD","SBILIFE","SHREECEM","SRF","SAMMAANCAP","MOTHERSON",
        "SHRIRAMFIN","SIEMENS","SOLARINDS","SONACOMS","SBIN","SAIL",
        "SUNPHARMA","SUPREMEIND","SUZLON","SYNGENE","TATACONSUM",
        "TITAGARH","TVSMOTOR","TCS","TATAELXSI","TATAPOWER","TATASTEEL",
        "TATATECH","TECHM","FEDERALBNK","INDHOTEL","PHOENIXLTD","TITAN",
        "TORNTPHARM","TORNTPOWER","TRENT","TIINDIA","UNOMINDA","UPL",
        "ULTRACEMCO","UNIONBANK","UNITDSPR","VBL","VEDL","IDEA",
        "VOLTAS","WIPRO","YESBANK","ZYDUSLIFE",
        "HYUNDAI","SWIGGY","PREMIERENE",
    ]

    stocks_txt = st.text_area("Stocks (one per line)", value="\n".join(fo_stocks), height=80)
    stocks = [s.strip().upper() for s in stocks_txt.split('\n') if s.strip()]
    st.caption(f"**{len(stocks)}** stocks in scan universe.")

    st.divider()

    run_btn   = st.button("▶️ RUN SCAN", use_container_width=True, type="primary")
    result_ph = st.empty()

    if run_btn:
        if not st.session_state['connected'] or not st.session_state['trade']:
            st.error("❌ Connect to Alice Blue first.")
        else:
            trade = st.session_state['trade']
            holiday_msg = None

            if is_live:
                today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
                if is_trading_day(today):
                    scan_date = today
                else:
                    scan_date = last_trading_day(today)
                    holiday_msg = (f"📅 Today is a holiday. "
                                   f"Scanning {scan_date.strftime('%d-%b-%Y')} instead.")
            else:
                scan_date = scan_date_input
                if not is_trading_day(scan_date):
                    holiday_msg = (f"⚠️ {scan_date.strftime('%d-%b-%Y')} "
                                   f"may be a holiday — data could be limited.")

            prog = st.progress(0)
            ptxt = st.empty()

            def on_progress(done, total, sym):
                pct = int(done / total * 100)
                prog.progress(pct)
                ptxt.text(f"⏳ {done}/{total}  ({pct}%)  ← {sym}")

            with st.spinner(f"Scanning {len(stocks)} stocks…"):
                trend_bull, trend_bear, all_bull, all_bear, diag = run_scan(
                    stocks, scan_date, cutoff_h, cutoff_m,
                    trade, status_cb=on_progress
                )

            prog.progress(100)
            ptxt.text(
                f"✅ Done — {len(trend_bull)} trending BUY  |  "
                f"{len(trend_bear)} trending SHORT  |  "
                f"{len(all_bull)+len(all_bear)} total movers"
            )

            bt_stats = None
            if not is_live and (trend_bull or trend_bear):
                with st.spinner("Checking actual EOD results…"):
                    c2_, l2_ = {}, threading.Lock()
                    trend_bull, trend_bear, bt_stats = verify_backtest(
                        trend_bull, trend_bear, scan_date, trade, c2_, l2_
                    )

            st.session_state['results'] = (
                trend_bull, trend_bear, all_bull, all_bear, diag,
                scan_date, not is_live, holiday_msg, cutoff_opt
            )
            st.session_state['bt_stats'] = bt_stats

    # ── SHOW RESULTS ──────────────────────────────────────────────────────
    if st.session_state.get('results') is not None:
        (trend_bull, trend_bear, all_bull, all_bear, diag,
         scan_date, is_hist, holiday_msg, cutoff_opt) = st.session_state['results']
        bt_stats = st.session_state.get('bt_stats')

        if holiday_msg:
            st.warning(holiday_msg)

        with result_ph.container():
            n_fail = sum(1 for v in diag.values() if v.startswith('FAILED'))
            n_ok   = len(diag) - n_fail

            st.markdown(f"## Results — {scan_date.strftime('%d %B %Y')}  |  Cutoff: {cutoff_opt}")

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("🟢 Trending BUY",   len(trend_bull))
            m2.metric("🔴 Trending SHORT", len(trend_bear))
            m3.metric("📈 All movers",     len(all_bull)+len(all_bear))
            m4.metric("✅ Fetched OK",     f"{n_ok}/{n_ok+n_fail}")

            if bt_stats and bt_stats['total'] > 0:
                st.divider()
                st.markdown("### 🔬 Backtest Result")
                bc1,bc2,bc3,bc4,bc5,bc6 = st.columns(6)
                bc1.metric("Trades",    bt_stats['total'])
                bc2.metric("🟢 Buy Win", bt_stats['bw'])
                bc3.metric("❌ Buy Loss",bt_stats['bl'])
                bc4.metric("🟢 Sell Win",bt_stats['sw'])
                bc5.metric("❌ Sell Loss",bt_stats['sl'])
                bc6.metric("🏆 Win Rate", f"{bt_stats['win_rate']}%")

            st.divider()

            # ── TRENDING BUY ──
            st.markdown(f"## 🟢 BULLISH TRENDING STOCKS — BUY  ({len(trend_bull)})")
            st.caption(
                f"R² ≥ {TREND_MIN_R2} and move ≥ +{TREND_MIN_MOVE_PCT}% since 9:15 AM, sorted by move. "
                "Enter on breakout of 10:45–10:59 candle HIGH. Exit 3:00 PM."
            )
            if trend_bull:
                st.markdown(build_table(trend_bull, is_bull=True, is_backtest=is_hist),
                            unsafe_allow_html=True)
            else:
                st.info(
                    "No bullish stock met the trend-consistency check today. "
                    "Check **All Scanned Stocks** below to see how close any "
                    "candidates were (their R² and Move% are shown there)."
                )

            st.divider()

            # ── TRENDING SHORT ──
            st.markdown(f"## 🔴 BEARISH TRENDING STOCKS — SHORT  ({len(trend_bear)})")
            st.caption(
                f"R² ≥ {TREND_MIN_R2} and move ≤ -{TREND_MIN_MOVE_PCT}% since 9:15 AM, sorted by move. "
                "Enter on breakdown of 10:45–10:59 candle LOW. Exit 3:00 PM."
            )
            if trend_bear:
                st.markdown(build_table(trend_bear, is_bull=False, is_backtest=is_hist),
                            unsafe_allow_html=True)
            else:
                st.info(
                    "No bearish stock met the trend-consistency check today. "
                    "Check **All Scanned Stocks** below to see how close any "
                    "candidates were (their R² and Move% are shown there)."
                )

            st.divider()

            st.markdown("""
### 📌 Trade Execution (Quick Reference)

| | BUY | SHORT |
|---|---|---|
| **Candle** | 10:45–10:59 AM | Same |
| **Entry** | Above candle HIGH | Below candle LOW |
| **Stop Loss** | Candle LOW | Candle HIGH |
| **Target** | 2× Risk extension | Same |
| **Exit time** | 3:00 PM hard stop | Same |
""")

            st.divider()

            # ── ALL SCANNED STOCKS (reference) ──
            with st.expander(
                f"📋 All Scanned Stocks — {len(all_bull)} up, {len(all_bear)} down "
                f"(sorted by Move%, includes non-trending)",
                expanded=False
            ):
                st.markdown("**All stocks that moved UP (Move% > 0)**")
                st.markdown(build_table(all_bull, is_bull=True, show_trend_badge=True),
                            unsafe_allow_html=True)
                st.markdown("**All stocks that moved DOWN (Move% < 0)**")
                st.markdown(build_table(all_bear, is_bull=False, show_trend_badge=True),
                            unsafe_allow_html=True)

            # ── DIAGNOSTICS ──
            with st.expander(
                f"🔧 Scan Diagnostics — {n_ok} OK, {n_fail} Failed",
                expanded=(n_fail > 0)
            ):
                diag_rows = [{'Symbol': s, 'Status': diag[s]} for s in sorted(diag.keys())]
                df_diag = pd.DataFrame(diag_rows)

                fails_only = st.checkbox("Show only failed stocks", value=(n_fail > 0))
                if fails_only:
                    df_diag = df_diag[df_diag['Status'].str.startswith('FAILED')]

                st.dataframe(df_diag, use_container_width=True, height=400)

                if n_fail > 0:
                    st.markdown("""
**Common reasons & fixes:**
- `instrument not found on Alice Blue` → symbol name doesn't match Alice Blue's master.
- `API returned empty list` → no data for that stock in this window.
- `too few candles` → market may not have opened yet, or stock is illiquid pre-cutoff.
- `no candles found for previous day` → previous trading day calc may be off for new listings.
""")


if __name__ == "__main__":
    main()
