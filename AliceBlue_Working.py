# File: Stock Candle Screener 30Min 4C/5C - Streamlit Version

import warnings
warnings.filterwarnings('ignore')

import streamlit as st
from TradeMaster.TradeSync import TradeHub, Exchange
import pytz
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import os, re
import json
import websocket
import hashlib
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# =====================================================================
# CONFIGURATION & CONSTANTS
# =====================================================================

MA_WINDOW = 44
CANDLES_BEFORE = 50
CANDLES_START = 5
SCAN_TIME = "11:45"

# Local credentials persistence file (keeps creds across app restarts)
# Use a single well-known credentials file in the user's home directory
CREDS_FILE = os.path.join(os.path.expanduser('~'), 'alice_creds.json')

# =====================================================================
# WEBSOCKET LIVE DATA FETCHING
# =====================================================================

def fetch_live_data_websocket(symbols, duration_minutes=2):
    """
    Fetch live streaming data for symbols using WebSocket.
    Returns OHLCV data aggregated into 1-minute candles.
    Works for individual traders via Alice Blue API.
    
    Args:
        symbols: List of symbols (e.g., ['ETERNAL.NS', 'INFY.NS'])
        duration_minutes: How many minutes of data to collect (default 2 for quick test)
    
    Returns:
        Dict with {symbol: DataFrame} containing Open, High, Low, Close, Volume
    """
    try:
        trade = st.session_state.get('alice_trade')
        if not trade:
            print("[ERROR] No trade object in session state")
            return {}
        
        # Get session ID
        session_id = trade.get_session_id()
        if not session_id or 'Not_ok' in str(session_id):
            print(f"[ERROR] Could not get valid session ID: {session_id}")
            return {}
        
        client_id = str(trade.client_id) if hasattr(trade, 'client_id') else "801426"
        print(f"[WEBSOCKET] Session ID obtained, Client ID: {client_id}")
        
        # Generate WebSocket token (SHA-256 encrypted session ID, twice)
        sha1 = hashlib.sha256(str(session_id).encode()).hexdigest()
        susertoken = hashlib.sha256(sha1.encode()).hexdigest()
        
        print(f"[WEBSOCKET] WebSocket token generated")
        print(f"[WEBSOCKET] Connecting to wss://ws1.aliceblueonline.com/NorenWS")
        
        # Use SDK websocket helpers (recommended by Alice Blue support)
        tick_data = {sym: [] for sym in symbols}
        socket_opened = False
        subscribe_flag = False
        subscribe_list = []

        def socket_open():
            nonlocal socket_opened, subscribe_flag
            print("[WEBSOCKET] Connected")
            socket_opened = True
            if subscribe_flag and subscribe_list:
                try:
                    trade.subscribe(subscribe_list)
                    print("[WEBSOCKET] Resubscribed after reconnect")
                except Exception as e:
                    print(f"[WEBSOCKET] Resubscribe failed: {e}")

        def socket_close():
            nonlocal socket_opened
            socket_opened = False
            print("[WEBSOCKET] Closed")

        def socket_error(message):
            print(f"[WEBSOCKET] Error : {message}")

        def feed_data(message):
            nonlocal subscribe_flag
            try:
                feed_message = json.loads(message)
            except Exception:
                return
            t = feed_message.get('t', '')
            if t in ('ck', 'cf'):
                # connection ack
                print(f"[WEBSOCKET] Connection Ack: {feed_message.get('s', feed_message)}")
                subscribe_flag = True
                return
            if t == 'tk':
                print(f"[WEBSOCKET] Token Ack: {feed_message}")
                return
            # tick feed
            if t in ('tf', 'df') or 'lp' in feed_message:
                token = str(feed_message.get('tk', ''))
                for symbol in symbols:
                    try:
                        sym_clean = symbol.split('.NS')[0]
                        inst = trade.get_instrument(exchange=Exchange.NSE, symbol=sym_clean)
                        if inst and str(inst.token) == token:
                            tick = {
                                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')),
                                'ltp': float(feed_message.get('lp', 0) or 0),
                                'volume': float(feed_message.get('v', 0) or 0),
                                'open': float(feed_message.get('o', 0) or 0),
                                'high': float(feed_message.get('h', 0) or 0),
                                'low': float(feed_message.get('l', 0) or 0),
                                'close': float(feed_message.get('c', 0) or 0),
                            }
                            tick_data[symbol].append(tick)
                    except Exception:
                        continue

        # Build subscribe_list using instrument tokens
        for sym in symbols:
            try:
                sym_clean = sym.split('.NS')[0]
                inst = trade.get_instrument(exchange=Exchange.NSE, symbol=sym_clean)
                if inst:
                    # SDK accepts instrument objects for subscribe
                    subscribe_list.append(inst)
            except Exception as e:
                print(f"[WEBSOCKET] Failed to get instrument for subscribe {sym}: {e}")

        # Start websocket using SDK
        try:
            trade.start_websocket(socket_open_callback=socket_open,
                                  socket_close_callback=socket_close,
                                  socket_error_callback=socket_error,
                                  subscription_callback=feed_data,
                                  run_in_background=True,
                                  market_depth=False)
        except Exception as e:
            print(f"[WEBSOCKET] start_websocket failed: {e}")
            return {}

        # Wait until socket opened
        start_time = time.time()
        while not socket_opened and (time.time() - start_time) < 15:
            time.sleep(0.1)

        # Subscribe
        try:
            if subscribe_list:
                trade.subscribe(subscribe_list)
                print(f"[WEBSOCKET] Subscribed to {len(subscribe_list)} instruments")
        except Exception as e:
            print(f"[WEBSOCKET] subscribe call failed: {e}")

        # Collect data for duration
        time.sleep(duration_minutes * 60)

        # Stop websocket
        try:
            trade.stop_websocket()
        except Exception as e:
            print(f"[WEBSOCKET] stop_websocket error: {e}")

        # Aggregate ticks into 1-minute candles
        result = {}
        for symbol, ticks in tick_data.items():
            if not ticks:
                print(f"[WEBSOCKET] No ticks for {symbol}")
                continue
            df = pd.DataFrame(ticks)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            candles = df.resample('1min').agg({
                'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'ltp': 'last'
            }).dropna()
            if len(candles) > 0:
                candles.columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'LTP']
                result[symbol] = candles
                print(f"[WEBSOCKET] {symbol}: Collected {len(candles)} candles")
        return result
        
    except Exception as e:
        print(f"[WEBSOCKET] ❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        return {}

# =====================================================================
# DATA FETCHING FUNCTIONS
# =====================================================================

def get_instrument_cached(symbol):
    """
    Get instrument with caching to avoid repeated API calls.
    """
    try:
        trade = st.session_state.get('alice_trade')
        if not trade:
            return None
        
        # Check cache first
        cache = st.session_state.get('instrument_cache', {})
        if symbol in cache:
            return cache[symbol]
        
        # Clean symbol and try to fetch
        sym_clean = symbol.split('.NS')[0] if symbol.upper().endswith('.NS') else symbol
        inst = trade.get_instrument(exchange=Exchange.NSE, symbol=sym_clean)
        
        if inst:
            cache[symbol] = inst
            st.session_state.instrument_cache = cache
        return inst
    except Exception:
        return None

def fetch_5min_data(symbol, start_date=None, end_date=None):
    """
    Fetch 5-minute OHLCV data using Alice Blue (Ant-A3) SDK only.
    Returns pandas DataFrame indexed by timezone-aware Asia/Kolkata DatetimeIndex
    with columns exactly: ['Open','High','Low','Close','Volume'].
    Returns None on failure.
    """
    try:
        trade = st.session_state.get('alice_trade')
        if not trade:
            print(f"[ERROR] {symbol}: No trade object in session state")
            return None

        # Normalize symbol (remove .NS suffix if present)
        sym = symbol.split('.NS')[0] if symbol.upper().endswith('.NS') else symbol

        # Build datetime range
        tz = pytz.timezone('Asia/Kolkata')
        if start_date is None:
            to_dt = datetime.now(tz)
            from_dt = to_dt - timedelta(days=7)
        else:
            try:
                from_dt = pd.to_datetime(start_date)
                if from_dt.tzinfo is None:
                    from_dt = tz.localize(from_dt)
            except Exception as e:
                print(f"[ERROR] {symbol}: Failed to parse start_date {start_date}: {e}")
                from_dt = datetime.now(tz) - timedelta(days=7)
            if end_date is None:
                to_dt = from_dt + timedelta(days=1)
            else:
                try:
                    to_dt = pd.to_datetime(end_date)
                    if to_dt.tzinfo is None:
                        to_dt = tz.localize(to_dt)
                except Exception as e:
                    print(f"[ERROR] {symbol}: Failed to parse end_date {end_date}: {e}")
                    to_dt = from_dt + timedelta(days=1)

        print(f"[FETCH] {symbol}: Fetching from {from_dt} to {to_dt} (symbol cleaned: {sym})")

        inst = get_instrument_cached(symbol)
        if inst is None:
            print(f"[ERROR] {symbol}: Could not get instrument object")
            return None

        df = None
        last_error = None
        
        # Resolution "1" works reliably - use only that for speed
        try:
            result = trade.get_HistoricalData(
                instrument=inst,
                resolution="1",
                from_datetime=from_dt,
                to_datetime=to_dt,
                indices=False
            )
            
            # Handle list response (most common)
            if isinstance(result, list) and len(result) > 0:
                df = pd.DataFrame(result)
            # Handle dict response
            elif isinstance(result, dict):
                if 'data' in result and result.get('stat') == 'Ok':
                    df = pd.DataFrame(result['data'])
                elif 'emsg' not in result and result.get('stat') != 'Ok':
                    print(f"[FETCH] {symbol}: API error - {result.get('stat', 'Unknown')}")
            # Handle DataFrame-like object
            elif isinstance(result, pd.DataFrame):
                df = result
            else:
                try:
                    df = pd.DataFrame(result)
                except Exception:
                    df = None
                    
        except Exception as e:
            last_error = str(e)
            print(f"[FETCH] {symbol}: Fetch failed - {e}")
        
        if df is None:
            print(f"[ERROR] {symbol}: DataFrame is None. Error: {last_error}")
            return None
        
        if hasattr(df, 'empty') and df.empty:
            print(f"[ERROR] {symbol}: DataFrame is empty after fetch")
            return None

        # Ensure we have a proper DataFrame
        if not isinstance(df, pd.DataFrame):
            print(f"[ERROR] {symbol}: Result is not a DataFrame after conversion")
            return None

        df = df.copy()
        
        # Print actual columns for debugging
        actual_cols = list(df.columns) if hasattr(df, 'columns') else []
        print(f"[FETCH] {symbol}: Actual dataframe columns: {actual_cols}")
        
        # Handle datetime column if present (Alice Blue returns 'datetime' column)
        if 'datetime' in actual_cols:
            print(f"[FETCH] {symbol}: Found 'datetime' column, setting as index...")
            try:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
            except Exception as e:
                print(f"[ERROR] {symbol}: Failed to set datetime as index: {e}")
        
        # Check for required columns (case-insensitive)
        required = ['Open', 'High', 'Low', 'Close', 'Volume']
        cols_lower = {col.lower(): col for col in df.columns}
        missing_cols = []
        col_mapping = {}
        
        for req in required:
            req_lower = req.lower()
            if req_lower in cols_lower:
                col_mapping[req] = cols_lower[req_lower]
            else:
                missing_cols.append(req)
        
        if missing_cols:
            print(f"[ERROR] {symbol}: Missing required columns: {missing_cols}. Available: {actual_cols}")
            return None
        
        # Rename columns to standard names if needed
        rename_dict = {v: k for k, v in col_mapping.items() if v != k}
        if rename_dict:
            print(f"[FETCH] {symbol}: Renaming columns: {rename_dict}")
            df = df.rename(columns=rename_dict)

        try:
            if df.index.tzinfo is None and df.index.tz is None:
                # API returns naive IST times - localize directly without UTC conversion
                df.index = pd.to_datetime(df.index).tz_localize('Asia/Kolkata')
            else:
                df.index = df.index.tz_convert('Asia/Kolkata')
        except Exception as e:
            print(f"[ERROR] {symbol}: Timezone conversion failed: {e}")
            try:
                # Fallback: try direct localization
                df.index = pd.to_datetime(df.index).tz_localize('Asia/Kolkata')
            except Exception as e2:
                print(f"[ERROR] {symbol}: Timezone conversion failed (retry): {e2}")
                return None

        df = df.sort_index()
        df = df[["Open", "High", "Low", "Close", "Volume"]]
        df = df[~df.index.duplicated(keep='last')]
        if df.empty:
            print(f"[ERROR] {symbol}: DataFrame became empty after processing")
            return None
        
        print(f"[FETCH] {symbol}: Fetched {len(df)} candles")
        return df
    except Exception as e:
        print(f"[ERROR] {symbol}: Outer exception in fetch_5min_data: {e}")
        import traceback
        traceback.print_exc()
        return None


def fetch_5min_data_worker(symbol, start_date=None, end_date=None, trade_obj=None, inst_cache=None, inst_cache_lock=None):
    """
    Thread-safe worker version of fetch_5min_data that uses provided `trade_obj`
    and a local `inst_cache` dict instead of accessing `st.session_state`.
    Returns the same DataFrame or None on failure.
    """
    try:
        trade = trade_obj
        if not trade:
            print(f"[WORKER ERROR] {symbol}: No trade object provided")
            return None

        # Normalize symbol
        sym = symbol.split('.NS')[0] if symbol.upper().endswith('.NS') else symbol

        tz = pytz.timezone('Asia/Kolkata')
        if start_date is None:
            to_dt = datetime.now(tz)
            from_dt = to_dt - timedelta(days=7)
        else:
            try:
                from_dt = pd.to_datetime(start_date)
                if from_dt.tzinfo is None:
                    from_dt = tz.localize(from_dt)
            except Exception:
                from_dt = datetime.now(tz) - timedelta(days=7)
            if end_date is None:
                to_dt = from_dt + timedelta(days=1)
            else:
                try:
                    to_dt = pd.to_datetime(end_date)
                    if to_dt.tzinfo is None:
                        to_dt = tz.localize(to_dt)
                except Exception:
                    to_dt = from_dt + timedelta(days=1)

        print(f"[FETCH-WORKER] {symbol}: Fetching from {from_dt} to {to_dt} (symbol cleaned: {sym})")

        # Use provided instrument cache mapping first
        inst = None
        try:
            if inst_cache and symbol in inst_cache:
                inst = inst_cache.get(symbol)
            else:
                try:
                    sym_clean = symbol.split('.NS')[0] if symbol.upper().endswith('.NS') else symbol
                    inst = trade.get_instrument(exchange=Exchange.NSE, symbol=sym_clean)
                    if inst_cache is not None:
                        try:
                            if inst_cache_lock is not None:
                                with inst_cache_lock:
                                    inst_cache[symbol] = inst
                            else:
                                inst_cache[symbol] = inst
                        except Exception:
                            pass
                except Exception:
                    inst = None
        except Exception:
            inst = None

        if inst is None:
            print(f"[WORKER ERROR] {symbol}: Could not get instrument object")
            return None

        df = None
        last_error = None
        try:
            result = trade.get_HistoricalData(
                instrument=inst,
                resolution="1",
                from_datetime=from_dt,
                to_datetime=to_dt,
                indices=False
            )

            if isinstance(result, list) and len(result) > 0:
                df = pd.DataFrame(result)
            elif isinstance(result, dict):
                if 'data' in result and result.get('stat') == 'Ok':
                    df = pd.DataFrame(result['data'])
                elif 'emsg' not in result and result.get('stat') != 'Ok':
                    print(f"[FETCH-WORKER] {symbol}: API error - {result.get('stat', 'Unknown')}")
            elif isinstance(result, pd.DataFrame):
                df = result
            else:
                try:
                    df = pd.DataFrame(result)
                except Exception:
                    df = None
        except Exception as e:
            last_error = str(e)
            print(f"[FETCH-WORKER] {symbol}: Fetch failed - {e}")

        if df is None:
            print(f"[WORKER ERROR] {symbol}: DataFrame is None. Error: {last_error}")
            return None

        if hasattr(df, 'empty') and df.empty:
            print(f"[WORKER ERROR] {symbol}: DataFrame is empty after fetch")
            return None

        if not isinstance(df, pd.DataFrame):
            print(f"[WORKER ERROR] {symbol}: Result is not a DataFrame after conversion")
            return None

        df = df.copy()
        actual_cols = list(df.columns) if hasattr(df, 'columns') else []
        if 'datetime' in actual_cols:
            try:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
            except Exception:
                pass

        required = ['Open', 'High', 'Low', 'Close', 'Volume']
        cols_lower = {col.lower(): col for col in df.columns}
        missing_cols = []
        col_mapping = {}
        for req in required:
            req_lower = req.lower()
            if req_lower in cols_lower:
                col_mapping[req] = cols_lower[req_lower]
            else:
                missing_cols.append(req)

        if missing_cols:
            print(f"[WORKER ERROR] {symbol}: Missing required columns: {missing_cols}. Available: {actual_cols}")
            return None

        rename_dict = {v: k for k, v in col_mapping.items() if v != k}
        if rename_dict:
            df = df.rename(columns=rename_dict)

        try:
            if df.index.tzinfo is None and df.index.tz is None:
                df.index = pd.to_datetime(df.index).tz_localize('Asia/Kolkata')
            else:
                df.index = df.index.tz_convert('Asia/Kolkata')
        except Exception:
            try:
                df.index = pd.to_datetime(df.index).tz_localize('Asia/Kolkata')
            except Exception:
                return None

        df = df.sort_index()
        df = df[["Open", "High", "Low", "Close", "Volume"]]
        df = df[~df.index.duplicated(keep='last')]
        if df.empty:
            return None

        print(f"[FETCH-WORKER] {symbol}: Fetched {len(df)} candles")
        return df
    except Exception as e:
        print(f"[WORKER ERROR] {symbol}: Outer exception in fetch_5min_data_worker: {e}")
        return None


def _validate_with_broker(symbol, last_close, breakout_bull, breakout_bear):
    """
    Validate breakout signals using Alice Blue daily data (previous close).
    Conservative: only suppress a breakout if broker data clearly contradicts it.
    Returns (breakout_bull, breakout_bear).
    """
    try:
        trade = st.session_state.get('alice_trade')
        if not trade:
            return breakout_bull, breakout_bear

        sym = symbol.split('.NS')[0] if symbol.upper().endswith('.NS') else symbol
        inst = get_instrument_cached(symbol)
        if not inst:
            return breakout_bull, breakout_bear

        tz = pytz.timezone('Asia/Kolkata')
        today = datetime.now(tz).date()
        prev_day = today - timedelta(days=1)
        from_dt = datetime.combine(prev_day, datetime.min.time())
        to_dt = datetime.combine(prev_day, datetime.max.time())
        from_dt = tz.localize(from_dt)
        to_dt = tz.localize(to_dt)

        try:
            df = trade.get_HistoricalData(instrument=inst, resolution="D", from_datetime=from_dt, to_datetime=to_dt, indices=False)
        except Exception:
            df = None

        if df is None or df.empty:
            return breakout_bull, breakout_bear

        try:
            prev_close = float(df['Close'].iloc[-1])
            if breakout_bull and last_close <= prev_close:
                breakout_bull = False
            if breakout_bear and last_close >= prev_close:
                breakout_bear = False
        except Exception:
            pass
        return breakout_bull, breakout_bear
    except Exception:
        return breakout_bull, breakout_bear


def resample_to_30min(df):
    """
    Resample the 1-minute data to 30-minute intervals using pandas resampling.
    Anchor to 09:15 market open.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    
    df = df.sort_index().copy()
    
    # Ensure we're in Asia/Kolkata timezone (should already be from fetch)
    if df.index.tz is None:
        df.index = df.index.tz_localize('Asia/Kolkata')
    elif str(df.index.tz) != 'Asia/Kolkata':
        df.index = df.index.tz_convert('Asia/Kolkata')
    
    # Filter market hours: 09:15 to 15:30
    df['time'] = df.index.time
    df = df[(df['time'] >= pd.Timestamp('09:15').time()) & 
            (df['time'] <= pd.Timestamp('15:30').time())]
    
    if df.empty:
        return pd.DataFrame()
    
    # Use pandas native resampling, anchored to 09:15
    try:
        df_30min = df[['Open', 'High', 'Low', 'Close', 'Volume']].resample(
            '30min', origin='start_day', offset='9h15min', label='left', closed='left'
        ).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        })

        # Remove rows with NaN (no data for that 30-min period)
        # Keep partially formed candles that have a Close (avoid dropping valid partials)
        # (Replaced dropna with explicit Close notna filter to preserve other fields)
        df_30min = df_30min[df_30min['Close'].notna()]

        # Add date column for filtering (use pandas method for timezone-aware index)
        df_30min['date'] = df_30min.index.to_series().dt.normalize().dt.date

        return df_30min
    except Exception as e:
        print(f"[ERROR] Resampling to 30min failed: {e}")
        return pd.DataFrame()


def get_snapshot_datetime_for_date(date_obj, time_str=None):
    """
    Return a timezone-aware `Asia/Kolkata` datetime for a given `date_obj` and time string `HH:MM`.
    """
    if time_str is None:
        time_str = '11:45'
    
    try:
        hh, mm = [int(x) for x in time_str.split(":")]
    except Exception:
        hh, mm = 11, 45
    dt = datetime.combine(date_obj, datetime.min.time()).replace(hour=hh, minute=mm, second=0, microsecond=0)
    try:
        import pytz
        tz = pytz.timezone('Asia/Kolkata')
        return tz.localize(dt)
    except Exception:
        return pd.Timestamp(dt).tz_localize('Asia/Kolkata')

def get_last_trading_day(from_date=None):
    """
    Get the last trading day (NSE working day) before or on the given date.
    Excludes Saturdays, Sundays, and major Indian market holidays.
    """
    if from_date is None:
        from_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
    
    # 2026 Indian market holidays (NSE closed)
    market_holidays = {
        (1, 26),   # Republic Day
        (2, 26),   # Maha Shivaratri
        (3, 15),   # Holi
        (4, 10),   # Good Friday
        (5, 24),   # Eid-ul-Fitr
        (5, 26),   # Buddha Purnima
        (8, 15),   # Independence Day
        (8, 27),   # Janmashtami
        (9, 30),   # Milad-un-Nabi
        (10, 2),   # Gandhi Jayanti
        (10, 12),  # Dussehra
        (10, 24),  # Diwali (Lakshmi Puja)
        (10, 25),  # Diwali (Govardhan Puja)
        (11, 1),   # Dev Deepavali
        (11, 11),  # Guru Nanak Jayanti
        (12, 25),  # Christmas
    }
    
    check_date = from_date
    for _ in range(30):  # Look back up to 30 days
        if check_date.weekday() < 5 and (check_date.month, check_date.day) not in market_holidays:
            return check_date
        check_date -= timedelta(days=1)
    
    return from_date

# =====================================================================
# SCREENING LOGIC FUNCTION
# =====================================================================

def run_screening(stocks, mode, selected_date, timeframe, candle_count, results_container=None, require_unusual_volume=True, status_text=None):
    """
    Main screening function that returns results list
    Optionally updates results_container in real-time
    """
    global CANDLES_START, SCAN_TIME
    
    # Map timeframe and candle count to settings (30m only)
    if candle_count == "4C":
        CANDLES_START = 4
        SCAN_TIME = "11:15"
    elif candle_count == "5C":
        CANDLES_START = 5
        SCAN_TIME = "11:45"
    elif candle_count == "6C":
        CANDLES_START = 6
        SCAN_TIME = "12:15"
    else:
        CANDLES_START = 5
        SCAN_TIME = "11:45"
    
    results = []
    total_stocks = len(stocks)
    progress_bar = st.progress(0)
    progress_text = st.empty()
    status_text = st.empty()
    
    tz = pytz.timezone('Asia/Kolkata')
    if mode == "Historical":
        try:
            start_date = datetime.strptime(selected_date, "%Y-%m-%d")
            today = datetime.now(tz)
            days_diff = (today.date() - start_date.date()).days
            if days_diff < 0 or days_diff > 30:
                status_text.error("Historical intraday data is limited. Please select a recent weekday within the last 30 days.")
                return results
            if start_date.weekday() > 4:
                prev_day = start_date
                while prev_day.weekday() > 4:
                    prev_day -= timedelta(days=1)
                status_text.info(f"Selected date {start_date.date()} is weekend. Using previous working day {prev_day.date()} for screening.")
                start_date = prev_day
            # If selected date is today, fetch up to now
            if start_date.date() == today.date():
                start_dt = today.replace(hour=9, minute=15, second=0, microsecond=0)
                end_dt = today
            else:
                start_dt = tz.localize(datetime.combine(start_date.date(), datetime.min.time()).replace(hour=9, minute=15))
                end_dt = tz.localize(datetime.combine(start_date.date(), datetime.min.time()).replace(hour=15, minute=30))
            fetch_start = (start_dt - timedelta(days=10)).strftime("%Y-%m-%d")
            fetch_end = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            filter_date = start_date.date()
        except ValueError:
            status_text.error("Invalid date format. Please use YYYY-MM-DD.")
            return results
    else:
        today = datetime.now(tz)
        last_trading_day = get_last_trading_day(today.date())
        
        # If today is not a trading day, show notification and use last trading day
        if today.date() != last_trading_day:
            if date.today().weekday() >= 5:
                msg = f"📅 Weekend detected! Using last trading day ({last_trading_day.strftime('%Y-%m-%d')}) for Live scan."
            else:
                msg = f"🏖️ Market holiday today! Using last trading day ({last_trading_day.strftime('%Y-%m-%d')}) for Live scan."
            status_text.warning(msg)
            # Use last trading day as the live scan date
            live_scan_date = last_trading_day
        else:
            status_text.info("📊 Live mode: Scanning today's market data")
            live_scan_date = today.date()

        start_dt = tz.localize(datetime.combine(live_scan_date, datetime.min.time()).replace(hour=9, minute=15))
        end_dt = datetime.now(tz)
        fetch_start = (start_dt - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
        fetch_end = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        filter_date = live_scan_date

    # NOTE: skipping sequential prefetch of instruments to avoid startup delay.
    # Workers will resolve instruments into the local `inst_cache` concurrently.

    # Fetch 5-min data in parallel (limit workers to avoid broker rate limits)
    results_data = {}
    trade_obj = st.session_state.get('alice_trade')
    inst_cache = {}
    inst_cache_lock = threading.Lock()
    if not trade_obj:
        status_text.error("❌ Not connected to Alice Blue. Please generate session before running scan.")
        return results
    try:
        # Seed local cache from session cache to reduce duplicate instrument calls
        if isinstance(st.session_state.get('instrument_cache', None), dict):
            inst_cache.update(st.session_state.get('instrument_cache', {}))
    except Exception:
        pass

    # FETCH PHASE (parallel)
    results_data = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_symbol = {
            executor.submit(fetch_5min_data_worker, symbol, fetch_start, fetch_end, trade_obj, inst_cache, inst_cache_lock): symbol
            for symbol in stocks
        }

        for idx, future in enumerate(as_completed(future_to_symbol)):
            symbol = future_to_symbol[future]
            try:
                df_5min = future.result()
                results_data[symbol] = df_5min
            except Exception as e:
                print(f"[SCAN ERROR] {symbol}: Fetch failed - {e}")
                results_data[symbol] = None

            # Show single unified progress 0-50% for fetch phase
            progress_percent = int(((idx + 1) / total_stocks) * 50) if total_stocks else 0
            progress_bar.progress(progress_percent)
            progress_text.text(f"Scanning: {progress_percent}% ({idx+1}/{total_stocks})")

    # Merge local inst_cache back into session cache
    try:
        sc = st.session_state.get('instrument_cache', {})
        sc.update(inst_cache)
        st.session_state.instrument_cache = sc
    except Exception:
        pass

    # PROCESS PHASE (sequential but shows results in real-time)
    processed_count = 0
    for idx2, (symbol, df_5min) in enumerate(results_data.items()):
        if df_5min is None or (hasattr(df_5min, 'empty') and df_5min.empty):
            processed_count += 1
            progress_percent = 50 + int((processed_count / total_stocks) * 50)
            progress_bar.progress(progress_percent)
            progress_text.text(f"Scanning: {progress_percent}%")
            continue

        # Resample 5-min data to 30-min
        df_30min_30m_full = resample_to_30min(df_5min)
        df_30min_30m_full = df_30min_30m_full.sort_index()
        df_30min_30m_full['date'] = df_30min_30m_full.index.to_series().dt.normalize().dt.date

        # Relax early-session full-data length rejection: allow processing with >=30 resampled candles
        if len(df_30min_30m_full) < 30:
            processed_count += 1
            progress_percent = 50 + int((processed_count / total_stocks) * 50)
            progress_bar.progress(progress_percent)
            progress_text.text(f"Scanning: {progress_percent}%")
            continue

        # Use 30m resampled data (full set prior to snapshot trimming)
        df_30min_full = df_30min_30m_full.copy()

        available_dates = sorted(set(df_30min_full['date']))
        chosen_date = filter_date
        is_today = (filter_date == datetime.now(pytz.timezone('Asia/Kolkata')).date())
        if filter_date not in available_dates:
            prior_dates = [d for d in available_dates if d < filter_date]
            if prior_dates:
                chosen_date = max(prior_dates)
            else:
                processed_count += 1
                progress_percent = 50 + int((processed_count / total_stocks) * 50)
                progress_bar.progress(progress_percent)
                progress_text.text(f"Scanning: {progress_percent}%")
                continue

        # Apply strict snapshot locking: only use data up to the snapshot datetime
        # Determine snapshot_time from selected candle_count / SCAN_TIME
        snapshot_time = SCAN_TIME if SCAN_TIME else ("11:45" if candle_count == "5C" else ("11:15" if candle_count=="4C" else "12:15"))
        snapshot_dt = get_snapshot_datetime_for_date(chosen_date, snapshot_time)

        # In Live mode, prevent using future (not-yet-completed) candles
        if mode == "Live":
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            if snapshot_dt > current_time:
                snapshot_dt = current_time

        # Trim the full 30-min dataset strictly to the snapshot datetime BEFORE any selection/analysis
        df_30min_30m_full = df_30min_30m_full[df_30min_30m_full.index <= snapshot_dt]

        # Now select the day's 30-min candles (only those up to snapshot_dt)
        day_30min = df_30min_30m_full[df_30min_30m_full['date'] == chosen_date]
        
        if len(day_30min) < CANDLES_START:
            # If we don't have enough candles up to the snapshot, we cannot evaluate this symbol for the chosen candle_count
            processed_count += 1
            progress_percent = 50 + int((processed_count / total_stocks) * 50)
            progress_bar.progress(progress_percent)
            progress_text.text(f"Scanning: {progress_percent}%")
            continue
        
        first5 = day_30min.head(CANDLES_START)

        if timeframe == '30m':
            ma44_source = df_30min_30m_full
        else:
            ma44_source = df_30min_30m_full

        # Early termination check for MA data (relaxed to 30 periods to allow early-session signals)
        prior_44 = ma44_source[ma44_source['date'] < chosen_date].tail(44)
        if len(prior_44) < 30:
            processed_count += 1
            progress_percent = 50 + int((processed_count / total_stocks) * 50)
            progress_bar.progress(progress_percent)
            progress_text.text(f"Scanning: {progress_percent}%")
            continue

        MA_TOLERANCE_PCT = 0.0005
        ma44_series = ma44_source['Close'].rolling(window=MA_WINDOW, min_periods=MA_WINDOW).mean()
        if ma44_series.count() < MA_WINDOW:
            try:
                ma44_series_fallback = ma44_source['Close'].rolling(window=MA_WINDOW, min_periods=1).mean()
                ma44_series = ma44_series_fallback
            except Exception:
                pass
        
        ma44_list = []
        for ma_idx in first5.index:
            try:
                if ma_idx in ma44_series.index:
                    ma44_val = ma44_series.loc[ma_idx]
                else:
                    pos = ma44_series.index.get_indexer([ma_idx], method='pad')[0]
                    if pos == -1:
                        ma44_val = None
                    else:
                        ma44_val = ma44_series.iloc[pos]
            except Exception:
                ma44_val = None
            ma44_list.append(ma44_val)

        # --- ANALYSIS LOGIC (UNCHANGED) ---
        prior_data = df_30min_full[df_30min_full.index < first5.index[0]].tail(44)
        supports = []
        for i in range(1, len(prior_data)-1):
            if (prior_data['Low'].iloc[i] < prior_data['Low'].iloc[i-1] and 
                prior_data['Low'].iloc[i] < prior_data['Low'].iloc[i+1]):
                supports.append(prior_data['Low'].iloc[i])
        resistances = []
        for i in range(1, len(prior_data)-1):
            if (prior_data['High'].iloc[i] > prior_data['High'].iloc[i-1] and 
                prior_data['High'].iloc[i] > prior_data['High'].iloc[i+1]):
                resistances.append(prior_data['High'].iloc[i])
        current_price = first5['Close'].iloc[-1]
        closest_support = min(supports, key=lambda x: abs(x - current_price)) if supports else None
        closest_resistance = min(resistances, key=lambda x: abs(x - current_price)) if resistances else None
        price_near_level = False
        if closest_support and closest_resistance:
            support_dist = abs(current_price - closest_support) / closest_support
            resistance_dist = abs(current_price - closest_resistance) / closest_resistance
            price_near_level = min(support_dist, resistance_dist) < 0.01

        # Calculate RSI once at the start for this stock
        if 'RSI' not in df_30min_30m_full.columns:
            delta = df_30min_30m_full['Close'].diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.ewm(span=14, adjust=False).mean()
            avg_loss = loss.ewm(span=14, adjust=False).mean()
            rs = avg_gain / (avg_loss + 1e-10)
            df_30min_30m_full['RSI'] = 100 - (100 / (1 + rs))

            vol_delta = delta * df_30min_30m_full['Volume']
            vol_gain = vol_delta.clip(lower=0)
            vol_loss = -vol_delta.clip(upper=0)
            vol_avg_gain = vol_gain.ewm(span=14, adjust=False).mean()
            vol_avg_loss = vol_loss.ewm(span=14, adjust=False).mean()
            vol_rs = vol_avg_gain / (vol_avg_loss + 1e-10)
            df_30min_30m_full['VRSI'] = 100 - (100 / (1 + vol_rs))

        try:
            rsi_at_4th = df_30min_30m_full.loc[first5.index[-1], 'RSI']
            vrsi_at_4th = df_30min_30m_full.loc[first5.index[-1], 'VRSI']
        except Exception:
            rsi_at_4th = None
            vrsi_at_4th = None

        volume_profile = []
        price_levels = []
        for i in range(CANDLES_START):
            candle = first5.iloc[i]
            vol = candle['Volume']
            price = (candle['High'] + candle['Low']) / 2
            volume_profile.append(vol)
            price_levels.append(price)

            # Use full resampled 30-min data for volume calculations
            df_30min = df_30min_full
            avg_volume = df_30min['Volume'].mean()
        institutional_volume = all(v > 1.2 * avg_volume for v in volume_profile)

        body_ratios = []
        shadows_ratios = []
        for i in range(CANDLES_START):
            o = first5['Open'].iloc[i]
            c = first5['Close'].iloc[i]
            h = first5['High'].iloc[i]
            l = first5['Low'].iloc[i]
            body = abs(c - o)
            rng = h - l
            body_ratio = (body / rng) if rng > 0 else 0
            if c >= o:
                upper_shadow = h - c
                lower_shadow = o - l
            else:
                upper_shadow = h - o
                lower_shadow = c - l
            shadow_ratio = ((upper_shadow + lower_shadow) / rng) if rng > 0 else 0
            body_ratios.append(body_ratio)
            shadows_ratios.append(shadow_ratio)

        price_gaps = [abs(first5['Close'].iloc[i] - first5['Open'].iloc[i+1])/first5['Close'].iloc[i] 
                     for i in range(CANDLES_START-1)]
        no_gaps = all(gap < 0.003 for gap in price_gaps)

        price_range = abs(first5['Close'].iloc[-1] - first5['Close'].iloc[0])
        avg_candle_size = (first5['High'] - first5['Low']).mean()
        current_volatility = first5['High'].sub(first5['Low']).std()
        historical_volatility = prior_data['High'].sub(prior_data['Low']).std()
        volatility_ratio = current_volatility / historical_volatility if historical_volatility > 0 else 1
        price_acceleration = [
            first5['Close'].iloc[i] - first5['Close'].iloc[i-1] for i in range(1, CANDLES_START)
        ]
        accelerating_momentum = all(price_acceleration[i] >= price_acceleration[i-1] 
                                 for i in range(1, len(price_acceleration)))
        vwap = (first5['Close'] * first5['Volume']).sum() / first5['Volume'].sum()
        vwap_trend = first5['Close'].iloc[-1] > vwap
        adx = 0.0
        if len(prior_data) >= 20:
            adx_window = 14
            tr1 = prior_data['High'] - prior_data['Low']
            tr2 = abs(prior_data['High'] - prior_data['Close'].shift(1))
            tr3 = abs(prior_data['Low'] - prior_data['Close'].shift(1))
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(adx_window).mean()
            pos_dm = (prior_data['High'] - prior_data['High'].shift(1)).clip(lower=0)
            neg_dm = (prior_data['Low'].shift(1) - prior_data['Low']).clip(lower=0)
            pos_dm[pos_dm < neg_dm] = 0
            neg_dm[neg_dm < pos_dm] = 0
            pos_di = 100 * (pos_dm.rolling(adx_window).mean() / atr)
            neg_di = 100 * (neg_dm.rolling(adx_window).mean() / atr)
            dx = 100 * np.abs(pos_di - neg_di) / (pos_di + neg_di + 1e-10)
            adx = dx.rolling(adx_window).mean().iloc[-1]
            strong_trend = adx > 25
        else:
            strong_trend = True

        strong_momentum = (
            price_range > (2 * avg_candle_size) and
            accelerating_momentum and
            no_gaps and
            institutional_volume and
            strong_trend and
            volatility_ratio > 0.8 and
            volatility_ratio < 2.0 and
            (price_near_level or vwap_trend)
        )

        short_ma = df_30min['Close'].rolling(5).mean()
        medium_ma = df_30min['Close'].rolling(10).mean()
        long_ma = df_30min['Close'].rolling(20).mean()
        last_idx = df_30min.index.get_loc(first5.index[-1])
        ma_alignment_bull = (short_ma.iloc[last_idx] > medium_ma.iloc[last_idx] > long_ma.iloc[last_idx])
        ma_alignment_bear = (short_ma.iloc[last_idx] < medium_ma.iloc[last_idx] < long_ma.iloc[last_idx])
        closes_increasing = all(first5['Close'].iloc[i] > first5['Close'].iloc[i-1] for i in range(1, CANDLES_START))
        closes_decreasing = all(first5['Close'].iloc[i] < first5['Close'].iloc[i-1] for i in range(1, CANDLES_START))
        
        tol = MA_TOLERANCE_PCT
        bullish_ma = all(
            (ma44_list[i] is not None) and
            (first5['Open'].iloc[i] > ma44_list[i] * (1 + tol)) and
            (first5['High'].iloc[i] > ma44_list[i] * (1 + tol)) and
            (first5['Low'].iloc[i] > ma44_list[i] * (1 + tol)) and
            (first5['Close'].iloc[i] > ma44_list[i] * (1 + tol))
            for i in range(CANDLES_START)
        )
        bearish_ma = all(
            (ma44_list[i] is not None) and
            (first5['Open'].iloc[i] < ma44_list[i] * (1 - tol)) and
            (first5['High'].iloc[i] < ma44_list[i] * (1 - tol)) and
            (first5['Low'].iloc[i] < ma44_list[i] * (1 - tol)) and
            (first5['Close'].iloc[i] < ma44_list[i] * (1 - tol))
            for i in range(CANDLES_START)
        )
        
        avg_vol_recent = first5['Volume'].mean()
        avg_vol_prior = df_30min[df_30min['date'] < chosen_date].tail(44)['Volume'].mean()
        strong_volume = avg_vol_recent > 1.5 * avg_vol_prior
        strong_bodies = all(r > 0.6 for r in body_ratios)
        
        bullish_ma_touch = bullish_ma
        bearish_ma_touch = bearish_ma
        
        prev_day_data = df_30min[df_30min['date'] < chosen_date].tail(13)
        prev_high = prev_day_data['High'].max() if not prev_day_data.empty else None
        prev_low = prev_day_data['Low'].min() if not prev_day_data.empty else None
        last_close = first5['Close'].iloc[-1]
        breakout_bull = (prev_high is not None) and (last_close > prev_high)
        breakout_bear = (prev_low is not None) and (last_close < prev_low)

        # SKIPPED: Broker validation disabled for speed (it was making sequential API calls)
        # Uncomment if you need conservative validation:
        # try:
        #     breakout_bull, breakout_bear = _validate_with_broker(symbol, last_close, breakout_bull, breakout_bear)
        # except Exception:
        #     pass
        
        volume_strong = (avg_vol_prior is not None) and (avg_vol_recent > 1.5 * avg_vol_prior)
        rsi_bull = (rsi_at_4th is not None) and (rsi_at_4th > 60)
        rsi_bear = (rsi_at_4th is not None) and (rsi_at_4th < 40)

        def calculate_signal_score(conds, is_bull=True):
            categories = {
                'candle_pattern': ['strong_bodies', 'clean_shadows', 'bullish_candles' if is_bull else 'bearish_candles', 'higher_highs' if is_bull else 'lower_lows'],
                'trend': ['above_ma' if is_bull else 'below_ma', 'resistance_break' if is_bull else 'support_break', 'clean_moves', 'ma_aligned', 'near_level', 'strong_trend'],
                'volume': ['strong_volume', 'inst_buying' if is_bull else 'inst_selling', 'above_vwap' if is_bull else 'below_vwap', 'vol_trend_aligned'],
                'technical': ['momentum', 'acceleration', 'rsi_strong' if is_bull else 'rsi_weak', 'volatility_good']
            }
            weights = {'candle_pattern': 25, 'trend': 30, 'volume': 25, 'technical': 20}
            breakdown = {}
            total = 0.0
            for cat, keys in categories.items():
                valid = 0
                possible = 0
                for k in keys:
                    if k in conds:
                        possible += 1
                        try:
                            if bool(conds[k]):
                                valid += 1
                        except Exception:
                            pass
                score = (valid / possible * 100) if possible else 0
                breakdown[cat] = score
                total += score * (weights[cat] / 100)
            return total, breakdown

        try:
            df_1h = df_5min.resample('60min', origin='start').agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
            }).dropna()
            ma20_1h = df_1h['Close'].rolling(20).mean()
            df_1h = df_1h.loc[df_1h.index <= first5.index[-1]]
            if not df_1h.empty and len(ma20_1h.dropna()) >= 2:
                ma20_1h_last = ma20_1h.loc[df_1h.index[-1]]
                ma20_1h_prev = ma20_1h.loc[df_1h.index[-2]] if len(ma20_1h.dropna()) >= 2 else ma20_1h_last
                ma20_1h_slope = (ma20_1h_last - ma20_1h_prev) / ma20_1h_prev * 100 if ma20_1h_prev else 0
            else:
                ma20_1h_last = None
                ma20_1h_slope = 0
        except Exception:
            ma20_1h_last = None
            ma20_1h_slope = 0

        ma1h_confirmation_bull = ma20_1h_last is not None and first5['Close'].iloc[-1] > ma20_1h_last
        ma1h_confirmation_bear = ma20_1h_last is not None and first5['Close'].iloc[-1] < ma20_1h_last

        breakout_retest_bull = False
        breakout_retest_bear = False
        if prev_high is not None:
            try:
                prior_lows = first5['Low'].iloc[:-1]
                breakout_retest_bull = breakout_bull and (prior_lows.min() < prev_high)
            except Exception:
                breakout_retest_bull = breakout_bull
        if prev_low is not None:
            try:
                prior_highs = first5['High'].iloc[:-1]
                breakout_retest_bear = breakout_bear and (prior_highs.max() > prev_low)
            except Exception:
                breakout_retest_bear = breakout_bear

        pos_di_last = None
        neg_di_last = None
        try:
            if len(prior_data) >= 20:
                pos_di_last = pos_di.iloc[-1]
                neg_di_last = neg_di.iloc[-1]
        except Exception:
            pos_di_last = None
            neg_di_last = None

        adx_strong_bull = (adx is not None and adx > 30 and pos_di_last is not None and pos_di_last > neg_di_last)
        adx_strong_bear = (adx is not None and adx > 30 and neg_di_last is not None and neg_di_last > pos_di_last)

        try:
            if ma44_list and ma44_list[0] and ma44_list[CANDLES_START-1]:
                ma44_slope_pct = (ma44_list[CANDLES_START-1] - ma44_list[0]) / ma44_list[0] * 100
            else:
                ma44_slope_pct = 0
        except Exception:
            ma44_slope_pct = 0

        ma44_slope_bull = ma44_slope_pct > 0.2
        ma44_slope_bear = ma44_slope_pct < -0.2

        try:
            atr_last = atr.iloc[-1] if 'atr' in locals() and not atr.isna().all() else None
            atr_pct = (atr_last / last_close * 100) if (atr_last and last_close) else None
        except Exception:
            atr_pct = None

        atr_ok = (atr_pct is not None) and (0.4 <= atr_pct <= 3.5)

        try:
            last_range = first5['High'].iloc[-1] - first5['Low'].iloc[-1]
            avg_range = (first5['High'] - first5['Low']).mean()
            range_expansion = last_range > 1.2 * avg_range
        except Exception:
            range_expansion = False
        try:
            volume_expansion = first5['Volume'].iloc[-1] > 1.8 * avg_vol_recent
        except Exception:
            volume_expansion = False

        # --- VOLUME LOGIC (CORE) ---
        try:
            prior_for_avg = ma44_source[ma44_source.index < first5.index[0]]
            avg_volume_20 = prior_for_avg['Volume'].tail(20).mean() if not prior_for_avg.empty else None
        except Exception:
            avg_volume_20 = None

        try:
            current_vol = float(first5['Volume'].iloc[-1])
        except Exception:
            current_vol = None

        try:
            unusual_volume = (avg_volume_20 is not None) and (current_vol is not None) and (current_vol >= 2.0 * avg_volume_20)
        except Exception:
            unusual_volume = False

        try:
            vol_seq_ok = False
            pos = None
            try:
                pos = df_30min.index.get_loc(first5.index[-1])
            except Exception:
                try:
                    pos = df_30min.index.get_indexer_for([first5.index[-1]])[0]
                except Exception:
                    pos = None

            if pos is not None and pos >= 2:
                v_prev = float(df_30min['Volume'].iloc[pos-1])
                v_prev2 = float(df_30min['Volume'].iloc[pos-2])
                vol_seq_ok = (current_vol is not None) and (current_vol > v_prev) and (v_prev > v_prev2)
            else:
                if len(first5) >= 3:
                    v_prev = float(first5['Volume'].iloc[-2])
                    v_prev2 = float(first5['Volume'].iloc[-3])
                    vol_seq_ok = (current_vol is not None) and (current_vol > v_prev) and (v_prev > v_prev2)
        except Exception:
            vol_seq_ok = False

        try:
            volume_confirmed = bool(unusual_volume and vol_seq_ok)
        except Exception:
            volume_confirmed = False

        try:
            last_o = first5['Open'].iloc[-1]
            last_h = first5['High'].iloc[-1]
            last_l = first5['Low'].iloc[-1]
            last_c = first5['Close'].iloc[-1]
        except Exception:
            last_o = last_h = last_l = last_c = None

        try:
            bullish_volume_confirmed = (
                volume_confirmed and last_c is not None and last_o is not None and last_h is not None and last_l is not None
                and (last_c > last_o) and (last_c >= (last_h - (last_h - last_l) * 0.25))
            )
        except Exception:
            bullish_volume_confirmed = False

        try:
            bearish_volume_confirmed = (
                volume_confirmed and last_c is not None and last_o is not None and last_h is not None and last_l is not None
                and (last_c < last_o) and (last_c <= (last_l + (last_h - last_l) * 0.25))
            )
        except Exception:
            bearish_volume_confirmed = False

        # If user opted NOT to consider unusual volume, override confirmations
        try:
            if not require_unusual_volume:
                bullish_volume_confirmed = True
                bearish_volume_confirmed = True
        except Exception:
            pass

        ma1h_slope_ok_bull = ma20_1h_slope > 0
        ma1h_slope_ok_bear = ma20_1h_slope < 0

        delta = df_30min['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / (loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))

        bullish_candle_pattern = (body_ratios[-1] > 0.6 and 
                                first5['Close'].iloc[-1] > first5['Open'].iloc[-1] and
                                first5['Close'].iloc[-1] > first5['Close'].iloc[-2])

        bearish_candle_pattern = (body_ratios[-1] > 0.6 and 
                                first5['Close'].iloc[-1] < first5['Open'].iloc[-1] and
                                first5['Close'].iloc[-1] < first5['Close'].iloc[-2])

        bullish_trend = all(ma44_list[i] < ma44_list[i+1] for i in range(len(ma44_list)-1))
        bearish_trend = all(ma44_list[i] > ma44_list[i+1] for i in range(len(ma44_list)-1))
        volume_ok = first5['Volume'].iloc[-1] > 1.2 * avg_vol_recent
        rsi_ok = (30 <= rsi.iloc[-1] <= 70) if not rsi.empty else False

        bull_score, bull_breakdown = calculate_signal_score({
            'candle_pattern': bullish_candle_pattern,
            'trend': bullish_trend,
            'volume': volume_ok,
            'technical': rsi_ok
        }, is_bull=True)

        bear_score, bear_breakdown = calculate_signal_score({
            'candle_pattern': bearish_candle_pattern,
            'trend': bearish_trend,
            'volume': volume_ok,
            'technical': rsi_ok
        }, is_bull=False)

        bullish_conditions = {
            'strong_bodies': strong_bodies and all(r > 0.65 for r in body_ratios),
            'clean_shadows': all(s < 0.25 for s in shadows_ratios),
            'bullish_candles': all(first5['Close'] > first5['Open']),
            'higher_highs': closes_increasing,
            'above_ma': bullish_ma_touch,
            'resistance_break': breakout_bull,
            'clean_moves': no_gaps,
            'ma_aligned': ma_alignment_bull,
            'near_level': price_near_level,
            'strong_trend': strong_trend,
            'strong_volume': volume_strong,
            'inst_buying': institutional_volume,
            'above_vwap': vwap_trend,
            'vol_trend_aligned': vrsi_at_4th > 60,
            'momentum': strong_momentum,
            'acceleration': accelerating_momentum,
            'rsi_strong': rsi_bull and (rsi_at_4th < 80),
            'volatility_good': 0.8 < volatility_ratio < 2.0
        }

        bearish_conditions = {
            'strong_bodies': strong_bodies and all(r > 0.65 for r in body_ratios),
            'clean_shadows': all(s < 0.25 for s in shadows_ratios),
            'bearish_candles': all(first5['Close'] < first5['Open']),
            'lower_lows': closes_decreasing,
            'below_ma': bearish_ma_touch,
            'support_break': breakout_bear,
            'clean_moves': no_gaps,
            'ma_aligned': ma_alignment_bear,
            'near_level': price_near_level,
            'strong_trend': strong_trend,
            'strong_volume': volume_strong,
            'inst_selling': institutional_volume,
            'below_vwap': not vwap_trend,
            'vol_trend_aligned': vrsi_at_4th < 40,
            'momentum': strong_momentum,
            'acceleration': accelerating_momentum,
            'rsi_weak': rsi_bear and (rsi_at_4th > 20),
            'volatility_good': 0.8 < volatility_ratio < 2.0
        }

        bull_score, bull_breakdown = calculate_signal_score(bullish_conditions, is_bull=True)
        bear_score, bear_breakdown = calculate_signal_score(bearish_conditions, is_bull=False)

        SURE_THRESHOLD = 75.0

        bull_essential = all([
            bullish_conditions.get('strong_bodies', False),
            bullish_conditions.get('bullish_candles', False),
            bullish_conditions.get('above_ma', False),
            bullish_conditions.get('strong_volume', False),
            bullish_conditions.get('momentum', False)
        ])

        bear_essential = all([
            bearish_conditions.get('strong_bodies', False),
            bearish_conditions.get('bearish_candles', False),
            bearish_conditions.get('below_ma', False),
            bearish_conditions.get('strong_volume', False),
            bearish_conditions.get('momentum', False)
        ])

        sure_bullish = (bull_score >= SURE_THRESHOLD) and bull_essential
        sure_bearish = (bear_score >= SURE_THRESHOLD) and bear_essential

        conditions_bullish = (
            all(first5['Close'] > first5['Open']) and
            bullish_ma
        )
        conditions_bearish = (
            all(first5['Close'] < first5['Open']) and
            bearish_ma
        )

        if sure_bullish:
            signal = "Sure Bullish"
        elif sure_bearish:
            signal = "Sure Bearish"
        else:
            signal = "Bullish" if conditions_bullish else "Bearish" if conditions_bearish else "No Signal"

        try:
            if signal == "Sure Bullish" and not bullish_volume_confirmed:
                signal = "Bullish"
            if signal == "Bullish" and not bullish_volume_confirmed:
                signal = "No Signal"

            if signal == "Sure Bearish" and not bearish_volume_confirmed:
                signal = "Bearish"
            if signal == "Bearish" and not bearish_volume_confirmed:
                signal = "No Signal"
        except Exception:
            pass

        volume_status = "High" if first5['Volume'].mean() > df_30min['Volume'].mean() else "Low"

        first_open = first5.iloc[0]['Open']
        last_close = first5.iloc[len(first5) - 1]['Close']
        if first_open and last_close:
            move_pct = ((last_close - first_open) / first_open) * 100
            if move_pct > 0.5:
                top_mover = f"Top Up ({move_pct:.2f}%)"
                top_gainer_loser = "Top Gainer"
            elif move_pct < -0.5:
                top_mover = f"Top Down ({move_pct:.2f}%)"
                top_gainer_loser = "Top Loser"
            else:
                top_mover = f"No Move ({move_pct:.2f}%)"
                top_gainer_loser = "No Change"
        else:
            top_mover = "NA"
            top_gainer_loser = "NA"

        try:
            prior_30min = df_30min[df_30min.index < first5.index[0]] if (not df_30min.empty and len(first5) > 0) else pd.DataFrame()
            prior_vol = prior_30min['Volume'].tail(44).mean() if len(prior_30min) >= 44 else None
        except Exception:
            prior_30min = pd.DataFrame()
            prior_vol = None
        first5_vol = first5['Volume'].mean()
        volume_spike = "Yes" if prior_vol and first5_vol > 1.5 * prior_vol else "No"

        ma_distance = "NA"
        try:
            if ma44_list and len(ma44_list) >= CANDLES_START and ma44_list[CANDLES_START-1] and len(first5) >= CANDLES_START and first5.iloc[CANDLES_START-1]['Close']:
                ma_distance_val = ((first5.iloc[CANDLES_START-1]['Close'] - ma44_list[CANDLES_START-1]) / ma44_list[CANDLES_START-1]) * 100
                ma_distance = f"{ma_distance_val:.2f}%"
        except Exception:
            ma_distance = "NA"

        strength_score = 0
        if all(first5['Close'] > first5['Open']):
            strength_score += 2
        elif all(first5['Close'] < first5['Open']):
            strength_score -= 2
        if volume_spike == "Yes":
            strength_score += 1 if signal == "Bullish" else -1
        try:
            if abs(float(ma_distance.strip('%'))) > 1.5:
                strength_score += 1 if signal == "Bullish" else -1
        except:
            pass

        result = {
            "Symbol": symbol,
            "Signal": signal,
            "Vol Confirm": "YES" if (bullish_volume_confirmed or bearish_volume_confirmed) else "NO",
            "bullish_vol_confirmed": bool(bullish_volume_confirmed),
            "bearish_vol_confirmed": bool(bearish_volume_confirmed),
            "Strength": strength_score,
            "Volume": volume_status,
            "Top Mover": top_mover,
            "Top": top_gainer_loser,
            "Volume Spike": volume_spike,
            "MA Distance": ma_distance,
            "1st Open": first5.iloc[0]['Open'] if len(first5) > 0 else None,
            "1st Close": first5.iloc[0]['Close'] if len(first5) > 0 else None,
            "2nd Open": first5.iloc[1]['Open'] if len(first5) > 1 else None,
            "2nd Close": first5.iloc[1]['Close'] if len(first5) > 1 else None,
            "3rd Open": first5.iloc[2]['Open'] if len(first5) > 2 else None,
            "3rd Close": first5.iloc[2]['Close'] if len(first5) > 2 else None,
            "4th Open": first5.iloc[3]['Open'] if len(first5) > 3 else None,
            "4th Close": first5.iloc[3]['Close'] if len(first5) > 3 else None,
            "5th Open": first5.iloc[4]['Open'] if len(first5) > 4 else None,
            "5th Close": first5.iloc[4]['Close'] if len(first5) > 4 else None,
            "6th Open": first5.iloc[5]['Open'] if len(first5) > 5 else None,
            "6th Close": first5.iloc[5]['Close'] if len(first5) > 5 else None,
            "1st 44MA": ma44_list[0] if len(ma44_list) > 0 else None,
            "2nd 44MA": ma44_list[1] if len(ma44_list) > 1 else None,
            "3rd 44MA": ma44_list[2] if len(ma44_list) > 2 else None,
            "4th 44MA": ma44_list[3] if len(ma44_list) > 3 else None,
            "5th 44MA": (ma44_list[4] if len(ma44_list) > 4 and CANDLES_START >= 5 else None),
            "6th 44MA": (ma44_list[5] if len(ma44_list) > 5 and CANDLES_START >= 6 else None),
        }
        results.append(result)
        processed_count += 1
        progress_percent = 50 + int((processed_count / total_stocks) * 50)
        progress_bar.progress(progress_percent)
        progress_text.text(f"Scanning: {progress_percent}%")
        
        # Update results container in real-time if provided
        if results_container is not None:
            st.session_state.screening_results = results.copy()

    progress_bar.progress(100)
    progress_text.text("Scanning: 100%")
    status_text.text(f"✅ Scan complete! Found {len(results)} signals.")
    print(f"[DEBUG] Screening finished. Total results: {len(results)}")
    
    return results

# =====================================================================
# STREAMLIT UI
# =====================================================================

def main():
    st.set_page_config(page_title="Stock Candle Screener", layout="wide")
    st.title("🔍 Stock Candle Screener 30Min 4C/5C/6C")
    
    # Alice Blue Credentials UI
    st.markdown("### 🔐 Alice Blue Credentials")
    
    # Load persisted credentials from local file (survive restarts)
    try:
        if os.path.exists(CREDS_FILE):
            with open(CREDS_FILE, 'r', encoding='utf-8') as f:
                _creds = json.load(f)
            st.session_state['saved_user_id'] = _creds.get('user_id', '')
            st.session_state['saved_auth_code'] = _creds.get('auth_code', '')
            st.session_state['saved_secret_key'] = _creds.get('secret_key', '')
        else:
            st.session_state['saved_user_id'] = ""
            st.session_state['saved_auth_code'] = ""
            st.session_state['saved_secret_key'] = ""
    except Exception as e:
        print(f"[CRED] Failed to load creds file: {e}")
        st.session_state['saved_user_id'] = ""
        st.session_state['saved_auth_code'] = ""
        st.session_state['saved_secret_key'] = ""

    cola, colb, colc = st.columns([3,3,4])
    with cola:
        user_id = st.text_input("User ID / Client ID", value=st.session_state['saved_user_id'], placeholder="e.g., ABC123", help="Your Alice Blue trading account ID", key="user_id_input")
    with colb:
        auth_code = st.text_input("Auth Code / App Key", value=st.session_state['saved_auth_code'], placeholder="OAuth code or App Key", help="From Alice Blue API dashboard", key="auth_code_input")
    with colc:
        secret_key = st.text_input("Secret Key / App Secret", value=st.session_state['saved_secret_key'], placeholder="e.g., abc123xyz", help="Your API Secret Key", type="password", key="secret_key_input")

    # Save/Clear logic (session only)
    gen_col1, gen_col2, save_col, clear_col = st.columns([1, 3, 1, 1])
    with gen_col1:
        gen_session = st.button("Generate Session", use_container_width=True, key="gen_session_btn")
    with gen_col2:
        conn_status_placeholder = st.empty()
    with save_col:
        if st.button("💾 Save Creds", use_container_width=True, help="Save credentials (persist across restarts)", key="save_creds_btn"):
            st.session_state['saved_user_id'] = user_id
            st.session_state['saved_auth_code'] = auth_code
            st.session_state['saved_secret_key'] = secret_key
            try:
                with open(CREDS_FILE, 'w', encoding='utf-8') as f:
                    json.dump({
                        'user_id': user_id,
                        'auth_code': auth_code,
                        'secret_key': secret_key
                    }, f)
                st.success("✅ Credentials saved to disk", icon="✔️")
            except Exception as e:
                st.error(f"❌ Failed to save credentials: {e}")
    with clear_col:
        if st.button("🗑️ Clear Creds", use_container_width=True, help="Clear saved credentials", key="clear_creds_btn"):
            st.session_state['saved_user_id'] = ""
            st.session_state['saved_auth_code'] = ""
            st.session_state['saved_secret_key'] = ""
            try:
                if os.path.exists(CREDS_FILE):
                    os.remove(CREDS_FILE)
                st.info("🗑️ Credentials cleared from disk", icon="ℹ️")
            except Exception as e:
                st.error(f"❌ Failed to clear credentials file: {e}")

    # Initialize session state for SDK connection and results storage
    if 'alice_trade' not in st.session_state:
        st.session_state.alice_trade = None
    if 'alice_connected' not in st.session_state:
        st.session_state.alice_connected = False
    if 'screening_results' not in st.session_state:
        st.session_state.screening_results = None
    if 'scan_running' not in st.session_state:
        st.session_state.scan_running = False
    if 'saved_user_id' not in st.session_state:
        st.session_state.saved_user_id = ""
    if 'saved_auth_code' not in st.session_state:
        st.session_state.saved_auth_code = ""
    if 'saved_secret_key' not in st.session_state:
        st.session_state.saved_secret_key = ""
    if 'instrument_cache' not in st.session_state:
        st.session_state.instrument_cache = {}  # Cache for instrument objects

    # Handle session generation
    if gen_session:
        if not user_id or not auth_code or not secret_key:
            conn_status_placeholder.error("❌ All fields required: User ID, Auth Code, Secret Key")
        else:
            with conn_status_placeholder.container():
                st.info("🔄 Attempting to authenticate...")
            
            connected = False
            trade_obj = None
            error_detail = ""
            
            attempts = [
                ("Standard (user_id, auth_code, secret_key)", lambda: TradeHub(user_id=user_id, auth_code=auth_code, secret_key=secret_key)),
                ("Alternative (auth_code as auth)", lambda: TradeHub(user_id=user_id, auth_code=secret_key, secret_key=auth_code)),
                ("Direct init", lambda: TradeHub(user_id, auth_code, secret_key)),
            ]
            
            for attempt_name, attempt_func in attempts:
                try:
                    trade_obj = attempt_func()
                    session_id = trade_obj.get_session_id()
                    
                    if session_id and str(session_id).strip() and 'Not_ok' not in str(session_id):
                        connected = True
                        st.session_state.alice_trade = trade_obj
                        st.session_state.alice_connected = True
                        # persist successful user id to creds file automatically
                        try:
                            with open(CREDS_FILE, 'w', encoding='utf-8') as f:
                                json.dump({'user_id': user_id, 'auth_code': auth_code, 'secret_key': secret_key}, f)
                            print("[CRED] Saved credentials to disk after successful connect")
                        except Exception as e:
                            print(f"[CRED] Failed to save creds after connect: {e}")
                        with conn_status_placeholder.container():
                            st.success(f"✅ **Connected!** Session obtained via: {attempt_name}")
                            st.write(f"Session ID (truncated): `{str(session_id)[:32]}...`")
                        break
                except Exception as e:
                    error_detail = str(e)
                    continue
            
            if not connected:
                with conn_status_placeholder.container():
                    st.error("❌ **Authentication Failed**")
                    st.write("**Possible causes:**")
                    st.write("1. Invalid **Auth Code** (check Alice Blue developer dashboard)")
                    st.write("2. Invalid **User ID** (verify your trading account ID)")
                    st.write("3. Invalid **Secret Key** (ensure you copied it correctly)")
                    st.write("4. Credentials expired or revoked")
                    if error_detail:
                        st.code(error_detail, language="text")
                    st.write("**Next steps:**")
                    st.write("- Verify all credentials in Alice Blue Dashboard")
                    st.write("- Regenerate API keys if necessary")
                    st.write("- Contact Alice Blue support if issue persists")

    # Display connection status
    if st.session_state.alice_connected:
        with conn_status_placeholder.container():
            st.success("✅ **Alice Blue: Connected & Ready**")
    elif not gen_session:
        conn_status_placeholder.warning("⚠️ Alice Blue: Not connected — Enter credentials above")

    # ===== SINGLE COLUMN LAYOUT: CONFIG, BUTTON, RESULTS VERTICALLY =====
    st.markdown("### ⚙️ Configuration")
    
    st.warning("💡 **TIP**: For best results, use **'Live' mode** to scan today's data in real-time. Historical mode requires complete trading day data (09:15-15:30).")
    
    # Mode and Timeframe side-by-side
    col1, col2 = st.columns(2, gap="small")
    
    with col1:
        st.markdown("**1️⃣ Scan Mode**")
        mode = st.radio("Select Mode", ["Live", "Historical"], label_visibility="collapsed", horizontal=True)
    
    with col2:
        st.markdown("**2️⃣ Timeframe**")
        timeframe = "30m"  # Only 30m mode supported
        st.info("📊 Scanning in 30-minute interval mode")
    
    # Historical date selector
    if mode == "Historical":
        selected_date = st.date_input("Select Date (Last 7 trading days)", label_visibility="collapsed")
        selected_date = selected_date.strftime("%Y-%m-%d")
    else:
        selected_date = None
    
    # Candle count for 30min timeframe
    candle_count = st.select_slider("Candle Count (30min)", options=["4C", "5C", "6C"], value="5C", label_visibility="collapsed")

    # Option: consider unusual volume in logic
    consider_unusual_volume = st.checkbox("Consider Unusual Volume (affects volume confirmations)", value=True, help="If unchecked, unusual-volume checks are ignored and volume confirmations are treated as passed.")
    
    st.divider()
    
    # Stocks list editor
    st.markdown("**3️⃣ Stocks List**")
    default_stocks = [
        "360ONE.NS", "ABB.NS", "APLAPOLLO.NS", "AUBANK.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ABCAPITAL.NS", 
"ALKEM.NS", "AMBER.NS", "AMBUJACEM.NS", "ANGELONE.NS", "APOLLOHOSP.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTRAL.NS", "AUROPHARMA.NS", 
"DMART.NS", "AXISBANK.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", 
"BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS", "BIOCON.NS", "BLUESTARCO.NS", "BOSCHLTD.NS", "BRITANNIA.NS", 
"CGPOWER.NS", "CANBK.NS", "CDSL.NS", "CHOLAFIN.NS", "CIPLA.NS", "COALINDIA.NS", "COFORGE.NS", "COLPAL.NS", "CAMS.NS", "CONCOR.NS", 
"CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DLF.NS", "DABUR.NS", "DALBHARAT.NS", "DELHIVERY.NS", "DIVISLAB.NS", "DIXON.NS", "DRREDDY.NS", 
"ETERNAL.NS", "EICHERMOT.NS", "EXIDEIND.NS", "NYKAA.NS", "FORTIS.NS", "GAIL.NS", "GMRAIRPORT.NS", "GLENMARK.NS", "GODREJCP.NS", 
"GODREJPROP.NS", "GRASIM.NS", "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAVELLS.NS", "HEROMOTOCO.NS", 
"HINDALCO.NS", "HAL.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HUDCO.NS", "ICICIBANK.NS", "ICICIGI.NS", 
"ICICIPRULI.NS", "IDFCFIRSTB.NS", "IIFL.NS", "ITC.NS", "INDIANB.NS", "IEX.NS", "IOC.NS", "IRCTC.NS", "IRFC.NS", "IREDA.NS", "IGL.NS", 
"INDUSTOWER.NS", "INDUSINDBK.NS", "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INDIGO.NS", "JINDALSTEL.NS", "JSWENERGY.NS", "JSWSTEEL.NS", 
"JIOFIN.NS", "JUBLFOOD.NS", "KEI.NS", "KPITTECH.NS", "KALYANKJIL.NS", "KAYNES.NS", "KFINTECH.NS", "KOTAKBANK.NS", "LTF.NS", "LICHSGFIN.NS", 
"LTIM.NS", "LT.NS", "LAURUSLABS.NS", "LICI.NS", "LODHA.NS", "LUPIN.NS", "M&M.NS", "MANAPPURAM.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", 
"MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NBCC.NS", "NCC.NS", "NHPC.NS", "NMDC.NS", "NTPC.NS", 
"NATIONALUM.NS", "NESTLEIND.NS", "NUVAMA.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "PAYTM.NS", "OFSS.NS", "POLICYBZR.NS", "PGEL.NS", "PIIND.NS", 
"PNBHOUSING.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PIDILITIND.NS", "PPLPHARMA.NS", "POLYCAB.NS", "PFC.NS", 
"POWERGRID.NS", "PRESTIGE.NS", "PNB.NS", "RBLBANK.NS", "RECLTD.NS", "RVNL.NS", "RELIANCE.NS", "SBICARD.NS", "SBILIFE.NS", "SHREECEM.NS", "SRF.NS", 
"SAMMAANCAP.NS", "MOTHERSON.NS", "SHRIRAMFIN.NS", "SIEMENS.NS", "SOLARINDS.NS", "SONACOMS.NS", "SBIN.NS", "SAIL.NS", "SUNPHARMA.NS", "SUPREMEIND.NS", 
"SUZLON.NS", "SYNGENE.NS", "TATACONSUM.NS", "TITAGARH.NS", "TVSMOTOR.NS", "TCS.NS", "TATAELXSI.NS", "TMPV.NS", "TATAPOWER.NS", "TATASTEEL.NS", 
"TATATECH.NS", "TECHM.NS", "FEDERALBNK.NS", "INDHOTEL.NS", "PHOENIXLTD.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TRENT.NS", "TIINDIA.NS", 
"UNOMINDA.NS", "UPL.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UNITDSPR.NS", "VBL.NS", "VEDL.NS", "IDEA.NS", "VOLTAS.NS", "WIPRO.NS", "YESBANK.NS", "ZYDUSLIFE.NS"
    ]
    
    stocks_text = st.text_area("📋 Stocks (one per line)", value="\n".join(default_stocks), height=68, label_visibility="collapsed")
    stocks = [s.strip().upper() for s in stocks_text.split('\n') if s.strip()]
    
    st.divider()
    
    # Run button - PROMINENT
    run_button = st.button("▶️ RUN SCREENER", key="run_btn", use_container_width=True, 
                          help=f"Scan {len(stocks)} stocks")
    
    st.divider()
    
    # ===== RESULTS SECTION (BELOW BUTTON) =====
    # Create placeholder for results that will update in real-time
    results_placeholder = st.empty()
    
    if run_button:
        st.session_state.screening_results = []
        st.session_state.scan_running = True
        results = run_screening(stocks, mode, selected_date, timeframe, candle_count, results_container=True, require_unusual_volume=consider_unusual_volume)
        st.session_state.screening_results = results
        st.session_state.scan_running = False
    
    # Display results if available
    if st.session_state.screening_results is not None:
        results = st.session_state.screening_results
        with results_placeholder.container():
            if results:
                st.markdown("### 📊 Results")
                # Sort results with exact same logic as Tkinter
                def get_move_pct(top_mover):
                    match = re.search(r"\(([-+]?[0-9]*\.?[0-9]+)%\)", top_mover)
                    if match:
                        return float(match.group(1))
                    return 0.0

                def sort_key(res):
                    signal = res['Signal']
                    volume = res['Volume']
                    top_mover = res['Top Mover']
                    top_status = res['Top']
                    move_pct = get_move_pct(top_mover)
                    
                    if signal == "Sure Bullish":
                        return (-2, -move_pct)
                    if signal == "Sure Bearish":
                        return (-1, -abs(move_pct))
                    if signal == "Bullish" and volume == "High" and top_mover.startswith("Top Up") and top_status == "Top Gainer":
                        return (0, -move_pct)
                    if signal == "Bearish" and volume == "High" and top_mover.startswith("Top Down") and top_status == "Top Loser":
                        return (1, -abs(move_pct))
                    if signal == "Bullish" and volume == "High":
                        return (2, 0)
                    if signal == "Bearish" and volume == "High":
                        return (3, 0)
                    if signal == "Bullish" and volume == "Low" and top_mover.startswith("Top Up") and top_status == "Top Gainer":
                        return (4, -move_pct)
                    if signal == "Bearish" and volume == "Low" and top_mover.startswith("Top Down") and top_status == "Top Loser":
                        return (5, -abs(move_pct))
                    if signal == "Bullish" and volume == "Low":
                        return (6, 0)
                    if signal == "Bearish" and volume == "Low":
                        return (7, 0)
                    
                    vol_conf = True if str(res.get('Vol Confirm', '')).upper() == 'YES' else False
                    if signal == "No Signal" and vol_conf and volume == "High" and top_mover.startswith("Top Up") and top_status == "Top Gainer":
                        return (8, -move_pct)
                    if signal == "No Signal" and vol_conf and volume == "Low" and top_mover.startswith("Top Down") and top_status == "Top Loser":
                        return (9, -abs(move_pct))
                    if signal == "No Signal" and vol_conf:
                        return (10, -move_pct)
                    if vol_conf:
                        return (11, -move_pct)
                    
                    return (12, 0)

                sorted_results = sorted(results, key=sort_key)
                
                # Build HTML table with exact Tkinter colors
                html_rows = []
                for r in sorted_results:
                    def fmt(val):
                        return f"{val:.2f}" if isinstance(val, float) and val is not None else "NA"
                    
                    signal = r['Signal']
                    volume = r['Volume']
                    top_mover = r['Top Mover']
                    top_status = r['Top']
                    
                    # Apply exact same colors as Tkinter
                    if signal == "Sure Bullish":
                        bg_color = "#00C853"
                        text_color = "white"
                    elif signal == "Sure Bearish":
                        bg_color = "#D50000"
                        text_color = "white"
                    elif signal == "Bullish" and volume == "High" and top_mover.startswith("Top Up") and top_status == "Top Gainer":
                        bg_color = "#00e676"
                        text_color = "#003300"
                    elif signal == "Bearish" and volume == "High" and top_mover.startswith("Top Down") and top_status == "Top Loser":
                        bg_color = "#ff5252"
                        text_color = "white"
                    elif signal == "Bullish" and volume == "High":
                        bg_color = "#b9f6ca"
                        text_color = "#003300"
                    elif signal == "Bearish" and volume == "High":
                        bg_color = "#ffcdd2"
                        text_color = "#b71c1c"
                    elif signal == "Bullish" and volume == "Low" and top_mover.startswith("Top Up") and top_status == "Top Gainer":
                        bg_color = "#b9f6ca"
                        text_color = "#003300"
                    elif signal == "Bearish" and volume == "Low" and top_mover.startswith("Top Down") and top_status == "Top Loser":
                        bg_color = "#ffcdd2"
                        text_color = "#b71c1c"
                    elif signal == "Bullish" and volume == "Low":
                        bg_color = "#b9f6ca"
                        text_color = "#003300"
                    elif signal == "Bearish" and volume == "Low":
                        bg_color = "#ffcdd2"
                        text_color = "#b71c1c"
                    else:
                        bg_color = "#ffffff"
                        text_color = "#000000"
                    
                    # Add volume confirm color highlight
                    vol_confirm = r.get('Vol Confirm', 'NO')
                    if vol_confirm == 'YES':
                        if signal.startswith("Bullish") or signal.startswith("Sure Bullish"):
                            border_left = "5px solid #00C853"
                        else:
                            border_left = "5px solid #D50000"
                    else:
                        border_left = "none"
                    
                    row_html = f"""
<tr style="background-color: {bg_color}; color: {text_color}; border-left: {border_left}; font-weight: bold;">
    <td>{r['Symbol']}</td>
    <td>{r['Signal']}</td>
    <td>{r['Strength']}</td>
    <td>{r['Volume']}</td>
    <td>{vol_confirm}</td>
    <td>{r['Top Mover']}</td>
    <td>{r['Top']}</td>
    <td>{r['Volume Spike']}</td>
    <td>{r['MA Distance']}</td>
    <td>{fmt(r['1st Open'])}</td>
    <td>{fmt(r['1st Close'])}</td>
    <td>{fmt(r['2nd Open'])}</td>
    <td>{fmt(r['2nd Close'])}</td>
    <td>{fmt(r['3rd Open'])}</td>
    <td>{fmt(r['3rd Close'])}</td>
    <td>{fmt(r['4th Open'])}</td>
    <td>{fmt(r['4th Close'])}</td>
    <td>{fmt(r['5th Open'])}</td>
    <td>{fmt(r['5th Close'])}</td>
    <td>{fmt(r.get('6th Open'))}</td>
    <td>{fmt(r.get('6th Close'))}</td>
    <td>{fmt(r['1st 44MA'])}</td>
    <td>{fmt(r['2nd 44MA'])}</td>
    <td>{fmt(r['3rd 44MA'])}</td>
    <td>{fmt(r['4th 44MA'])}</td>
    <td>{fmt(r['5th 44MA'])}</td>
    <td>{fmt(r.get('6th 44MA'))}</td>
</tr>
"""
                    html_rows.append(row_html)
                
                # Create HTML table (after loop)
                html_table = f"""
<div style="overflow-x: auto; margin-top: 20px;">
    <table style="width: 100%; border-collapse: collapse; font-size: 12px;">
        <thead>
            <tr style="background-color: #003d6b; color: white; font-weight: bold; position: sticky; top: 0; font-size: 14px;">
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">Symbol</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">Signal</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">Strength</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">Volume</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">Vol Cnf</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">Top Mover</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">Top</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">Spike</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">MA Dis</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">1st O</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">1st C</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">2nd O</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">2nd C</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">3rd O</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">3rd C</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">4th O</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">4th C</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">5th O</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">5th C</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">6th O</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">6th C</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">1st MA</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">2nd MA</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">3rd MA</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">4th MA</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">5th MA</th>
                <th style="padding: 10px; border: 1px solid #ccc; text-align: center;">6th MA</th>
            </tr>
        </thead>
        <tbody>
            {''.join(html_rows)}
        </tbody>
    </table>
</div>
"""
                
                st.markdown(html_table, unsafe_allow_html=True)
                
                # Summary stats
                st.divider()
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    sure_bull_count = len([r for r in results if r['Signal'] == "Sure Bullish"])
                    st.metric("🟢 Sure Bullish", sure_bull_count)
                with col2:
                    sure_bear_count = len([r for r in results if r['Signal'] == "Sure Bearish"])
                    st.metric("🔴 Sure Bearish", sure_bear_count)
                with col3:
                    bull_count = len([r for r in results if r['Signal'] == "Bullish"])
                    st.metric("📈 Bullish", bull_count)
                with col4:
                    bear_count = len([r for r in results if r['Signal'] == "Bearish"])
                    st.metric("📉 Bearish", bear_count)
                with col5:
                    no_signal_count = len([r for r in results if r['Signal'] == "No Signal"])
                    st.metric("⚪ No Signal", no_signal_count)
            else:
                st.warning(f"⚠️ No trading signals found from {len(stocks)} stocks analyzed.")
                st.info("**Possible reasons:**\n"
                       "1. Selected date may not have enough intraday data\n"
                       "2. Stocks don't meet the screening criteria\n"
                       "3. Try a different date or use 'Live' mode\n"
                       "4. Check the console logs (terminal) for debugging info")

if __name__ == "__main__":
    main()
