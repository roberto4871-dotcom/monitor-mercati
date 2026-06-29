"""
Monitor Mercati - Server locale con yfinance
Avvia con: python3 server.py
Apri nel browser: http://localhost:8080/monitor-mercati.html
"""
import http.server
import urllib.parse
import urllib.request
import json
import math
import os
import time
import threading
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import yfinance as yf
except ImportError:
    print("Installa yfinance: pip3 install yfinance")
    raise

# ── Timeout globale su TUTTE le richieste HTTP (incluse quelle di yfinance) ──
# Previene blocchi infiniti quando Yahoo Finance è lento o fa rate-limit
import requests as _req
_orig_session_request = _req.Session.request
def _session_request_timeout(self, method, url, **kwargs):
    kwargs.setdefault('timeout', 20)   # max 20s per qualsiasi richiesta HTTP
    return _orig_session_request(self, method, url, **kwargs)
_req.Session.request = _session_request_timeout

PORT = int(os.environ.get('PORT', 8080))

# ── Semaforo globale: max 2 richieste Yahoo Finance in parallelo ──────────
_yf_semaphore = threading.Semaphore(2)

def _yf_call(fn, timeout=10, default=None):
    """Esegue fn() in un thread daemon con timeout — non blocca mai il thread chiamante."""
    result = [default]
    exc    = [None]
    def _run():
        try:    result[0] = fn()
        except Exception as e: exc[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        print(f'  [yf_call] timeout {timeout}s — thread lasciato girare in background')
        return default
    if exc[0] and not isinstance(exc[0], Exception):
        pass  # ignora eccezioni non critiche
    return result[0]

def _yf_history(ticker, timeout=15, **kwargs):
    """Wrapper sicuro per ticker.history() con timeout."""
    import pandas as pd
    r = _yf_call(lambda: ticker.history(**kwargs), timeout=timeout, default=None)
    return r if r is not None else pd.DataFrame()

def _yf_download(*args, timeout=30, **kwargs):
    """Wrapper sicuro per yf.download() con timeout."""
    import pandas as pd
    r = _yf_call(lambda: yf.download(*args, **kwargs), timeout=timeout, default=None)
    return r if r is not None else pd.DataFrame()

_yf_cooldown_until = 0.0   # timestamp: se > now, aspetta prima di fare richieste YF
_yf_cooldown_lock  = threading.Lock()

# ── Reset automatico crumb Yahoo Finance ogni 2 ore ───────────────────────────
# Il crumb (token di sessione) scade periodicamente → 401 "Invalid Crumb" su tutte
# le richieste. Il reset forza yfinance a ottenerne uno nuovo senza riavviare il server.
def _yf_reset_crumb():
    try:
        import requests
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        # Endpoint pubblico Yahoo Finance: richiesta di handshake che rigenera il crumb
        session.get('https://finance.yahoo.com', timeout=10)
        # Resetta la sessione interna di yfinance
        if hasattr(yf, 'shared') and hasattr(yf.shared, '_session'):
            yf.shared._session = None
        print(f'  [crumb] reset sessione Yahoo Finance — {time.strftime("%H:%M:%S")}')
    except Exception as e:
        print(f'  [crumb] reset fallito (non critico): {e}')

def _schedule_crumb_reset():
    """Avvia un thread daemon che resetta il crumb ogni 2 ore."""
    def _loop():
        while True:
            time.sleep(7200)   # 2 ore
            _yf_reset_crumb()
    t = threading.Thread(target=_loop, daemon=True, name='crumb-reset')
    t.start()

_schedule_crumb_reset()

def _yf_wait_cooldown():
    """Aspetta se siamo in cooldown da rate limit."""
    with _yf_cooldown_lock:
        wait = _yf_cooldown_until - time.time()
    if wait > 0:
        time.sleep(wait)

def _yf_set_cooldown(seconds=15):
    """Imposta un cooldown globale dopo un rate limit 429."""
    with _yf_cooldown_lock:
        global _yf_cooldown_until
        _yf_cooldown_until = max(_yf_cooldown_until, time.time() + seconds)

def is_rate_limit_error(e):
    s = str(e).lower()
    return '429' in s or 'too many' in s or 'rate limit' in s or 'rate-limit' in s

def _is_market_hours():
    """Restituisce True solo se almeno un mercato rilevante potrebbe essere aperto.
    Weekend e notte → False: i dati storici di ieri sono già quelli definitivi,
    inutile fare fast_info per ogni simbolo rallentando il caricamento."""
    now_utc = datetime.datetime.utcnow()
    if now_utc.weekday() >= 5:   # sabato=5, domenica=6
        return False
    h = now_utc.hour
    # Europa apre ~7 UTC, USA chiude ~21 UTC
    return 7 <= h <= 21

# Helper resample mensile — compatibile pandas <2.2 ('M') e >=2.2 ('ME')
def _resample_month_end(series):
    try:
        return series.resample('ME').last().dropna()   # pandas >= 2.2
    except Exception:
        return series.resample('M').last().dropna()    # pandas < 2.2 fallback

# Cache: chiave = "symbol|period|interval"
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 900   # 15 minuti (era 5 — riduce carico CPU/Railway)
CACHE_TTL_LONG = 3600  # 1 ora per 2Y/3Y/5Y/max

# Cache per fondamentali, news, calendario, AI
_fund_cache    = {}; _fund_lock    = threading.Lock(); FUND_TTL    = 7200   # 2h (era 1h)
_news_cache    = {}; _news_lock    = threading.Lock(); NEWS_TTL    = 1800   # 30min (era 15)
_cal_cache     = {}; _cal_lock     = threading.Lock(); CAL_TTL     = 3600   # 1h (era 30min)
_monthly_cache = {}; _monthly_lock = threading.Lock(); MONTHLY_TTL = 7200   # 2h (era 1h)
_weekly_cache  = {}; _weekly_lock  = threading.Lock(); WEEKLY_TTL  = 7200   # 2h (era 1h)

# ── Liste preferiti condivise (sync multi-PC) ──────────────────────────────
# LISTS_PATH può essere impostato come variabile d'ambiente Railway (es. /data/lists.json)
# per usare un Volume persistente che sopravvive ai deploy.
# Se non impostato, usa il file locale (non persiste su Railway tra deploy).
_LISTS_PATH_ENV = os.environ.get('LISTS_PATH', '')
LISTS_FILE = _LISTS_PATH_ENV if _LISTS_PATH_ENV else os.path.join(os.path.dirname(__file__), 'lists.json')

_lists_lock = threading.Lock()
_LISTS_DEFAULT = {
    'mm_fav': [], 'mm_fav2': [], 'mm_fav3': [], 'mm_fav4': [], 'mm_fav5': [],
    'mm_fav_name': 'Lista 1', 'mm_fav2_name': 'Lista 2',
    'mm_fav3_name': 'Lista 3', 'mm_fav4_name': 'Lista 4', 'mm_fav5_name': 'Lista 5',
}

def _load_lists():
    try:
        if os.path.exists(LISTS_FILE):
            with open(LISTS_FILE, 'r') as f:
                data = json.load(f)
                return {**_LISTS_DEFAULT, **data}
    except Exception:
        pass
    return dict(_LISTS_DEFAULT)

def _save_lists(data):
    try:
        # Crea la cartella padre se non esiste (es. /data su Railway)
        os.makedirs(os.path.dirname(LISTS_FILE) or '.', exist_ok=True)
        with open(LISTS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f'  [lists] errore salvataggio: {e}')

_lists_data = _load_lists()
print(f'  [lists] file={LISTS_FILE} | fav:{len(_lists_data["mm_fav"])} fav2:{len(_lists_data["mm_fav2"])} fav3:{len(_lists_data["mm_fav3"])} fav4:{len(_lists_data["mm_fav4"])} fav5:{len(_lists_data["mm_fav5"])}')
# ───────────────────────────────────────────────────────────────────────────
_ma_cache      = {}; _ma_lock      = threading.Lock(); MA_TTL      = 900    # 15min (era 5)
_seasonal_cache= {}; _seasonal_lock= threading.Lock(); SEASONAL_TTL= 43200  # 12h (era 6h)
_corr_cache  = {}; _corr_lock  = threading.Lock(); CORR_TTL  = 7200    # 2h (era 30min)
_rss_cache   = {}; _rss_lock   = threading.Lock(); RSS_TTL   = 1800    # 30min (era 10)
_macro_cache     = {}; _macro_lock     = threading.Lock(); MACRO_TTL     = 7200   # 2h (era 15min)
_sovereign_cache = {}; _sovereign_lock = threading.Lock(); SOVEREIGN_TTL = 7200   # 2h (era 30min)
_global_yields_cache = None; _global_yields_ts = 0.0; GLOBAL_YIELDS_TTL = 3600   # 1h (era 15min)
_ecb_irs_cache = {}; _ecb_irs_ts = 0.0; ECB_IRS_TTL = 86400  # 24h (dati mensili ECB)
_macro_live_cache = None; _macro_live_ts = 0.0; MACRO_LIVE_TTL = 7200   # 2h (era 30min)
# ISIN cache — permanente (ISIN non cambia mai)


def fetch_symbol(symbol, period='3mo', interval='1d'):
    cache_key = f'{symbol}|{period}|{interval}'
    ttl = CACHE_TTL if period in ('3mo', '6mo', '1mo') else CACHE_TTL_LONG

    with _cache_lock:
        cached = _cache.get(cache_key)
        if cached and (time.time() - cached['ts']) < ttl:
            return cached['data']

    # Mappa periodi lunghi → anni (il parametro period= di yfinance è inaffidabile per periodi >3mo)
    LONG_PERIODS = {'1y': 1, '2y': 2, '3y': 3, '5y': 5, '10y': 10}

    try:
        ticker = yf.Ticker(symbol)
        today  = datetime.date.today()
        if period in LONG_PERIODS:
            start = today.replace(year=today.year - LONG_PERIODS[period])
            hist  = _yf_history(ticker, timeout=20, start=start, end=today, interval=interval, auto_adjust=False)
        elif period == 'max':
            hist  = _yf_history(ticker, timeout=25, start='1970-01-01', end=today, interval=interval, auto_adjust=False)
        else:
            hist  = _yf_history(ticker, timeout=15, period=period, interval=interval, auto_adjust=False)
        if hist is None or hist.empty:
            return {'error': f'Nessun dato per {symbol} (timeout o dati mancanti)'}
        info   = ticker.fast_info

        closes     = hist['Close'].tolist()
        opens      = hist['Open'].tolist()
        highs      = hist['High'].tolist()
        lows       = hist['Low'].tolist()
        volumes    = [int(v) if v == v else 0 for v in hist['Volume'].tolist()]
        timestamps = [int(dt.timestamp()) for dt in hist.index]
        cur_price  = float(info.last_price) if hasattr(info, 'last_price') and info.last_price else closes[-1]
        prev_close = float(info.previous_close) if hasattr(info, 'previous_close') and info.previous_close else (closes[-2] if len(closes) > 1 else None)
        cur_time   = int(hist.index[-1].timestamp())
        tz         = str(hist.index.tz) if hist.index.tz else 'UTC'

        # Rendimento (yield) — solo per fetch principale (non per periodi lunghi del grafico)
        yield_pct = None
        if period in ('3mo', '6mo'):
            try:
                full_info = _yf_call(lambda: ticker.info, timeout=10, default={}) or {}
                raw = (full_info.get('yield') or full_info.get('dividendYield')
                       or full_info.get('trailingAnnualDividendYield'))
                if raw and raw > 0:
                    yield_pct = round(raw * 100, 3)
            except Exception:
                pass
            # Fallback: calcola yield da dividendi degli ultimi 12 mesi
            if yield_pct is None:
                try:
                    divs = _yf_call(lambda: ticker.dividends, timeout=8, default=None)
                    if divs is not None and not divs.empty:
                        annual_div = float(divs.last('365D').sum())
                        if annual_div > 0 and cur_price > 0:
                            yield_pct = round(annual_div / cur_price * 100, 2)
                except Exception:
                    pass

        data = {
            'meta': {
                'regularMarketPrice': cur_price,
                'chartPreviousClose': prev_close,
                'regularMarketTime':  cur_time,
                'exchangeTimezoneName': tz,
                'currency': getattr(info, 'currency', 'N/A'),
            },
            'timestamp': timestamps,
            'closes':    closes,
            'opens':     opens,
            'highs':     highs,
            'lows':      lows,
            'volumes':   volumes,
            'yield_pct': yield_pct,
        }

        with _cache_lock:
            _cache[cache_key] = {'data': data, 'ts': time.time()}
        return data

    except Exception as e:
        return {'error': str(e)}


def fetch_batch_bulk(symbols, period='3mo', interval='1d'):
    """Scarica tutti i simboli con una sola chiamata yf.download() — molto più veloce."""
    results = {}
    to_fetch = []
    ttl = CACHE_TTL if period in ('3mo', '6mo', '1mo') else CACHE_TTL_LONG

    with _cache_lock:
        for sym in symbols:
            key = f'{sym}|{period}|{interval}'
            c = _cache.get(key)
            if c and (time.time() - c['ts']) < ttl:
                results[sym] = c['data']
            else:
                to_fetch.append(sym)

    if not to_fetch:
        return results

    LONG_PERIODS = {'1y': 1, '2y': 2, '3y': 3, '5y': 5, '10y': 10}
    today = datetime.date.today()

    def safe_float(v):
        try:
            f = float(v)
            return None if f != f else f  # NaN → None
        except Exception:
            return None

    try:
        if period in LONG_PERIODS:
            start = today.replace(year=today.year - LONG_PERIODS[period])
            raw = _yf_download(to_fetch, start=str(start), end=str(today),
                              interval=interval, auto_adjust=False,
                              group_by='ticker', progress=False, threads=True)
        elif period == 'max':
            raw = _yf_download(to_fetch, start='1970-01-01', end=str(today),
                              interval=interval, auto_adjust=False,
                              group_by='ticker', progress=False, threads=True)
        else:
            # 400 giorni garantisce: priceAtDec31 (fine anno prec.) + p180 (118 barre) + p365 (240 barre)
            start = today - datetime.timedelta(days=400)
            end   = today + datetime.timedelta(days=1)
            raw = _yf_download(to_fetch, start=str(start), end=str(end),
                              interval=interval, auto_adjust=False,
                              group_by='ticker', progress=False, threads=True)
        if raw is None or raw.empty:
            raise Exception('timeout o dati vuoti da yf.download')
    except Exception as e:
        print(f'  [batch] yf.download fallito ({e}), uso fetch individuale a chunk')
        if is_rate_limit_error(e):
            _yf_set_cooldown(20)
        # Suddividi in chunk da 15 con pausa tra chunk per evitare rate limit
        CHUNK = 15
        for i in range(0, len(to_fetch), CHUNK):
            chunk = to_fetch[i:i + CHUNK]
            _yf_wait_cooldown()
            with ThreadPoolExecutor(max_workers=3) as ex:
                futures = {ex.submit(fetch_symbol, sym, period, interval): sym for sym in chunk}
                for future in as_completed(futures):
                    sym = futures[future]
                    try:
                        results[sym] = future.result()
                    except Exception as e2:
                        if is_rate_limit_error(e2):
                            _yf_set_cooldown(20)
                        results[sym] = {'error': str(e2)}
            if i + CHUNK < len(to_fetch):
                time.sleep(1)   # pausa 1s tra chunk
        return results

    import pandas as pd
    is_multi = isinstance(raw.columns, pd.MultiIndex)

    for sym in to_fetch:
        try:
            if is_multi:
                try:
                    hist = raw[sym].copy()
                except KeyError:
                    results[sym] = {'error': f'Nessun dato per {sym}'}
                    continue
            else:
                hist = raw.copy()

            hist = hist.dropna(subset=['Close'])
            if hist.empty:
                results[sym] = {'error': f'Nessun dato per {sym}'}
                continue

            closes     = [safe_float(v) for v in hist['Close'].tolist()]
            opens      = [safe_float(v) for v in hist['Open'].tolist()]
            highs      = [safe_float(v) for v in hist['High'].tolist()]
            lows       = [safe_float(v) for v in hist['Low'].tolist()]
            volumes    = [int(v) if v == v else 0 for v in hist['Volume'].tolist()]
            timestamps = [int(dt.timestamp()) for dt in hist.index]

            # prev_close = ultimo close storico (sempre il giorno prima dell'ultimo bar)
            # cur_price  = last close dal download — potrebbe essere ieri se borsa aperta oggi
            cur_price  = closes[-1]
            prev_close = closes[-2] if len(closes) > 1 else None
            cur_time   = timestamps[-1]
            tz         = str(hist.index.tz) if hist.index.tz else 'UTC'

            data = {
                'meta': {
                    'regularMarketPrice':   cur_price,
                    'chartPreviousClose':   prev_close,
                    'regularMarketTime':    cur_time,
                    'exchangeTimezoneName': tz,
                    'currency': 'N/A',
                    '_stale': False,  # verrà marcato True se i dati sono di ieri
                },
                'timestamp': timestamps,
                'closes':    closes,
                'opens':     opens,
                'highs':     highs,
                'lows':      lows,
                'volumes':   volumes,
                'yield_pct': None,
            }

            # Marca come stale se l'ultimo bar non è di oggi
            today_midnight = int(datetime.datetime.combine(today, datetime.time.min).timestamp())
            if cur_time < today_midnight:
                data['meta']['_stale'] = True

            results[sym] = data
            with _cache_lock:
                _cache[f'{sym}|{period}|{interval}'] = {'data': data, 'ts': time.time()}

        except Exception as e:
            results[sym] = {'error': str(e)}

    # ── Aggiornamento prezzi real-time per simboli con dati stantii ────────
    # Se l'ultimo bar storico è di ieri (borsa ancora aperta oggi), prova a
    # ottenere il prezzo live tramite fast_info (quota real-time / 15min delay).
    stale_syms = [s for s in to_fetch
                  if isinstance(results.get(s), dict) and results[s].get('meta', {}).get('_stale')]
    # Salta fast_info fuori orario di mercato (weekend/notte): i dati di ieri
    # sono già quelli definitivi — inutile fare 400 chiamate individuali e rallentare il load.
    if stale_syms and not _is_market_hours():
        print(f'  [batch] {len(stale_syms)} simboli stale ma mercati chiusi — skip fast_info')
        stale_syms = []
    if stale_syms:
        print(f'  [batch] {len(stale_syms)} simboli con dati di ieri → fetch real-time prices')

        def _rt_price(sym):
            try:
                with _yf_semaphore:
                    _yf_wait_cooldown()
                    fi = yf.Ticker(sym).fast_info
                    price = getattr(fi, 'last_price', None)
                    prev  = getattr(fi, 'previous_close', None)
                    ts    = getattr(fi, 'last_fetch_time', None) or time.time()
                    if price and float(price) > 0:
                        return sym, float(price), float(prev) if prev else None, int(ts)
            except Exception as e:
                if is_rate_limit_error(e):
                    _yf_set_cooldown(15)
            return sym, None, None, None

        with ThreadPoolExecutor(max_workers=min(len(stale_syms), 4)) as pool:
            rt_res = list(pool.map(_rt_price, stale_syms))

        for sym, rt_price, rt_prev, rt_ts in rt_res:
            if rt_price and sym in results and isinstance(results[sym], dict):
                m = results[sym]['meta']
                # prev_close corretto = storico close di ieri (già in chartPreviousClose)
                # cur_price aggiornato = real-time
                m['regularMarketPrice'] = rt_price
                m['regularMarketTime']  = rt_ts or int(time.time())
                m['_stale'] = False
                print(f'  [rt] {sym}: {rt_price:.4f}')
                # Aggiorna cache con prezzo live
                key = f'{sym}|{period}|{interval}'
                with _cache_lock:
                    if key in _cache:
                        _cache[key]['data']['meta']['regularMarketPrice'] = rt_price
                        _cache[key]['data']['meta']['regularMarketTime']  = m['regularMarketTime']

    return results


def fetch_ma_batch(symbols):
    """Calcola MA50/MA200 per una lista di simboli — cache 5 min.
    Scarica 365 giorni di dati per avere ≥200 barre di borsa.
    Restituisce {sym: {above50: bool|null, above200: bool|null, ma50: float|null, ma200: float|null}}
    """
    results = {}
    to_fetch = []

    with _ma_lock:
        for sym in symbols:
            c = _ma_cache.get(sym)
            if c and (time.time() - c['ts']) < MA_TTL:
                results[sym] = c['data']
            else:
                to_fetch.append(sym)

    if not to_fetch:
        return results

    today = datetime.date.today()
    start = today - datetime.timedelta(days=365)

    def safe_float(v):
        try:
            f = float(v)
            return None if f != f else f  # NaN → None
        except Exception:
            return None

    def calc_ma(sym, closes):
        n = len(closes)
        if n == 0:
            return {'above50': None, 'above200': None, 'ma50': None, 'ma200': None}
        cur   = closes[-1]
        ma50  = round(sum(closes[-50:])  / 50,  6) if n >= 50  else None
        ma200 = round(sum(closes[-200:]) / 200, 6) if n >= 200 else None
        return {
            'above50':  (cur > ma50)  if ma50  is not None else None,
            'above200': (cur > ma200) if ma200 is not None else None,
            'ma50':     ma50,
            'ma200':    ma200,
            'cur':      round(cur, 6),
        }

    try:
        import pandas as pd
        raw = _yf_download(
            to_fetch,
            start=str(start),
            end=str(today + datetime.timedelta(days=1)),
            interval='1d',
            auto_adjust=False,
            group_by='ticker',
            progress=False,
            threads=True,
        )
        if raw is None or raw.empty:
            raise Exception('timeout o dati vuoti da yf.download per MA')

        is_multi = isinstance(raw.columns, pd.MultiIndex)

        for sym in to_fetch:
            try:
                if is_multi:
                    try:
                        hist = raw[sym].copy()
                    except KeyError:
                        data = {'above50': None, 'above200': None, 'ma50': None, 'ma200': None}
                        results[sym] = data
                        with _ma_lock:
                            _ma_cache[sym] = {'data': data, 'ts': time.time()}
                        continue
                else:
                    hist = raw.copy()

                hist   = hist.dropna(subset=['Close'])
                closes = [safe_float(v) for v in hist['Close'].tolist()]
                closes = [c for c in closes if c is not None]

                data = calc_ma(sym, closes)
                results[sym] = data
                with _ma_lock:
                    _ma_cache[sym] = {'data': data, 'ts': time.time()}

            except Exception as e:
                data = {'above50': None, 'above200': None, 'ma50': None, 'ma200': None}
                results[sym] = data

    except Exception as e:
        print(f'  [ma] yf.download fallito ({e}), uso fetch individuale')

        def _fetch_one(sym):
            try:
                today_l = datetime.date.today()
                start_l = today_l - datetime.timedelta(days=365)
                _tk = yf.Ticker(sym)
                hist = _yf_history(_tk, timeout=12, start=str(start_l), end=str(today_l),
                    interval='1d', auto_adjust=False)
                if hist.empty:
                    return sym, {'above50': None, 'above200': None, 'ma50': None, 'ma200': None}
                closes = [float(v) for v in hist['Close'].tolist() if v == v]
                return sym, calc_ma(sym, closes)
            except Exception:
                return sym, {'above50': None, 'above200': None, 'ma50': None, 'ma200': None}

        with ThreadPoolExecutor(max_workers=4) as ex:
            for sym, data in ex.map(_fetch_one, to_fetch):
                results[sym] = data
                with _ma_lock:
                    _ma_cache[sym] = {'data': data, 'ts': time.time()}

    return results


def fetch_fundamentals(symbol):
    """Dati fondamentali via yfinance — cache 1h."""
    with _fund_lock:
        c = _fund_cache.get(symbol)
        if c and (time.time() - c['ts']) < FUND_TTL:
            return c['data']
    import math
    info = None
    t = yf.Ticker(symbol)
    for attempt in range(3):
        _yf_wait_cooldown()
        try:
            with _yf_semaphore:
                info = _yf_call(lambda: t.info, timeout=25)
            if info is None:
                wait = 3 * (attempt + 1)
                print(f'  [fundamentals] timeout t.info per {symbol}, retry {attempt+1} fra {wait}s')
                time.sleep(wait)
                continue
            # yfinance a volte restituisce una stringa di errore come valore
            if not info or (isinstance(list(info.values())[0] if info else None, str) and 'Too Many' in str(list(info.values())[0])):
                raise Exception('Too Many Requests')
            break   # successo
        except Exception as e:
            if is_rate_limit_error(e):
                wait = 2 ** (attempt + 1)  # 2s, 4s, 8s
                print(f'  [fundamentals] rate limit {symbol}, retry {attempt+1} fra {wait}s')
                _yf_set_cooldown(wait)
                time.sleep(wait)
                continue
            # Errore non-rate-limit: esci subito
            return {'error': str(e)}
    else:
        return {'error': 'Timeout dati fondamentali. Riprova.'}
    if info is None:
        return {'error': 'Nessun dato disponibile.'}
    try:
        dy   = info.get('dividendYield')
        tady = info.get('trailingAnnualDividendYield')
        # dividendYield da Yahoo Finance è GIÀ in formato percentuale (0.38=0.38%, 4.71=4.71%)
        # NON moltiplicare per 100!
        # trailingAnnualDividendYield è in decimale (0.003767=0.377%) → usato come validazione
        # Fonte primaria: trailingAnnualDividendYield (affidabile, 12 mesi reali)
        # Fallback: dividendYield diretto se trailing è zero (es. dividendi speciali)
        dy_norm = None
        if tady and tady > 0:
            candidate = round(tady * 100, 2)
            if candidate <= 25:
                dy_norm = candidate
        elif dy and 0 < dy <= 25:
            dy_norm = round(dy, 2)
        data = {
            'trailingPE':        info.get('trailingPE'),
            'forwardPE':         info.get('forwardPE'),
            'priceToBook':       info.get('priceToBook'),
            'dividendYield':     dy_norm,
            'marketCap':         info.get('marketCap'),
            'beta':              info.get('beta'),
            'fiftyTwoWeekHigh':  info.get('fiftyTwoWeekHigh'),
            'fiftyTwoWeekLow':   info.get('fiftyTwoWeekLow'),
            'volume':            info.get('volume'),
            'avgVolume':         info.get('averageVolume'),
            'totalExpenseRatio': info.get('annualReportExpenseRatio'),
            'currency':          info.get('currency'),
            'description':       _translate_it((info.get('longBusinessSummary') or '')[:800]),
            'sector':            info.get('sector'),
            'industry':          info.get('industry'),
            'country':           info.get('country'),
            'website':           info.get('website'),
            'profitMargins':     info.get('profitMargins'),
            'revenueGrowth':     info.get('revenueGrowth'),
            'earningsGrowth':    info.get('earningsGrowth'),
        }

        # ── Ultimi 4 trimestri (conto economico) ──────────────────
        try:
            qi = _yf_call(lambda: t.quarterly_income_stmt, timeout=10)
            quarters = []
            if qi is not None and not qi.empty and len(qi.columns) > 0:
                for col in list(qi.columns[:4]):
                    def _safe(row_name, _col=col):
                        try:
                            v = qi.loc[row_name, _col]
                            return None if (v is None or (isinstance(v, float) and math.isnan(v))) else float(v)
                        except Exception:
                            return None
                    eps = _safe('Diluted EPS') or _safe('Basic EPS')
                    quarters.append({
                        'date':      col.strftime('%Y-%m-%d') if hasattr(col, 'strftime') else str(col)[:10],
                        'revenue':   _safe('Total Revenue'),
                        'netIncome': _safe('Net Income'),
                        'opIncome':  _safe('Operating Income'),
                        'eps':       eps,
                    })
            data['quarters'] = quarters
        except Exception:
            data['quarters'] = []

        # ── Prossima trimestrale (calendar) ───────────────────────
        try:
            import datetime
            today  = datetime.date.today()
            cal    = _yf_call(lambda: t.calendar, timeout=8) or {}
            dates  = cal.get('Earnings Date', [])
            # Prendi solo date FUTURE (ignora quelle già passate)
            future = [d for d in dates if hasattr(d, 'strftime') and d >= today]
            next_d = future[0] if future else None
            if next_d:
                data['nextEarnings'] = {
                    'date':    next_d.strftime('%Y-%m-%d'),
                    'epsAvg':  cal.get('Earnings Average'),
                    'epsHigh': cal.get('Earnings High'),
                    'epsLow':  cal.get('Earnings Low'),
                    'revAvg':  cal.get('Revenue Average'),
                    'revHigh': cal.get('Revenue High'),
                    'revLow':  cal.get('Revenue Low'),
                }
            else:
                data['nextEarnings'] = {}
        except Exception:
            data['nextEarnings'] = {}

        # ── Storico dividendi ultimi 3 anni ──────────────────
        try:
            import datetime as _dt
            three_yrs_ago = _dt.date.today() - _dt.timedelta(days=3*366)
            div_series = _yf_call(lambda: t.dividends, timeout=10)
            div_history = []
            if div_series is not None and not div_series.empty:
                for dt_idx, amount in div_series.items():
                    try:
                        d_date = dt_idx.date() if hasattr(dt_idx, 'date') else _dt.date.fromisoformat(str(dt_idx)[:10])
                        if d_date >= three_yrs_ago:
                            div_history.append({'date': d_date.strftime('%Y-%m-%d'), 'amount': round(float(amount), 4)})
                    except Exception:
                        pass
            div_history.sort(key=lambda x: x['date'], reverse=True)
            data['dividendsHistory'] = div_history
        except Exception:
            data['dividendsHistory'] = []

        # Ex-dividend date, dividend rate, last dividend
        try:
            import datetime as _dt
            ex_ts = info.get('exDividendDate')
            data['exDividendDate']   = _dt.datetime.utcfromtimestamp(ex_ts).strftime('%Y-%m-%d') if ex_ts else None
            last_ts = info.get('lastDividendDate')
            data['lastDividendDate'] = _dt.datetime.utcfromtimestamp(last_ts).strftime('%Y-%m-%d') if last_ts else None
            data['dividendRate']     = info.get('dividendRate')
            data['lastDivValue']     = info.get('lastDividendValue')
        except Exception:
            data['exDividendDate'] = None
            data['lastDividendDate'] = None
            data['dividendRate'] = None
            data['lastDivValue'] = None

        # ── Consensus analisti ────────────────────────────────
        # Fonte primaria: info (può essere vuota su alcuni ambienti)
        data['analystConsensus'] = info.get('recommendationKey')
        data['analystCount']     = info.get('numberOfAnalystOpinions')
        data['analystScore']     = info.get('recommendationMean')
        data['targetMean']       = info.get('targetMeanPrice')
        data['targetHigh']       = info.get('targetHighPrice')
        data['targetLow']        = info.get('targetLowPrice')
        data['targetMedian']     = info.get('targetMedianPrice')

        # Fallback prezzi target: t.analyst_price_targets (yfinance 1.x, più affidabile)
        try:
            apt = t.analyst_price_targets
            if isinstance(apt, dict) and apt:
                if not data['targetMean']:  data['targetMean']   = apt.get('mean')
                if not data['targetHigh']:  data['targetHigh']   = apt.get('high')
                if not data['targetLow']:   data['targetLow']    = apt.get('low')
                if not data['targetMedian']:data['targetMedian'] = apt.get('median')
        except Exception:
            pass

        # Trend mensile (ultimi 4 mesi) + breakdown corrente
        try:
            rec = t.recommendations_summary
            if rec is not None and not rec.empty and 'period' in rec.columns:
                def _ri(r, col):
                    try: return int(r.get(col, 0) or 0)
                    except: return 0
                trend = []
                for _, r in rec.iterrows():
                    trend.append({
                        'period':    str(r.get('period', '')),
                        'strongBuy': _ri(r,'strongBuy'),
                        'buy':       _ri(r,'buy'),
                        'hold':      _ri(r,'hold'),
                        'sell':      _ri(r,'sell'),
                        'strongSell':_ri(r,'strongSell'),
                    })
                data['analystTrend'] = trend
                # Breakdown corrente = periodo 0m (o primo disponibile)
                curr_rows = rec[rec['period'] == '0m']
                if curr_rows.empty: curr_rows = rec.iloc[:1]
                curr = curr_rows.iloc[0]
                bd = {
                    'strongBuy':  _ri(curr,'strongBuy'),
                    'buy':        _ri(curr,'buy'),
                    'hold':       _ri(curr,'hold'),
                    'sell':       _ri(curr,'sell'),
                    'strongSell': _ri(curr,'strongSell'),
                }
                data['analystBreakdown'] = bd
                # Fallback consensus da breakdown se info non lo ha
                if not data['analystConsensus']:
                    total = sum(bd.values())
                    if total > 0:
                        if not data['analystCount']: data['analystCount'] = total
                        sb = bd['strongBuy']; b = bd['buy']; h = bd['hold']
                        s = bd['sell'];      ss = bd['strongSell']
                        bulls = sb + b; bears = s + ss
                        if bulls / total >= 0.6:   data['analystConsensus'] = 'buy'
                        elif bears / total >= 0.5: data['analystConsensus'] = 'sell'
                        else:                      data['analystConsensus'] = 'hold'
                        # Score sintetico 1-5
                        if not data['analystScore']:
                            data['analystScore'] = round(
                                (1*sb + 2*b + 3*h + 4*s + 5*ss) / total, 2)
            else:
                data['analystTrend']     = []
                data['analystBreakdown'] = {}
        except Exception:
            data['analystTrend']     = []
            data['analystBreakdown'] = {}

        # Azioni recenti firma per firma (upgrades / downgrades)
        try:
            ud = t.upgrades_downgrades
            if ud is not None and not ud.empty:
                actions = []
                for dt, row in ud.head(25).iterrows():
                    actions.append({
                        'date':      dt.strftime('%Y-%m-%d') if hasattr(dt,'strftime') else str(dt)[:10],
                        'firm':      str(row.get('Firm','') or ''),
                        'toGrade':   str(row.get('ToGrade','') or ''),
                        'fromGrade': str(row.get('FromGrade','') or ''),
                        'action':    str(row.get('Action','') or '').lower(),
                    })
                data['analystActions'] = actions
            else:
                data['analystActions'] = []
        except Exception:
            data['analystActions'] = []

    except Exception as e:
        data = {'error': str(e)}
    with _fund_lock:
        _fund_cache[symbol] = {'data': data, 'ts': time.time()}
    return data




def _translate_it(text):
    """Traduce in italiano via Google Translate (API pubblica, nessuna chiave)."""
    if not text or len(text.strip()) < 4:
        return text
    try:
        import requests as req
        r = req.get(
            'https://translate.googleapis.com/translate_a/single',
            params={'client': 'gtx', 'sl': 'auto', 'tl': 'it', 'dt': 't', 'q': text[:800]},
            timeout=5, headers={'User-Agent': 'Mozilla/5.0'}
        )
        data = r.json()
        return ''.join(seg[0] for seg in data[0] if seg[0])
    except Exception:
        return text


def _symbol_to_query(symbol):
    """Converte ticker in keyword di ricerca per indici/valute/ETF."""
    import re
    s = symbol.upper()
    s = re.sub(r'^\^', '', s)             # ^GSPC → GSPC
    s = re.sub(r'=X$', '', s)             # EURUSD=X → EURUSD
    s = re.sub(r'=F$', '', s)             # GC=F → GC
    s = re.sub(r'-[A-Z]{3}$', '', s)      # BTC-USD → BTC
    s = re.sub(r'\.[A-Z0-9]+$', '', s)    # FTSEMIB.MI → FTSEMIB, AEEM.PA → AEEM
    s = re.sub(r'-[A-Z]$', '', s)         # DX-Y → DX
    return s.strip()


def _parse_news_item(n):
    """Estrae titolo/url/publisher da un news item (formato vecchio o nuovo)."""
    cnt       = n.get('content', {}) or {}
    title     = cnt.get('title')    or n.get('title', '')
    summary   = (cnt.get('summary') or n.get('summary', ''))[:250]
    # URL: canonicalUrl (formato nuovo) → link (formato vecchio) → stringa vuota
    canon     = cnt.get('canonicalUrl') or {}
    url       = (canon.get('url') if canon else None) or n.get('link', '') or ''
    publisher = (cnt.get('provider') or {}).get('displayName') \
                if cnt.get('provider') else n.get('publisher', '')
    pub_time  = cnt.get('pubDate') or n.get('providerPublishTime', '')
    return title, summary, url, publisher or '', pub_time


def fetch_correlation(symbols, days=252):
    """Matrice di correlazione rendimenti giornalieri — cache 30 min.
    Usa yf.download() per allineare correttamente le date cross-exchange."""
    import pandas as pd
    import warnings
    symbols = [s for s in symbols if s][:40]
    if len(symbols) < 2:
        return {'error': 'Servono almeno 2 strumenti'}
    cache_key = f"{','.join(sorted(symbols))}|{days}"
    with _corr_lock:
        c = _corr_cache.get(cache_key)
        if c and (time.time() - c['ts']) < CORR_TTL:
            return c['data']
    try:
        today    = datetime.date.today()
        start    = today - datetime.timedelta(days=days + 60)
        end_date = today + datetime.timedelta(days=1)

        # yf.download allinea automaticamente tutte le serie per data di calendario,
        # evitando il problema di timestamp con fusi orari diversi per borsa.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            raw = _yf_download(
                symbols, start=str(start), end=str(end_date),
                interval='1d', auto_adjust=False,
                group_by='ticker', progress=False, threads=True,
                timeout=35
            )
        if raw is None or raw.empty:
            return {'error': 'Timeout download dati correlazione'}

        is_multi = isinstance(raw.columns, pd.MultiIndex)
        prices = {}
        for sym in symbols:
            try:
                col = raw[sym]['Close'] if is_multi else raw['Close']
                col = col.dropna()
                if len(col) > 20:
                    prices[sym] = col
            except Exception:
                pass

        if len(prices) < 2:
            result = {'error': 'Dati storici insufficienti'}
        else:
            df      = pd.DataFrame(prices).dropna(how='all')
            returns = df.pct_change().dropna(how='all')
            corr    = returns.corr()
            valid   = [s for s in symbols if s in corr.columns
                       and not corr[s].isna().all()]
            matrix  = []
            for s1 in valid:
                row = []
                for s2 in valid:
                    try:
                        v = corr.loc[s1, s2]
                        row.append(None if pd.isna(v) else round(float(v), 2))
                    except Exception:
                        row.append(None)
                matrix.append(row)
            result = {'symbols': valid, 'matrix': matrix, 'days': days}
    except Exception as e:
        result = {'error': str(e)}
    with _corr_lock:
        _corr_cache[cache_key] = {'data': result, 'ts': time.time()}
    return result


RSS_SOURCES = [
    # ── Italiane ─────────────────────────────────────────────────────────────
    # Sole 24 Ore e Milano Finanza hanno rimosso i feed diretti → via Google News
    {'url':'https://news.google.com/rss/search?q=sole+24+ore+finanza+mercati+borsa&hl=it&gl=IT&ceid=IT:it',     'src':'Il Sole 24 Ore',  'lang':'it'},
    {'url':'https://news.google.com/rss/search?q=milano+finanza+borsa+azioni+mercati&hl=it&gl=IT&ceid=IT:it',   'src':'Milano Finanza',  'lang':'it'},
    {'url':'https://www.ansa.it/sito/notizie/economia/economia_rss.xml',                                         'src':'ANSA',            'lang':'it'},
    {'url':'https://www.corriere.it/rss/economia.xml',                                                           'src':'Corriere Eco.',   'lang':'it'},
    {'url':'https://www.repubblica.it/rss/economia/rss2.0.xml',                                                  'src':'Repubblica Eco.', 'lang':'it'},
    {'url':'https://news.google.com/rss/search?q=borsa+italiana+ftse+mib+azioni+titoli&hl=it&gl=IT&ceid=IT:it', 'src':'Borsa IT News',   'lang':'it'},
    # ── Reuters (feed RSS diretto dismesso 2020 — via Google News Search) ──────
    {'url':'https://news.google.com/rss/search?q=reuters+finance+markets+stocks&hl=en&gl=US&ceid=US:en',       'src':'Reuters Mkts',    'lang':'en'},
    {'url':'https://news.google.com/rss/search?q=reuters+economy+central+bank+inflation&hl=en&gl=US&ceid=US:en','src':'Reuters Macro',   'lang':'en'},
    # ── Internazionali (dirette) ──────────────────────────────────────────────
    {'url':'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114','src':'CNBC',              'lang':'en'},
    {'url':'https://feeds.content.dowjones.io/public/rss/mw_topstories',                         'src':'MarketWatch',       'lang':'en'},
    {'url':'https://feeds.content.dowjones.io/public/rss/mw_marketpulse',                        'src':'MarketWatch Mkt',  'lang':'en'},
    {'url':'https://feeds.bloomberg.com/markets/news.rss',                                        'src':'Bloomberg',         'lang':'en'},
    {'url':'https://feeds.bbci.co.uk/news/business/rss.xml',                                     'src':'BBC Business',      'lang':'en'},
    {'url':'https://www.ft.com/rss/home',                                                         'src':'Financial Times',   'lang':'en'},
    {'url':'https://www.economist.com/finance-and-economics/rss.xml',                             'src':'The Economist',     'lang':'en'},
    {'url':'https://seekingalpha.com/feed.xml',                                                   'src':'Seeking Alpha',     'lang':'en'},
    {'url':'https://www.euronews.com/rss?format=mrss&level=theme&name=business',                  'src':'Euronews Business', 'lang':'en'},
    {'url':'https://finance.yahoo.com/news/rssindex',                                              'src':'Yahoo Finance',     'lang':'en'},
    # ── Investing.com (per asset class) ──────────────────────────────────────
    {'url':'https://www.investing.com/rss/news_25.rss',                                           'src':'Inv. Azioni',       'lang':'en'},
    {'url':'https://www.investing.com/rss/news_14.rss',                                           'src':'Inv. Forex',        'lang':'en'},
    {'url':'https://www.investing.com/rss/news_301.rss',                                          'src':'Inv. Commodities',  'lang':'en'},
    {'url':'https://www.investing.com/rss/news_95.rss',                                           'src':'Inv. Bonds',        'lang':'en'},
    # ── WSJ, Barron's, Handelsblatt, Les Echos (via Google News — RSS diretto bloccato) ──
    {'url':'https://news.google.com/rss/search?q=wall+street+journal+markets+stocks&hl=en&gl=US&ceid=US:en', 'src':'WSJ',           'lang':'en'},
    {'url':'https://news.google.com/rss/search?q=barrons+markets+investing&hl=en&gl=US&ceid=US:en',          'src':"Barron's",      'lang':'en'},
    {'url':'https://news.google.com/rss/search?q=handelsblatt+finanzen+maerkte&hl=de&gl=DE&ceid=DE:de',      'src':'Handelsblatt',  'lang':'de'},
    {'url':'https://news.google.com/rss/search?q=les+echos+finance+marches&hl=fr&gl=FR&ceid=FR:fr',          'src':'Les Echos',     'lang':'fr'},
    # ── Crypto ───────────────────────────────────────────────────────────────
    {'url':'https://cointelegraph.com/rss',                                                       'src':'CoinTelegraph',     'lang':'en'},
    # ── Banche Centrali ──────────────────────────────────────────────────────
    {'url':'https://www.ecb.europa.eu/rss/press.html',                                            'src':'BCE',               'lang':'en'},
    {'url':'https://www.federalreserve.gov/feeds/press_all.xml',                                  'src':'Federal Reserve',   'lang':'en'},
    # ── Yahoo Finance (simboli chiave) ───────────────────────────────────────
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US',    'src':'YF S&P 500',        'lang':'en'},
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=GC%3DF&region=US&lang=en-US',     'src':'YF Gold',           'lang':'en'},
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=CL%3DF&region=US&lang=en-US',     'src':'YF Oil',            'lang':'en'},
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=EURUSD%3DX&region=US&lang=en-US', 'src':'YF EUR/USD',        'lang':'en'},
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=BTC-USD&region=US&lang=en-US',    'src':'YF Bitcoin',        'lang':'en'},
]


def _parse_rss_source(src):
    """Scarica e parsa un singolo feed RSS/Atom. Restituisce lista di item."""
    import xml.etree.ElementTree as ET
    import re as _re
    try:
        import requests as req
        r = req.get(src['url'], timeout=9,
                    headers={'User-Agent': 'Mozilla/5.0 (compatible; MonitorMercati/1.0)'})
        if r.status_code != 200:
            return []
        raw = r.content
        # Rimuovi caratteri di controllo che rompono il parser XML
        raw = _re.sub(b'[\x00-\x08\x0b\x0c\x0e-\x1f]', b'', raw)
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            return []

        # RSS 2.0 → //item ; Atom → //entry
        items = root.findall('.//item')
        if not items:
            items = root.findall('.//{http://www.w3.org/2005/Atom}entry')

        results = []
        for item in items[:20]:
            def _t(tag, alt=None):
                v = item.findtext(tag)
                if v is None and alt:
                    v = item.findtext(alt) or item.findtext('{http://www.w3.org/2005/Atom}' + alt)
                return (v or '').strip()

            title   = _t('title')
            link    = _t('link') or _t('guid')
            summary = _t('description') or _t('summary')
            pubdate = _t('pubDate') or _t('updated') or _t('dc:date')

            # Pulisci HTML dal sommario
            summary = _re.sub(r'<[^>]+>', '', summary)[:280].strip()

            # Timestamp
            ts = 0
            if pubdate:
                try:
                    from email.utils import parsedate_to_datetime
                    ts = int(parsedate_to_datetime(pubdate).timestamp())
                except Exception:
                    try:
                        import datetime as _dt
                        dt = _dt.datetime.fromisoformat(pubdate.replace('Z', '+00:00'))
                        ts = int(dt.timestamp())
                    except Exception:
                        ts = int(time.time())

            if title:
                # Rimuovi suffissi " - NomeFonte" / " | NomeFonte" aggiunti da Google News
                src_name = src['src']
                title = _re.sub(r'\s*[\-\|]\s*' + _re.escape(src_name) + r'\s*$', '', title).strip()
                results.append({'title': title, 'link': link, 'summary': summary,
                                'ts': ts or int(time.time()), 'src': src_name, 'lang': src['lang']})
        return results
    except Exception:
        return []


def _stooq_fetch(sym):
    """Scarica CSV da Stooq — usa requests (più robusto di urllib su Railway)."""
    try:
        import requests as req
        HDRS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://stooq.com/',
        }
        def parse_row(row):
            parts = row.split(',')
            return parts[0], float(parts[4])

        for domain in ['stooq.com', 'stooq.pl']:
            try:
                url = f'https://{domain}/q/d/l/?s={sym}&i=d'
                r = req.get(url, headers=HDRS, timeout=12)
                if r.status_code != 200:
                    continue
                text = r.text
                lines = [l for l in text.strip().split('\n')
                         if l.strip() and not l.lower().startswith('date')
                         and not l.lower().startswith('no data') and ',' in l]
                if len(lines) < 1:
                    continue
                date, last = parse_row(lines[-1])
                _, prev    = parse_row(lines[-2]) if len(lines) >= 2 else (date, last)
                return {'yield': round(last, 3), 'prev': round(prev, 3),
                        'chg': round(last - prev, 3), 'date': date}
            except Exception:
                continue
    except Exception:
        pass
    return None


def _fred_parse_csv(text):
    """Parsa CSV FRED: filtra header e valori mancanti ('.' = no data)."""
    valid = []
    for line in text.strip().split('\n'):
        parts = line.split(',')
        if len(parts) < 2 or parts[0].strip() == 'DATE':
            continue
        val_str = parts[1].strip()
        if val_str in ('', '.', 'ND', 'NA'):
            continue
        try:
            valid.append((parts[0].strip(), float(val_str)))
        except ValueError:
            continue
    return valid


def _fred_fetch_yield(series_id):
    """Fetch ultimo valore yield da FRED via requests — dati mensili o daily.
    Gestisce robusto i valori mancanti ('.' = no data in FRED)."""
    try:
        import requests as req
        url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}'
        r = req.get(url, timeout=14, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return None
        valid = _fred_parse_csv(r.text)
        if len(valid) < 2:
            return None
        date, last = valid[-1]
        _, prev    = valid[-2]
        return {'yield': round(last, 3), 'prev': round(prev, 3),
                'chg': round(last - prev, 3), 'date': f'{date} (FRED)'}
    except Exception as e:
        print(f'  [FRED] errore {series_id}: {e}')
        return None


# ─── Global Yields: rendimenti governativi internazionali ─────────────────────

# Valori statici di riferimento (Aprile 2025)
GLOBAL_YIELDS_STATIC = {
    'US': {'name':'USA',      'flag':'🇺🇸','yields':{'3M':4.35,'2Y':3.88,'5Y':3.92,'10Y':4.31,'30Y':4.92}},
    'DE': {'name':'Germania', 'flag':'🇩🇪','yields':{'3M':2.35,'2Y':2.10,'5Y':2.30,'10Y':2.53,'30Y':2.72}},
    'IT': {'name':'Italia',   'flag':'🇮🇹','yields':{'3M':3.15,'2Y':2.85,'5Y':3.15,'10Y':3.65,'30Y':4.08}},
    'FR': {'name':'Francia',  'flag':'🇫🇷','yields':{'3M':2.75,'2Y':2.45,'5Y':2.65,'10Y':3.18,'30Y':3.55}},
    'ES': {'name':'Spagna',   'flag':'🇪🇸','yields':{'3M':2.88,'2Y':2.55,'5Y':2.80,'10Y':3.30,'30Y':3.78}},
    'GB': {'name':'UK',       'flag':'🇬🇧','yields':{'3M':4.55,'2Y':4.10,'5Y':4.22,'10Y':4.62,'30Y':5.20}},
    'JP': {'name':'Giappone', 'flag':'🇯🇵','yields':{'3M':0.47,'2Y':0.68,'5Y':1.02,'10Y':1.50,'30Y':2.38}},
}

def _ecb_yc_fetch(mat_key):
    """Scarica la curva AAA dell'area euro da ECB per una scadenza (3M,2Y,5Y,10Y,30Y)."""
    try:
        import requests as req
        url = (f'https://data-api.ecb.europa.eu/service/data/YC/'
               f'B.U2.EUR.4F.G_N_A.SV_C_YM.SR_{mat_key}'
               f'?lastNObservations=2&format=csvdata')
        r = req.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return None
        lines = r.text.strip().split('\n')
        if len(lines) < 2:
            return None
        headers = [h.strip().strip('"') for h in lines[0].split(',')]
        obs_idx = next((i for i, h in enumerate(headers) if 'OBS_VALUE' in h.upper()), -1)
        if obs_idx < 0:
            return None
        for line in reversed(lines[1:]):
            parts = line.split(',')
            if len(parts) > obs_idx:
                val_str = parts[obs_idx].strip().strip('"')
                if val_str not in ('', '.', 'NaN', 'NA'):
                    try:
                        return round(float(val_str), 3)
                    except ValueError:
                        continue
        return None
    except Exception as e:
        print(f'  [ECB YC] {mat_key}: {e}')
        return None


def _boe_fetch():
    """Fetch UK Gilt par yields from Bank of England Statistics DB."""
    try:
        import requests as req
        # BOE UK Government Liability Nominal Par Yield Curve
        # Series: IUDAMLPY2Y / 5Y / 10Y / 30Y  (daily)
        today_str  = datetime.datetime.now().strftime('%d/%m/%Y')
        start_str  = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%d/%m/%Y')
        codes = 'IUDAMLPY2Y,IUDAMLPY5Y,IUDAMLPY10Y,IUDAMLPY30Y'
        url = (f'https://www.bankofengland.co.uk/boeapps/database/_iadb-FromShowColumns.asp'
               f'?csv.x=yes&Datefrom={start_str}&Dateto={today_str}'
               f'&SeriesCodes={codes}&CSVF=TT&UsingCodes=Y&VPD=Y&VFD=N')
        r = req.get(url, timeout=12, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            print(f'  [BOE] HTTP {r.status_code}')
            return None
        lines = [l.strip() for l in r.text.strip().split('\n') if l.strip()]
        if len(lines) < 2:
            return None
        headers = [h.strip().strip('"') for h in lines[0].split(',')]
        mat_map = {
            'IUDAMLPY2Y':'2Y', 'IUDAMLPY5Y':'5Y',
            'IUDAMLPY10Y':'10Y', 'IUDAMLPY30Y':'30Y'
        }
        for line in reversed(lines[1:]):
            parts = [p.strip().strip('"') for p in line.split(',')]
            yields = {}
            for i, h in enumerate(headers):
                if h in mat_map and i < len(parts):
                    try:
                        v = float(parts[i])
                        yields[mat_map[h]] = round(v, 3)
                    except (ValueError, IndexError):
                        pass
            if yields:
                print(f'  [BOE] OK: {yields}')
                return yields
        return None
    except Exception as e:
        print(f'  [BOE] {e}')
        return None


def _mof_japan_fetch():
    """Fetch Japan JGB benchmark yields from MOF (Ministry of Finance) CSV."""
    try:
        import requests as req
        url = 'https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv'
        r = req.get(url, timeout=12, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            print(f'  [MOF JP] HTTP {r.status_code}')
            return None
        # Prova UTF-8, poi Shift-JIS
        for enc in ('utf-8', 'shift_jis', 'cp932'):
            try:
                text = r.content.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            return None
        lines = [l.strip() for l in text.strip().split('\n') if l.strip()]
        if len(lines) < 2:
            return None
        # MOF CSV columns: 基準日,1年,2年,3年,4年,5年,6年,7年,8年,9年,10年,15年,20年,25年,30年,40年
        # Indices:          0    1   2   3   4   5   6   7   8   9   10   11   12   13   14   15
        last = [p.strip() for p in lines[-1].split(',')]
        mat_idx = {'2Y': 2, '5Y': 5, '10Y': 10, '30Y': 14}
        yields = {}
        for mat, idx in mat_idx.items():
            if idx < len(last) and last[idx] not in ('', '-', 'ND'):
                try:
                    yields[mat] = round(float(last[idx]), 3)
                except ValueError:
                    pass
        if yields:
            print(f'  [MOF JP] OK: {yields}')
        return yields if yields else None
    except Exception as e:
        print(f'  [MOF JP] {e}')
        return None


def _boc_fetch():
    """Fetch Canada Government Bond yields from Bank of Canada Valet API."""
    try:
        import requests as req
        # BOC Valet: Government of Canada benchmark bond yields
        series = ('BD.CDN.2YR.DQ.YLD,BD.CDN.5YR.DQ.YLD,'
                  'BD.CDN.10YR.DQ.YLD,BD.CDN.LONGBND.DQ.YLD')
        url = f'https://www.bankofcanada.ca/valet/observations/{series}/json?recent=5'
        r = req.get(url, timeout=12, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            print(f'  [BOC] HTTP {r.status_code}')
            return None
        data = r.json()
        obs  = data.get('observations', [])
        if not obs:
            return None
        mat_keys = {
            'BD.CDN.2YR.DQ.YLD':   '2Y',
            'BD.CDN.5YR.DQ.YLD':   '5Y',
            'BD.CDN.10YR.DQ.YLD':  '10Y',
            'BD.CDN.LONGBND.DQ.YLD':'30Y',
        }
        for o in reversed(obs):
            yields = {}
            for sid, mat in mat_keys.items():
                val = o.get(sid, {}).get('v')
                if val and val not in ('', 'null', None):
                    try:
                        yields[mat] = round(float(val), 3)
                    except ValueError:
                        pass
            if yields:
                print(f'  [BOC] OK: {yields}')
                return yields
        return None
    except Exception as e:
        print(f'  [BOC] {e}')
        return None


def _ecb_irs_10y_all():
    """Rendimento 10A governativo ufficiale (IRS convergence) per paesi eurozona.
    Fonte: ECB Data API — mensile, nessuna API key.
    Cache 24h (i dati ECB si aggiornano ogni mese).
    """
    global _ecb_irs_cache, _ecb_irs_ts
    if _ecb_irs_cache and (time.time() - _ecb_irs_ts) < ECB_IRS_TTL:
        return _ecb_irs_cache

    import requests as req
    IRS_CCS = ['DE', 'IT', 'ES', 'FR', 'NL', 'BE', 'AT', 'PT', 'GR']
    hdrs = {'User-Agent': 'Mozilla/5.0'}

    def _one(cc):
        try:
            url = (f'https://data-api.ecb.europa.eu/service/data/'
                   f'IRS/M.{cc}.L.L40.CI.0000.EUR.N.Z'
                   f'?lastNObservations=1&format=csvdata')
            r = req.get(url, timeout=10, headers=hdrs)
            if r.status_code != 200:
                return cc, None
            for line in reversed(r.text.splitlines()):
                if not line or line.startswith('KEY') or not line.strip():
                    continue
                parts = line.split(',')
                # IRS CSV: TIME_PERIOD(col10), OBS_VALUE(col11)
                if len(parts) >= 12:
                    try:
                        return cc, {'v': float(parts[11]), 'date': parts[10][:7]}
                    except (ValueError, IndexError):
                        pass
        except Exception as e:
            print(f'  [global-yields] ECB IRS {cc}: {e}')
        return cc, None

    result = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_one, cc) for cc in IRS_CCS]
        for fut in as_completed(futures, timeout=20):
            try:
                cc, data = fut.result()
                if data:
                    result[cc] = data
            except Exception:
                pass

    if result:
        _ecb_irs_cache = result
        _ecb_irs_ts    = time.time()
        irs_summary = ', '.join(f'{c}={d["v"]:.3f}%' for c, d in sorted(result.items()))
        print(f'  [global-yields] ECB IRS 10A: {irs_summary}')
    return result


def fetch_global_yields():
    """Rendimenti governativi internazionali: USA/EU live, UK/JP/CA se API raggiungibili."""
    global _global_yields_cache, _global_yields_ts
    now = time.time()
    if _global_yields_cache and now - _global_yields_ts < GLOBAL_YIELDS_TTL:
        return _global_yields_cache

    out = {}

    # --- USA: Yahoo Finance (confermato funzionante su Railway) ---
    YF_US = [('3M', '^IRX'), ('5Y', '^FVX'), ('10Y', '^TNX'), ('30Y', '^TYX')]
    us_yields = {}
    for mat, sym in YF_US:
        try:
            _yf_wait_cooldown()
            with _yf_semaphore:
                _tk = yf.Ticker(sym)
                hist = _yf_history(_tk, timeout=10, period='5d', interval='1d')
            if not hist.empty:
                closes = hist['Close'].dropna()
                if len(closes) >= 1:
                    us_yields[mat] = round(float(closes.iloc[-1]), 3)
        except Exception as e:
            if is_rate_limit_error(e): _yf_set_cooldown(10)
            print(f'  [global] US {sym}: {e}')
    if '2Y' not in us_yields and '3M' in us_yields and '5Y' in us_yields:
        us_yields['2Y'] = round(us_yields['3M'] * 0.45 + us_yields['5Y'] * 0.55, 3)
    if us_yields:
        out['US'] = {'name':'USA','flag':'🇺🇸','yields':us_yields,'source':'Yahoo Finance','live':True}
    # Se YF fallisce US → non mostrare (nessun fallback statico)

    # --- Canada: Bank of Canada Valet API ---
    ca_yields = _boc_fetch()
    if ca_yields:
        out['CA'] = {'name':'Canada','flag':'🇨🇦','yields':ca_yields,'source':'BOC','live':True}

    # --- UK: Bank of England Stats API ---
    gb_yields = _boe_fetch()
    if gb_yields:
        out['GB'] = {'name':'UK (Gilt)','flag':'🇬🇧','yields':gb_yields,'source':'BOE','live':True}

    # --- EU: ECB AAA Yield Curve → Germania + spread per altri paesi ---
    ECB_MATS = [('3M','3M'), ('2Y','2Y'), ('5Y','5Y'), ('10Y','10Y'), ('30Y','30Y')]
    ecb_yields = {}
    for mat, ecb_key in ECB_MATS:
        v = _ecb_yc_fetch(ecb_key)
        if v is not None:
            ecb_yields[mat] = v

    if ecb_yields:
        out['DE'] = {'name':'Germania','flag':'🇩🇪','yields':ecb_yields,'source':'ECB YC','live':True}

        # Spread di riferimento (fallback aggiornati al giu-2026, calibrati su ECB IRS)
        # Struttura: 10Y è il riferimento; 3M/2Y/5Y/30Y calcolati proporzionalmente
        EU_OFFSETS = {
            'NL': {'name':'Olanda',     'flag':'🇳🇱','off':{'3M':0.16,'2Y':0.16,'5Y':0.22,'10Y':0.25,'30Y':0.31}},
            'AT': {'name':'Austria',    'flag':'🇦🇹','off':{'3M':0.26,'2Y':0.23,'5Y':0.29,'10Y':0.38,'30Y':0.49}},
            'BE': {'name':'Belgio',     'flag':'🇧🇪','off':{'3M':0.43,'2Y':0.43,'5Y':0.61,'10Y':0.68,'30Y':0.90}},
            'FR': {'name':'Francia',    'flag':'🇫🇷','off':{'3M':0.50,'2Y':0.44,'5Y':0.48,'10Y':0.82,'30Y':1.07}},
            'ES': {'name':'Spagna',     'flag':'🇪🇸','off':{'3M':0.38,'2Y':0.35,'5Y':0.38,'10Y':0.57,'30Y':0.80}},
            'PT': {'name':'Portogallo', 'flag':'🇵🇹','off':{'3M':0.42,'2Y':0.38,'5Y':0.45,'10Y':0.50,'30Y':0.64}},
            'IT': {'name':'Italia',     'flag':'🇮🇹','off':{'3M':0.60,'2Y':0.55,'5Y':0.65,'10Y':0.92,'30Y':1.16}},
            'GR': {'name':'Grecia',     'flag':'🇬🇷','off':{'3M':0.63,'2Y':0.59,'5Y':0.72,'10Y':0.83,'30Y':1.01}},
        }

        # ECB IRS 10A: rendimento governativo ufficiale mensile per ciascun paese
        # Quando disponibile, usa IRS direttamente per 10A e scala le altre scadenze
        irs_10y    = _ecb_irs_10y_all()
        ecb_10y_base = ecb_yields.get('10Y')

        for cc, info in EU_OFFSETS.items():
            irs   = irs_10y.get(cc) if ecb_10y_base else None
            if irs and ecb_10y_base:
                actual_10y        = irs['v']
                implied_spread_10y = actual_10y - ecb_10y_base
                hardcoded_10y_off  = info['off']['10Y']
                # Scala tutti gli offset proporzionalmente allo spread 10A osservato
                scale = implied_spread_10y / hardcoded_10y_off if hardcoded_10y_off else 1.0
                yields = {}
                for m, base_v in ecb_yields.items():
                    if m == '10Y':
                        yields[m] = round(actual_10y, 3)
                    else:
                        yields[m] = round(base_v + info['off'].get(m, 0) * scale, 3)
                src = f'ECB IRS {irs["date"]}'
            else:
                yields = {m: round(ecb_yields[m] + info['off'].get(m, 0), 3) for m in ecb_yields}
                src = 'ECB+spread'
            out[cc] = {'name':info['name'],'flag':info['flag'],'yields':yields,
                       'source':src,'live':True}
    # Se ECB fallisce → nessun dato EU (nessun fallback statico)

    # --- Giappone: MOF Japan CSV ---
    jp_yields = _mof_japan_fetch()
    if jp_yields:
        out['JP'] = {'name':'Giappone','flag':'🇯🇵','yields':jp_yields,'source':'MOF','live':True}

    result = {'countries': out, 'ts': int(now)}
    _global_yields_cache = result
    _global_yields_ts = now
    print(f'  [global-yields] paesi disponibili: {list(out.keys())}')
    return result


def _bls_fetch():
    """CPI USA e Core CPI da BLS (Bureau of Labor Statistics). Nessuna API key."""
    try:
        import requests as req
        year_now = datetime.datetime.now().year
        payload = {
            'seriesid': ['CUUR0000SA0', 'CUUR0000SA0L1E'],
            'startyear': str(year_now - 1),
            'endyear':   str(year_now),
        }
        r = req.post('https://api.bls.gov/publicAPI/v1/timeseries/data/',
                     json=payload, timeout=15,
                     headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return {}
        result = {}
        for series in r.json().get('Results', {}).get('series', []):
            sid  = series['seriesID']
            data = series['data']
            if len(data) >= 13:
                latest   = float(data[0]['value'])
                prev_yr  = float(data[12]['value'])
                pct      = round((latest - prev_yr) / prev_yr * 100, 1)
                date_lbl = f"{data[0]['periodName'][:3]} {data[0]['year']}"
                if sid == 'CUUR0000SA0':
                    result['cpi']      = {'v': pct, 'date': date_lbl}
                elif sid == 'CUUR0000SA0L1E':
                    result['cpi_core'] = {'v': pct, 'date': date_lbl}
        return result
    except Exception as e:
        print(f'  [macro-live] BLS: {e}')
        return {}


def _ecb_hicp(country_code):
    """HICP variazione annua (%) per paese.
    Fonte primaria: BIS WS_LONG_CPI (aggiornato mensile, dati 2026).
    Fallback: ECB ICP (serie discontinued, ultima obs dic-2025).
    country_code: codice ECB (U2=eurozona, DE, IT, FR, ES).
    """
    import requests as req
    # Mappa codici ECB → BIS (U2=eurozona in ECB, XM=eurozona in BIS)
    BIS_MAP = {'U2': 'XM', 'DE': 'DE', 'IT': 'IT', 'FR': 'FR', 'ES': 'ES',
               'GB': 'GB', 'US': 'US', 'JP': 'JP'}
    bis_cc = BIS_MAP.get(country_code, country_code)
    hdrs = {'User-Agent': 'Mozilla/5.0'}

    # 1) Prova BIS WS_LONG_CPI — dati mensili aggiornati
    try:
        url = (f'https://stats.bis.org/api/v1/data/BIS,WS_LONG_CPI,1.0/'
               f'M.{bis_cc}.771?lastNObservations=3&format=csvdata')
        r = req.get(url, timeout=10, headers=hdrs)
        if r.status_code == 200:
            best = None
            for line in r.text.splitlines():
                if not line or line.startswith('FREQ'):
                    continue
                parts = line.split(',')
                # Cols: FREQ(0) REF_AREA(1) UNIT_MEASURE(2) ... TIME_PERIOD(9) OBS_VALUE(10)
                if len(parts) >= 11:
                    try:
                        period = parts[9]  # YYYY-MM
                        val    = float(parts[10])
                        if best is None or period > best['date']:
                            best = {'v': round(val, 2), 'date': period, 'src': 'BIS'}
                    except (ValueError, IndexError):
                        pass
            if best:
                return best
    except Exception as e:
        print(f'  [macro-live] BIS HICP {bis_cc}: {e}')

    # 2) Fallback ECB ICP (discontinuato feb-2026, ultima obs dic-2025)
    try:
        url = (f'https://data-api.ecb.europa.eu/service/data/'
               f'ICP/M.{country_code}.N.000000.4.ANR'
               f'?lastNObservations=2&format=csvdata')
        r = req.get(url, timeout=10, headers=hdrs)
        if r.status_code == 200:
            for line in reversed(r.text.splitlines()):
                if not line or line.startswith('KEY') or line.startswith('#') or line.startswith('{'):
                    continue
                parts = line.split(',')
                if len(parts) >= 9:
                    try:
                        return {'v': float(parts[8]), 'date': parts[7][:7], 'src': 'ECB'}
                    except ValueError:
                        continue
    except Exception as e:
        print(f'  [macro-live] ECB HICP {country_code}: {e}')
    return None


def _fred_rate(series_id):
    """Ultimo valore da una serie FRED (es. DFEDTARU, IRSTJPRESLN, IRSTCHRESLN)."""
    try:
        import requests as req
        url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}'
        r = req.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return None
        lines = [l for l in r.text.splitlines() if l and not l.startswith('DATE')]
        # scorri dalla fine finché trovi un valore valido (FRED usa '.' per N/D)
        for line in reversed(lines):
            parts = line.split(',')
            if len(parts) < 2:
                continue
            val_str = parts[1].strip()
            if val_str == '.' or not val_str:
                continue
            try:
                return {'v': float(val_str), 'date': parts[0].strip()}
            except ValueError:
                continue
        return None
    except Exception as e:
        print(f'  [macro-live] FRED {series_id}: {e}')
        return None


def _ecb_policy_rates():
    """Tasso BCE depositi (DFR) e rifinanziamento principale (MRR). Restituisce dict."""
    import requests as req

    def _parse_ecb_csv(text):
        """Estrae (value, date) dall'ultima riga dati CSV ECB."""
        for line in reversed(text.splitlines()):
            if not line or line.startswith('KEY') or line.startswith('#') or line.startswith('{'):
                continue
            parts = line.split(',')
            # CSV: KEY(0)…DATA_TYPE_FM(7),TIME_PERIOD(8),OBS_VALUE(9)
            if len(parts) >= 10:
                try:
                    return float(parts[9]), parts[8][:10]
                except (ValueError, IndexError):
                    continue
        return None, None

    result = {}
    hdrs = {'User-Agent': 'Mozilla/5.0 (compatible; MarketMonitor/1.0)'}

    for key, series_id in [('dfr', 'B.U2.EUR.4F.KR.DFR.LEV'),
                            ('mrr', 'B.U2.EUR.4F.KR.MRR_FR.LEV')]:
        v, date = None, None

        # Tentativo 1: ECB Data API principale (CSV)
        try:
            url = f'https://data-api.ecb.europa.eu/service/data/FM/{series_id}?lastNObservations=1&format=csvdata'
            r = req.get(url, timeout=12, headers=hdrs)
            if r.status_code == 200:
                v, date = _parse_ecb_csv(r.text)
        except Exception as e:
            print(f'  [macro-live] ECB CSV {key}: {e}')

        # Tentativo 2: ECB Data API — formato JSON (stesso server, path diverso)
        if v is None:
            try:
                url = f'https://data-api.ecb.europa.eu/service/data/FM/{series_id}?lastNObservations=1&format=jsondata&detail=dataonly'
                r = req.get(url, timeout=12, headers={**hdrs, 'Accept': 'application/json'})
                if r.status_code == 200:
                    d = r.json()
                    datasets = d.get('dataSets', [{}])
                    series = datasets[0].get('series', {}) if datasets else {}
                    for sv in series.values():
                        obs = sv.get('observations', {})
                        if obs:
                            last_key = max(obs.keys(), key=int)
                            v = float(obs[last_key][0])
                            # data dalla struttura structure
                            try:
                                struct = d['structure']['dimensions']['observation']
                                times  = next(dim for dim in struct if dim['id'] == 'TIME_PERIOD')
                                date   = times['values'][int(last_key)]['id'][:10]
                            except Exception:
                                date = str(datetime.date.today())
                            break
            except Exception as e:
                print(f'  [macro-live] ECB JSON {key}: {e}')

        if v is not None:
            result[key] = {'v': v, 'date': date or str(datetime.date.today())}
        else:
            print(f'  [macro-live] ECB {key}: tutti i tentativi falliti')

    return result


def _boe_base_rate():
    """Tasso ufficiale BOE (IUDBEDR) via CSV API."""
    try:
        import requests as req
        today = datetime.date.today()
        start = (today - datetime.timedelta(days=180)).strftime('%d/%b/%Y')
        end   = today.strftime('%d/%b/%Y')
        url   = (f'https://www.bankofengland.co.uk/boeapps/database/_iadb-FromShowColumns.asp'
                 f'?csv.x=yes&Datefrom={start}&Dateto={end}'
                 f'&SeriesCodes=IUDBEDR&CSVF=TT&UsingCodes=Y&VPD=Y&VFD=N')
        r = req.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return None
        for line in reversed(r.text.splitlines()):
            if not line or line.startswith('DATE') or line.startswith('"'):
                continue
            parts = line.split(',')
            if len(parts) >= 2:
                try:
                    return {'v': float(parts[1].strip()), 'date': parts[0].strip()}
                except ValueError:
                    continue
        return None
    except Exception as e:
        print(f'  [macro-live] BOE rate: {e}')
        return None


def fetch_macro_live():
    """Inflazione live (BLS + ECB) + tassi ufficiali live (ECB + BOE). Cache 30 min."""
    global _macro_live_cache, _macro_live_ts
    now = time.time()
    if _macro_live_cache and (now - _macro_live_ts) < MACRO_LIVE_TTL:
        return _macro_live_cache

    print('[macro-live] Fetching inflazione e tassi...')

    # ── Inflazione ─────────────────────────────────────────────
    infl = {}
    us = _bls_fetch()
    if us.get('cpi'):
        infl['US']      = {'name':'USA',      'flag':'🇺🇸', 'label':'CPI',      **us['cpi']}
    if us.get('cpi_core'):
        infl['US_core'] = {'name':'USA Core', 'flag':'🇺🇸', 'label':'CPI Core', **us['cpi_core']}

    for code, name, flag in [('U2','Eurozona','🇪🇺'),('DE','Germania','🇩🇪'),
                               ('IT','Italia','🇮🇹'),('FR','Francia','🇫🇷'),('ES','Spagna','🇪🇸')]:
        d = _ecb_hicp(code)
        if d:
            infl[code] = {'name': name, 'flag': flag, 'label': 'HICP', **d}

    # ── Tassi ufficiali ────────────────────────────────────────
    rates = {}
    ecb = _ecb_policy_rates()
    if ecb.get('dfr'):
        rates['ECB_DFR'] = {'name':'BCE — Depositi', 'flag':'🇪🇺', 'src':'ECB', **ecb['dfr']}
    if ecb.get('mrr'):
        rates['ECB_MRR'] = {'name':'BCE — Rif. Princ.', 'flag':'🇪🇺', 'src':'ECB', **ecb['mrr']}
    boe = _boe_base_rate()
    if boe:
        rates['BOE'] = {'name':'Bank of England', 'flag':'🇬🇧', 'src':'BOE', **boe}

    # Fed funds upper target (DFEDTARU — aggiornato giornalmente da FRED)
    fed = _fred_rate('DFEDTARU')
    if fed:
        rates['FED'] = {'name':'Federal Reserve', 'flag':'🇺🇸', 'src':'FRED', **fed}

    # BOJ policy rate — uncollateralized overnight call rate (IRSTJPRESLN)
    boj = _fred_rate('IRSTJPRESLN')
    if boj:
        rates['BOJ'] = {'name':'Banca del Giappone', 'flag':'🇯🇵', 'src':'FRED', **boj}

    # SNB policy rate — overnight rate (IRSTCHRESLN)
    snb = _fred_rate('IRSTCHRESLN')
    if snb:
        rates['SNB'] = {'name':'SNB Svizzera', 'flag':'🇨🇭', 'src':'FRED', **snb}

    print(f'  [macro-live] inflazione={list(infl.keys())} tassi={list(rates.keys())}')
    result = {'inflation': infl, 'rates': rates, 'ts': int(now)}
    _macro_live_cache = result
    _macro_live_ts = now
    return result


def fetch_sovereign_yields():
    """
    Rendimenti sovrani 10A per DE/IT/ES/FR/PT/PL + Bund curve (2A/5A/10A/30A).
    Fonte: Stooq (dati giornalieri, cache 30 min).
    """
    with _sovereign_lock:
        c = _sovereign_cache.get('sov')
        if c and (time.time() - c['ts']) < SOVEREIGN_TTL:
            return c['data']

    # Simboli 10A per calcolo spread
    SPREAD_BONDS = {
        'DE': {'sym': '10de.b', 'name': 'Germania (Bund)',  'flag': '🇩🇪'},
        'IT': {'sym': '10it.b', 'name': 'Italia (BTP)',     'flag': '🇮🇹'},
        'ES': {'sym': '10es.b', 'name': 'Spagna (Bonos)',   'flag': '🇪🇸'},
        'FR': {'sym': '10fr.b', 'name': 'Francia (OAT)',    'flag': '🇫🇷'},
        'PT': {'sym': '10pt.b', 'name': 'Portogallo (OT)',  'flag': '🇵🇹'},
        'PL': {'sym': '10pl.b', 'name': 'Polonia',          'flag': '🇵🇱'},
    }
    # Simboli Bund per la curva europea
    BUND_CURVE = {
        '2A':  '2de.b',
        '5A':  '5de.b',
        '10A': '10de.b',
        '30A': '30de.b',
    }

    # FRED series IDs per fallback (dati mensili, nessun API key)
    FRED_SERIES = {
        'DE': 'IRLTLT01DEM156N',
        'IT': 'IRLTLT01ITM156N',
        'ES': 'IRLTLT01ESM156N',
        'FR': 'IRLTLT01FRM156N',
        'PT': 'IRLTLT01PTM156N',
        'PL': 'IRLTLT01PLM156N',
    }

    all_tasks = {}
    for country, info in SPREAD_BONDS.items():
        all_tasks[f'sov_{country}'] = info['sym']
    for mat, sym in BUND_CURVE.items():
        all_tasks[f'bund_{mat}'] = sym

    raw = {}
    def _fetch(key, sym):
        return key, _stooq_fetch(sym)

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_fetch, k, v): k for k, v in all_tasks.items()}
        for fut in as_completed(futures, timeout=20):
            try:
                k, v = fut.result()
                if v:
                    raw[k] = v
            except Exception:
                pass

    # Fallback 1: FRED per i paesi sovrani che Stooq non ha restituito
    missing_countries = [cc for cc in SPREAD_BONDS if f'sov_{cc}' not in raw]
    if missing_countries:
        print(f'  [sovereign] Stooq mancante per {missing_countries}, tentativo FRED…')
        def _fred_fallback(cc):
            series = FRED_SERIES.get(cc)
            if not series:
                return cc, None
            return cc, _fred_fetch_yield(series)
        with ThreadPoolExecutor(max_workers=3) as ex:
            for cc, d in ex.map(_fred_fallback, missing_countries):
                if d:
                    raw[f'sov_{cc}'] = d
                    print(f'  [sovereign] FRED OK per {cc}: {d["yield"]}%')

    # Fallback 2: ECB Data API per paesi Eurozona (Stooq e FRED entrambi bloccati)
    # ECB API: https://data-api.ecb.europa.eu — server BCE, diverso da FRED/Stooq
    ECB_COUNTRIES = {'DE', 'IT', 'ES', 'FR', 'PT'}  # Paesi eurozona con dati ECB IRS
    missing_ecb = [cc for cc in SPREAD_BONDS if f'sov_{cc}' not in raw and cc in ECB_COUNTRIES]
    if missing_ecb:
        print(f'  [sovereign] FRED mancante per {missing_ecb}, tentativo ECB API…')
        def _ecb_fetch(cc):
            try:
                import requests as req
                # ECB Interest Rate Statistics (IRS): rendimenti titoli di stato 10A
                url = (f'https://data-api.ecb.europa.eu/service/data/IRS/'
                       f'M.{cc}.L.L40.CI.0.EUR.N.Z'
                       f'?lastNObservations=3&format=csvdata')
                r = req.get(url, timeout=10,
                            headers={'Accept': 'text/csv', 'User-Agent': 'Mozilla/5.0'})
                if r.status_code != 200:
                    return cc, None
                lines = [l for l in r.text.strip().split('\n') if l.strip()]
                # CSV: KEY,FREQ,...,TIME_PERIOD,OBS_VALUE,...
                # Header nella prima riga, dati nelle successive
                data_lines = [l for l in lines[1:] if l.strip()]
                if len(data_lines) < 1:
                    return cc, None
                # Parse: TIME_PERIOD = col 10, OBS_VALUE = col 11 (0-indexed)
                vals = []
                for line in data_lines:
                    parts = line.split(',')
                    if len(parts) < 12:
                        continue
                    try:
                        period = parts[10].strip()  # e.g. "2025-03"
                        val    = float(parts[11].strip())
                        vals.append((period, val))
                    except (ValueError, IndexError):
                        continue
                if len(vals) < 1:
                    return cc, None
                date, last = vals[-1]
                _, prev    = vals[-2] if len(vals) >= 2 else (date, last)
                return cc, {'yield': round(last,3), 'prev': round(prev,3),
                            'chg': round(last-prev,3), 'date': f'{date} (ECB)'}
            except Exception as e:
                print(f'  [sovereign] ECB {cc}: {e}')
                return cc, None

        with ThreadPoolExecutor(max_workers=3) as ex:
            for cc, d in ex.map(_ecb_fetch, missing_ecb):
                if d:
                    raw[f'sov_{cc}'] = d
                    print(f'  [sovereign] ECB OK per {cc}: {d["yield"]}%')

    # Fallback finale: valori statici etichettati (quando tutte le fonti sono bloccate)
    STATIC_FALLBACK = {
        'DE': 2.92, 'IT': 3.84, 'ES': 3.69,
        'FR': 3.57, 'PT': 3.64, 'PL': 5.25,
    }
    still_missing = [cc for cc in SPREAD_BONDS if f'sov_{cc}' not in raw]
    if still_missing:
        print(f'  [sovereign] tutte le fonti bloccate per {still_missing}, uso statico')
        for cc in still_missing:
            v = STATIC_FALLBACK.get(cc)
            if v:
                raw[f'sov_{cc}'] = {
                    'yield': v, 'prev': v, 'chg': 0.0,
                    'date': '⚠ statico Apr 2025',
                }

    # Spread vs Bund
    de_data = raw.get('sov_DE')
    de_yield = de_data['yield'] if de_data else None
    spreads = {}
    for country, info in SPREAD_BONDS.items():
        d = raw.get(f'sov_{country}')
        if not d:
            continue
        spread_bp = round((d['yield'] - de_yield) * 100, 1) if de_yield is not None and country != 'DE' else 0
        spreads[country] = {
            'yield': d['yield'], 'prev': d['prev'],
            'chg': d['chg'], 'date': d['date'],
            'spread_bp': spread_bp,
            'name': info['name'], 'flag': info['flag'],
        }

    # Curva Bund
    bund_curve = {}
    for mat in BUND_CURVE:
        d = raw.get(f'bund_{mat}')
        if d:
            bund_curve[mat] = d

    result = {
        'spreads':    spreads,
        'bund_curve': bund_curve,
        'de_yield':   de_yield,
        'ts':         int(time.time()),
    }
    with _sovereign_lock:
        _sovereign_cache['sov'] = {'data': result, 'ts': time.time()}
    return result


def fetch_macro_data():
    """Yield curve USA + VIX/DXY tramite Yahoo Finance Ticker.history() sequenziale.
    Confermato funzionante su Railway. Cache 15 min."""
    with _macro_lock:
        c = _macro_cache.get('macro')
        if c and (time.time() - c['ts']) < MACRO_TTL:
            return c['data']

    # Fetch sequenziale: un Ticker.history() alla volta, protetto da semaforo + cooldown.
    # ^TNX confermato funzionante da /debug-sources. FRED/Stooq timeout su Railway.
    YF_YIELD_SYMS = [
        ('y3m',  '^IRX'),     # 13-week T-bill ≈ 3 mesi
        ('y5y',  '^FVX'),     # 5-year T-note
        ('y10y', '^TNX'),     # 10-year T-note
        ('y30y', '^TYX'),     # 30-year T-bond
        ('vix',  '^VIX'),
        ('dxy',  'DX-Y.NYB'),
    ]
    out = {}
    for key, sym in YF_YIELD_SYMS:
        try:
            _yf_wait_cooldown()
            with _yf_semaphore:
                _tk = yf.Ticker(sym)
                hist = _yf_history(_tk, timeout=10, period='5d', interval='1d')
            if hist.empty:
                continue
            closes = hist['Close'].dropna()
            if len(closes) < 1:
                continue
            last = float(closes.iloc[-1])
            prev = float(closes.iloc[-2]) if len(closes) >= 2 else last
            out[key] = {
                'v':   round(last, 4),
                'p':   round(prev, 4),
                'chg': round(last - prev, 4),
                'pct': round((last / prev - 1) * 100, 2) if prev else 0,
            }
        except Exception as e:
            if is_rate_limit_error(e):
                _yf_set_cooldown(10)
            print(f'  [macro] YF {sym}: {e}')

    # 2Y: interpola tra 3M e 5Y (Yahoo Finance non ha ^IRX2Y)
    if 'y2y' not in out and 'y3m' in out and 'y5y' in out:
        est = round(out['y3m']['v'] * 0.45 + out['y5y']['v'] * 0.55, 4)
        out['y2y'] = {'v': est, 'p': est, 'chg': 0, 'pct': 0, 'estimated': True}

    print(f'  [macro] yields ottenuti: {list(out.keys())}')
    result = {'yields': out, 'ts': int(time.time())}
    with _macro_lock:
        _macro_cache['macro'] = {'data': result, 'ts': time.time()}
    return result


def fetch_aggregated_news():
    """Flusso aggregato da più fonti RSS — cache 10 min."""
    with _rss_lock:
        c = _rss_cache.get('feed')
        if c and (time.time() - c['ts']) < RSS_TTL:
            return c['data']

    all_items = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_parse_rss_source, src): src for src in RSS_SOURCES}
        for fut in as_completed(futures, timeout=18):
            try:
                all_items.extend(fut.result())
            except Exception:
                pass

    # Traduzione in parallelo — solo titoli (sommari in italiano già dall'utente)
    to_tr = [it for it in all_items if it.get('lang') != 'it']
    if to_tr:
        def _tr(item):
            item['title']   = _translate_it(item['title'])
            if item['summary']:
                item['summary'] = _translate_it(item['summary'][:300])
            return item
        with ThreadPoolExecutor(max_workers=4) as ex:
            list(ex.map(_tr, to_tr))

    # Ordina per data, deduplicazione per titolo (primi 55 caratteri)
    all_items.sort(key=lambda x: x['ts'], reverse=True)
    seen, unique = set(), []
    for it in all_items:
        key = it['title'][:55].lower().strip()
        if key not in seen and it['title']:
            seen.add(key)
            unique.append(it)

    result = unique[:150]
    with _rss_lock:
        _rss_cache['feed'] = {'data': result, 'ts': time.time()}
    return result


def fetch_news(symbol):
    """Ultime news via yfinance — cache 15 min.
    Prova prima con il ticker diretto; se vuoto (indici/valute/ETF) usa
    yf.Search() con la keyword derivata dal simbolo."""
    with _news_lock:
        c = _news_cache.get(symbol)
        if c and (time.time() - c['ts']) < NEWS_TTL:
            return c['data']
    data = []
    try:
        # ── Tentativo 1: news diretta per ticker (funziona per azioni) ──
        _tk_news = yf.Ticker(symbol)
        news = _yf_call(lambda: _tk_news.news, timeout=10, default=[]) or []

        # ── Tentativo 2: ricerca per keyword da simbolo (indici/valute) ──
        if not news:
            query = _symbol_to_query(symbol)
            try:
                sr   = _yf_call(lambda: yf.Search(query, news_count=8, enable_fuzzy_query=False), timeout=8, default=None)
                news = getattr(sr, 'news', []) or [] if sr else []
            except Exception:
                pass

        # ── Tentativo 3: nome completo da info (ETF europei) ─────────────
        if not news:
            try:
                info       = _yf_call(lambda: yf.Ticker(symbol).info, timeout=10, default={}) or {}
                short_name = info.get('shortName') or info.get('longName', '')
                if short_name:
                    sr   = yf.Search(short_name, news_count=8, enable_fuzzy_query=False)
                    news = getattr(sr, 'news', []) or []
            except Exception:
                pass

        for n in news[:8]:
            title, summary, url, publisher, pub_time = _parse_news_item(n)
            if not title:
                continue
            data.append({'title': title, 'summary': summary, 'url': url,
                         'publisher': publisher, 'time': pub_time})
    except Exception:
        pass

    # Traduci titoli e sommari in italiano (parallelo)
    if data:
        def translate_item(item):
            item['title']   = _translate_it(item['title'])
            item['summary'] = _translate_it(item['summary'])
            return item
        with ThreadPoolExecutor(max_workers=4) as ex:
            data = list(ex.map(translate_item, data))
    with _news_lock:
        _news_cache[symbol] = {'data': data, 'ts': time.time()}
    return data


def fetch_monthly(symbol):
    """Rendimenti mensili per matrice anno×mese — cache 1h.

    USA DATI GIORNALIERI + resample pandas per evitare il bug di yfinance
    con interval='1mo' (etichettatura barre ambigua tra versioni).
    auto_adjust=False: prezzi raw (coerente col resto del dashboard).
    """
    with _monthly_lock:
        c = _monthly_cache.get(symbol)
        if c and (time.time() - c['ts']) < MONTHLY_TTL:
            return c['data']
    try:
        import pandas as pd
        today    = datetime.date.today()
        # Partiamo da dicembre 5 anni fa → serve per calcolare gennaio dell'anno successivo
        start    = datetime.date(today.year - 6, 12, 1)
        end_date = today + datetime.timedelta(days=1)

        _tk_m = yf.Ticker(symbol)
        # Dati giornalieri auto-adjusted (dividendi + split già incorporati)
        hist = _yf_history(_tk_m, timeout=25, start=str(start), end=str(end_date),
                           interval='1d', auto_adjust=False)
        if hist is None or hist.empty:
            return {'error': 'Nessun dato disponibile'}

        # Ultimo close di ogni mese (Month End) — attributo inequivocabile
        closes_daily = hist['Close'].dropna()
        if len(closes_daily) < 2:
            return {'error': 'Dati insufficienti'}

        monthly_closes = _resample_month_end(closes_daily)
        if len(monthly_closes) < 2:
            return {'error': 'Dati mensili insufficienti'}

        monthly = {}
        prev_price = None
        for dt, price in monthly_closes.items():
            price = float(price)
            if math.isnan(price) or price <= 0:
                prev_price = price
                continue
            if prev_price is None or math.isnan(prev_price) or prev_price <= 0:
                prev_price = price
                continue

            # Il ritorno appartiene al MESE della barra corrente (dt = fine mese)
            year  = str(dt.year)
            month = str(dt.month)
            ret   = round((price / prev_price - 1) * 100, 2)
            if not math.isnan(ret):
                if year not in monthly:
                    monthly[year] = {}
                monthly[year][month] = ret
            prev_price = price

        # Filtra solo anni richiesti (ultimi 6)
        min_year = str(today.year - 5)
        monthly = {y: m for y, m in monthly.items() if y >= min_year}

        # YTD composto per ogni anno
        ytd = {}
        for year, months in monthly.items():
            c = 1.0
            for m in sorted(months.keys(), key=int):
                v = months[m]
                if not math.isnan(v):
                    c *= 1 + v / 100
            ytd[year] = round((c - 1) * 100, 2)

        result = {'data': monthly, 'years': sorted(monthly.keys()), 'ytd': ytd}
    except Exception as e:
        result = {'error': str(e)}

    with _monthly_lock:
        _monthly_cache[symbol] = {'data': result, 'ts': time.time()}
    return result


def fetch_weekly(symbol):
    """Rendimenti settimanali da inizio anno corrente (ISO) — cache 1h."""
    with _weekly_lock:
        c = _weekly_cache.get(symbol)
        if c and (time.time() - c['ts']) < WEEKLY_TTL:
            return c['data']
    try:
        today    = datetime.date.today()
        cur_year = today.year
        # Partiamo da dicembre dell'anno precedente per avere il close di chiusura
        # dell'ultima settimana dell'anno scorso (necessario per calcolare il rendimento
        # della settimana 1 dell'anno corrente, che spesso inizia a fine dicembre ISO)
        # Usa dati giornalieri + resample settimanale (W-FRI = settimana chiusa venerdì)
        # per evitare bug di yfinance con interval='1wk'
        start    = datetime.date(cur_year - 1, 12, 1)
        end_date = today + datetime.timedelta(days=1)
        _tk_w = yf.Ticker(symbol)
        hist  = _yf_history(_tk_w, timeout=20, start=str(start), end=str(end_date),
                            interval='1d', auto_adjust=False)
        if hist is None or hist.empty:
            return {'error': 'Nessun dato settimanale'}

        import pandas as pd
        closes_daily = hist['Close'].dropna()
        # Resample a settimane (venerdì come fine settimana ISO)
        weekly_closes = closes_daily.resample('W').last().dropna()

        weekly = {}
        prev_price = None
        for dt, price in weekly_closes.items():
            price = float(price)
            if math.isnan(price) or price <= 0:
                prev_price = price
                continue
            if prev_price is None or math.isnan(prev_price) or prev_price <= 0:
                prev_price = price
                continue
            iso      = dt.isocalendar()
            iso_year = str(iso[0])
            iso_week = str(iso[1])
            if iso_year != str(cur_year):
                prev_price = price
                continue
            ret = round((price / prev_price - 1) * 100, 2)
            if not math.isnan(ret):
                if iso_year not in weekly:
                    weekly[iso_year] = {}
                weekly[iso_year][iso_week] = ret
            prev_price = price

        years = sorted(weekly.keys())
        result = {'data': weekly, 'years': years}
    except Exception as e:
        result = {'error': str(e)}

    with _weekly_lock:
        _weekly_cache[symbol] = {'data': result, 'ts': time.time()}
    return result


def fetch_seasonal(symbol):
    """Stagionalità: rendimento medio mensile su 10 anni — cache 6h.

    Usa dati giornalieri + resample pandas per evitare il bug di yfinance
    con interval='1mo' (attribuzione errata del mese).
    """
    with _seasonal_lock:
        c = _seasonal_cache.get(symbol)
        if c and (time.time() - c['ts']) < SEASONAL_TTL:
            return c['data']
    try:
        import pandas as pd
        today = datetime.date.today()
        # Inizia da novembre dell'anno -10 per avere dicembre come riferimento per gennaio
        start    = datetime.date(today.year - 11, 12, 1)
        end_date = today + datetime.timedelta(days=1)
        _tk_s = yf.Ticker(symbol)
        hist  = _yf_history(_tk_s, timeout=25, start=str(start), end=str(end_date),
                            interval='1d', auto_adjust=False)
        if hist is None or hist.empty:
            result = {'error': 'Nessun dato stagionale disponibile'}
        else:
            closes_daily = hist['Close'].dropna()
            # Ultimo close di ogni mese — etichetta = fine mese (es. 2023-04-30)
            monthly_closes = _resample_month_end(closes_daily)

            MONTHS_IT = ['Gen','Feb','Mar','Apr','Mag','Giu',
                         'Lug','Ago','Set','Ott','Nov','Dic']
            by_month = {m: [] for m in range(1, 13)}
            yearly   = {}

            prev_price = None
            for dt, price in monthly_closes.items():
                price = float(price)
                if math.isnan(price) or price <= 0:
                    prev_price = price
                    continue
                if prev_price is None or math.isnan(prev_price) or prev_price <= 0:
                    prev_price = price
                    continue

                # dt = fine mese → dt.month è il mese corretto del ritorno
                ret  = round((price / prev_price - 1) * 100, 2)
                year = str(dt.year)
                mon  = dt.month

                # Includi solo gli ultimi 10 anni completi
                if dt.year < today.year - 10:
                    prev_price = price
                    continue

                if not math.isnan(ret):
                    by_month[mon].append(ret)
                    if year not in yearly:
                        yearly[year] = {}
                    yearly[year][str(mon)] = ret
                prev_price = price

            avg_by_month = {}
            pos_rate     = {}
            for m in range(1, 13):
                vals = by_month[m]
                avg_by_month[str(m)] = round(sum(vals) / len(vals), 2) if vals else None
                pos_rate[str(m)]     = round(sum(1 for v in vals if v > 0) / len(vals) * 100, 0) if vals else None

            result = {
                'avg':     avg_by_month,
                'posRate': pos_rate,
                'months':  MONTHS_IT,
                'yearly':  yearly,
                'years':   sorted(yearly.keys()),
            }
    except Exception as e:
        result = {'error': str(e)}

    with _seasonal_lock:
        _seasonal_cache[symbol] = {'data': result, 'ts': time.time()}
    return result


def fetch_calendar():
    """Calendario macro alto impatto da ForexFactory — cache 30 min."""
    with _cal_lock:
        c = _cal_cache.get('cal')
        if c and (time.time() - c['ts']) < CAL_TTL:
            return c['data']
    data = []
    try:
        import requests as req
        headers = {'User-Agent': 'Mozilla/5.0'}
        events = []
        for url in ['https://nfs.faireconomy.media/ff_calendar_thisweek.json',
                    'https://nfs.faireconomy.media/ff_calendar_nextweek.json']:
            try:
                r = req.get(url, timeout=8, headers=headers)
                events.extend(r.json())
            except Exception:
                pass
        high = [e for e in events if e.get('impact') == 'High']
        high.sort(key=lambda e: e.get('date', ''))
        data = high[:40]
    except Exception:
        pass
    with _cal_lock:
        _cal_cache['cal'] = {'data': data, 'ts': time.time()}
    return data



class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        path = self.path[4:] if self.path.startswith('/yf/') else self.path
        print(f'  {args[1]}  {path}')

    def _json(self, data):
        def _sanitize(obj):
            if isinstance(obj, float):
                return None if (math.isnan(obj) or math.isinf(obj)) else obj
            if isinstance(obj, dict):
                return {k: _sanitize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_sanitize(v) for v in obj]
            return obj
        body = json.dumps(_sanitize(data), default=str).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self):
        # Impedisce la cache del browser per HTML e JS
        # Consenti cache solo per route con dati frequenti; /macro-data e /sovereign-yields
        # devono avere no-cache (NON escluderli con /ma che fa startswith match errato!)
        _p = self.path
        _allow_cache = (_p.startswith('/yf') or _p.startswith('/fundamentals')
                        or _p.startswith('/news') or _p.startswith('/weekly')
                        or _p.startswith('/monthly') or _p.startswith('/calendar')
                        or (_p.startswith('/ma') and not _p.startswith('/macro')))
        if not _allow_cache:
            self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
            self.send_header('Pragma', 'no-cache')
        super().end_headers()

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return
        if self.path.startswith('/yf/batch'):
            self.handle_batch()
        elif self.path.startswith('/yf/'):
            self.handle_yf()
        elif self.path.startswith('/weekly/'):
            sym = urllib.parse.unquote(self.path[len('/weekly/'):].split('?')[0])
            self._json(fetch_weekly(sym))
        elif self.path.startswith('/monthly/'):
            sym = urllib.parse.unquote(self.path[len('/monthly/'):].split('?')[0])
            self._json(fetch_monthly(sym))
        elif self.path.startswith('/fundamentals/'):
            sym = urllib.parse.unquote(self.path[len('/fundamentals/'):].split('?')[0])
            self._json(fetch_fundamentals(sym))
        elif self.path.startswith('/news/'):
            sym = urllib.parse.unquote(self.path[len('/news/'):].split('?')[0])
            self._json(fetch_news(sym))
        elif self.path.startswith('/calendar'):
            self._json(fetch_calendar())
        # ATTENZIONE: /macro-data e /macro- DEVONO stare PRIMA di /ma
        # perché '/macro-data'.startswith('/ma') == True!
        elif self.path.startswith('/macro-live'):
            self._json(fetch_macro_live())
        elif self.path.startswith('/macro-data'):
            self._json(fetch_macro_data())
        elif self.path.startswith('/global-yields'):
            self._json(fetch_global_yields())
        elif self.path.startswith('/sovereign-yields'):
            self._json(fetch_sovereign_yields())
        elif self.path.startswith('/debug-sources'):
            self._json(self._debug_sources())
        elif self.path == '/ma' or self.path.startswith('/ma?'):
            parsed   = urllib.parse.urlparse(self.path)
            params   = urllib.parse.parse_qs(parsed.query)
            syms_raw = params.get('syms', [''])[0]
            symbols  = [s.strip() for s in syms_raw.split(',') if s.strip()]
            self._json(fetch_ma_batch(symbols))
        elif self.path.startswith('/seasonal/'):
            sym = urllib.parse.unquote(self.path[len('/seasonal/'):].split('?')[0])
            self._json(fetch_seasonal(sym))
        elif self.path.startswith('/correlation'):
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            syms   = [s for s in params.get('symbols', [''])[0].split(',') if s]
            days   = int(params.get('days', ['252'])[0])
            self._json(fetch_correlation(syms, days))
        elif self.path.startswith('/news-feed'):
            self._json(fetch_aggregated_news())
        elif self.path == '/api/lists':
            with _lists_lock:
                self._json(dict(_lists_data))
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == '/api/lists':
            try:
                length = int(self.headers.get('Content-Length', 0))
                body   = self.rfile.read(length)
                data   = json.loads(body)
                with _lists_lock:
                    global _lists_data
                    _lists_data = {**_LISTS_DEFAULT, **data}
                    _save_lists(_lists_data)
                self._json({'ok': True})
            except Exception as e:
                self._json({'ok': False, 'error': str(e)})
        else:
            self.send_response(404)
            self.end_headers()

    def _debug_sources(self):
        """Endpoint di debug: testa le sorgenti dati e restituisce i risultati."""
        import requests as req
        results = {}
        def _yf_test(sym, label):
            try:
                hist = yf.Ticker(sym).history(period='3d', interval='1d')
                closes = hist['Close'].dropna() if not hist.empty else []
                results[label] = {'ok': not hist.empty, 'rows': len(hist),
                                  'last': float(closes.iloc[-1]) if len(closes) else None}
            except Exception as e:
                results[label] = {'ok': False, 'error': str(e)[:120]}
        def _http_test(url, label, timeout=8):
            try:
                r = req.get(url, timeout=timeout, headers={'User-Agent': 'Mozilla/5.0'})
                results[label] = {'ok': r.status_code == 200, 'status': r.status_code,
                                  'preview': r.text[:150]}
            except Exception as e:
                results[label] = {'ok': False, 'error': str(e)[:120]}
        # Yahoo Finance: simboli yield
        for sym, label in [('^TNX','yf_10y'),('^IRX','yf_3m'),('^FVX','yf_5y'),('^TYX','yf_30y'),('^VIX','yf_vix')]:
            _yf_test(sym, label)
        # FRED
        _http_test('https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10&observation_start=2025-03-01', 'fred')
        # Stooq
        _http_test('https://stooq.com/q/d/l/?s=10de.b&i=d', 'stooq')
        # ECB API
        _http_test('https://data-api.ecb.europa.eu/service/data/IRS/M.IT.L.L40.CI.0.EUR.N.Z?lastNObservations=2&format=csvdata', 'ecb')
        return results

    def handle_batch(self):
        parsed   = urllib.parse.urlparse(self.path)
        params   = urllib.parse.parse_qs(parsed.query)
        syms_raw = params.get('syms', [''])[0]
        symbols  = [s.strip() for s in syms_raw.split(',') if s.strip()]
        period   = params.get('period',   ['3mo'])[0]
        interval = params.get('interval', ['1d'])[0]

        self._json(fetch_batch_bulk(symbols, period, interval))

    def handle_yf(self):
        parts  = self.path[4:].split('?', 1)
        symbol = urllib.parse.unquote(parts[0].lstrip('/'))

        params   = {}
        if len(parts) > 1:
            for kv in parts[1].split('&'):
                k, _, v = kv.partition('=')
                params[k] = urllib.parse.unquote(v)

        period   = params.get('period', '3mo')
        interval = params.get('interval', '1d')

        self._json(fetch_symbol(symbol, period=period, interval=interval))


def _start_cache_warmup():
    """Popola le cache macro/yields in background all'avvio, così il primo utente non aspetta."""
    def _run():
        time.sleep(3)  # breve pausa per completare il binding del server
        print('[warmup] Avvio pre-riscaldamento cache macro e rendimenti…')
        try:
            fetch_global_yields()
            print('[warmup] global-yields OK')
        except Exception as e:
            print(f'[warmup] global-yields: {e}')
        try:
            fetch_macro_live()
            print('[warmup] macro-live OK')
        except Exception as e:
            print(f'[warmup] macro-live: {e}')
        print('[warmup] Pre-riscaldamento completato.')
    threading.Thread(target=_run, daemon=True, name='cache-warmup').start()

_start_cache_warmup()


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f'\n  Monitor Mercati — http://localhost:{PORT}/monitor-mercati.html\n')
    print('  (Ctrl+C per fermare)\n')
    class _Server(http.server.ThreadingHTTPServer):
        daemon_threads = True          # I thread hung non bloccano il restart
        request_queue_size = 50        # Coda connessioni OS
        allow_reuse_address = True

    with _Server(('', PORT), Handler) as srv:
        print(f'  Server avviato (daemon_threads=True, max queue={srv.request_queue_size})')
        srv.serve_forever()
