"""
Monitor Mercati - Server locale con yfinance
Avvia con: python3 server.py
Apri nel browser: http://localhost:8080/monitor-mercati.html
"""
import http.server
import urllib.parse
import urllib.request
import json
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

PORT = int(os.environ.get('PORT', 8080))

# ── Semaforo globale: max 6 richieste Yahoo Finance in parallelo ──────────
_yf_semaphore = threading.Semaphore(6)
_yf_cooldown_until = 0.0   # timestamp: se > now, aspetta prima di fare richieste YF
_yf_cooldown_lock  = threading.Lock()

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

# Cache: chiave = "symbol|period|interval"
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 300  # 5 minuti (periodi lunghi durano di più)
CACHE_TTL_LONG = 3600  # 1 ora per 2Y/3Y/5Y/max

# Cache per fondamentali, news, calendario, AI
_fund_cache    = {}; _fund_lock    = threading.Lock(); FUND_TTL    = 3600
_news_cache    = {}; _news_lock    = threading.Lock(); NEWS_TTL    = 900
_cal_cache     = {}; _cal_lock     = threading.Lock(); CAL_TTL     = 1800
_monthly_cache = {}; _monthly_lock = threading.Lock(); MONTHLY_TTL = 3600
_weekly_cache  = {}; _weekly_lock  = threading.Lock(); WEEKLY_TTL  = 3600
_ma_cache      = {}; _ma_lock      = threading.Lock(); MA_TTL      = 300
_seasonal_cache= {}; _seasonal_lock= threading.Lock(); SEASONAL_TTL= 21600  # 6 ore
_corr_cache  = {}; _corr_lock  = threading.Lock(); CORR_TTL  = 1800
_rss_cache   = {}; _rss_lock   = threading.Lock(); RSS_TTL   = 600   # 10 min
_macro_cache     = {}; _macro_lock     = threading.Lock(); MACRO_TTL     = 900   # 15 min
_sovereign_cache = {}; _sovereign_lock = threading.Lock(); SOVEREIGN_TTL = 1800  # 30 min
_global_yields_cache = None; _global_yields_ts = 0.0; GLOBAL_YIELDS_TTL = 900  # 15 min
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
            hist  = ticker.history(start=start, end=today, interval=interval, auto_adjust=False)
        elif period == 'max':
            hist  = ticker.history(start='1970-01-01', end=today, interval=interval, auto_adjust=False)
        else:
            hist  = ticker.history(period=period, interval=interval, auto_adjust=False)
        info   = ticker.fast_info

        if hist.empty:
            return {'error': f'Nessun dato per {symbol}'}

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
                full_info = ticker.info
                raw = (full_info.get('yield') or full_info.get('dividendYield')
                       or full_info.get('trailingAnnualDividendYield'))
                if raw and raw > 0:
                    yield_pct = round(raw * 100, 3)
            except Exception:
                pass
            # Fallback: calcola yield da dividendi degli ultimi 12 mesi
            if yield_pct is None:
                try:
                    divs = ticker.dividends
                    if not divs.empty:
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
            raw = yf.download(to_fetch, start=str(start), end=str(today),
                              interval=interval, auto_adjust=False,
                              group_by='ticker', progress=False, threads=True)
        elif period == 'max':
            raw = yf.download(to_fetch, start='1970-01-01', end=str(today),
                              interval=interval, auto_adjust=False,
                              group_by='ticker', progress=False, threads=True)
        else:
            # 400 giorni garantisce: priceAtDec31 (fine anno prec.) + p180 (118 barre) + p365 (240 barre)
            start = today - datetime.timedelta(days=400)
            end   = today + datetime.timedelta(days=1)
            raw = yf.download(to_fetch, start=str(start), end=str(end),
                              interval=interval, auto_adjust=False,
                              group_by='ticker', progress=False, threads=True)
    except Exception as e:
        print(f'  [batch] yf.download fallito ({e}), uso fetch individuale a chunk')
        if is_rate_limit_error(e):
            _yf_set_cooldown(20)
        # Suddividi in chunk da 15 con pausa tra chunk per evitare rate limit
        CHUNK = 15
        for i in range(0, len(to_fetch), CHUNK):
            chunk = to_fetch[i:i + CHUNK]
            _yf_wait_cooldown()
            with ThreadPoolExecutor(max_workers=5) as ex:
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
                },
                'timestamp': timestamps,
                'closes':    closes,
                'opens':     opens,
                'highs':     highs,
                'lows':      lows,
                'volumes':   volumes,
                'yield_pct': None,
            }

            results[sym] = data
            with _cache_lock:
                _cache[f'{sym}|{period}|{interval}'] = {'data': data, 'ts': time.time()}

        except Exception as e:
            results[sym] = {'error': str(e)}

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
        raw = yf.download(
            to_fetch,
            start=str(start),
            end=str(today + datetime.timedelta(days=1)),
            interval='1d',
            auto_adjust=False,
            group_by='ticker',
            progress=False,
            threads=True,
        )

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
                hist = yf.Ticker(sym).history(
                    start=str(start_l), end=str(today_l),
                    interval='1d', auto_adjust=False
                )
                if hist.empty:
                    return sym, {'above50': None, 'above200': None, 'ma50': None, 'ma200': None}
                closes = [float(v) for v in hist['Close'].tolist() if v == v]
                return sym, calc_ma(sym, closes)
            except Exception:
                return sym, {'above50': None, 'above200': None, 'ma50': None, 'ma200': None}

        with ThreadPoolExecutor(max_workers=10) as ex:
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
    for attempt in range(4):
        _yf_wait_cooldown()
        try:
            with _yf_semaphore:
                t    = yf.Ticker(symbol)
                info = t.info
            # yfinance a volte restituisce una stringa di errore come valore
            if not info or isinstance(list(info.values())[0] if info else None, str) and 'Too Many' in str(list(info.values())[0]):
                raise Exception('Too Many Requests')
            break   # successo
        except Exception as e:
            if is_rate_limit_error(e):
                wait = 2 ** (attempt + 1)  # 2s, 4s, 8s, 16s
                print(f'  [fundamentals] rate limit {symbol}, retry {attempt+1} fra {wait}s')
                _yf_set_cooldown(wait)
                time.sleep(wait)
                continue
            # Errore non-rate-limit: esci subito
            return {'error': str(e)}
    else:
        return {'error': 'Troppi tentativi, riprova tra qualche secondo.'}
    if info is None:
        return {'error': 'Nessun dato disponibile.'}
    try:
        dy  = info.get('dividendYield')
        # yfinance restituisce solitamente decimale (0.018 = 1.8%)
        # Valori > 0.50 come decimale = > 50% → quasi certamente errati (es. BABA 0.77)
        # Normalizzazione: decimale < 0.50 → *100; già-percentuale ≤ 25 → as-is; > 25 → scarta
        dy_norm = None
        if dy:
            if dy < 0.50:        # formato decimale, es. 0.018 → 1.8%
                dy_norm = round(dy * 100, 2)
            elif dy <= 25.0:     # già in percentuale, es. 1.8 → 1.8%
                dy_norm = round(dy, 2)
            # else > 25% → implausibile, dy_norm resta None
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
            qi = t.quarterly_income_stmt
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
            cal    = t.calendar
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

        # ── Consensus analisti ────────────────────────────────
        data['analystConsensus'] = info.get('recommendationKey')       # 'strong_buy','buy','hold','sell','underperform'
        data['analystCount']     = info.get('numberOfAnalystOpinions')
        data['analystScore']     = info.get('recommendationMean')      # 1=Strong Buy … 5=Strong Sell
        data['targetMean']       = info.get('targetMeanPrice')
        data['targetHigh']       = info.get('targetHighPrice')
        data['targetLow']        = info.get('targetLowPrice')
        data['targetMedian']     = info.get('targetMedianPrice')

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
                data['analystBreakdown'] = {
                    'strongBuy':  _ri(curr,'strongBuy'),
                    'buy':        _ri(curr,'buy'),
                    'hold':       _ri(curr,'hold'),
                    'sell':       _ri(curr,'sell'),
                    'strongSell': _ri(curr,'strongSell'),
                }
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
            raw = yf.download(
                symbols, start=str(start), end=str(end_date),
                interval='1d', auto_adjust=False,
                group_by='ticker', progress=False, threads=True
            )

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
    # ── Italiane (dirette) ────────────────────────────────────────────────────
    {'url':'https://www.ilsole24ore.com/rss/economia-e-finanza.xml',                              'src':'Il Sole 24 Ore',   'lang':'it'},
    {'url':'https://www.milanofinanza.it/rss',                                                    'src':'Milano Finanza',   'lang':'it'},
    {'url':'https://www.ansa.it/sito/notizie/economia/economia_rss.xml',                          'src':'ANSA',             'lang':'it'},
    {'url':'https://www.corriere.it/rss/economia.xml',                                            'src':'Corriere Eco.',    'lang':'it'},
    {'url':'https://www.repubblica.it/rss/economia/rss2.0.xml',                                   'src':'Repubblica Eco.',  'lang':'it'},
    # ── Reuters (feed RSS diretto dismesso 2020 — via Google News Search) ──────
    {'url':'https://news.google.com/rss/search?q=reuters+finance+markets+stocks&hl=en&gl=US&ceid=US:en',       'src':'Reuters', 'lang':'en'},
    {'url':'https://news.google.com/rss/search?q=reuters+economy+central+bank+bonds&hl=en&gl=US&ceid=US:en',  'src':'Reuters', 'lang':'en'},
    # ── Internazionali (dirette) ──────────────────────────────────────────────
    {'url':'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114','src':'CNBC',             'lang':'en'},
    {'url':'https://feeds.content.dowjones.io/public/rss/mw_topstories',                         'src':'MarketWatch',      'lang':'en'},
    {'url':'https://feeds.content.dowjones.io/public/rss/mw_marketpulse',                        'src':'MarketWatch Mkt',  'lang':'en'},
    {'url':'https://feeds.bloomberg.com/markets/news.rss',                                        'src':'Bloomberg',        'lang':'en'},
    {'url':'https://feeds.bbci.co.uk/news/business/rss.xml',                                     'src':'BBC Business',     'lang':'en'},
    {'url':'https://www.ft.com/rss/home',                                                         'src':'Financial Times',  'lang':'en'},
    {'url':'https://www.economist.com/finance-and-economics/rss.xml',                             'src':'The Economist',    'lang':'en'},
    {'url':'https://www.investing.com/rss/news_25.rss',                                           'src':'Inv. Azioni',      'lang':'en'},
    {'url':'https://www.investing.com/rss/news_14.rss',                                           'src':'Inv. Forex',       'lang':'en'},
    {'url':'https://www.investing.com/rss/news_301.rss',                                          'src':'Inv. Commodities', 'lang':'en'},
    {'url':'https://www.investing.com/rss/news_95.rss',                                           'src':'Inv. Bonds',       'lang':'en'},
    # ── WSJ, Barron's, Handelsblatt, Les Echos (via Google News — RSS diretto bloccato) ──
    {'url':'https://news.google.com/rss/search?q=wall+street+journal+markets+stocks&hl=en&gl=US&ceid=US:en', 'src':'WSJ',          'lang':'en'},
    {'url':'https://news.google.com/rss/search?q=barrons+markets+investing&hl=en&gl=US&ceid=US:en',          'src':"Barron's",     'lang':'en'},
    {'url':'https://news.google.com/rss/search?q=handelsblatt+finanzen+maerkte&hl=de&gl=DE&ceid=DE:de',      'src':'Handelsblatt', 'lang':'de'},
    {'url':'https://news.google.com/rss/search?q=les+echos+finance+marches&hl=fr&gl=FR&ceid=FR:fr',          'src':'Les Echos',    'lang':'fr'},
    # ── Banche Centrali ──────────────────────────────────────────────────────
    {'url':'https://www.ecb.europa.eu/rss/press.html',                                            'src':'BCE',              'lang':'en'},
    {'url':'https://www.federalreserve.gov/feeds/press_all.xml',                                  'src':'Federal Reserve',  'lang':'en'},
    # ── Yahoo Finance (simboli chiave) ───────────────────────────────────────
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US',    'src':'YF S&P 500',       'lang':'en'},
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=GC%3DF&region=US&lang=en-US',     'src':'YF Gold',          'lang':'en'},
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=CL%3DF&region=US&lang=en-US',     'src':'YF Oil',           'lang':'en'},
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=EURUSD%3DX&region=US&lang=en-US', 'src':'YF EUR/USD',       'lang':'en'},
    {'url':'https://feeds.finance.yahoo.com/rss/2.0/headline?s=BTC-USD&region=US&lang=en-US',    'src':'YF Bitcoin',       'lang':'en'},
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
                hist = yf.Ticker(sym).history(period='5d', interval='1d')
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
        # Paesi EU: spread medio storico su Bund (stabile nel medio periodo)
        EU_OFFSETS = {
            'NL': {'name':'Olanda',     'flag':'🇳🇱','off':{'3M':0.05,'2Y':0.05,'5Y':0.07,'10Y':0.08,'30Y':0.10}},
            'AT': {'name':'Austria',    'flag':'🇦🇹','off':{'3M':0.25,'2Y':0.22,'5Y':0.28,'10Y':0.37,'30Y':0.48}},
            'BE': {'name':'Belgio',     'flag':'🇧🇪','off':{'3M':0.30,'2Y':0.30,'5Y':0.42,'10Y':0.47,'30Y':0.62}},
            'FR': {'name':'Francia',    'flag':'🇫🇷','off':{'3M':0.40,'2Y':0.35,'5Y':0.38,'10Y':0.65,'30Y':0.85}},
            'ES': {'name':'Spagna',     'flag':'🇪🇸','off':{'3M':0.52,'2Y':0.47,'5Y':0.52,'10Y':0.77,'30Y':1.08}},
            'PT': {'name':'Portogallo', 'flag':'🇵🇹','off':{'3M':0.60,'2Y':0.55,'5Y':0.65,'10Y':0.72,'30Y':0.92}},
            'IT': {'name':'Italia',     'flag':'🇮🇹','off':{'3M':0.80,'2Y':0.75,'5Y':0.85,'10Y':1.12,'30Y':1.36}},
            'GR': {'name':'Grecia',     'flag':'🇬🇷','off':{'3M':0.70,'2Y':0.65,'5Y':0.80,'10Y':0.92,'30Y':1.12}},
        }
        for cc, info in EU_OFFSETS.items():
            yields = {m: round(ecb_yields[m] + info['off'].get(m, 0), 3) for m in ecb_yields}
            out[cc] = {'name':info['name'],'flag':info['flag'],'yields':yields,
                       'source':'ECB+spread','live':True}
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

    with ThreadPoolExecutor(max_workers=8) as ex:
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
        with ThreadPoolExecutor(max_workers=6) as ex:
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

        with ThreadPoolExecutor(max_workers=5) as ex:
            for cc, d in ex.map(_ecb_fetch, missing_ecb):
                if d:
                    raw[f'sov_{cc}'] = d
                    print(f'  [sovereign] ECB OK per {cc}: {d["yield"]}%')

    # Fallback finale: valori statici etichettati (quando tutte le fonti sono bloccate)
    STATIC_FALLBACK = {
        'DE': 2.50, 'IT': 3.58, 'ES': 3.38,
        'FR': 3.22, 'PT': 2.97, 'PL': 5.25,
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
                hist = yf.Ticker(sym).history(period='5d', interval='1d')
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
    with ThreadPoolExecutor(max_workers=12) as ex:
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
        with ThreadPoolExecutor(max_workers=10) as ex:
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
        news = yf.Ticker(symbol).news or []

        # ── Tentativo 2: ricerca per keyword da simbolo (indici/valute) ──
        if not news:
            query = _symbol_to_query(symbol)
            try:
                sr   = yf.Search(query, news_count=8, enable_fuzzy_query=False)
                news = getattr(sr, 'news', []) or []
            except Exception:
                pass

        # ── Tentativo 3: nome completo da info (ETF europei) ─────────────
        if not news:
            try:
                info       = yf.Ticker(symbol).info or {}
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
    """Rendimenti mensili per matrice anno×mese — cache 1h."""
    with _monthly_lock:
        c = _monthly_cache.get(symbol)
        if c and (time.time() - c['ts']) < MONTHLY_TTL:
            return c['data']
    try:
        today = datetime.date.today()
        start    = datetime.date(today.year - 5, 1, 1)
        end_date = datetime.date(today.year + 1, 12, 31)
        hist  = yf.Ticker(symbol).history(
            start=str(start), end=str(end_date), interval='1mo', auto_adjust=False
        )
        if hist.empty:
            return {'error': 'Nessun dato mensile disponibile'}

        closes = [float(v) for v in hist['Close'].tolist()]
        dates  = hist.index.tolist()

        monthly = {}
        for i in range(1, len(closes)):
            dt    = dates[i]
            year  = str(dt.year)
            month = str(dt.month)
            ret   = round((closes[i] / closes[i - 1] - 1) * 100, 2)
            if year not in monthly:
                monthly[year] = {}
            monthly[year][month] = ret

        # YTD composto per ogni anno
        ytd = {}
        for year, months in monthly.items():
            c = 1.0
            for m in sorted(months.keys(), key=int):
                c *= 1 + months[m] / 100
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
        start    = datetime.date(cur_year - 1, 12, 1)
        end_date = today + datetime.timedelta(days=7)
        hist  = yf.Ticker(symbol).history(
            start=str(start), end=str(end_date), interval='1wk', auto_adjust=False
        )
        if hist.empty:
            return {'error': 'Nessun dato settimanale'}

        closes = [float(v) for v in hist['Close'].tolist()]
        dates  = hist.index.tolist()

        weekly = {}
        for i in range(1, len(closes)):
            dt  = dates[i]
            # Usa isocalendar per ENTRAMBI anno e settimana (ISO 8601 coerente)
            # Esempio: 29 dic 2025 → ISO year=2026, week=1 (non year=2025!)
            iso      = dt.isocalendar()
            iso_year = str(iso[0])
            iso_week = str(iso[1])
            # Mostra solo l'anno corrente (l'utente vuole solo da inizio anno)
            if iso_year != str(cur_year):
                continue
            ret = round((closes[i] / closes[i - 1] - 1) * 100, 2)
            if iso_year not in weekly:
                weekly[iso_year] = {}
            weekly[iso_year][iso_week] = ret

        years = sorted(weekly.keys())
        result = {'data': weekly, 'years': years}
    except Exception as e:
        result = {'error': str(e)}

    with _weekly_lock:
        _weekly_cache[symbol] = {'data': result, 'ts': time.time()}
    return result


def fetch_seasonal(symbol):
    """Stagionalità: rendimento medio mensile su 10 anni — cache 6h."""
    with _seasonal_lock:
        c = _seasonal_cache.get(symbol)
        if c and (time.time() - c['ts']) < SEASONAL_TTL:
            return c['data']
    try:
        today = datetime.date.today()
        start = datetime.date(today.year - 10, 1, 1)
        hist  = yf.Ticker(symbol).history(
            start=str(start), end=str(today), interval='1mo', auto_adjust=False
        )
        if hist.empty:
            result = {'error': 'Nessun dato stagionale disponibile'}
        else:
            closes = [float(v) for v in hist['Close'].tolist()]
            dates  = hist.index.tolist()

            MONTHS_IT = ['Gen','Feb','Mar','Apr','Mag','Giu',
                         'Lug','Ago','Set','Ott','Nov','Dic']
            by_month = {m: [] for m in range(1, 13)}
            yearly   = {}
            for i in range(1, len(closes)):
                dt  = dates[i]
                ret = round((closes[i] / closes[i - 1] - 1) * 100, 2)
                by_month[dt.month].append(ret)
                year = str(dt.year)
                if year not in yearly:
                    yearly[year] = {}
                yearly[year][str(dt.month)] = ret

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
        body = json.dumps(data, default=str).encode()
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
        elif self.path.startswith('/macro-data'):
            self._json(fetch_macro_data())
        elif self.path.startswith('/global-yields'):
            self._json(fetch_global_yields())
        elif self.path.startswith('/sovereign-yields'):
            self._json(fetch_sovereign_yields())
        elif self.path.startswith('/debug-sources'):
            self._json(self._debug_sources())
        elif self.path.startswith('/ma'):
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
        else:
            super().do_GET()

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


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f'\n  Monitor Mercati — http://localhost:{PORT}/monitor-mercati.html\n')
    print('  (Ctrl+C per fermare)\n')
    with http.server.ThreadingHTTPServer(('', PORT), Handler) as srv:
        srv.serve_forever()
