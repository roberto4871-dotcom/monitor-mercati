"""
Monitor Mercati - Server locale con yfinance
Avvia con: python3 server.py
Apri nel browser: http://localhost:8080/monitor-mercati.html
"""
import http.server
import urllib.parse
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
# ISIN cache — permanente (ISIN non cambia mai)
_isin_cache    = {}; _isin_lock    = threading.Lock()


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
            hist  = ticker.history(start=start, end=today, interval=interval, auto_adjust=True)
        elif period == 'max':
            hist  = ticker.history(start='1970-01-01', end=today, interval=interval, auto_adjust=True)
        else:
            hist  = ticker.history(period=period, interval=interval, auto_adjust=True)
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
                              interval=interval, auto_adjust=True,
                              group_by='ticker', progress=False, threads=True)
        elif period == 'max':
            raw = yf.download(to_fetch, start='1970-01-01', end=str(today),
                              interval=interval, auto_adjust=True,
                              group_by='ticker', progress=False, threads=True)
        else:
            # Partiamo dal 26 dic anno precedente; end=domani per includere barra di oggi
            start = datetime.date(today.year - 1, 12, 26)
            end   = today + datetime.timedelta(days=1)
            raw = yf.download(to_fetch, start=str(start), end=str(end),
                              interval=interval, auto_adjust=True,
                              group_by='ticker', progress=False, threads=True)
    except Exception as e:
        print(f'  [batch] yf.download fallito ({e}), uso fetch individuale')
        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = {ex.submit(fetch_symbol, sym, period, interval): sym for sym in to_fetch}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    results[sym] = future.result()
                except Exception as e2:
                    results[sym] = {'error': str(e2)}
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


def fetch_fundamentals(symbol):
    """Dati fondamentali via yfinance — cache 1h."""
    with _fund_lock:
        c = _fund_cache.get(symbol)
        if c and (time.time() - c['ts']) < FUND_TTL:
            return c['data']
    try:
        info = yf.Ticker(symbol).info
        dy = info.get('dividendYield')
        # yfinance a volte restituisce decimale (0.018) a volte già percentuale (1.8)
        data = {
            'trailingPE':        info.get('trailingPE'),
            'forwardPE':         info.get('forwardPE'),
            'priceToBook':       info.get('priceToBook'),
            'dividendYield':     round(dy * 100, 2) if (dy and dy < 1) else (round(dy, 2) if dy else None),
            'marketCap':         info.get('marketCap'),
            'beta':              info.get('beta'),
            'fiftyTwoWeekHigh':  info.get('fiftyTwoWeekHigh'),
            'fiftyTwoWeekLow':   info.get('fiftyTwoWeekLow'),
            'volume':            info.get('volume'),
            'avgVolume':         info.get('averageVolume'),
            'totalExpenseRatio': info.get('annualReportExpenseRatio'),
            'currency':          info.get('currency'),
            'description':       (info.get('longBusinessSummary') or '')[:600],
            'sector':            info.get('sector'),
            'industry':          info.get('industry'),
            'country':           info.get('country'),
            'website':           info.get('website'),
            'profitMargins':     info.get('profitMargins'),
            'revenueGrowth':     info.get('revenueGrowth'),
            'earningsGrowth':    info.get('earningsGrowth'),
        }
    except Exception as e:
        data = {'error': str(e)}
    with _fund_lock:
        _fund_cache[symbol] = {'data': data, 'ts': time.time()}
    return data


def _debug_isin(symbol):
    """Endpoint di debug: restituisce le risposte raw da Yahoo Finance per ISIN lookup."""
    import requests as req
    out = {'symbol': symbol, 'crumb': None, 'v7': None, 'search': None, 'v10': None, 'error': None}
    try:
        s = req.Session()
        s.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Referer': 'https://finance.yahoo.com/',
        })
        # crumb
        s.get('https://fc.yahoo.com', timeout=5)
        rc = s.get('https://query2.finance.yahoo.com/v1/test/getcrumb', timeout=6)
        crumb = rc.text.strip() if rc.status_code == 200 else None
        out['crumb'] = crumb
        sym_enc = urllib.parse.quote(symbol)
        # v7
        r2 = s.get(f'https://query2.finance.yahoo.com/v7/finance/quote?symbols={sym_enc}', timeout=8)
        if r2.status_code == 200:
            res = r2.json().get('quoteResponse', {}).get('result', [{}])
            out['v7'] = {k: v for k, v in res[0].items() if 'isin' in k.lower() or k in ('symbol','shortName','isin')} if res else 'empty'
            out['v7_all_keys'] = list(res[0].keys()) if res else []
        else:
            out['v7'] = f'HTTP {r2.status_code}'
        # search
        r3 = s.get(f'https://query2.finance.yahoo.com/v1/finance/search?q={sym_enc}&quotesCount=2&newsCount=0&quotesQueryId=tss_match_phrase_query', timeout=8)
        if r3.status_code == 200:
            quotes = r3.json().get('quotes', [])
            for q in quotes:
                if q.get('symbol') == symbol:
                    out['search'] = q
                    break
            if not out['search']:
                out['search'] = f'symbol not found in {[q.get("symbol") for q in quotes]}'
        else:
            out['search'] = f'HTTP {r3.status_code}'
        # v10 (con crumb)
        if crumb:
            r1 = s.get(f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym_enc}?modules=defaultKeyStatistics&crumb={urllib.parse.quote(crumb)}', timeout=10)
            if r1.status_code == 200:
                ks = r1.json().get('quoteSummary', {}).get('result', [{}])[0].get('defaultKeyStatistics', {})
                out['v10_isin'] = ks.get('isin')
            else:
                out['v10'] = f'HTTP {r1.status_code}'
    except Exception as e:
        out['error'] = str(e)
    return out


def fetch_isin(symbol):
    """Restituisce l'ISIN — cache permanente.
    Tenta 3 metodi Yahoo Finance + crumb token se necessario.
    """
    with _isin_lock:
        if symbol in _isin_cache:
            return _isin_cache[symbol]

    # Indici, valute e futures non hanno ISIN
    if symbol.startswith('^') or '=X' in symbol or '=F' in symbol:
        with _isin_lock:
            _isin_cache[symbol] = None
        return None

    isin = None
    try:
        import requests as req
        s = req.Session()
        s.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://finance.yahoo.com/',
        })

        # 0) Ottieni cookie + crumb (necessario per endpoint v10)
        crumb = None
        try:
            s.get('https://fc.yahoo.com', timeout=5)
            rc = s.get('https://query2.finance.yahoo.com/v1/test/getcrumb', timeout=6)
            if rc.status_code == 200 and rc.text.strip():
                crumb = rc.text.strip()
        except Exception:
            pass

        sym_enc = urllib.parse.quote(symbol)

        # 1) v10/quoteSummary defaultKeyStatistics — ha isin per ETF e azioni EU
        if crumb:
            try:
                url1 = (f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym_enc}'
                        f'?modules=defaultKeyStatistics&crumb={urllib.parse.quote(crumb)}')
                r1 = s.get(url1, timeout=10)
                print(f'[ISIN] {symbol} quoteSummary status={r1.status_code}', flush=True)
                if r1.status_code == 200:
                    ks = (r1.json().get('quoteSummary', {})
                          .get('result', [{}])[0]
                          .get('defaultKeyStatistics', {}))
                    raw = ks.get('isin')
                    if isinstance(raw, dict):
                        raw = raw.get('raw') or raw.get('fmt')
                    if raw and isinstance(raw, str) and len(raw) >= 12 and raw not in ('-', 'N/A'):
                        isin = raw.strip()
                        print(f'[ISIN] {symbol} trovato via quoteSummary: {isin}', flush=True)
            except Exception as e:
                print(f'[ISIN] {symbol} quoteSummary err: {e}', flush=True)

        # 2) v7/finance/quote — restituisce isin per alcuni mercati
        if not isin:
            try:
                url2 = f'https://query2.finance.yahoo.com/v7/finance/quote?symbols={sym_enc}'
                if crumb:
                    url2 += f'&crumb={urllib.parse.quote(crumb)}'
                r2 = s.get(url2, timeout=8)
                print(f'[ISIN] {symbol} v7quote status={r2.status_code}', flush=True)
                if r2.status_code == 200:
                    res = r2.json().get('quoteResponse', {}).get('result', [])
                    if res:
                        raw2 = res[0].get('isin')
                        if raw2 and isinstance(raw2, str) and len(raw2) >= 12:
                            isin = raw2.strip()
                            print(f'[ISIN] {symbol} trovato via v7: {isin}', flush=True)
                        else:
                            print(f'[ISIN] {symbol} v7 keys={list(res[0].keys())}', flush=True)
            except Exception as e:
                print(f'[ISIN] {symbol} v7 err: {e}', flush=True)

        # 3) v1/finance/search
        if not isin:
            try:
                url3 = (f'https://query2.finance.yahoo.com/v1/finance/search'
                        f'?q={sym_enc}&quotesCount=3&newsCount=0'
                        f'&quotesQueryId=tss_match_phrase_query&enableFuzzyQuery=false')
                r3 = s.get(url3, timeout=8)
                print(f'[ISIN] {symbol} search status={r3.status_code}', flush=True)
                if r3.status_code == 200:
                    for q in r3.json().get('quotes', []):
                        if q.get('symbol') == symbol:
                            raw3 = q.get('isin') or q.get('ISIN')
                            if raw3 and isinstance(raw3, str) and len(raw3) >= 12:
                                isin = raw3.strip()
                                print(f'[ISIN] {symbol} trovato via search: {isin}', flush=True)
                            else:
                                print(f'[ISIN] {symbol} search keys={list(q.keys())}', flush=True)
                            break
            except Exception as e:
                print(f'[ISIN] {symbol} search err: {e}', flush=True)

    except Exception as e:
        print(f'[ISIN] {symbol} outer err: {e}', flush=True)

    print(f'[ISIN] {symbol} risultato finale: {isin}', flush=True)
    with _isin_lock:
        _isin_cache[symbol] = isin
    return isin


def _translate_it(text):
    """Traduce in italiano via Google Translate (API pubblica, nessuna chiave)."""
    if not text or len(text.strip()) < 4:
        return text
    try:
        import requests as req
        r = req.get(
            'https://translate.googleapis.com/translate_a/single',
            params={'client': 'gtx', 'sl': 'auto', 'tl': 'it', 'dt': 't', 'q': text[:500]},
            timeout=5, headers={'User-Agent': 'Mozilla/5.0'}
        )
        data = r.json()
        return ''.join(seg[0] for seg in data[0] if seg[0])
    except Exception:
        return text


def fetch_news(symbol):
    """Ultime news via yfinance — cache 15 min."""
    with _news_lock:
        c = _news_cache.get(symbol)
        if c and (time.time() - c['ts']) < NEWS_TTL:
            return c['data']
    data = []
    try:
        news = yf.Ticker(symbol).news or []
        for n in news[:8]:
            # Supporta sia il formato vecchio che il nuovo (content nested)
            cnt = n.get('content', {}) or {}
            title     = cnt.get('title')     or n.get('title', '')
            summary   = (cnt.get('summary')  or n.get('summary', ''))[:250]
            url_obj   = cnt.get('canonicalUrl') or {}
            url       = url_obj.get('url') if isinstance(url_obj, dict) else n.get('link', '')
            publisher = (cnt.get('provider') or {}).get('displayName') if cnt.get('provider') else n.get('publisher', '')
            pub_time  = cnt.get('pubDate') or n.get('providerPublishTime', '')
            if not title:
                continue
            data.append({'title': title, 'summary': summary, 'url': url or '',
                         'publisher': publisher or '', 'time': pub_time})
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
        start = datetime.date(today.year - 5, 1, 1)
        hist  = yf.Ticker(symbol).history(
            start=str(start), end=str(today), interval='1mo', auto_adjust=True
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
    """Rendimenti settimanali ultimi 3 anni — cache 1h."""
    with _weekly_lock:
        c = _weekly_cache.get(symbol)
        if c and (time.time() - c['ts']) < WEEKLY_TTL:
            return c['data']
    try:
        today = datetime.date.today()
        start = datetime.date(today.year - 3, 1, 1)
        hist  = yf.Ticker(symbol).history(
            start=str(start), end=str(today), interval='1wk', auto_adjust=True
        )
        if hist.empty:
            return {'error': 'Nessun dato settimanale'}

        closes = [float(v) for v in hist['Close'].tolist()]
        dates  = hist.index.tolist()

        weekly = {}
        for i in range(1, len(closes)):
            dt   = dates[i]
            year = str(dt.year)
            week = str(dt.isocalendar()[1])  # numero settimana ISO
            ret  = round((closes[i] / closes[i - 1] - 1) * 100, 2)
            if year not in weekly:
                weekly[year] = {}
            weekly[year][week] = ret

        years = sorted(weekly.keys())
        result = {'data': weekly, 'years': years}
    except Exception as e:
        result = {'error': str(e)}

    with _weekly_lock:
        _weekly_cache[symbol] = {'data': result, 'ts': time.time()}
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
        elif self.path.startswith('/isin/'):
            sym = urllib.parse.unquote(self.path[len('/isin/'):].split('?')[0])
            self._json({'isin': fetch_isin(sym)})
        elif self.path.startswith('/debug/isin/'):
            # Endpoint di debug: mostra risposte raw da Yahoo Finance senza cache
            sym = urllib.parse.unquote(self.path[len('/debug/isin/'):].split('?')[0])
            self._json(_debug_isin(sym))
        elif self.path.startswith('/news/'):
            sym = urllib.parse.unquote(self.path[len('/news/'):].split('?')[0])
            self._json(fetch_news(sym))
        elif self.path.startswith('/calendar'):
            self._json(fetch_calendar())
        else:
            super().do_GET()

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
