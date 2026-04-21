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
_ma_cache      = {}; _ma_lock      = threading.Lock(); MA_TTL      = 300
_seasonal_cache= {}; _seasonal_lock= threading.Lock(); SEASONAL_TTL= 21600  # 6 ore
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
            auto_adjust=True,
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
                    interval='1d', auto_adjust=True
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
    try:
        import math
        t   = yf.Ticker(symbol)
        info = t.info
        dy  = info.get('dividendYield')
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
        start    = datetime.date(today.year - 5, 1, 1)
        end_date = datetime.date(today.year + 1, 12, 31)
        hist  = yf.Ticker(symbol).history(
            start=str(start), end=str(end_date), interval='1mo', auto_adjust=True
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
    """Rendimenti settimanali ultimi 4 anni — cache 1h."""
    with _weekly_lock:
        c = _weekly_cache.get(symbol)
        if c and (time.time() - c['ts']) < WEEKLY_TTL:
            return c['data']
    try:
        today = datetime.date.today()
        # Usa start 4 anni fa e end 1 anno avanti per catturare
        # sempre le settimane più recenti a prescindere dal clock del server
        start    = datetime.date(today.year - 4, 1, 1)
        end_date = datetime.date(today.year + 1, 12, 31)
        hist  = yf.Ticker(symbol).history(
            start=str(start), end=str(end_date), interval='1wk', auto_adjust=True
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
            start=str(start), end=str(today), interval='1mo', auto_adjust=True
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
        if not self.path.startswith('/yf') and not self.path.startswith('/ma') \
                and not self.path.startswith('/fundamentals') and not self.path.startswith('/news') \
                and not self.path.startswith('/weekly') and not self.path.startswith('/monthly') \
                and not self.path.startswith('/calendar'):
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
        elif self.path.startswith('/ma'):
            parsed   = urllib.parse.urlparse(self.path)
            params   = urllib.parse.parse_qs(parsed.query)
            syms_raw = params.get('syms', [''])[0]
            symbols  = [s.strip() for s in syms_raw.split(',') if s.strip()]
            self._json(fetch_ma_batch(symbols))
        elif self.path.startswith('/seasonal/'):
            sym = urllib.parse.unquote(self.path[len('/seasonal/'):].split('?')[0])
            self._json(fetch_seasonal(sym))
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
