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


def fetch_symbol(symbol, period='3mo', interval='1d'):
    cache_key = f'{symbol}|{period}|{interval}'
    ttl = CACHE_TTL if period in ('3mo', '1mo') else CACHE_TTL_LONG

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
        timestamps = [int(dt.timestamp()) for dt in hist.index]
        cur_price  = float(info.last_price) if hasattr(info, 'last_price') and info.last_price else closes[-1]
        prev_close = float(info.previous_close) if hasattr(info, 'previous_close') and info.previous_close else (closes[-2] if len(closes) > 1 else None)
        cur_time   = int(hist.index[-1].timestamp())
        tz         = str(hist.index.tz) if hist.index.tz else 'UTC'

        # Rendimento (yield) — solo per fetch principale 3mo
        yield_pct = None
        if period == '3mo':
            try:
                full_info = ticker.info
                raw = (full_info.get('yield') or full_info.get('dividendYield')
                       or full_info.get('trailingAnnualDividendYield'))
                if raw and raw > 0:
                    yield_pct = round(raw * 100, 3)
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
            'yield_pct': yield_pct,
        }

        with _cache_lock:
            _cache[cache_key] = {'data': data, 'ts': time.time()}
        return data

    except Exception as e:
        return {'error': str(e)}


class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        path = self.path[4:] if self.path.startswith('/yf/') else self.path
        print(f'  {args[1]}  {path}')

    def do_GET(self):
        if self.path.startswith('/yf/'):
            self.handle_yf()
        else:
            super().do_GET()

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

        data = fetch_symbol(symbol, period=period, interval=interval)

        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f'\n  Monitor Mercati — http://localhost:{PORT}/monitor-mercati.html\n')
    print('  (Ctrl+C per fermare)\n')
    with http.server.ThreadingHTTPServer(('', PORT), Handler) as srv:
        srv.serve_forever()
