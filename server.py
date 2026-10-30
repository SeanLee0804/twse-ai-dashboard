"""TWSE Pro local data gateway. Python 3.9+ stdlib only. Binds to localhost.
Data: TWSE & TPEx public market snapshots; FinMind optional OHLC, TWSE historic OHLC fallback.
Never manufactures prices. This is NOT a trading or real-time quotation service.
"""
from __future__ import annotations
import json, os, re, sys, time, threading, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

BASE = Path(__file__).resolve().parent
HOST = '127.0.0.1'
PORT = int(os.environ.get('TWSE_PORT', '8000'))
CACHE = {}
LOCK = threading.Lock()
MAX_BYTES = 30_000_000
USER_AGENT = 'TWSE-Research-Dashboard/1.1 (personal research; https://openapi.twse.com.tw/)'
REGISTRIES = {
    '上市': 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L',
    '上櫃': 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O',
}
MARKETS = {
    '上市': 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
    '上櫃': 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes',
}

class DataError(Exception):
    def __init__(self, message, status=502):
        super().__init__(message)
        self.status = status


def fetch_json(url, headers=None, timeout=14):
    req = Request(url, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json', **(headers or {})})
    try:
        with urlopen(req, timeout=timeout) as res:
            data = res.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise DataError('資料來源回應過大')
            return json.loads(data.decode('utf-8-sig'))
    except HTTPError as e:
        try:
            details = e.read(1024).decode('utf-8', 'replace')
        except Exception:
            details = ''
        raise DataError(f'資料來源 HTTP {e.code}: {details[:110]}', 502)
    except (URLError, TimeoutError) as e:
        raise DataError(f'資料來源連線逾時或被拒：{e}', 503)
    except (ValueError, UnicodeDecodeError) as e:
        raise DataError(f'資料來源 JSON 解析錯誤：{e}', 502)


def cached(key, ttl, loader):
    now = time.time()
    with LOCK:
        old = CACHE.get(key)
        if old and now - old[0] < ttl:
            return old[1]
    value = loader()
    with LOCK:
        CACHE[key] = (time.time(), value)
    return value


def num(value):
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value or '').strip().replace(',', '').replace('＋', '+').replace('－', '-')
    if not s or s in ('--', '---', 'N/A', '除權', '除息', '－'):
        return None
    try:
        value = float(s)
        return value if value == value and abs(value) < 1e17 else None
    except ValueError:
        return None


def iso_date(value):
    s = str(value or '').strip()
    m = re.fullmatch(r'(\d{3})(\d{2})(\d{2})', s)
    if m:
        return f'{int(m[1])+1911:04d}-{m[2]}-{m[3]}'
    m = re.fullmatch(r'(\d{2,3})/(\d{1,2})/(\d{1,2})', s)
    if m:
        return f'{int(m[1])+1911:04d}-{int(m[2]):02d}-{int(m[3]):02d}'
    m = re.fullmatch(r'(\d{4})-(\d{2})-(\d{2})', s)
    return s if m else None


def symbol_ok(code):
    # 4-digit common stocks; 00-prefixed ETFs (5/6 digit). Exclude warrants and bonds.
    return bool(re.fullmatch(r'\d{4}', code) or re.fullmatch(r'00\d{3,4}', code))


def normalize_quote(row, market):
    code = str(row.get('Code') or row.get('SecuritiesCompanyCode') or row.get('code') or '').strip()
    if not symbol_ok(code):
        return None
    name = str(row.get('Name') or row.get('CompanyName') or row.get('SecuritiesCompanyName') or '').strip()
    if not name:
        return None
    close = num(row.get('ClosingPrice') if market == '上市' else row.get('Close'))
    change = num(row.get('Change'))
    if market == '上市' and change is not None:
        # TWSE carries separate +/- sign column in some endpoints. Never infer if absent.
        sign = str(row.get('ChangeSign') or row.get('Sign') or '')
        if '－' in sign or '-' in sign: change = -abs(change)
        elif '＋' in sign or '+' in sign: change = abs(change)
        else: change = None
    day = iso_date(row.get('Date'))
    return {
        'code': code, 'name': name, 'market': market,
        'type': 'ETF' if code.startswith('00') and len(code) >= 5 else '股票',
        'price': close,
        'change': change,
        'open': num(row.get('OpeningPrice') if market == '上市' else row.get('Open')),
        'high': num(row.get('HighestPrice') if market == '上市' else row.get('High')),
        'low': num(row.get('LowestPrice') if market == '上市' else row.get('Low')),
        'volume': num(row.get('TradeVolume') if market == '上市' else row.get('TradingShares')),
        'date': day,
        'source': 'TWSE 官方盤後' if market == '上市' else 'TPEx 官方盤後',
    }


def universe():
    def loader():
        stocks = {}
        errors = []
        for market, url in MARKETS.items():
            try:
                data = fetch_json(url)
                if not isinstance(data, list):
                    raise DataError('股票行情清單不是陣列')
                for row in data:
                    if not isinstance(row, dict): continue
                    s = normalize_quote(row, market)
                    if s: stocks[s['code']] = s
            except Exception as e:
                errors.append(f'{market}：{str(e)[:140]}')
        # Merge master company lists. Companies without trades still appear in search, with price=null.
        for market, url in REGISTRIES.items():
            try:
                data = fetch_json(url)
                if not isinstance(data, list): raise DataError('公司基本資料格式錯誤')
                for row in data:
                    code = str(row.get('公司代號') or row.get('SecuritiesCompanyCode') or '').strip()
                    name = str(row.get('公司簡稱') or row.get('CompanyAbbreviation') or row.get('公司名稱') or row.get('CompanyName') or '').strip()
                    if not symbol_ok(code) or not name: continue
                    if code not in stocks:
                        stocks[code] = {'code':code,'name':name,'market':market,'type':'股票',
                            'price':None,'change':None,'open':None,'high':None,'low':None,
                            'volume':None,'date':None,'source':'公司名冊（尚無盤後行情）'}
            except Exception as e:
                errors.append(f'{market}公司名冊：{str(e)[:130]}')
        if not stocks:
            raise DataError('上市、上櫃資料皆無法取得：' + ' / '.join(errors), 503)
        return {'stocks': sorted(stocks.values(), key=lambda x: x['code']), 'total': len(stocks),
                'errors': errors, 'updatedAt': datetime.now().astimezone().isoformat(timespec='seconds')}
    return cached('universe', 600, loader)


def finmind_candles(code, days, token=''):
    start = (date.today() - timedelta(days=days+12)).isoformat()
    params = {'dataset':'TaiwanStockPrice', 'data_id':code, 'start_date':start}
    if token: params['token'] = token
    uri = 'https://api.finmindtrade.com/api/v4/data?' + urlencode(params)
    data = fetch_json(uri, timeout=17)
    if not isinstance(data, dict) or not isinstance(data.get('data'), list):
        raise DataError(str(data.get('msg','FinMind 回應無資料')) if isinstance(data, dict) else 'FinMind 格式錯誤')
    if data.get('status') not in (None, 200) and data.get('msg') != 'success':
        raise DataError(str(data.get('msg') or data.get('status')))
    rows = []
    for x in data['data']:
        d = iso_date(x.get('date'))
        o, h, l, c = (num(x.get(k)) for k in ('open','max','min','close'))
        if d and all(v is not None and v > 0 for v in (o,h,l,c)) and l <= min(o,c) <= max(o,c) <= h:
            rows.append({'time': d, 'open': o, 'high': h, 'low': l, 'close': c,
                         'volume': num(x.get('Trading_Volume'))})
    return rows


def twse_month(code, year, month):
    # Official TWSE month-by-month history, format in ROC year.
    uri = 'https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?' + urlencode({
        'response':'json','date':f'{year:04d}{month:02d}01','stockNo':code})
    data = fetch_json(uri)
    rows = []
    for item in (data.get('data') or []):
        if not isinstance(item, list) or len(item) < 7: continue
        d = iso_date(item[0]); o=num(item[3]); h=num(item[4]); l=num(item[5]); c=num(item[6]); v=num(item[1])
        if d and all(x is not None and x > 0 for x in (o,h,l,c)) and l<=min(o,c)<=max(o,c)<=h:
            rows.append({'time':d, 'open':o, 'high':h, 'low':l, 'close':c, 'volume':v})
    return rows


def twse_history(code, months):
    now = date.today()
    outputs = []
    errors = []
    def fetch_month(back):
        value = now.year*12 + now.month - 1 - back
        y, m = divmod(value, 12)
        return cached(('twse-month',code,y,m+1), 3600 if back == 0 else 86400*7,
                      lambda: twse_month(code,y,m+1))
    # Parallelizes monthly endpoints; requests limited to 4 concurrent.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_month,i):i for i in range(months)}
        for future in as_completed(futures):
            try: outputs.extend(future.result())
            except Exception as e: errors.append(str(e)[:130])
    return outputs, errors


def candles(code, window, token=''):
    if not symbol_ok(code): raise DataError('股票代號格式不正確', 400)
    spans = {'5D': 12, '1M': 40, '3M': 108, '6M': 205, '1Y': 390}
    if window not in spans: raise DataError('不支援的時間範圍', 400)
    days = spans[window]
    key = ('candles',code,window, bool(token))
    def loader():
        issues = []
        result = []
        source = None
        market_item = next((x for x in universe()['stocks'] if x['code'] == code), None)
        # Listed history is officially available from TWSE without a FinMind account.
        if market_item and market_item['market'] == '上市':
            result, problems = twse_history(code, min(13, max(1, (days+31)//28)))
            issues.extend(problems[:2])
            if result: source = 'TWSE 官方歷史日 K'
        if not source:
            try:
                result = finmind_candles(code, days, token)
                if len(result) >= 2:
                    source = 'FinMind 日成交歷史'
            except Exception as e:
                issues.append('FinMind：' + str(e)[:130])
        if not source:
            return {'code':code, 'candles':[], 'source':None, 'errors':issues,
                    'message':'無法取得歷史 K 線；上櫃歷史資料可能需要 FinMind 有效存取權限。'}
        unique = {r['time']:r for r in result}
        ordered = sorted(unique.values(),key=lambda x:x['time'])
        ordered = ordered[-({'5D':5,'1M':25,'3M':70,'6M':135,'1Y':265}[window]):]
        return {'code':code,'candles':ordered,'source':source,'errors':issues,
                'asOf':ordered[-1]['time'] if ordered else None}
    return cached(key, 1800, loader)

class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(BASE), **kw)

    def log_message(self, format, *args):
        # Do not log secrets. URLs contain only code/window; token is POST/body or header.
        print('%s - %s' % (self.address_string(), (format % args).split('?')[0]), flush=True)

    def json_reply(self, code, payload):
        data = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Content-Length',str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlsplit(self.path)
        if parsed.path == '/api/health':
            return self.json_reply(200, {'ok':True, 'backend':'TWSE Pro', 'time':datetime.now().isoformat(timespec='seconds')})
        if parsed.path == '/api/stocks':
            try: return self.json_reply(200, universe())
            except DataError as e: return self.json_reply(e.status, {'error':str(e)})
            except Exception as e: return self.json_reply(502, {'error':str(e)})
        if parsed.path == '/api/candles':
            args = parse_qs(parsed.query)
            code = (args.get('code') or [''])[0].strip()
            window = (args.get('range') or ['3M'])[0]
            try: return self.json_reply(200, candles(code, window))
            except DataError as e: return self.json_reply(e.status, {'error':str(e)})
            except Exception as e: return self.json_reply(502, {'error':str(e)})
        if parsed.path == '/':
            self.path = '/index.html'
        return super().do_GET()

    def do_POST(self):
        parsed = urlsplit(self.path)
        if parsed.path != '/api/candles':
            return self.json_reply(404, {'error':'未知端點'})
        try:
            size = int(self.headers.get('Content-Length','0'))
            if size <= 0 or size > 8192: return self.json_reply(400, {'error':'請求格式錯誤'})
            payload = json.loads(self.rfile.read(size))
            code = str(payload.get('code','')).strip()
            window = str(payload.get('range','3M'))
            token = str(payload.get('finmindToken','')).strip()
            if len(token) > 2048: return self.json_reply(400, {'error':'Token 過長'})
            # Token is never logged or persisted; cache only by presence, so don't cache authenticated results.
            if token:
                obj = candles_no_cache(code, window, token)
            else:
                obj = candles(code, window)
            return self.json_reply(200, obj)
        except DataError as e: return self.json_reply(e.status, {'error':str(e)})
        except Exception as e: return self.json_reply(502, {'error':str(e)})

def candles_no_cache(code, window, token):
    if not symbol_ok(code) or window not in ('5D','1M','3M','6M','1Y'):
        raise DataError('代號或時間範圍錯誤',400)
    spans = {'5D':12, '1M':40, '3M':108, '6M':205, '1Y':390}
    try:
        rows = finmind_candles(code,spans[window],token)
        if rows:
            rows = sorted({r['time']:r for r in rows}.values(),key=lambda r:r['time'])
            return {'code':code,'candles':rows[-({'5D':5,'1M':25,'3M':70,'6M':135,'1Y':265}[window]):], 'source':'FinMind 日成交歷史', 'asOf':rows[-1]['time']}
    except Exception:
        pass
    return candles(code,window)

if __name__ == '__main__':
    print(f'\nTWSE Pro 全台股資料伺服器：http://localhost:{PORT}/')
    print('資料來源：TWSE / TPEx 官方公開資料；FinMind 歷史日 K 可選擇使用既有 Token')
    print('停止：Ctrl + C\n')
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
