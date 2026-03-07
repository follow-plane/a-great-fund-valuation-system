import akshare as ak
import pandas as pd
import datetime
import streamlit as st
import concurrent.futures
import requests
import re
import json
import threading

# Global lock for akshare calls to prevent py_mini_racer (V8) crashes in multi-threaded environments
ak_lock = threading.Lock()

def safe_ak_call(func, *args, **kwargs):
    """
    Wrapper to call akshare functions with a global lock.
    """
    with ak_lock:
        return func(*args, **kwargs)

# --- Cached Data Fetching Functions ---

@st.cache_data(ttl=86400) # Cache for 24 hours
def _fetch_all_fund_names():
    """
    Fetch list of all funds. Heavy payload (~10MB+).
    """
    try:
        return safe_ak_call(ak.fund_name_em)
    except Exception as e:
        print(f"Error fetching fund names: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600) # Cache for 1 hour
def _fetch_fund_history_raw(fund_code):
    """
    Fetch full history for a fund.
    """
    try:
        df = safe_ak_call(ak.fund_open_fund_info_em, symbol=fund_code, indicator="单位净值走势")
        if not df.empty:
            df['净值日期'] = pd.to_datetime(df['净值日期'])
            df['单位净值'] = df['单位净值'].astype(float)
            return df
    except Exception as e:
        print(f"Error fetching history raw for {fund_code}: {e}")
    return pd.DataFrame()

def _fetch_single_fund_realtime(fund_code):
    """
    Fetch real-time estimation/price for a SINGLE fund.
    For on-exchange funds (ETF/LOF), prioritize Sina Finance API (EastMoney is blocking).
    For off-exchange funds, use Tiantian Fund Estimation API.
    """
    is_exchange = fund_code.startswith(('15', '16', '18', '50', '51', '56', '58'))
    
    # --- Try Sina Finance API first for exchange funds ---
    if is_exchange:
        try:
            # Determine prefix
            if fund_code.startswith(('5', '6')):
                symbol = f"sh{fund_code}"
            else:
                symbol = f"sz{fund_code}"
                
            sina_url = f"http://hq.sinajs.cn/list={symbol}"
            headers = {
                "Referer": "https://finance.sina.com.cn/"
            }
            s_resp = requests.get(sina_url, headers=headers, timeout=2.0)
            
            if s_resp.status_code == 200:
                content = s_resp.text
                # Format: var hq_str_sz161226="name,open,pre_close,price,..."
                if "=" in content:
                    data_str = content.split('=')[1].strip().strip('";')
                    if len(data_str) > 10:
                        parts = data_str.split(',')
                        if len(parts) > 30:
                            # Parse fields
                            # 0: name, 1: open, 2: pre_close, 3: price
                            pre_close = float(parts[2])
                            price = float(parts[3])
                            
                            # If price is 0 (e.g. before open), use pre_close
                            current_price = price if price > 0 else pre_close
                            
                            # Calculate percentage
                            pct = 0.0
                            if pre_close > 0:
                                pct = (current_price - pre_close) / pre_close * 100
                            
                            # Date/Time (30: date, 31: time)
                            data_date = parts[30]
                            data_time = parts[31]
                            
                            return {
                                'code': fund_code,
                                'gz': round(current_price, 4),
                                'zzl': round(pct, 2),
                                'est_date': data_date,
                                'pre_close': round(pre_close, 4),
                                'confirmed_nav': round(pre_close, 4),
                                'time': data_time
                            }
        except Exception:
            pass

    # --- Fallback or Default to Tiantian Fund API ---
    url = f"http://fundgz.1234567.com.cn/js/{fund_code}.js"
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=1.5)
        
        if resp.status_code == 200:
            content = resp.text
            match = re.search(r'jsonpgz\((.*)\);', content)
            if match and match.group(1):
                data = json.loads(match.group(1))
                return {
                    'code': data['fundcode'],
                    'gz': data['gsz'],
                    'zzl': data['gszzl'],
                    'est_date': data['gztime'].split(' ')[0],
                    'pre_close': data['dwjz'],
                    'confirmed_nav': data['dwjz'],
                    'time': data['gztime'].split(' ')[1]
                }
    except Exception:
        pass
    
    # --- Fallback: Use AkShare historical NAV if real-time data is not available ---
    try:
        df = _fetch_fund_history_raw(fund_code)
        if not df.empty:
            latest = df.iloc[-1]
            latest_nav = float(latest['单位净值'])
            latest_date = str(latest['净值日期'].date())
            
            # Get daily growth rate if available
            pct = 0.0
            if '日增长率' in latest and pd.notna(latest['日增长率']):
                pct = float(latest['日增长率'])
            elif len(df) > 1:
                prev_nav = float(df.iloc[-2]['单位净值'])
                pct = ((latest_nav - prev_nav) / prev_nav) * 100
            
            return {
                'code': fund_code,
                'gz': latest_nav,
                'zzl': round(pct, 2),
                'est_date': latest_date,
                'pre_close': latest_nav,
                'confirmed_nav': latest_nav,
                'time': '15:00:00'
            }
    except Exception:
        pass
        
    return None

def get_batch_realtime_estimates(fund_codes):
    """
    Fetch real-time estimates for multiple funds in parallel.
    """
    results = {}
    if not fund_codes:
        return results
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_code = {executor.submit(_fetch_single_fund_realtime, code): code for code in fund_codes}
        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            try:
                data = future.result()
                if data:
                    results[code] = data
            except Exception:
                pass
    return results

@st.cache_data(ttl=60) # Cache for 60 seconds
def _fetch_realtime_estimations():
    """
    Fetch real-time estimation for ALL funds.
    Returns a dict {code: {gz, zzl, est_date, pre_close, confirmed_nav}}
    WARNING: Slow (~3s). Use get_batch_realtime_estimates for specific funds instead.
    """
    try:
        df = safe_ak_call(ak.fund_value_estimation_em, symbol="全部")
        if not df.empty:
            rename_map = {}
            estimate_date = None
            pre_close_col = None
            confirmed_nav_col = None
            
            # Dynamic column mapping
            for col in df.columns:
                if "估算值" in col:
                    rename_map[col] = "gz"
                    # Extract date
                    parts = col.split('-')
                    if len(parts) >= 3:
                        estimate_date = f"{parts[0]}-{parts[1]}-{parts[2]}"
                elif "估算增长率" in col:
                    rename_map[col] = "zzl"
                elif "基金代码" in col:
                    rename_map[col] = "code"
                elif "公布数据-单位净值" in col:
                    # Today's confirmed NAV
                    rename_map[col] = "confirmed_nav"
                elif col.endswith("-单位净值") and "公布数据" not in col:
                    # Previous day's NAV (Pre-Close)
                    rename_map[col] = "pre_close"
            
            df = df.rename(columns=rename_map)
            
            if 'code' in df.columns:
                df['code'] = df['code'].astype(str)
                
                if estimate_date:
                    df['est_date'] = estimate_date
                else:
                    df['est_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                
                # Ensure columns exist
                if 'pre_close' not in df.columns: df['pre_close'] = 0.0
                if 'confirmed_nav' not in df.columns: df['confirmed_nav'] = 0.0
                
                # Convert to dict
                return df.set_index('code')[['gz', 'zzl', 'est_date', 'pre_close', 'confirmed_nav']].to_dict('index')
    except Exception as e:
        print(f"Error fetching realtime estimations: {e}")
    return {}


def prefetch_data(fund_codes):
    """
    Prefetch data for a list of funds in parallel.
    """
    # Always fetch real-time estimations first (single call)
    # We use a ThreadPool to do this alongside history fetching
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # 1. Real-time estimations (Global)
        executor.submit(_fetch_realtime_estimations)
        
        # 2. Fund Histories (Per Fund)
        if fund_codes:
            # Deduplicate codes
            unique_codes = list(set(fund_codes))
            for code in unique_codes:
                executor.submit(_fetch_fund_history_raw, code)

@st.cache_data(ttl=60)
def get_market_index():
    """从新浪财经获取沪深300指数实时数据"""
    try:
        # s_sh000300 是沪深300指数在新浪的行情代码
        url = "http://hq.sinajs.cn/list=s_sh000300"
        headers = {"Referer": "https://finance.sina.com.cn/"}
        resp = requests.get(url, headers=headers, timeout=2.0)
        
        if resp.status_code == 200:
            content = resp.text
            if "=" in content:
                # 解析格式: var hq_str_s_sh000300="沪深300,3924.34,-12.45,-0.32,123456,789012";
                data_str = content.split('=')[1].strip().strip('";')
                parts = data_str.split(',')
                if len(parts) >= 4:
                    return {
                        '名称': parts[0],
                        '最新价': float(parts[1]),
                        '涨跌额': float(parts[2]),
                        '涨跌幅': float(parts[3])
                    }
    except Exception as e:
        print(f"Error fetching HS300 via Sina: {e}")
    
    return {'最新价': 0.0, '涨跌幅': 0.0, '名称': '获取失败'}

@st.cache_data(ttl=60)
def get_global_indices():
    """
    Fetch global indices (S&P 500, Dow Jones, Nasdaq) from Sina.
    """
    indices = {
        'int_dji': '道琼斯',
        'int_nasdaq': '纳斯达克',
        'int_sp500': '标普500'
    }
    
    results = []
    try:
        codes = ",".join(indices.keys())
        url = f"http://hq.sinajs.cn/list={codes}"
        headers = {"Referer": "https://finance.sina.com.cn/"}
        resp = requests.get(url, headers=headers, timeout=2.0)
        
        if resp.status_code == 200:
            content = resp.text
            # Format: var hq_str_int_dji="道琼斯,39087.38,90.99,0.23";
            
            for code, name in indices.items():
                if f"var hq_str_{code}=" in content:
                    try:
                        line = content.split(f"var hq_str_{code}=\"")[1].split('";')[0]
                        parts = line.split(',')
                        if len(parts) >= 4:
                            results.append({
                                'name': name, # Use our fixed name or parts[0]
                                'price': float(parts[1]),
                                'change': float(parts[2]),
                                'pct': float(parts[3])
                            })
                    except Exception:
                        pass
    except Exception as e:
        print(f"Error fetching global indices: {e}")
        
    return results

# --- Stock Data API ---

def search_stocks(keyword):
    """
    Search stocks using multiple APIs to support A-share, HK, US and other markets.
    Returns a list of dicts: {'code': 'sh600519', 'name': '贵州茅台', 'symbol': '600519', 'market': 'sh'}
    """
    results = []
    
    em_results = search_stocks_eastmoney(keyword)
    if em_results:
        return em_results
    
    try:
        url = f"http://suggest3.sinajs.cn/suggest/type=&key={keyword}"
        resp = requests.get(url, timeout=2.0)
        if resp.status_code == 200:
            content = resp.text
            if '"' in content:
                data_str = content.split('"')[1]
                if not data_str: return []
                
                items = data_str.split(';')
                for item in items:
                    parts = item.split(',')
                    if len(parts) >= 5:
                        full_code = parts[3]
                        symbol = parts[2]
                        name = parts[4]
                        
                        market_prefix = full_code[:2].lower()
                        
                        market_map = {
                            'sh': {'market': 'sh', 'market_name': '沪市'},
                            'sz': {'market': 'sz', 'market_name': '深市'},
                            'hk': {'market': 'hk', 'market_name': '港股'},
                            'us': {'market': 'us', 'market_name': '美股'},
                            'gb': {'market': 'gb', 'market_name': '英股'},
                            'dk': {'market': 'dk', 'market_name': '德股'},
                            'fr': {'market': 'fr', 'market_name': '法股'},
                        }
                        
                        if market_prefix in market_map:
                            market_info = market_map[market_prefix]
                            results.append({
                                'label': f"{name} ({full_code}) - {market_info['market_name']}",
                                'value': full_code,
                                'symbol': symbol,
                                'market': market_info['market'],
                                'market_name': market_info['market_name'],
                                'name': name
                            })
    except Exception as e:
        print(f"Error searching stocks from Sina: {e}")
    
    return results

def search_stocks_eastmoney(keyword):
    """
    Search stocks using East Money API for comprehensive market coverage.
    """
    results = []
    try:
        url = "https://searchapi.eastmoney.com/api/suggest/get"
        params = {
            "input": keyword,
            "type": 14,
            "count": 20
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.eastmoney.com/"
        }
        resp = requests.get(url, params=params, headers=headers, timeout=3.0)
        if resp.status_code == 200:
            data = resp.json()
            table_data = data.get('QuotationCodeTable', {}).get('Data', [])
            if table_data:
                for item in table_data:
                    code = item.get('Code', '')
                    name = item.get('Name', '')
                    market_type = item.get('MarketType', '')
                    classify = item.get('Classify', '')
                    
                    if classify in ['BK', 'Futures', 'HKDerivative']:
                        continue
                    
                    market_map = {
                        '1': {'market': 'sh', 'market_name': '沪市'},
                        '2': {'market': 'sz', 'market_name': '深市'},
                        '3': {'market': 'bj', 'market_name': '北交所'},
                        '4': {'market': 'hk', 'market_name': '港股'},
                        '5': {'market': 'hk', 'market_name': '港股'},
                        '6': {'market': 'gb', 'market_name': '英股'},
                        '7': {'market': 'us', 'market_name': '美股'},
                        '8': {'market': 'jp', 'market_name': '日股'},
                        '9': {'market': 'kr', 'market_name': '韩股'},
                        '10': {'market': 'de', 'market_name': '德股'},
                    }
                    
                    if market_type in market_map:
                        market_info = market_map[market_type]
                        full_code = f"{market_info['market']}{code}"
                        results.append({
                            'label': f"{name} ({full_code}) - {market_info['market_name']}",
                            'value': full_code,
                            'symbol': code,
                            'market': market_info['market'],
                            'market_name': market_info['market_name'],
                            'name': name
                        })
    except Exception as e:
        print(f"Error in East Money search: {e}")
        import traceback
        traceback.print_exc()
    
    return results

def get_stock_realtime_detail(full_code):
    """
    Get detailed real-time stock data from Sina or East Money.
    full_code: e.g. 'sh600519', 'hk00700', 'usAAPL'
    """
    market_prefix = full_code[:2].lower() if len(full_code) > 2 else ''
    
    if market_prefix in ['sh', 'sz']:
        return get_stock_realtime_sina(full_code)
    else:
        return get_stock_realtime_eastmoney(full_code)

def get_stock_realtime_sina(full_code):
    """
    Get detailed real-time stock data from Sina (for A-shares only).
    full_code: e.g. 'sh600519'
    """
    try:
        url = f"http://hq.sinajs.cn/list={full_code}"
        headers = {"Referer": "https://finance.sina.com.cn/"}
        resp = requests.get(url, headers=headers, timeout=2.0)
        
        if resp.status_code == 200:
            content = resp.text
            if "=" in content:
                data_str = content.split('=')[1].strip().strip('";')
                parts = data_str.split(',')
                if len(parts) > 30:
                    price = float(parts[3])
                    pre_close = float(parts[2])
                    
                    change = 0.0
                    pct_change = 0.0
                    if pre_close > 0:
                        change = price - pre_close
                        pct_change = (change / pre_close) * 100
                        
                    return {
                        'name': parts[0],
                        'price': price,
                        'change': change,
                        'pct_change': pct_change,
                        'open': float(parts[1]),
                        'pre_close': pre_close,
                        'high': float(parts[4]),
                        'low': float(parts[5]),
                        'volume': float(parts[8]),
                        'amount': float(parts[9]),
                        'date': parts[30],
                        'time': parts[31],
                        'bid_ask': {
                            'b1_v': float(parts[10]), 'b1_p': float(parts[11]),
                            'b2_v': float(parts[12]), 'b2_p': float(parts[13]),
                            'b3_v': float(parts[14]), 'b3_p': float(parts[15]),
                            'b4_v': float(parts[16]), 'b4_p': float(parts[17]),
                            'b5_v': float(parts[18]), 'b5_p': float(parts[19]),
                            'a1_v': float(parts[20]), 'a1_p': float(parts[21]),
                            'a2_v': float(parts[22]), 'a2_p': float(parts[23]),
                            'a3_v': float(parts[24]), 'a3_p': float(parts[25]),
                            'a4_v': float(parts[26]), 'a4_p': float(parts[27]),
                            'a5_v': float(parts[28]), 'a5_p': float(parts[29]),
                        }
                    }
    except Exception as e:
        print(f"Error fetching stock detail for {full_code}: {e}")
    return None

def get_stock_realtime_eastmoney(full_code):
    """
    Get detailed real-time stock data from East Money (for HK, US and other markets).
    full_code: e.g. 'hk00700', 'usAAPL'
    """
    try:
        market_prefix = full_code[:2].lower()
        symbol = full_code[2:]
        
        em_market_map = {
            'hk': {'mkt': '116', 'market': '港股'},
            'us': {'mkt': '105', 'market': '美股'},
            'gb': {'mkt': '106', 'market': '英股'},
            'jp': {'mkt': '107', 'market': '日股'},
            'kr': {'mkt': '108', 'market': '韩股'},
            'de': {'mkt': '109', 'market': '德股'},
            'fr': {'mkt': '110', 'market': '法股'},
        }
        
        if market_prefix not in em_market_map:
            return None
            
        em_info = em_market_map[market_prefix]
        url = f"https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
            'invt': '2',
            'fltt': '2',
            'fields': 'f43,f44,f45,f46,f47,f48,f50,f51,f52,f55,f57,f58,f59,f60,f116,f117,f118,f162,f163,f164,f167,f168,f169,f170,f171,f173,f177',
            'secid': f"{em_info['mkt']}.{symbol}"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/"
        }
        
        resp = requests.get(url, params=params, headers=headers, timeout=3.0)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('data'):
                stock_data = data['data']
                price = float(stock_data.get('f43', 0)) if stock_data.get('f43') else 0
                pre_close = float(stock_data.get('f44', 0)) if stock_data.get('f44') else 0
                open_price = float(stock_data.get('f45', 0)) if stock_data.get('f45') else 0
                high = float(stock_data.get('f46', 0)) if stock_data.get('f46') else 0
                low = float(stock_data.get('f47', 0)) if stock_data.get('f47') else 0
                volume = float(stock_data.get('f48', 0)) if stock_data.get('f48') else 0
                amount = float(stock_data.get('f50', 0)) if stock_data.get('f50') else 0
                name = stock_data.get('f58', symbol)
                
                # 智能检测并修复外股的不合理数值
                if pre_close > 0:
                    # 如果最高/最低明显不合理（大于昨收的10倍或小于0），则用合理的替代值
                    if high > pre_close * 10 or high <= 0:
                        high = max(price, open_price, pre_close)
                    if low > pre_close * 10 or low <= 0:
                        low = min(price, open_price, pre_close)
                
                change = 0.0
                pct_change = 0.0
                if pre_close > 0:
                    change = price - pre_close
                    pct_change = (change / pre_close) * 100
                    
                return {
                    'name': name,
                    'price': price,
                    'change': change,
                    'pct_change': pct_change,
                    'open': open_price,
                    'pre_close': pre_close,
                    'high': high,
                    'low': low,
                    'volume': volume,
                    'amount': amount,
                    'date': '',
                    'time': '',
                    'market': em_info['market']
                }
    except Exception as e:
        print(f"Error fetching stock detail from East Money for {full_code}: {e}")
    return None

def get_stock_trends(symbol, market):
    """
    Get minute-level trends (Intraday) from EastMoney.
    market: 'sh', 'sz', 'hk', 'us', 'gb', 'jp', 'kr', 'de', 'fr'
    """
    try:
        em_market_map = {
            'sh': '1',
            'sz': '0',
            'hk': '116',
            'us': '105',
            'gb': '106',
            'jp': '107',
            'kr': '108',
            'de': '109',
            'fr': '110',
        }
        
        secid_prefix = em_market_map.get(market, '1')
        secid = f"{secid_prefix}.{symbol}"
        url = f"http://push2.eastmoney.com/api/qt/stock/trends2/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6,f7,f8&fields2=f51,f53&iscr=0"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "http://quote.eastmoney.com/"
        }
        
        resp = requests.get(url, headers=headers, timeout=2.0)
        if resp.status_code == 200:
            data = resp.json()
            if data and data.get('data') and data['data'].get('trends'):
                trends = data['data']['trends']
                processed_data = []
                pre_close = data['data']['preClose']
                
                for t in trends:
                    parts = t.split(',')
                    processed_data.append({
                        'time': parts[0],
                        'price': float(parts[1])
                    })
                
                return {
                    'pre_close': pre_close,
                    'trends': processed_data
                }
    except Exception as e:
        print(f"Error fetching trends for {symbol}: {e}")
    return None

def get_stock_kline(symbol, market, period='101'):
    """
    Get K-line data from EastMoney.
    period: '101' (Day), '102' (Week), '103' (Month)
    market: 'sh', 'sz', 'hk', 'us', 'gb', 'jp', 'kr', 'de', 'fr'
    """
    try:
        em_market_map = {
            'sh': '1',
            'sz': '0',
            'hk': '116',
            'us': '105',
            'gb': '106',
            'jp': '107',
            'kr': '108',
            'de': '109',
            'fr': '110',
        }
        
        secid_prefix = em_market_map.get(market, '1')
        secid = f"{secid_prefix}.{symbol}"
        url = f"http://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6,f7,f8&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt={period}&fqt=1&end=20500101&lmt=120"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "http://quote.eastmoney.com/"
        }
        
        resp = requests.get(url, headers=headers, timeout=2.0)
        if resp.status_code == 200:
            data = resp.json()
            if data and data.get('data') and data['data'].get('klines'):
                klines = data['data']['klines']
                processed_data = []
                
                for k in klines:
                    parts = k.split(',')
                    processed_data.append({
                        'date': parts[0],
                        'open': float(parts[1]),
                        'close': float(parts[2]),
                        'high': float(parts[3]),
                        'low': float(parts[4]),
                        'volume': float(parts[5]),
                        'amount': float(parts[6])
                    })
                return processed_data
    except Exception as e:
        print(f"Error fetching kline for {symbol}: {e}")
    return None

@st.cache_data(ttl=300) # Cache for 5 minutes
def _fetch_financial_news():
    """
    Fetch real-time financial news from EastMoney (with URLs) or Cailian Press (CLS).
    """
    try:
        # 1. Try EastMoney first (Has direct URLs)
        df = safe_ak_call(ak.stock_info_global_em)
        if not df.empty:
            news_list = []
            for _, row in df.head(15).iterrows():
                title = row['标题']
                url = row['链接']
                news_list.append({
                    'title': title,
                    'time': str(row['发布时间']),
                    'tag': '东方财富',
                    'url': url
                })
            return news_list
            
    except Exception as e:
        print(f"Error fetching EastMoney news: {e}")
        
    try:
        # 2. Fallback to Cailian Press (CLS) - No direct URLs, use Search
        df = safe_ak_call(ak.stock_info_global_cls)
        if not df.empty:
            news_list = []
            for _, row in df.head(15).iterrows():
                title = row['标题'] if row['标题'] else row['内容'][:60] + "..."
                # Use a more direct search link for the news title
                url = f"https://www.baidu.com/s?wd={title}"
                news_list.append({
                    'title': title,
                    'time': str(row['发布时间']),
                    'tag': '财联社',
                    'url': url
                })
            return news_list
    except Exception as e:
        print(f"Error fetching CLS news: {e}")
        
    return []



# --- Public API Functions ---

def get_financial_news():
    """
    Get latest financial news.
    """
    return _fetch_financial_news()

@st.cache_data(ttl=3600*24)
def _fetch_fund_details_xq(fund_code):
    """
    Fetch detailed fund info (Manager, Start Date) from XueQiu.
    """
    try:
        df = safe_ak_call(ak.fund_individual_basic_info_xq, symbol=fund_code)
        if not df.empty:
            return df.set_index('item')['value'].to_dict()
    except Exception as e:
        print(f"Error fetching fund details XQ for {fund_code}: {e}")
    return {}

def get_fund_base_info(fund_code):
    """
    Fetch basic fund information using cached full list + detailed info.
    """
    # 1. Basic Info from cached list
    fund_name_df = _fetch_all_fund_names()
    
    info = {
         'code': fund_code,
         'name': '未知基金',
         'type': '--',
         'manager': '--', 
         'start_date': '--',
         'scale': '--',
         'company': '--',
         'rating': '--',
         'strategy': '暂无描述',
         'goal': '暂无描述',
         'benchmark': '--'
     }
    
    if not fund_name_df.empty:
        fund_match = fund_name_df[fund_name_df['基金代码'] == fund_code]
        if not fund_match.empty:
            info['name'] = fund_match['基金简称'].iloc[0]
            info['type'] = fund_match['基金类型'].iloc[0]

    # 2. Detailed Info from Real API (XueQiu)
    details = _fetch_fund_details_xq(fund_code)
    if details:
        info['manager'] = details.get('基金经理', info['manager'])
        info['start_date'] = details.get('成立时间', info['start_date'])
        
        # Additional Fields
        info['full_name'] = details.get('基金全称', '--')
        info['scale'] = details.get('最新规模', '--')
        info['company'] = details.get('基金公司', '--')
        info['rating'] = details.get('基金评级', '--')
        info['strategy'] = details.get('投资策略', '暂无描述')
        info['goal'] = details.get('投资目标', '暂无描述')
        info['benchmark'] = details.get('业绩比较基准', '--')
        
        # Fallback for name/type if missing in basic list
        if info['name'] == '未知基金' and '基金简称' in details:
             info['name'] = details['基金简称']
        if info['type'] == '--' and '基金类型' in details:
             info['type'] = details['基金类型']
             
    return info

def search_funds(keyword):
    """
    Search funds by keyword (code or name).
    """
    fund_name_df = _fetch_all_fund_names()
    if fund_name_df.empty:
        return pd.DataFrame()
        
    # 1. Exact Code Match
    if keyword.isdigit() and len(keyword) == 6:
        match = fund_name_df[fund_name_df['基金代码'] == keyword]
        if not match.empty:
            return match.rename(columns={
                '基金代码': 'code',
                '基金简称': 'name',
                '基金类型': 'type'
            })[['code', 'name', 'type']]

    # 2. Fuzzy Name Match
    # Filter by name contains keyword (case insensitive)
    match = fund_name_df[fund_name_df['基金简称'].str.contains(keyword, na=False, case=False)]
    
    if not match.empty:
        return match.rename(columns={
            '基金代码': 'code',
            '基金简称': 'name',
            '基金类型': 'type'
        })[['code', 'name', 'type']].head(20) # Limit to 20 results
            
    return pd.DataFrame()

def get_fund_basic_info(fund_code):
    """
    Alias for get_fund_base_info to match app.py usage.
    """
    return get_fund_base_info(fund_code)

def get_fund_nav_history(fund_code, start_date='2020-01-01', end_date=None):
    """
    Get historical NAV data.
    """
    df = _fetch_fund_history_raw(fund_code)
    
    if df.empty:
        return pd.DataFrame()
        
    if not end_date:
        end_date = datetime.datetime.now().strftime("%Y%m%d")
    start_date = start_date.replace('-', '')
    
    mask = (df['净值日期'] >= pd.to_datetime(start_date)) & (df['净值日期'] <= pd.to_datetime(end_date))
    return df.loc[mask].sort_values('净值日期')

@st.cache_data(ttl=60) # Cache for 60 seconds
def get_fund_intraday_trend(fund_code):
    """
    Fetch intraday estimation trend for a fund.
    Returns a DataFrame with columns ['时间', '估算值', '估算涨跌幅']
    """
    try:
        df = safe_ak_call(ak.fund_open_fund_info_em, symbol=fund_code, indicator="单位净值估算走势")
        if not df.empty:
            # The columns are usually ['时间', '估算值', '估算涨跌幅']
            df['时间'] = pd.to_datetime(df['时间'])
            return df
    except Exception as e:
        print(f"Error fetching intraday trend for {fund_code}: {e}")
    return pd.DataFrame()

def get_batch_intraday_trends(fund_codes):
    """
    Fetch intraday trends for multiple funds in parallel.
    Returns a dict {code: {'values': [], 'pct': [], 'times': []}}
    """
    results = {}
    if not fund_codes:
        return results

    def _fetch_values(code):
        df = get_fund_intraday_trend(code)
        
        # If empty, try to get historical trend (last available trading day)
        is_history = False
        if df.empty:
            # We don't have a direct "historical intraday" API in AkShare for mutual funds,
            # but EastMoney's trend API usually returns the LAST available session if it's currently night/weekend.
            # If it's truly empty, it might be the very early morning gap.
            pass 

        if not df.empty:
            # Check if the data is from today or previous days
            last_time = df['时间'].iloc[-1]
            if last_time.date() < datetime.datetime.now().date():
                is_history = True
                
            # Ensure columns exist
            if '估算值' in df.columns and '估算涨跌幅' in df.columns and '时间' in df.columns:
                 return code, {
                     'values': df['估算值'].tolist(),
                     'pct': df['估算涨跌幅'].tolist(),
                     'times': df['时间'].dt.strftime('%H:%M').tolist(),
                     'is_history': is_history
                 }
        return code, {'values': [], 'pct': [], 'times': [], 'is_history': False}

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_fetch_values, code) for code in fund_codes]
        for future in concurrent.futures.as_completed(futures):
            code, data = future.result()
            results[code] = data
    return results

def get_real_time_estimate(fund_code, pre_fetched_data=None):
    """
    Get real-time estimated valuation.
    Strictly from network, no mock data.
    """
    # 1. Try DIRECT Fast API first (Most accurate and fast for single fund)
    est_data = pre_fetched_data if pre_fetched_data else _fetch_single_fund_realtime(fund_code)
    
    # 2. Try to get confirmed NAV if available (Fallback to history)
    last_nav = None
    last_date = '--'
    
    if est_data:
        try:
            # Check confirmed_nav (Today's confirmed)
            if est_data.get('confirmed_nav') and float(est_data['confirmed_nav']) > 0:
                 last_nav = float(est_data['confirmed_nav'])
                 last_date = est_data.get('est_date', '--') 
            # Else check pre_close (Yesterday's confirmed)
            elif est_data.get('pre_close') and float(est_data['pre_close']) > 0:
                 last_nav = float(est_data['pre_close'])
                 last_date = "前一交易日"
        except:
            pass
            
    # If still no last_nav, fetch from history (Real data)
    if last_nav is None:
        history_df = _fetch_fund_history_raw(fund_code)
        if not history_df.empty:
            latest = history_df.iloc[-1]
            last_nav = float(latest['单位净值'])
            last_date = str(latest['净值日期'].date())

    # 3. Combine with Estimation
    if est_data:
        try:
            gz_val = float(est_data['gz'])
            zzl_raw = est_data['zzl']
            est_date = est_data.get('est_date', datetime.datetime.now().strftime("%Y-%m-%d"))
            
            if isinstance(zzl_raw, str):
                zzl_val = float(zzl_raw.replace('%', ''))
            else:
                zzl_val = float(zzl_raw)
                
            return {
                'gz': round(gz_val, 4),
                'zzl': round(zzl_val, 2),
                'time': datetime.datetime.now().strftime("%H:%M:%S"),
                'data_date': est_date,
                'last_nav': last_nav if last_nav else gz_val,
                'last_date': last_date,
                'pre_close': float(est_data.get('pre_close', 0.0))
            }
        except (ValueError, TypeError):
            pass

    # Final Fallback - ONLY if network call failed or returned nothing
    # We return a clear "Data Unavailable" structure
    return {
        'gz': last_nav if last_nav else 0.0, 
        'zzl': 0.00, 
        'time': '数据暂不可用', 
        'data_date': last_date,
        'last_nav': last_nav if last_nav else 0.0, 
        'last_date': last_date,
        'pre_close': last_nav if last_nav else 0.0,
        'is_error': True if last_nav is None else False
    }

def get_portfolio_history(holdings, days=30):
    """
    Calculate the historical market value of the current portfolio over the last N days.
    Uses Parallel Processing for speed.
    """
    if not holdings:
        return pd.DataFrame()
        
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=days + 10)
    start_date_str = start_date.strftime('%Y-%m-%d')
    
    # 1. Fetch all histories in parallel
    codes = [item['fund_code'] for item in holdings]
    
    # We use a ThreadPoolExecutor to fetch data in parallel
    # Note: Streamlit cache is thread-safe
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Pre-fetch / Ensure cache is populated
        list(executor.map(_fetch_fund_history_raw, codes))
    
    # 2. Process (now hitting cache)
    total_value_series = None
    
    for item in holdings:
        code = item['fund_code']
        share = item['share']
        
        # This will now be instant (cached)
        df = get_fund_nav_history(code, start_date=start_date_str)
        
        if not df.empty:
            df = df.set_index('净值日期')
            val_series = df['单位净值'] * share
            
            if total_value_series is None:
                total_value_series = val_series
            else:
                total_value_series = total_value_series.add(val_series, fill_value=0)
    
    if total_value_series is not None:
        total_value_series = total_value_series.sort_index().dropna()
        mask = total_value_series.index >= pd.to_datetime(end_date - datetime.timedelta(days=days))
        return total_value_series[mask]
        
    return pd.Series()

def prefetch_data(fund_codes):
    """
    Prefetch data for a list of funds in parallel.
    Call this at app startup.
    """
    if not fund_codes:
        return
        
    # We use a ThreadPoolExecutor to fetch data in parallel
    # NOTE: We NO LONGER call _fetch_realtime_estimations (bulk) here
    # because it is too slow. We rely on on-demand fast single fetches.
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # Submit history fetch tasks for all funds
        for code in fund_codes:
            executor.submit(_fetch_fund_history_raw, code)


# ============================================================================
# 外汇、贵金属、商品功能
# ============================================================================

@st.cache_data(ttl=60)  # 缓存60秒，外汇数据变化较快
def get_currency_rates():
    """
    获取主要货币汇率
    返回格式: [{'symbol': 'USDCNY', 'name': '美元人民币', 'price': 7.25, 'pct': 0.12, ...}]
    """
    try:
        # 主要货币对列表
        forex_pairs = [
            ('USDCNY', '美元人民币'),
            ('USDCNH', '美元离岸人民币'),
            ('EURCNY', '欧元人民币'),
            ('GBPCNY', '英镑人民币'),
            ('HKDCNY', '港元人民币'),
            ('AUDCNY', '澳元人民币'),
            ('CADCNY', '加元人民币'),
            ('SGDCNY', '新加坡元人民币'),
            ('CHFCNY', '瑞士法郎人民币'),
            ('JPYCNY', '日元人民币'),
        ]
        
        results = []
        
        for symbol, name in forex_pairs:
            try:
                # 新浪财经外汇API
                url = f"http://hq.sinajs.cn/list={symbol}"
                headers = {
                    "Referer": "https://finance.sina.com.cn/",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
                
                response = requests.get(url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    content = response.text
                    
                    if "=" in content and len(content.split('=')[1]) > 10:
                        data_str = content.split('=')[1].strip().strip('";')
                        parts = data_str.split(',')
                        
                        if len(parts) >= 11:
                            # 解析字段（基于测试结果）
                            # 格式: 时间,开盘价?,最高价?,最低价?,成交量,买价,卖价,昨收,最新价,名称,日期
                            try:
                                time_str = parts[0]  # 时间
                                open_price = float(parts[1]) if parts[1] else 0
                                high_price = float(parts[2]) if parts[2] else 0
                                low_price = float(parts[3]) if parts[3] else 0
                                volume = float(parts[4]) if parts[4] else 0
                                bid_price = float(parts[5]) if parts[5] else 0
                                ask_price = float(parts[6]) if parts[6] else 0
                                pre_close = float(parts[7]) if parts[7] else 0
                                price = float(parts[8]) if parts[8] else 0
                                display_name = parts[9] if len(parts) > 9 else name
                                date_str = parts[10] if len(parts) > 10 else datetime.datetime.now().strftime('%Y-%m-%d')
                                
                                # 计算涨跌幅
                                pct = 0.0
                                if pre_close > 0:
                                    pct = (price - pre_close) / pre_close * 100
                                
                                # 使用实际名称
                                if display_name and display_name != time_str:
                                    name = display_name
                                
                                results.append({
                                    'symbol': symbol,
                                    'name': name,
                                    'price': price,
                                    'pct': round(pct, 4),
                                    'open': open_price,
                                    'high': high_price,
                                    'low': low_price,
                                    'pre_close': pre_close,
                                    'volume': volume,
                                    'amount': 0,
                                    'bid': bid_price,
                                    'ask': ask_price,
                                    'time': time_str,
                                    'date': date_str
                                })
                            except Exception as e:
                                # 如果解析失败，尝试简化版本
                                try:
                                    if len(parts) >= 9:
                                        price = float(parts[8]) if parts[8] else 0
                                        pre_close = float(parts[7]) if parts[7] else price
                                        pct = ((price - pre_close) / pre_close * 100) if pre_close > 0 else 0
                                        
                                        results.append({
                                            'symbol': symbol,
                                            'name': name,
                                            'price': price,
                                            'pct': round(pct, 4),
                                            'open': price,
                                            'high': price,
                                            'low': price,
                                            'pre_close': pre_close,
                                            'volume': 0,
                                            'amount': 0,
                                            'time': time_str,
                                            'date': datetime.datetime.now().strftime('%Y-%m-%d')
                                        })
                                except:
                                    continue
            except Exception as e:
                # 单个货币对失败，继续尝试下一个
                continue
        
        # 如果新浪API失败，尝试使用akshare作为备用
        if not results:
            try:
                # 尝试使用东方财富外汇现货数据
                df = safe_ak_call(ak.forex_spot_em)
                
                if not df.empty:
                    for _, row in df.iterrows():
                        try:
                            symbol = row.get('代码', '')
                            name = row.get('名称', symbol)
                            
                            # 获取价格
                            price = None
                            for col in ['最新价', 'price', '现价']:
                                if col in df.columns:
                                    try:
                                        price = float(row[col])
                                        break
                                    except:
                                        continue
                            
                            if not price:
                                continue
                            
                            # 获取涨跌幅
                            pct = 0.0
                            for col in ['涨跌幅', '涨跌%', 'pct_change']:
                                if col in df.columns:
                                    try:
                                        pct_str = str(row[col]).replace('%', '')
                                        pct = float(pct_str)
                                        break
                                    except:
                                        continue
                            
                            results.append({
                                'symbol': symbol,
                                'name': name,
                                'price': price,
                                'pct': pct,
                                'open': price,
                                'high': price,
                                'low': price,
                                'pre_close': price * (1 - pct/100) if pct != 0 else price,
                                'volume': 0,
                                'amount': 0,
                                'time': datetime.datetime.now().strftime('%H:%M:%S'),
                                'date': datetime.datetime.now().strftime('%Y-%m-%d')
                            })
                        except Exception as e:
                            continue
            except Exception as e:
                print(f"备用akshare API也失败: {e}")
        
        return results
    except Exception as e:
        print(f"Error fetching currency rates: {e}")
        return []


@st.cache_data(ttl=60)  # 缓存60秒
def get_precious_metals():
    """
    获取贵金属行情（黄金、白银等）
    返回格式: [{'symbol': 'XAUUSD', 'name': '黄金现货', 'price': 1950.25, 'pct': 0.52, ...}]
    """
    try:
        # 新浪财经贵金属代码
        metal_symbols = [
            ('hf_XAU', '黄金现货', 'XAU'),
            ('hf_XAG', '白银现货', 'XAG'),
            ('gds_AUTD', '黄金延期', 'AU'),
            ('gds_AGTD', '白银延期', 'AG'),
            ('nf_AU0', '黄金期货', 'AU_FUTURES'),
            ('nf_AG0', '白银期货', 'AG_FUTURES'),
        ]
        
        results = []
        
        for symbol, name, short_symbol in metal_symbols:
            try:
                # 新浪财经API
                url = f"http://hq.sinajs.cn/list={symbol}"
                headers = {
                    "Referer": "https://finance.sina.com.cn/",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
                
                response = requests.get(url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    content = response.text
                    
                    if "=" in content and len(content.split('=')[1]) > 5:
                        data_str = content.split('=')[1].strip().strip('";')
                        parts = data_str.split(',')
                        
                        # 根据字段数量解析
                        if len(parts) >= 14:
                            # hf_XAU/hf_XAG格式: 最新价,开盘价,最高价,最低价,买价,卖价,昨收,今开,最高,最低,成交量,成交额,持仓量,时间
                            try:
                                price = float(parts[0]) if parts[0] else 0
                                open_price = float(parts[1]) if len(parts) > 1 and parts[1] else price
                                high_price = float(parts[2]) if len(parts) > 2 and parts[2] else price
                                low_price = float(parts[3]) if len(parts) > 3 and parts[3] else price
                                bid_price = float(parts[4]) if len(parts) > 4 and parts[4] else price
                                ask_price = float(parts[5]) if len(parts) > 5 and parts[5] else price
                                pre_close = float(parts[6]) if len(parts) > 6 and parts[6] else price
                                
                                # 有些格式可能有重复的字段
                                if len(parts) > 7 and float(parts[7]) > 0:
                                    open_price = float(parts[7])
                                if len(parts) > 8 and float(parts[8]) > 0:
                                    high_price = float(parts[8])
                                if len(parts) > 9 and float(parts[9]) > 0:
                                    low_price = float(parts[9])
                                    
                                volume = float(parts[10]) if len(parts) > 10 and parts[10] else 0
                                amount = float(parts[11]) if len(parts) > 11 and parts[11] else 0
                                position = float(parts[12]) if len(parts) > 12 and parts[12] else 0
                                time_str = parts[13] if len(parts) > 13 else datetime.datetime.now().strftime('%H:%M:%S')
                                
                                # 计算涨跌幅
                                pct = 0.0
                                if pre_close > 0:
                                    pct = (price - pre_close) / pre_close * 100
                                
                                results.append({
                                    'symbol': short_symbol,
                                    'name': name,
                                    'price': price,
                                    'pct': round(pct, 4),
                                    'open': open_price,
                                    'high': high_price,
                                    'low': low_price,
                                    'pre_close': pre_close,
                                    'volume': volume,
                                    'amount': amount,
                                    'bid': bid_price,
                                    'ask': ask_price,
                                    'position': position,
                                    'time': time_str,
                                    'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                                    'source': 'sina'
                                })
                            except Exception as e:
                                # 简化解析
                                try:
                                    if len(parts) >= 7:
                                        price = float(parts[0]) if parts[0] else 0
                                        pre_close = float(parts[6]) if parts[6] else price
                                        pct = ((price - pre_close) / pre_close * 100) if pre_close > 0 else 0
                                        
                                        results.append({
                                            'symbol': short_symbol,
                                            'name': name,
                                            'price': price,
                                            'pct': round(pct, 4),
                                            'open': price,
                                            'high': price,
                                            'low': price,
                                            'pre_close': pre_close,
                                            'volume': 0,
                                            'amount': 0,
                                            'time': datetime.datetime.now().strftime('%H:%M:%S'),
                                            'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                                            'source': 'sina'
                                        })
                                except:
                                    continue
            except Exception as e:
                # 单个金属失败，继续尝试下一个
                continue
        
        # 如果新浪API没有数据，使用上海黄金交易所数据作为备用
        if not results:
            try:
                # 黄金基准价
                gold_df = safe_ak_call(ak.spot_golden_benchmark_sge)
                if not gold_df.empty:
                    latest = gold_df.iloc[-1]
                    price = float(latest['晚盘价']) if '晚盘价' in gold_df.columns and latest['晚盘价'] else 0
                    results.append({
                        'symbol': 'XAUCNY_SGE',
                        'name': '上海黄金交易所黄金基准价',
                        'price': price,
                        'pct': 0.0,
                        'open': price,
                        'high': price,
                        'low': price,
                        'pre_close': price,
                        'volume': 0,
                        'amount': 0,
                        'time': latest['交易时间'] if '交易时间' in gold_df.columns else datetime.datetime.now().strftime('%Y-%m-%d'),
                        'date': latest['交易时间'] if '交易时间' in gold_df.columns else datetime.datetime.now().strftime('%Y-%m-%d'),
                        'source': 'sge'
                    })
                
                # 白银基准价
                silver_df = safe_ak_call(ak.spot_silver_benchmark_sge)
                if not silver_df.empty:
                    latest = silver_df.iloc[-1]
                    price = float(latest['晚盘价']) if '晚盘价' in silver_df.columns and latest['晚盘价'] else 0
                    results.append({
                        'symbol': 'XAGCNY_SGE',
                        'name': '上海黄金交易所白银基准价',
                        'price': price,
                        'pct': 0.0,
                        'open': price,
                        'high': price,
                        'low': price,
                        'pre_close': price,
                        'volume': 0,
                        'amount': 0,
                        'time': latest['交易时间'] if '交易时间' in silver_df.columns else datetime.datetime.now().strftime('%Y-%m-%d'),
                        'date': latest['交易时间'] if '交易时间' in silver_df.columns else datetime.datetime.now().strftime('%Y-%m-%d'),
                        'source': 'sge'
                    })
            except Exception as e:
                print(f"Error fetching SGE data: {e}")
        
        return results
    except Exception as e:
        print(f"Error fetching precious metals: {e}")
        return []


@st.cache_data(ttl=60)  # 缓存60秒
def get_commodity_prices():
    """
    获取主要商品价格（原油、铜、农产品等）
    返回格式: [{'symbol': 'CL', 'name': '原油', 'price': 75.25, 'pct': -0.52, ...}]
    """
    try:
        # 新浪财经商品代码
        commodity_symbols = [
            ('hf_OIL', '原油', 'OIL'),
            ('hf_CU', '铜', 'COPPER'),
            ('hf_AL', '铝', 'ALUMINUM'),
            ('hf_ZN', '锌', 'ZINC'),
            ('hf_NI', '镍', 'NICKEL'),
            ('hf_PB', '铅', 'LEAD'),
            ('hf_SN', '锡', 'TIN'),
            ('hf_RB', '螺纹钢', 'REBAR'),
            ('hf_HC', '热轧卷板', 'HOT_ROLLED_COIL'),
            ('hf_FU', '燃料油', 'FUEL_OIL'),
            ('hf_BU', '沥青', 'ASPHALT'),
            ('hf_RU', '橡胶', 'RUBBER'),
            ('nf_SC0', '原油期货', 'CRUDE_FUTURES'),
            ('nf_FU0', '燃料油期货', 'FUEL_OIL_FUTURES'),
            ('nf_BU0', '沥青期货', 'ASPHALT_FUTURES'),
        ]
        
        results = []
        
        for symbol, name, short_symbol in commodity_symbols:
            try:
                # 新浪财经API
                url = f"http://hq.sinajs.cn/list={symbol}"
                headers = {
                    "Referer": "https://finance.sina.com.cn/",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
                
                response = requests.get(url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    content = response.text
                    
                    if "=" in content and len(content.split('=')[1]) > 5:
                        data_str = content.split('=')[1].strip().strip('";')
                        parts = data_str.split(',')
                        
                        # 尝试解析数据
                        if len(parts) >= 7:
                            try:
                                # 常见格式: 最新价,开盘价,最高价,最低价,买价,卖价,昨收,... 
                                price = float(parts[0]) if parts[0] else 0
                                open_price = float(parts[1]) if len(parts) > 1 and parts[1] else price
                                high_price = float(parts[2]) if len(parts) > 2 and parts[2] else price
                                low_price = float(parts[3]) if len(parts) > 3 and parts[3] else price
                                bid_price = float(parts[4]) if len(parts) > 4 and parts[4] else price
                                ask_price = float(parts[5]) if len(parts) > 5 and parts[5] else price
                                pre_close = float(parts[6]) if len(parts) > 6 and parts[6] else price
                                
                                # 计算涨跌幅
                                pct = 0.0
                                if pre_close > 0:
                                    pct = (price - pre_close) / pre_close * 100
                                
                                # 获取时间
                                time_str = datetime.datetime.now().strftime('%H:%M:%S')
                                if len(parts) >= 14:
                                    time_str = parts[13] if parts[13] else time_str
                                
                                results.append({
                                    'symbol': short_symbol,
                                    'name': name,
                                    'price': price,
                                    'pct': round(pct, 4),
                                    'open': open_price,
                                    'high': high_price,
                                    'low': low_price,
                                    'pre_close': pre_close,
                                    'volume': float(parts[10]) if len(parts) > 10 and parts[10] else 0,
                                    'amount': float(parts[11]) if len(parts) > 11 and parts[11] else 0,
                                    'bid': bid_price,
                                    'ask': ask_price,
                                    'time': time_str,
                                    'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                                    'source': 'sina'
                                })
                            except Exception as e:
                                # 简化解析
                                try:
                                    if len(parts) >= 7:
                                        price = float(parts[0]) if parts[0] else 0
                                        pre_close = float(parts[6]) if parts[6] else price
                                        pct = ((price - pre_close) / pre_close * 100) if pre_close > 0 else 0
                                        
                                        results.append({
                                            'symbol': short_symbol,
                                            'name': name,
                                            'price': price,
                                            'pct': round(pct, 4),
                                            'open': price,
                                            'high': price,
                                            'low': price,
                                            'pre_close': pre_close,
                                            'volume': 0,
                                            'amount': 0,
                                            'time': datetime.datetime.now().strftime('%H:%M:%S'),
                                            'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                                            'source': 'sina'
                                        })
                                except:
                                    continue
            except Exception as e:
                # 单个商品失败，继续尝试下一个
                continue
        
        # 如果新浪API没有足够数据，尝试使用akshare作为备用
        if len(results) < 5:
            try:
                # 尝试主要商品交易所
                exchanges = ['NYMEX', 'CBOT', 'LME', 'ICE']
                
                for exchange in exchanges:
                    try:
                        df = safe_ak_call(ak.futures_foreign_commodity_realtime, symbol=exchange)
                        
                        if not df.empty:
                            for _, row in df.iterrows():
                                try:
                                    name = row.get('商品名称', '')
                                    
                                    # 跳过贵金属（已在另一个函数中处理）
                                    metals = ['黄金', '白银', '铂金', '钯金']
                                    if any(metal in name for metal in metals):
                                        continue
                                    
                                    # 跳过已获取的商品
                                    existing_names = [r['name'] for r in results]
                                    if name in existing_names:
                                        continue
                                    
                                    price = float(row.get('最新价', 0))
                                    pct_str = str(row.get('涨跌幅', '0%')).replace('%', '')
                                    pct = float(pct_str)
                                    
                                    results.append({
                                        'symbol': f"{exchange}_{name}",
                                        'name': name,
                                        'price': price,
                                        'pct': pct,
                                        'open': float(row.get('开盘价', price)),
                                        'high': float(row.get('最高价', price)),
                                        'low': float(row.get('最低价', price)),
                                        'pre_close': price * (1 - pct/100) if pct != 0 else price,
                                        'volume': 0,
                                        'amount': 0,
                                        'exchange': exchange,
                                        'time': datetime.datetime.now().strftime('%H:%M:%S'),
                                        'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                                        'source': 'akshare'
                                    })
                                except Exception as e:
                                    continue
                    except Exception as e:
                        continue
            except Exception as e:
                print(f"备用akshare商品API失败: {e}")
        
        return results
    except Exception as e:
        print(f"Error fetching commodity prices: {e}")
        return []


def search_forex_commodity(keyword):
    """
    搜索外汇、贵金属或商品
    参数: keyword - 搜索关键词（如"USDCNH", "黄金", "原油"）
    返回格式: [{'symbol': '...', 'name': '...', 'type': 'forex'|'metal'|'commodity', ...}]
    """
    keyword_lower = keyword.lower().strip()
    results = []
    
    # 1. 搜索货币
    currency_rates = get_currency_rates()
    for item in currency_rates:
        if (keyword_lower in item['symbol'].lower() or 
            keyword_lower in item['name'].lower()):
            item['type'] = 'forex'
            results.append(item)
    
    # 2. 搜索贵金属
    precious_metals = get_precious_metals()
    for item in precious_metals:
        if (keyword_lower in item['symbol'].lower() or 
            keyword_lower in item['name'].lower()):
            item['type'] = 'metal'
            results.append(item)
    
    # 3. 搜索商品
    commodities = get_commodity_prices()
    for item in commodities:
        if (keyword_lower in item['symbol'].lower() or 
            keyword_lower in item['name'].lower()):
            item['type'] = 'commodity'
            results.append(item)
    
    return results


def get_forex_commodity_detail(symbol):
    """
    获取外汇/商品详情
    参数: symbol - 代码（如"USDCNY", "XAUUSD"等）
    返回格式: 与search_forex_commodity类似，但包含更多细节
    """
    # 尝试在所有类别中查找
    all_items = []
    all_items.extend([{**item, 'type': 'forex'} for item in get_currency_rates()])
    all_items.extend([{**item, 'type': 'metal'} for item in get_precious_metals()])
    all_items.extend([{**item, 'type': 'commodity'} for item in get_commodity_prices()])
    
    for item in all_items:
        if item['symbol'].lower() == symbol.lower():
            return item
    
    # 如果没有精确匹配，尝试部分匹配
    for item in all_items:
        if symbol.lower() in item['symbol'].lower() or symbol.lower() in item['name'].lower():
            return item
    
    return None

