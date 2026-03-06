import sqlite3
import pandas as pd
import os
from datetime import datetime

DB_FILE = 'fund_data.db'

def init_db():
    """Initialize the database with necessary tables."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Holdings table: Stores user's fund holdings
    c.execute('''
        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL,
            fund_name TEXT,
            share REAL NOT NULL,
            cost_price REAL NOT NULL,
            purchase_date TEXT
        )
    ''')
    
    # Investment Plans table: Stores auto-investment (定投) plans
    c.execute('''
        CREATE TABLE IF NOT EXISTS investment_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL,
            fund_name TEXT,
            amount REAL NOT NULL,
            frequency TEXT NOT NULL,
            execution_day TEXT,
            start_date TEXT,
            status TEXT DEFAULT 'active'
        )
    ''')
    
    # Attempt to add execution_day column if it doesn't exist (Migration for existing DB)
    try:
        c.execute('ALTER TABLE investment_plans ADD COLUMN execution_day TEXT')
    except sqlite3.OperationalError:
        pass # Column likely already exists

    # Knowledge/Favorites table
    c.execute('''
        CREATE TABLE IF NOT EXISTS favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            url TEXT,
            category TEXT,
            added_date TEXT
        )
    ''')
    
    # Intraday Ticks table: Stores real-time ticks for charts
    # We store timestamp as TEXT (ISO format)
    c.execute('''
        CREATE TABLE IF NOT EXISTS intraday_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL,
            record_time TEXT NOT NULL,
            pct REAL NOT NULL,
            price REAL,
            UNIQUE(fund_code, record_time)
        )
    ''')
    
    # Settings table: Stores global configuration like API keys
    c.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    # User Indices table: Stores user-selected indices/stocks for the dashboard
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_indices (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            market TEXT
        )
    ''')
    
    # Search History table: Stores recent search keywords
    c.execute('''
        CREATE TABLE IF NOT EXISTS search_history (
            keyword TEXT PRIMARY KEY,
            timestamp TEXT
        )
    ''')
    
    # Asset History table: Stores daily snapshots of total portfolio value
    c.execute('''
        CREATE TABLE IF NOT EXISTS asset_history (
            date TEXT PRIMARY KEY,
            total_market_value REAL,
            total_cost REAL,
            day_profit REAL
        )
    ''')
    
    # Fund Daily Performance table: Stores daily performance for each fund
    c.execute('''
        CREATE TABLE IF NOT EXISTS fund_daily_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL,
            date TEXT NOT NULL,
            nav REAL NOT NULL,
            daily_growth REAL NOT NULL,
            confirmed_nav REAL,
            UNIQUE(fund_code, date)
        )
    ''')
    

    
    conn.commit()
    conn.close()

def get_connection():
    return sqlite3.connect(DB_FILE)

# --- Settings Operations ---
def get_setting(key, default=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key = ?', (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else default

def save_setting(key, value):
    conn = get_connection()
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
    conn.commit()
    conn.close()

# --- User Indices Operations ---
def get_user_indices():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM user_indices", conn)
    conn.close()
    return df

def add_user_index(symbol, name, market=''):
    conn = get_connection()
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO user_indices (symbol, name, market) VALUES (?, ?, ?)', (symbol, name, market))
    conn.commit()
    conn.close()

def remove_user_index(symbol):
    conn = get_connection()
    c = conn.cursor()
    c.execute('DELETE FROM user_indices WHERE symbol = ?', (symbol,))
    conn.commit()
    conn.close()

# --- Intraday Ticks Operations ---
def save_tick_batch(ticks_data):
    """
    Save a batch of tick data.
    ticks_data: list of tuples (fund_code, record_time, pct, price)
    """
    if not ticks_data:
        return
        
    conn = get_connection()
    c = conn.cursor()
    # Use INSERT OR IGNORE to avoid duplicates if we fetch same second twice
    c.executemany('''
        INSERT OR IGNORE INTO intraday_ticks (fund_code, record_time, pct, price)
        VALUES (?, ?, ?, ?)
    ''', ticks_data)
    conn.commit()
    conn.close()

def get_today_ticks(fund_code):
    """
    Get ticks for a specific fund for the current day.
    Returns DataFrame with columns ['record_time', 'pct', 'price']
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    conn = get_connection()
    # Filter by time starting with today's date
    query = f"SELECT record_time, pct, price FROM intraday_ticks WHERE fund_code = ? AND record_time LIKE '{today_str}%' ORDER BY record_time ASC"
    df = pd.read_sql(query, conn, params=(fund_code,))
    conn.close()
    return df

def cleanup_old_ticks(days_to_keep=2):
    """Delete ticks older than N days to save space."""
    conn = get_connection()
    c = conn.cursor()
    # Simple date string comparison works for ISO format
    # But calculating the cutoff date string is safer
    # For simplicity, we just delete anything not from today? 
    # User might want to see yesterday's data if today hasn't started.
    # Let's keep it simple: Delete everything that is NOT today.
    # Wait, if user opens app at night, they want to see today's data.
    # If they open tomorrow morning before market, they might want to see yesterday's.
    # Let's keep last 3 days.
    
    # actually, SQLite date modifier: date('now', '-2 days')
    c.execute("DELETE FROM intraday_ticks WHERE record_time < date('now', '-3 days')")
    conn.commit()
    conn.close()

# --- Holdings Operations ---
def add_holding(fund_code, fund_name, share, cost_price, purchase_date=None):
    if not purchase_date:
        purchase_date = datetime.now().strftime("%Y-%m-%d")
    conn = get_connection()
    c = conn.cursor()
    c.execute('INSERT INTO holdings (fund_code, fund_name, share, cost_price, purchase_date) VALUES (?, ?, ?, ?, ?)',
              (fund_code, fund_name, share, cost_price, purchase_date))
    conn.commit()
    conn.close()

def get_holdings():
    conn = get_connection()
    df = pd.read_sql('SELECT * FROM holdings', conn)
    conn.close()
    return df

def delete_holding(holding_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('DELETE FROM holdings WHERE id = ?', (holding_id,))
    conn.commit()
    conn.close()

def update_holding(holding_id, share, cost_price):
    """Update share and cost price for an existing holding."""
    conn = get_connection()
    c = conn.cursor()
    c.execute('UPDATE holdings SET share = ?, cost_price = ? WHERE id = ?',
              (share, cost_price, holding_id))
    conn.commit()
    conn.close()

# --- Investment Plan Operations ---
def add_plan(fund_code, fund_name, amount, frequency, execution_day, start_date):
    conn = get_connection()
    c = conn.cursor()
    c.execute('INSERT INTO investment_plans (fund_code, fund_name, amount, frequency, execution_day, start_date) VALUES (?, ?, ?, ?, ?, ?)',
              (fund_code, fund_name, amount, frequency, execution_day, start_date))
    conn.commit()
    conn.close()

def get_plans():
    conn = get_connection()
    df = pd.read_sql('SELECT * FROM investment_plans', conn)
    conn.close()
    return df

def delete_plan(plan_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('DELETE FROM investment_plans WHERE id = ?', (plan_id,))
    conn.commit()
    conn.close()

def update_plan_status(plan_id, status):
    conn = get_connection()
    c = conn.cursor()
    c.execute('UPDATE investment_plans SET status = ? WHERE id = ?', (status, plan_id))
    conn.commit()
    conn.close()

# --- Investment Plan Execution ---  
def execute_investment_plans():
    """
    Check and execute investment plans that are due.
    Returns list of executed plans.
    """
    import datetime
    from data_api import get_real_time_estimate
    
    conn = get_connection()
    c = conn.cursor()
    
    # Get all active plans
    c.execute("SELECT * FROM investment_plans WHERE status = 'active'")
    plans = c.fetchall()
    
    executed_plans = []
    
    for plan in plans:
        # Table structure: id, fund_code, fund_name, amount, frequency, start_date, status, execution_day
        plan_id = plan[0]
        fund_code = plan[1]
        fund_name = plan[2]
        amount = plan[3]
        frequency = plan[4]
        start_date = plan[5]
        status = plan[6]
        execution_day = plan[7]
        
        # Check if plan should be executed today
        if _should_execute_plan(frequency, execution_day, start_date):
            # Get current NAV
            est = get_real_time_estimate(fund_code)
            if est and 'gz' in est:
                nav = float(est['gz'])
                if nav > 0:
                    # Calculate shares
                    shares = amount / nav
                    
                    # Check if already holding this fund
                    c.execute("SELECT id, share, cost_price FROM holdings WHERE fund_code = ?", (fund_code,))
                    holding = c.fetchone()
                    
                    if holding:
                        # Update existing holding
                        holding_id, old_share, old_cost = holding
                        # Calculate new cost using weighted average
                        total_cost = old_share * old_cost + amount
                        total_share = old_share + shares
                        new_cost = total_cost / total_share
                        
                        c.execute('UPDATE holdings SET share = ?, cost_price = ? WHERE id = ?',
                                  (total_share, new_cost, holding_id))
                    else:
                        # Add new holding
                        purchase_date = datetime.datetime.now().strftime("%Y-%m-%d")
                        c.execute('INSERT INTO holdings (fund_code, fund_name, share, cost_price, purchase_date) VALUES (?, ?, ?, ?, ?)',
                                  (fund_code, fund_name, shares, nav, purchase_date))
                    
                    executed_plans.append({
                        'fund_code': fund_code,
                        'fund_name': fund_name,
                        'amount': amount,
                        'shares': shares,
                        'nav': nav
                    })
    
    conn.commit()
    conn.close()
    return executed_plans

def _should_execute_plan(frequency, execution_day, start_date):
    """
    Check if a plan should be executed today.
    """
    import datetime
    
    today = datetime.datetime.now()
    start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    
    # Check if plan has started
    if today.date() < start_date_obj.date():
        return False
    
    if frequency == '每日':
        return True
    elif frequency == '每周':
        # execution_day: 1-5 for Mon-Fri
        try:
            exec_day = int(execution_day)
            if 1 <= exec_day <= 5:
                return today.weekday() == exec_day - 1  # 0=Mon, 4=Fri
        except:
            pass
    elif frequency == '每月':
        # execution_day: 1-28
        try:
            exec_day = int(execution_day)
            if 1 <= exec_day <= 28:
                return today.day == exec_day
        except:
            pass
    
    return False

# --- Search History Operations ---
def add_search_history(keyword):
    """Add a search keyword to history. Keeps only top 10."""
    if not keyword:
        return
    conn = get_connection()
    c = conn.cursor()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Insert or Replace to update timestamp if exists
    c.execute('INSERT OR REPLACE INTO search_history (keyword, timestamp) VALUES (?, ?)', (keyword, ts))
    
    # Check count and delete old entries if > 10
    c.execute("SELECT count(*) FROM search_history")
    count = c.fetchone()[0]
    
    if count > 10:
        c.execute('''
            DELETE FROM search_history 
            WHERE keyword NOT IN (
                SELECT keyword FROM search_history ORDER BY timestamp DESC LIMIT 10
            )
        ''')
        
    conn.commit()
    conn.close()

def get_search_history():
    """Get top 10 recent search keywords."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT keyword FROM search_history ORDER BY timestamp DESC LIMIT 10")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def clear_search_history():
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM search_history")
    conn.commit()
    conn.close()

# --- Asset History Operations ---
def save_asset_snapshot(date_str, total_market_value, total_cost, day_profit):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO asset_history (date, total_market_value, total_cost, day_profit)
        VALUES (?, ?, ?, ?)
    ''', (date_str, total_market_value, total_cost, day_profit))
    conn.commit()
    conn.close()

def get_asset_history():
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM asset_history ORDER BY date ASC", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df

# --- Fund Daily Performance Operations ---
def save_fund_daily_performance(fund_code, date_str, nav, daily_growth, confirmed_nav=None):
    """
    Save daily performance data for a fund.
    """
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO fund_daily_performance (fund_code, date, nav, daily_growth, confirmed_nav)
        VALUES (?, ?, ?, ?, ?)
    ''', (fund_code, date_str, nav, daily_growth, confirmed_nav))
    conn.commit()
    conn.close()

def save_fund_daily_batch(performance_data):
    """
    Save multiple daily performance records at once.
    performance_data: list of tuples (fund_code, date, nav, daily_growth, confirmed_nav)
    """
    if not performance_data:
        return
        
    conn = get_connection()
    c = conn.cursor()
    c.executemany('''
        INSERT OR REPLACE INTO fund_daily_performance (fund_code, date, nav, daily_growth, confirmed_nav)
        VALUES (?, ?, ?, ?, ?)
    ''', performance_data)
    conn.commit()
    conn.close()

def get_fund_daily_performance(fund_code, start_date=None, end_date=None):
    """
    Get daily performance data for a fund within date range.
    Returns DataFrame with columns: ['date', 'nav', 'daily_growth', 'confirmed_nav']
    """
    conn = get_connection()
    
    query = "SELECT date, nav, daily_growth, confirmed_nav FROM fund_daily_performance WHERE fund_code = ?"
    params = [fund_code]
    
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY date ASC"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def get_all_fund_codes_with_performance():
    """
    Get all fund codes that have daily performance data.
    """
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT DISTINCT fund_code FROM fund_daily_performance")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

# Initialize DB on module load if not exists
if not os.path.exists(DB_FILE):
    init_db()
else:
    # Check if tables exist to avoid errors if file exists but empty
    init_db()
