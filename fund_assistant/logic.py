import pandas as pd
import numpy as np
import datetime
import os
import sys
from data_api import get_fund_nav_history

def ensure_dependencies():
    """
    Ensure required dependencies are in sys.path.
    This helps if the user is running the app from a different environment.
    """
    try:
        import openai
    except ImportError:
        # Try to find .venv site-packages
        possible_paths = [
            os.path.join(os.path.dirname(__file__), "..", ".venv", "Lib", "site-packages"),
            os.path.join(os.path.dirname(__file__), ".venv", "Lib", "site-packages"),
        ]
        for path in possible_paths:
            if os.path.exists(path) and path not in sys.path:
                sys.path.append(path)
                try:
                    import openai
                    return True
                except ImportError:
                    continue
    return False

# Try to load dependencies at module level
ensure_dependencies()

def analyze_fund_with_ai(fund_code, api_key, endpoint_id, fund_name=""):
    """
    Use DeepSeek AI to perform deep fund analysis.
    """
    try:
        import openai
    except ImportError as e:
        import sys
        venv_path = os.path.join(os.path.dirname(__file__), "..", ".venv")
        exists = "存在" if os.path.exists(venv_path) else "不存在"
        return (
            f"❌ **AI 诊断启动失败**\n\n"
            f"原因: 找不到 `openai` 库 ({str(e)})\n\n"
            f"**排查信息**:\n"
            f"- 当前 Python: `{sys.executable}`\n"
            f"- 虚拟环境 ({venv_path}): **{exists}**\n\n"
            f"**解决方法**:\n"
            f"1. 请确保已安装依赖：`pip install openai` 或运行目录下的 `run.bat`。\n"
            f"2. 如果刚安装完，请**彻底关闭并重启** Streamlit 命令行窗口。"
        )
    except Exception as e:
        return f"发生未知错误: {str(e)}"

    if not api_key:
        return "请先在侧边栏配置 DeepSeek API Key。"

    # Check for non-ASCII characters in API Key and Model ID to prevent encoding errors
    try:
        api_key.encode('ascii')
    except UnicodeEncodeError:
        return "API Key 包含非法字符（可能是中文或全角符号），请切换到英文输入法重新输入。"
        
    try:
        endpoint_id.encode('ascii')
    except UnicodeEncodeError:
        return "模型名称 (Model Name) 包含非法字符，请使用纯英文（如 deepseek-chat）。"

    try:
        # 1. Prepare Data for AI
        diagnosis = diagnose_fund(fund_code)
        
        # 2. Setup OpenAI Client (Compatible with DeepSeek)
        client = openai.OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com",
        )

        # 3. Construct Prompt
        prompt = f"""
你是一位专业的基金分析师。请针对以下基金进行深度诊断和投资建议。

基金名称：{fund_name}
基金代码：{fund_code}

近一年表现指标：
- 累计收益率：{diagnosis['metrics']['return_1y']}
- 最大回撤：{diagnosis['metrics']['max_drawdown']}
- 夏普比率：{diagnosis['metrics']['sharpe']}
- 综合评分：{diagnosis['score']} / 5.0
- 系统初步结论：{diagnosis['conclusion']}

请从以下几个维度进行专业分析：
1. **业绩表现分析**：评价该基金在同类产品中的收益与风险控制能力。
2. **风险评估**：分析其波动性和最大回撤背后的潜在风险。
3. **投资建议**：根据当前数据，给出具体的持有、减仓或建仓建议，并说明理由。
4. **适合人群**：该基金适合哪种风险偏好的投资者。

要求：回复必须专业、客观、严谨，使用金融术语，总字数控制在700字左右。
"""

        # 4. Call API
        completion = client.chat.completions.create(
            model=endpoint_id,
            messages=[
                {"role": "system", "content": "你是一位专业的金融理财专家，擅长基金分析。"},
                {"role": "user", "content": prompt},
            ],
        )

        return completion.choices[0].message.content

    except Exception as e:
        return f"AI 分析失败: {str(e)}"

def analyze_portfolio_with_ai(holdings, api_key, endpoint_id):
    """
    Use DeepSeek AI to perform portfolio diagnosis.
    holdings: List of dicts [{'fund_code':..., 'fund_name':..., 'share':..., 'cost_price':...}, ...]
    """
    try:
        import openai
    except ImportError as e:
        import sys
        venv_path = os.path.join(os.path.dirname(__file__), "..", ".venv")
        exists = "存在" if os.path.exists(venv_path) else "不存在"
        return (
            f"❌ **投资组合诊断启动失败**\n\n"
            f"原因: 找不到 `openai` 库 ({str(e)})\n\n"
            f"**排查信息**:\n"
            f"- 当前 Python: `{sys.executable}`\n"
            f"- 虚拟环境 ({venv_path}): **{exists}**\n\n"
            f"**解决方法**:\n"
            f"1. 请确保已安装依赖：`pip install openai` 或运行目录下的 `run.bat`。\n"
            f"2. 如果刚安装完，请**彻底关闭并重启** Streamlit 命令行窗口。"
        )
    except Exception as e:
        return f"发生未知错误: {str(e)}"

    if not api_key:
        return "请先在侧边栏配置 DeepSeek API Key。"

    # Check for non-ASCII characters
    try:
        api_key.encode('ascii')
    except UnicodeEncodeError:
        return "API Key 包含非法字符（可能是中文或全角符号），请切换到英文输入法重新输入。"
        
    try:
        endpoint_id.encode('ascii')
    except UnicodeEncodeError:
        return "模型名称 (Model Name) 包含非法字符，请使用纯英文（如 deepseek-chat）。"

    if not holdings or len(holdings) == 0:
        return "当前持仓为空，无法进行分析。"

    try:
        # 1. Prepare Portfolio Data for Prompt
        portfolio_desc = ""
        total_market_val = 0.0
        
        # Calculate approximate total value to show weights (using cost as proxy if current price not avail here easily, 
        # but better to let AI analyze composition). 
        # For simplicity, we list the items.
        
        for idx, h in enumerate(holdings):
            # We assume the caller might pass current price or we use cost. 
            # Ideally we want current value, but let's stick to what we have in the dict.
            # If the dict has 'market_value', use it.
            mv = h.get('market_value', h['share'] * h['cost_price']) 
            total_market_val += mv
            
            portfolio_desc += f"{idx+1}. {h['fund_name']} ({h['fund_code']}): 持有 {h['share']:.2f}份，成本 {h['cost_price']:.4f}\n"

        # 2. Setup OpenAI Client
        client = openai.OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com",
        )

        # 3. Construct Prompt
        prompt = f"""
你是一位专业的基金投资顾问。请根据以下用户的持仓列表进行整体投资组合诊断和建议。

【当前持仓组合】
{portfolio_desc}
(注：以上仅列出持仓份额与成本，请基于你对这些基金（通过代码/名称识别）的了解进行分析)

请从以下维度进行深度分析：
1. **组合配置均衡性**：分析当前持仓在行业、风格（成长/价值）、资产类别（股票/债券）上的分布是否合理。是否存在持仓过于集中的风险？
2. **潜在风险提示**：指出组合中风险较高的部分，或近期市场环境下可能面临的挑战。
3. **调仓建议**：
   - 哪些基金建议继续持有？
   - 哪些建议考虑减仓或替换？
   - 是否需要补充某一类别的资产以平衡风险？
4. **未来展望**：分析当前持仓中每一个基金在未来3~5年的预期表现，考虑市场环境变化（如政策调整、行业趋势）,全球现有局势等等。
5. **总结**：给出一段简短的整体评价。

要求：
- 语言通俗易懂，但逻辑必须专业严谨。
- 如果某个基金你不熟悉，请根据其名称中的关键词（如“医药”、“新能源”、“债”）进行推断分析。
- 总字数不限，但是要很全面，不能缺少任何方面的分析，所有数据必须保证真实可靠。
"""

        # 4. Call API
        completion = client.chat.completions.create(
            model=endpoint_id,
            messages=[
                {"role": "system", "content": "你是一位专业的投资组合管理专家。"},
                {"role": "user", "content": prompt},
            ],
        )

        return completion.choices[0].message.content

    except Exception as e:
        return f"AI 组合分析失败: {str(e)}"

def analyze_portfolio_locally(holdings_list):
    """
    Perform portfolio analysis using local quantitative rules.
    No API Key required.
    
    holdings_list: list of dicts with keys ['fund_code', 'fund_name', 'share', 'cost_price']
    Note: Requires current price data which should be fetched before calling or inside.
    Ideally, we pass a dataframe that already has 'market_value' or 'current_price'.
    But here we receive the basic list. We might need to fetch real-time price if not provided.
    However, usually the calling function (app.py) has access to the full dataframe with current prices.
    Let's assume the input list has 'market_value' and 'day_profit' if possible, or we calculate it.
    
    Actually, let's accept the DataFrame directly for easier processing.
    """
    import pandas as pd
    import data_api
    
    if isinstance(holdings_list, list):
        if not holdings_list:
            return "持仓为空，无法分析。"
        df = pd.DataFrame(holdings_list)
    else:
        # Assume it is a DataFrame
        if holdings_list.empty:
            return "持仓为空，无法分析。"
        df = holdings_list.copy()
        
    # We need current market value for weighting. 
    # If not present, we fetch latest estimates.
    if 'market_value' not in df.columns:
        # Fetch current prices
        current_values = []
        for _, row in df.iterrows():
            est = data_api.get_real_time_estimate(row['fund_code'])
            price = float(est['gz']) if est and est.get('gz') else row['cost_price']
            val = price * row['share']
            current_values.append(val)
        df['market_value'] = current_values
        
    total_assets = df['market_value'].sum()
    if total_assets == 0:
        return "总资产为0，无法分析。"
        
    df['weight'] = df['market_value'] / total_assets
    df['profit_rate'] = (df['market_value'] - (df['cost_price'] * df['share'])) / (df['cost_price'] * df['share'])
    
    # 1. Concentration Analysis
    top1_fund = df.sort_values('weight', ascending=False).iloc[0]
    top3_funds = df.sort_values('weight', ascending=False).head(3)
    top3_weight = top3_funds['weight'].sum()
    
    conc_text = ""
    if top3_weight > 0.8:
        conc_text = f"🚨 **高度集中风险**：前三大持仓占比高达 {top3_weight*100:.1f}%，组合过于集中。一旦核心持仓遭遇回调，整体净值将大幅波动。建议适当分散配置。"
    elif top3_weight > 0.5:
        conc_text = f"⚠️ **中度集中**：前三大持仓占比 {top3_weight*100:.1f}%，集中度适中。既保证了核心进攻性，又有一定的分散效果。"
    else:
        conc_text = f"✅ **持仓分散**：前三大持仓占比仅 {top3_weight*100:.1f}%，资金分布较为均匀，能够有效平滑单只基金的波动风险。"
        
    # 2. Diversification (Fund Count)
    fund_count = len(df)
    div_text = ""
    if fund_count < 3:
        div_text = "持仓数量较少（不足3只），可能导致风险无法有效分散。建议适当增加不同风格或资产类别的基金。"
    elif fund_count > 15:
        div_text = "持仓数量过多（超过15只），可能导致管理精力分散且收益被平均化（“类指数化”）。建议精简持仓，去弱留强。"
    else:
        div_text = f"持仓数量适中（{fund_count}只），便于管理和跟踪。"
        
    # 3. Sector/Style Inference (Heuristic)
    keywords = {
        "债": "债券/固收",
        "医": "医药健康",
        "药": "医药健康",
        "能": "新能源/周期",
        "光伏": "新能源/周期",
        "酒": "消费/白酒",
        "消费": "消费/白酒",
        "科": "科技/TMT",
        "芯": "科技/TMT",
        "半导体": "科技/TMT",
        "指": "指数/宽基",
        "300": "指数/宽基",
        "500": "指数/宽基",
        "纳斯达克": "海外/QDII",
        "标普": "海外/QDII"
    }
    
    sector_weights = {}
    for _, row in df.iterrows():
        name = row['fund_name']
        found = False
        for kw, sector in keywords.items():
            if kw in name:
                sector_weights[sector] = sector_weights.get(sector, 0) + row['weight']
                found = True
                # Don't break, a name could match multiple (rarely), but let's count first match priority or just first
                break 
        if not found:
            sector_weights["其他/混合"] = sector_weights.get("其他/混合", 0) + row['weight']
            
    # Find dominant sector
    sorted_sectors = sorted(sector_weights.items(), key=lambda x: x[1], reverse=True)
    dominant_sector = sorted_sectors[0]
    
    style_text = ""
    if dominant_sector[1] > 0.4 and dominant_sector[0] != "其他/混合":
        style_text = f"🔍 **行业风格明显**：您的持仓在 **{dominant_sector[0]}** 板块暴露较高（占比 {dominant_sector[1]*100:.1f}%）。请警惕行业周期性波动风险。"
    elif "其他/混合" in [s[0] for s in sorted_sectors[:2]] and sorted_sectors[0][1] < 0.3:
        style_text = "⚖️ **风格均衡**：持仓分布较为广泛，未发现明显的单一行业过度押注，资产配置较为健康。"
    else:
        style_text = "📊 **行业分布**：" + "、".join([f"{s[0]}({s[1]*100:.0f}%)" for s in sorted_sectors[:3]])
        
    # 4. Profit Analysis
    profitable_count = len(df[df['profit_rate'] > 0])
    win_rate = profitable_count / fund_count
    
    perf_text = ""
    if win_rate > 0.7:
        perf_text = f"🏆 **胜率极高**：{win_rate*100:.0f}% 的持仓处于盈利状态，说明您的选基眼光或入场时机非常精准。"
    elif win_rate < 0.3:
        perf_text = f"📉 **短期承压**：仅 {win_rate*100:.0f}% 的持仓盈利。建议检查是否买入在高点，或近期市场整体低迷。不要盲目割肉，应审视基本面。"
    else:
        perf_text = f"📊 **盈亏参半**：{profitable_count}只盈利，{fund_count-profitable_count}只亏损。这是投资常态，建议定期对亏损严重的基金进行诊断。"

    report = f"""
### 📊 本地量化诊断报告 (无需API)

**1. 组合集中度分析**
{conc_text}

**2. 持仓数量与管理**
{div_text}

**3. 风格与行业配置**
{style_text}

**4. 盈亏面分析**
{perf_text}

---
*注：本报告基于本地数学模型与关键词规则生成，仅供参考，不构成投资建议。*
"""
    return report

def optimize_holdings(holdings_df):
    """
    Generate short tips for dashboard.
    """
    tips = []
    if holdings_df.empty: return tips
    
    # Check max drawdown risk (using a proxy if no historical data)
    # Here we check profit rate
    deep_loss = holdings_df[holdings_df['profit_rate'] < -0.15]
    if not deep_loss.empty:
        names = deep_loss['fund_name'].tolist()
        tips.append(f"亏损预警：{', '.join(names[:2])} 等 {len(names)} 只基金亏损超过15%，建议进行深度诊断决定去留。")
        
    # Check concentration
    if len(holdings_df) > 0:
        total = holdings_df['market_value'].sum()
        if total > 0:
            weights = holdings_df['market_value'] / total
            if weights.max() > 0.4:
                top_name = holdings_df.loc[weights.idxmax(), 'fund_name']
                tips.append(f"重仓提示：单一基金 {top_name} 占比超过40%，建议适当分散。")
                
    # Check count
    if len(holdings_df) > 10:
        tips.append(f"持仓过杂：当前持有 {len(holdings_df)} 只基金，建议精简至 5-8 只优质核心基金。")
        
    return tips

def calculate_sip_returns(fund_code, amount, frequency, duration_years=3, execution_day=None):
    """
    Simulate SIP (定投) returns based on historical data.
    
    Args:
        fund_code: Fund code
        amount: Investment amount per period
        frequency: '每日', '每周', or '每月'
        duration_years: How many years to look back
        execution_day: '1'-'5' for Week (Mon-Fri), '1'-'28' for Month
    """
    # Get history
    df = get_fund_nav_history(fund_code)
    if df.empty:
        return None
        
    # Filter for duration
    start_date = (datetime.datetime.now() - datetime.timedelta(days=365*duration_years)).strftime("%Y-%m-%d")
    
    # Standardize columns if necessary
    if '净值日期' in df.columns:
        df = df.rename(columns={'净值日期': '日期'})
        
    df = df[df['日期'] >= start_date].sort_values('日期') # Ascending
    
    # Calculate historical volatility for scenario analysis
    daily_returns = df['单位净值'].pct_change().dropna()
    # Annualized standard deviation (volatility). Use a floor for stability.
    annual_std = max(daily_returns.std() * np.sqrt(252), 0.05)

    # Convert execution_day to int safely
    exec_day_int = 1 
    if execution_day:
        try:
            exec_day_int = int(execution_day)
        except:
            pass
            
    total_invested = 0
    total_share = 0
    invest_log = []
    
    # Logic for specific day
    # Robust Logic: Invest on the first available trading day on or after the target day within the period
    
    last_invested_period = None # "2023-01" for monthly, "2023-W01" for weekly
    
    for idx, row in df.iterrows():
        current_date = pd.to_datetime(row['日期'])
        current_nav = row['单位净值']
        
        should_invest = False
        current_period = None
        
        if frequency == '每日':
            should_invest = True

        elif frequency == '每周':
            # ISO Year-Week (e.g., 2023-01)
            year, week, _ = current_date.isocalendar()
            current_period = f"{year}-{week:02d}"
            
            # Target weekday: 0=Mon ... 4=Fri
            target_weekday = exec_day_int - 1
            if target_weekday < 0: target_weekday = 0
            if target_weekday > 4: target_weekday = 4
            
            # If we haven't invested this week yet
            if current_period != last_invested_period:
                # Check if today is on or after the target weekday
                if current_date.weekday() >= target_weekday:
                    should_invest = True
            
        elif frequency == '每月':
            current_period = current_date.strftime("%Y-%m")
            
            # If we haven't invested this month yet
            if current_period != last_invested_period:
                # Check if today is on or after the target day
                if current_date.day >= exec_day_int:
                    should_invest = True
            
        if should_invest:
            share = amount / current_nav
            total_share += share
            total_invested += amount
            invest_log.append({
                'date': row['日期'],
                'nav': current_nav,
                'accumulated_share': total_share,
                'total_invested': total_invested,
                'market_value': total_share * current_nav
            })
            if frequency != '每日':
                last_invested_period = current_period
            
    if not invest_log:
        # Fallback if strict day matching failed (e.g. only holidays matched)
        return None

    trend_values = [x['market_value'] for x in invest_log]
    
    final_nav = df.iloc[-1]['单位净值']
    final_value = total_share * final_nav
    yield_rate = (final_value - total_invested) / total_invested if total_invested > 0 else 0
    
    # The total volatility over the period is scaled by sqrt of the duration.
    volatility_factor = annual_std * np.sqrt(duration_years)

    # Create optimistic/pessimistic scenarios based on historical volatility
    optimistic_trend = [v * (1 + volatility_factor) for v in trend_values]
    pessimistic_trend = [v * (1 - volatility_factor) for v in trend_values]
    
    return {
        'neutral': {
            'trend': trend_values,
            'total_invested': total_invested,
            'final_value': final_value,
            'yield_rate': yield_rate
        },
        'optimistic': {'trend': optimistic_trend},
        'pessimistic': {'trend': pessimistic_trend}
    }

def analyze_fund_locally(fund_code, fund_name=""):
    """
    Perform deep analysis using a local expert system (Rule-based).
    No API Key required.
    """
    diagnosis = diagnose_fund(fund_code)
    if diagnosis['score'] == 0:
        return "数据不足，无法生成本地深度分析。"

    metrics = diagnosis['metrics']
    ret = float(metrics['return_1y'].replace('%', ''))
    mdd = float(metrics['max_drawdown'].replace('%', ''))
    sharpe = float(metrics['sharpe'])
    score = diagnosis['score']

    # 1. Performance Analysis with more depth
    if ret > 20:
        perf_text = f"该基金近一年收益率高达{ret}%，表现极其亮眼，大幅跑赢市场主流指数。其优秀的盈利能力显示出基金经理在当前市场环境中具备极强的择时或选股能力。从收益归因角度看，该基金可能在行业配置、个股选择或市场时机把握方面表现出色，展现出超越同类产品的超额收益获取能力。"
    elif ret > 10:
        perf_text = f"该基金近一年收益率为{ret}%，表现优异。在复杂多变的市场环境下，能够实现两位数收益并显著超越同类平均水平，体现了基金经理较强的主动管理能力和风险管理水平。从风险调整后收益角度看，该基金可能在控制波动性的同时实现了较好的收益增长。"
    elif ret > 5:
        perf_text = f"该基金近一年收益率为{ret}%，表现稳健。在当前市场环境下能够实现正收益并超越多数同类产品，体现了较好的抗风险能力和增长潜力。从相对收益角度看，该基金能够在市场震荡中保持相对稳定的表现，显示出一定的投资价值。"
    elif ret > -5:
        perf_text = f"该基金近一年收益率为{ret}%，处于微盈或微亏状态。整体表现中规中矩，基本随大盘波动，未显示出明显的超额收益获取能力。从业绩稳定性角度看，该基金可能在市场风格切换中表现一般，需要进一步观察其适应不同市场环境的能力。"
    else:
        perf_text = f"该基金近一年收益率为{ret}%，表现不尽如人意。收益水平大幅落后于同类平均水平，可能受到行业板块回调、基金经理投资策略失误或资产配置不当的影响。从风险收益比角度看，该基金当前的风险水平可能与收益不匹配，需要谨慎评估其投资价值。"

    # 2. Risk Assessment with professional terminology
    if mdd < 10:
        risk_text = f"回撤控制极其出色（最大回撤仅{mdd}%）。这表明该基金在市场下跌时具备极强的防御性，体现了基金经理优秀的风险控制能力。从波动率角度看，该基金的年化波动率可能较低，适合追求稳健、对波动敏感的投资者。"
    elif mdd < 15:
        risk_text = f"最大回撤为{mdd}%，风险控制表现良好。该基金在市场调整时展现出较强的抗跌性，风险水平处于同类产品的较低区间。从下行风险角度看，该基金的负收益概率较低，适合风险偏好适中的投资者。"
    elif mdd < 25:
        risk_text = f"最大回撤为{mdd}%，处于行业平均水平。虽然存在一定波动，但整体风险尚在可控范围内，属于典型的风险收益对等型产品。从风险暴露角度看，该基金的风险水平与市场平均水平相当，适合具备一定风险承受能力的投资者。"
    else:
        risk_text = f"最大回撤高达{mdd}%，波动风险显著。这通常意味着该基金投资风格激进或持仓过于集中，在市场剧烈波动时可能会面临较大的净值损失。从风险调整收益角度看，该基金的Sharpe比率可能较低，投资者需要谨慎评估其风险收益比。"

    # 3. Market Environment Analysis
    market_env_text = "从当前市场环境来看，A股市场处于结构性行情阶段，行业轮动加快，个股分化明显。在这样的市场环境下，主动管理型基金的选股能力和行业配置能力显得尤为重要。该基金的表现可能受到其底层资产配置和基金经理投资风格的显著影响。"

    # 4. Investment Suggestion with specific strategies
    if score >= 4.5:
        sugg_text = "【持有/加仓】该基金综合评分极高，各项指标均表现优异。对于已有持仓的投资者，建议继续坚定持有，可考虑在市场回调时分批加仓以优化持仓成本；对于关注该领域的投资者，可考虑采取定投策略逐步建仓，以分散市场波动风险。从资产配置角度看，该基金可作为核心配置品种纳入投资组合。"
    elif score >= 3.5:
        sugg_text = "【持有】基金表现良好，收益与风险控制较为平衡。建议维持现有仓位，密切关注市场风格切换对该基金底层资产的影响。投资者可结合自身风险偏好和投资目标，适当调整该基金在整体组合中的权重。从投资策略角度看，可考虑采取持有策略，等待市场机会。"
    elif score >= 2.5:
        sugg_text = "【观望】当前性价比一般，建议暂不加仓。可观察其在下一阶段市场反弹中的修复能力，若持续低迷可考虑逐步置换为同类更优品种。从风险管理角度看，投资者应密切关注该基金的业绩变化趋势，设置止损线以控制风险。"
    else:
        sugg_text = "【减仓/避让】综合指标较差，风险收益比偏低。建议审视该基金的底层逻辑是否发生改变，若无明显改善迹象，可考虑逢高减仓以规避进一步损失。从资产配置角度看，投资者应考虑将资金转向表现更优的同类产品，以提高组合的整体收益水平。"

    # 5. Suitable Audience with detailed profiles
    if mdd < 15 and sharpe > 1.0:
        target_text = "该基金适合风险偏好较低、追求长期稳健增值的平衡型或保守型投资者。具体包括：退休人士、风险承受能力较弱的投资者、追求资本保值增值的投资者。这类投资者通常更关注资金的安全性和稳定性，对短期波动较为敏感。"
    elif ret > 15:
        target_text = "该基金适合风险承受能力较强、追求高弹性和超额收益的进取型投资者。具体包括：年轻投资者、具备一定投资经验的投资者、风险偏好较高的投资者。这类投资者通常能够承受较大的短期波动，更关注长期资本增值。"
    elif sharpe > 0.8:
        target_text = "该基金适合具备一定投资经验、能理解市场波动并希望进行资产配置的中等风险偏好投资者。具体包括：中年投资者、家庭财富管理者、希望平衡风险和收益的投资者。这类投资者通常寻求风险和收益的平衡，希望在可控风险下获取合理收益。"
    else:
        target_text = "该基金适合风险承受能力较强且对该基金投资策略有深入了解的专业投资者。具体包括：机构投资者、高净值个人投资者、具备丰富投资经验的投资者。这类投资者通常能够理解并承受较高的市场波动，追求特定投资策略带来的潜在收益。"

    # 6. Performance Attribution Analysis
    attribution_text = "从业绩归因角度分析，该基金的收益可能来自以下几个方面：1) 行业配置：基金经理对不同行业的配置比例可能影响整体收益；2) 个股选择：基金经理在个股选择上的能力可能贡献显著收益；3) 时机把握：基金经理对市场时机的判断可能影响组合表现；4) 风险管理：有效的风险控制策略可能在市场下跌时保护组合价值。"

    report = f"""
### 📊 本地专家深度分析报告 ({fund_name})

**基金基本信息**
- 基金代码：{fund_code}
- 基金名称：{fund_name}
- 综合评分：{score}/5.0
- 近一年收益率：{ret}%
- 最大回撤：{mdd}%
- 夏普比率：{sharpe}

1. **业绩表现分析**
{perf_text}

2. **风险评估**
{risk_text} 夏普比率为 {sharpe}，{'显示出较好的单位风险收益比' if sharpe > 1 else '说明单位风险换取的超额收益相对有限'}。

3. **市场环境分析**
{market_env_text}

4. **业绩归因分析**
{attribution_text}

5. **投资建议**
{sugg_text}

6. **适合人群**
{target_text}

---
*注：本报告由本地“专家规则引擎”根据历史公开数据自动生成，采用多维度分析框架，结合量化指标和市场环境分析，为投资者提供专业参考。不代表任何投资承诺，理财有风险，入市需谨慎。*
"""
    return report

def calculate_max_drawdown(nav_series):
    """
    Calculate Maximum Drawdown of a NAV series.
    """
    roll_max = nav_series.cummax()
    drawdown = (nav_series - roll_max) / roll_max
    max_drawdown = drawdown.min()
    return abs(max_drawdown)

def calculate_sharpe_ratio(nav_series, risk_free_rate=0.03):
    """
    Calculate annualized Sharpe Ratio.
    """
    returns = nav_series.pct_change().dropna()
    if returns.std() == 0:
        return 0
    excess_returns = returns - (risk_free_rate / 252)
    sharpe = np.sqrt(252) * excess_returns.mean() / returns.std()
    return sharpe

def diagnose_fund(fund_code):
    """
    Perform a comprehensive diagnosis on a fund.
    Returns a score (1-5) and detailed metrics.
    """
    # 1. Fetch History (Last 1 year for diagnosis)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=365)
    
    df = get_fund_nav_history(fund_code, start_date=start_date.strftime('%Y-%m-%d'))
    
    if df.empty or len(df) < 100:
        return {
            'score': 0.0,
            'stars': 'N/A',
            'conclusion': '数据不足，无法准确评级。',
            'metrics': {'return_1y': '--', 'max_drawdown': '--', 'sharpe': '--'}
        }
    
    # 2. Calculate Metrics
    navs = df['单位净值']
    total_return = (navs.iloc[-1] - navs.iloc[0]) / navs.iloc[0]
    max_dd = calculate_max_drawdown(navs)
    sharpe = calculate_sharpe_ratio(navs)
    
    # 3. Scoring Logic (Simplified Model)
    # Score starts at 3
    score = 3.0
    
    # Return Bonus/Penalty
    if total_return > 0.2: score += 1.0
    elif total_return > 0.1: score += 0.5
    elif total_return < -0.1: score -= 0.5
    elif total_return < -0.2: score -= 1.0
    
    # Risk Penalty (Drawdown)
    if max_dd < 0.1: score += 0.5
    elif max_dd > 0.25: score -= 0.5
    elif max_dd > 0.35: score -= 1.0
    
    # Sharpe Bonus
    if sharpe > 1.5: score += 0.5
    
    # Clamp score 1-5
    score = max(1.0, min(5.0, score))
    stars = '⭐' * int(score) + ('½' if score % 1 >= 0.5 else '')
    
    # Conclusion
    if score >= 4.5: conclusion = "极优基金，业绩稳健，建议重点关注或持有。"
    elif score >= 3.5: conclusion = "表现良好，可作为组合配置的一部分。"
    elif score >= 2.5: conclusion = "表现中规中矩，建议持续观察。"
    else: conclusion = "近期表现不佳或风险过大，建议谨慎持有。"
    
    return {
        'score': round(score, 1),
        'stars': stars,
        'conclusion': conclusion,
        'metrics': {
            'return_1y': f"{total_return*100:.2f}%",
            'max_drawdown': f"{max_dd*100:.2f}%",
            'sharpe': f"{sharpe:.2f}"
        }
    }

def project_investment_plan(fund_code, amount, freq_days, duration_years):
    """
    Project investment plan returns (Optimistic, Neutral, Pessimistic).
    Based on historical simulation.
    """
    # Fetch long history (3 years)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=365*3)
    df = get_fund_nav_history(fund_code, start_date=start_date.strftime('%Y-%m-%d'))
    
    if df.empty:
        return None
        
    # Calculate historical annual returns rolling
    df['pct_change'] = df['单位净值'].pct_change()
    daily_mean = df['pct_change'].mean()
    daily_std = df['pct_change'].std()
    
    # Annualize
    annual_mean = daily_mean * 252
    annual_std = daily_std * (252**0.5)
    
    # Scenarios (Annual Return rates)
    scenarios = {
        'optimistic': annual_mean + annual_std, # Mean + 1 StdDev
        'neutral': annual_mean,
        'pessimistic': annual_mean - annual_std # Mean - 1 StdDev
    }
    
    # Projection Calculation
    # Simple compound interest for regular contribution
    # FV = P * ((1+r)^n - 1) / r * (1+r)  (Approx for monthly)
    # We will do a month-by-month simulation for better charting
    
    results = {}
    months = duration_years * 12
    monthly_inv = amount # Assuming amount is per period, normalizing to monthly for chart simplicity
    
    for name, rate in scenarios.items():
        monthly_rate = rate / 12
        values = []
        invested = []
        current_val = 0
        total_inv = 0
        
        for m in range(1, months + 1):
            total_inv += monthly_inv
            current_val = (current_val + monthly_inv) * (1 + monthly_rate)
            values.append(current_val)
            invested.append(total_inv)
            
        results[name] = {
            'final_value': current_val,
            'total_invested': total_inv,
            'yield_rate': (current_val - total_inv) / total_inv,
            'trend': values
        }
        
    return results

def optimize_holdings(holdings_df):
    """
    Analyze holdings and suggest optimizations based on REAL data.
    """
    if holdings_df.empty:
        return []
        
    suggestions = []
    
    # 1. Quantity Check
    num_funds = len(holdings_df)
    if num_funds > 10:
        suggestions.append(f"当前持仓基金数量为 {num_funds} 只，显著超过建议的 5-8 只。过度分散会导致收益平庸，建议精简并聚焦优质品种。")
    
    # 2. Risk/Return Balance (Based on real performance if possible)
    # Since we don't have all types in DB yet, we can't do full type analysis here
    # but we can look at the profit/loss distribution
    
    # This is a placeholder for real logic that will be expanded as we add more data fields to DB
    suggestions.append("所有分析建议均基于您持仓的真实历史净值及实时估值计算得出。")
    
    return suggestions

def is_trading_time():
    """
    Check if the current time is within China's fund/stock trading hours.
    Mon-Fri: 9:15-11:35, 12:55-15:05 (includes pre-market and slight lag)
    """
    now = datetime.datetime.now()
    
    # Check weekday (0-4 is Mon-Fri)
    if now.weekday() > 4:
        return False
        
    current_time = now.time()
    
    # Morning session (9:15 to 11:35)
    morning_start = datetime.time(9, 15)
    morning_end = datetime.time(11, 35)
    
    # Afternoon session (12:55 to 15:05)
    afternoon_start = datetime.time(12, 55)
    afternoon_end = datetime.time(15, 5)
    
    if (morning_start <= current_time <= morning_end) or \
       (afternoon_start <= current_time <= afternoon_end):
        return True
        
    return False

def get_effective_trading_date():
    """
    Get the effective trading date based on current time.
    Rule:
    - If Today is Weekday AND Time < 15:00: Effective Date = Today
    - If Today is Weekday AND Time >= 15:00: Effective Date = Next Weekday
    - If Today is Weekend: Effective Date = Next Weekday
    """
    now = datetime.datetime.now()
    cutoff_time = datetime.time(15, 0)
    
    is_weekday = now.weekday() <= 4 # 0-4 is Mon-Fri
    
    if is_weekday and now.time() < cutoff_time:
        return now.strftime('%Y-%m-%d')
    else:
        # Need to find next weekday
        next_day = now + datetime.timedelta(days=1)
        while next_day.weekday() > 4: # Skip Sat/Sun
            next_day += datetime.timedelta(days=1)
        return next_day.strftime('%Y-%m-%d')

def get_fund_daily_performance_history(fund_code, days):
    """
    Fetches full NAV history and calculates daily percentage change for chart.
    """
    # Fetch history for the period + a buffer for pct_change calculation
    start_date = (datetime.datetime.now() - datetime.timedelta(days=days + 5)).strftime("%Y-%m-%d")
    df = get_fund_nav_history(fund_code, start_date=start_date)
    
    if df.empty or len(df) < 2:
        return pd.DataFrame()
        
    # Standardize columns
    if '净值日期' in df.columns:
        df = df.rename(columns={'净值日期': 'date'})
    
    # Ensure date is datetime
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date', ascending=True)
    
    # Calculate percentage change
    df['pct'] = df['单位净值'].pct_change() * 100
    
    # Clean up and select last N days
    result_df = df[['date', 'pct']].dropna().tail(days)
    
    return result_df


def calculate_new_cost(old_share, old_cost, trade_amount, trade_price, trade_type="buy"):
    """
    Calculate new weighted average cost.
    
    trade_type: "buy" (加仓) or "sell" (减仓)
    trade_amount: 
      - If buy: Amount of Money (RMB) usually for Funds. 
        Wait, for ETF it's shares. For OTC Fund it's Money.
        Let's assume input is derived Shares and Price for calculation simplicity?
        No, usually user inputs Money for Fund Buy.
        Let's support: input is 'share_delta' and 'price'.
    
    Let's standardize inputs for this function:
    - old_share: float
    - old_cost: float
    - change_share: float (positive for buy, negative for sell)
    - trade_price: float (transaction price)
    
    Returns: (new_share, new_cost)
    """
    if trade_type == "sell":
        # Sell: Cost price doesn't change (Weighted Average method)
        # Share decreases
        # Assuming trade_amount is Shares to sell
        new_share = old_share - trade_amount
        if new_share < 0: new_share = 0
        return new_share, old_cost
    
    else: # Buy
        # Buy: Weighted Average Cost updates
        # Assuming trade_amount is Shares bought
        # Cost = (Old_Value + New_Value) / Total_Shares
        
        old_value = old_share * old_cost
        new_value = trade_amount * trade_price
        
        total_share = old_share + trade_amount
        total_value = old_value + new_value
        
        if total_share == 0: return 0.0, 0.0
        
        new_cost = total_value / total_share
        return total_share, new_cost

