import schedule
import time
import threading
from database import execute_investment_plans
from logic import is_market_open
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('定投自动化')

def run_scheduled_jobs():
    """Run scheduled jobs in a separate thread"""
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

def setup_schedule():
    """Setup定投自动执行的定时任务"""
    # 每天交易时间执行定投检查
    # 上午交易开始后检查
    schedule.every().day.at("09:30").do(check_and_execute_plans)
    # 下午交易开始后检查
    schedule.every().day.at("13:00").do(check_and_execute_plans)
    # 交易结束前再次检查
    schedule.every().day.at("14:30").do(check_and_execute_plans)
    
    logger.info("定投自动化任务已设置")
    
    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduled_jobs, daemon=True)
    scheduler_thread.start()
    logger.info("定投自动化任务线程已启动")

def check_and_execute_plans():
    """检查并执行到期的定投计划"""
    try:
        logger.info("开始检查到期定投计划...")
        
        # 检查市场是否开放
        if not is_market_open():
            logger.info("市场未开放，跳过定投执行")
            return
        
        # 执行定投计划
        executed = execute_investment_plans()
        
        if executed:
            logger.info(f"成功执行了 {len(executed)} 个定投计划")
            for plan in executed:
                logger.info(f"  - {plan['fund_name']} ({plan['fund_code']}): 定投 {plan['amount']}元, 购买 {plan['shares']:.2f}份, 净值 {plan['nav']:.4f}")
        else:
            logger.info("当前没有需要执行的定投计划")
            
    except Exception as e:
        logger.error(f"执行定投计划时出错: {str(e)}")

if __name__ == "__main__":
    setup_schedule()
    while True:
        time.sleep(60)