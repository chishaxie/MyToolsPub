import baostock as bs
import pandas as pd
import os
import datetime
from typing import List

from bt.bs_connection import login_baostock, check_baostock_login

# -------------------------- 配置常量（可按需修改） --------------------------
START_DATE = "2014-01-01"  # 交易日起始日期
CACHE_FILE = "./data/TDays.csv"  # 缓存文件路径
HOUR_BOUNDARY = 18  # 当日是否计入的时间边界（18点）
DATE_FORMAT = "%Y-%m-%d"  # 日期统一格式

# -------------------------- 内部工具函数（外部无需调用） --------------------------
def _need_update_cache() -> bool:
    """
    核心判断：是否需要更新缓存文件
    判断逻辑：
    1. 缓存目录/文件不存在 → 需要更新
    2. 当前时间已过今日18点，且缓存文件更新时间在今日18点前 → 需要更新
    3. 其他情况（未过18点/文件已在今日18点后更新）→ 无需更新
    """
    # 确保缓存目录存在，不存在则创建并标记需要更新
    cache_dir = os.path.dirname(CACHE_FILE)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)
        return True
    
    # 缓存文件不存在 → 需要更新
    if not os.path.exists(CACHE_FILE):
        return True
    
    # 获取当前时间、文件更新时间、今日18点边界时间
    now = datetime.datetime.now()
    file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
    today_18 = datetime.datetime(now.year, now.month, now.day, HOUR_BOUNDARY)
    
    # 核心更新判断：已过18点 且 文件是18点前更新的
    if now > today_18 and file_mtime < today_18:
        return True
    return False

def _fetch_baostock_data() -> List[str]:
    """
    从baostock获取原始交易日数据（仅更新缓存时调用，含login/logout）
    最终适配：你的baostock实际返回英文字段 calendar_date / is_trading_day
    返回：2014-01-01至今的交易日列表（字符串格式，按时间升序）
    """
    # 登录baostock，判断登录结果
    login_baostock()
    check_baostock_login()
    print("✅ baostock登录成功，开始查询交易日历...")
    
    # 调用接口查询交易日历，end_date为空则默认查至当前
    rs = bs.query_trade_dates(start_date=START_DATE, end_date="")
    if rs.error_code != "0":
        # bs.logout()  # 失败也要登出
        raise ValueError(f"查询交易日历失败：{rs.error_msg}")
    
    # 转换为DataFrame，打印列名（调试用，已确认你的字段是英文）
    trade_date_df = rs.get_data()
    print(f"📌 baostock返回字段：{trade_date_df.columns.tolist()}")
    print(f"📌 数据前3行：\n{trade_date_df.head(3)}")
    
    # 🔥 核心修复：适配你的实际英文字段 🔥
    trading_days_df = trade_date_df[trade_date_df["is_trading_day"] == "1"]  # 筛选交易日
    trading_days = trading_days_df["calendar_date"].sort_values().tolist()   # 提取日期列表
    
    # 登出baostock（必须执行，避免连接占用）
    # bs.logout()
    print(f"✅ baostock查询完成，共获取{len(trading_days)}个交易日")
    return trading_days

def _save_cache(trading_days: List[str]) -> None:
    """将交易日列表保存到缓存文件"""
    pd.DataFrame({"trading_day": trading_days}).to_csv(CACHE_FILE, index=False, encoding="utf-8")
    print(f"✅ 交易日数据已缓存至：{CACHE_FILE}")

def _load_cache() -> List[str]:
    """从缓存文件读取交易日列表"""
    df = pd.read_csv(CACHE_FILE, encoding="utf-8")
    print(f"✅ 从缓存读取交易日数据，共{len(df)}条")
    return df["trading_day"].tolist()

# -------------------------- 外部暴露的核心接口 --------------------------
def get_trading_days() -> List[str]:
    """
    获取2014-01-01至今的全部交易日（按时间升序）
    核心规则：当天18点前，即使是交易日也排除；18点后则包含
    """
    # 智能判断是否更新缓存，无需更新则直接读缓存（不调用baostock）
    if _need_update_cache():
        trading_days = _fetch_baostock_data()
        _save_cache(trading_days)
    else:
        trading_days = _load_cache()
    
    # 动态判断是否排除今日（核心时间边界逻辑）
    now = datetime.datetime.now()
    today_str = now.strftime(DATE_FORMAT)
    today_18 = datetime.datetime(now.year, now.month, now.day, HOUR_BOUNDARY)
    # 未过18点 且 今日是交易日 → 移除今日
    if now < today_18 and today_str in trading_days:
        trading_days.remove(today_str)
        print(f"⏰ 当前时间{now.strftime('%H:%M')}未过{HOUR_BOUNDARY}点，排除今日交易日：{today_str}")
    
    return trading_days

def get_last_trading_day() -> str:
    """返回最新的交易日（即get_trading_days结果的最后一个）"""
    trading_days = get_trading_days()
    if not trading_days:
        raise ValueError("未获取到任何交易日数据，请检查baostock连接或起始日期")
    return trading_days[-1]

# -------------------------- 测试代码（直接运行脚本可验证） --------------------------
if __name__ == "__main__":
    try:
        # 获取全部交易日并打印前5、后5个
        all_days = get_trading_days()
        print(f"\n✅ 最终可用交易日总数：{len(all_days)}")
        print(f"前5个交易日：{all_days[:5]}")
        print(f"后5个交易日：{all_days[-5:]}")
        
        # 获取最新交易日
        last_day = get_last_trading_day()
        print(f"\n✅ 最新交易日：{last_day}")
        
    except Exception as e:
        print(f"\n❌ 执行失败：{str(e)}")
        import traceback
        traceback.print_exc()  # 打印详细报错栈，方便排查