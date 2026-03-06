import akshare as ak
import pandas as pd
import os
import datetime
from typing import List

# -------------------------- 配置常量（可按需修改） --------------------------
START_DATE = "2014-01-01"  # 港股交易日起始日期（按需求补充）
CACHE_FILE = "./data/TDays_hk.csv"  # 港股缓存文件路径
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

def _fetch_akshare_data() -> List[str]:
    """
    从akshare（新浪财经）获取港股原始交易日数据（基于恒生指数日线数据提取，仅更新缓存时调用）
    返回：START_DATE至当前日期的港股交易日列表（字符串格式，按时间升序）
    修复点：过滤掉数据源中包含的未来日期
    """
    print("✅ 开始从新浪财经查询恒生指数日线数据（提取港股交易日）...")
    
    # 调用akshare接口获取恒生指数日线数据（核心修改：替换为港股数据源）
    try:
        # 获取恒生指数日线数据（symbol参数可根据akshare版本调整，也可使用^HSI）
        hk_index_df = ak.stock_hk_index_daily_sina(symbol="HSI")
    except Exception as e:
        raise ConnectionError(f"akshare查询恒生指数日线数据失败：{str(e)}")
    
    # 打印列名和前3行（调试用）
    print(f"📌 akshare返回字段：{hk_index_df.columns.tolist()}")
    print(f"📌 数据前3行：\n{hk_index_df.head(3)}")
    
    # 格式标准化（恒生指数日线数据的日期字段为"date"）
    hk_index_df["date"] = pd.to_datetime(hk_index_df["date"]).dt.strftime(DATE_FORMAT)
    
    # 🔥 核心修复：过滤未来日期 + 筛选起始日期后的数据 🔥
    current_date = datetime.datetime.now().strftime(DATE_FORMAT)  # 当前日期（字符串）
    trading_days_df = hk_index_df[
        (hk_index_df["date"] >= START_DATE) &  # 保留起始日期后
        (hk_index_df["date"] <= current_date)   # 过滤未来日期
    ]
    
    # 去重（防止数据源重复）+ 排序并提取列表
    trading_days = trading_days_df["date"].drop_duplicates().sort_values().tolist()
    
    print(f"✅ 港股交易日提取完成，共获取{len(trading_days)}个港股交易日（{START_DATE} 至 {current_date}）")
    return trading_days

def _save_cache(trading_days: List[str]) -> None:
    """将港股交易日列表保存到缓存文件"""
    pd.DataFrame({"trading_day": trading_days}).to_csv(CACHE_FILE, index=False, encoding="utf-8")
    print(f"✅ 港股交易日数据已缓存至：{CACHE_FILE}")

def _load_cache() -> List[str]:
    """从缓存文件读取港股交易日列表"""
    df = pd.read_csv(CACHE_FILE, encoding="utf-8")
    print(f"✅ 从缓存读取港股交易日数据，共{len(df)}条")
    return df["trading_day"].tolist()

# -------------------------- 外部暴露的核心接口 --------------------------
def get_trading_days() -> List[str]:
    """
    获取START_DATE至当前的全部港股交易日（按时间升序）
    核心规则：当天18点前，即使是交易日也排除；18点后则包含
    """
    # 智能判断是否更新缓存，无需更新则直接读缓存（不调用akshare）
    if _need_update_cache():
        trading_days = _fetch_akshare_data()
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
        print(f"⏰ 当前时间{now.strftime('%H:%M')}未过{HOUR_BOUNDARY}点，排除今日港股交易日：{today_str}")
    
    return trading_days

def get_last_trading_day() -> str:
    """返回最新的港股交易日（即get_trading_days结果的最后一个）"""
    trading_days = get_trading_days()
    if not trading_days:
        raise ValueError("未获取到任何港股交易日数据，请检查akshare连接或起始日期")
    return trading_days[-1]

# -------------------------- 测试代码（直接运行脚本可验证） --------------------------
if __name__ == "__main__":
    try:
        # 获取全部港股交易日并打印前5、后5个
        all_days = get_trading_days()
        print(f"\n✅ 最终可用港股交易日总数：{len(all_days)}")
        print(f"前5个港股交易日：{all_days[:5]}")
        print(f"后5个港股交易日：{all_days[-5:]}")
        
        # 获取最新港股交易日
        last_day = get_last_trading_day()
        print(f"\n✅ 最新港股交易日：{last_day}")
        
    except Exception as e:
        print(f"\n❌ 执行失败：{str(e)}")
        import traceback
        traceback.print_exc()  # 打印详细报错栈，方便排查