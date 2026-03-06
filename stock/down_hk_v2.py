import akshare as ak
import pandas as pd
import os
import datetime
import logging
import time
import warnings
import json
import signal
import sys
import random
import numpy as np  # 用于浮点数比对
import traceback  # 新增：用于详细异常追踪

# 引入项目自有模块
from get_hk_list import get_hk_stock_list
import trading_day

warnings.filterwarnings('ignore')

# ===================== 配置项 =====================
START_DATE = "20140101"  # 全量下载的起始日期
BASE_DATA_PATH = "./data"
HK_DATA_PATH = os.path.join(BASE_DATA_PATH, "hk")
CACHE_FILE_PATH = "./data/__down_hk__.json"

# 校验回溯天数（交易日估算，设为15个自然日足够覆盖一段交易区间）
CHECK_DAYS = 15
# 浮点数比对容差
FLOAT_TOLERANCE = 1e-4

# 核心字段定义
TARGET_COLUMNS = [
    "date", "open", "high", "low", "close", "volume", 
    "amount", "amplitude", "pctChg", "chgAmt", "turn"
]
# 校验字段（用于判断数据是否发生复权变化）
CHECK_FIELDS = ["open", "high", "low", "close", "volume"]

# 东财接口列映射
EM_COL_MAPPING = {
    "日期": "date",
    "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
    "成交量": "volume", "成交额": "amount",
    "振幅": "amplitude", "涨跌幅": "pctChg", "涨跌额": "chgAmt", "换手率": "turn"
}

# 重试与休眠配置
RETRY_MAX_ATTEMPTS = 3
SLEEP_BASE = 1

# ===================== 全局变量 =====================
EMPTY_CODE_CACHE = {}
NEED_SAVE_CACHE = False

# ===================== 日志配置 =====================
# 1. 创建 Logger
logger = logging.getLogger(__name__)
# 【关键点1】必须把 Logger 的总级别设为最低 (DEBUG)，
# 这样所有消息才能流向 Handlers，由 Handlers 自己去筛选。
logger.setLevel(logging.DEBUG)

# 2. 定义 Formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# 3. 配置控制台 Handler (StreamHandler) -> 只看 INFO
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)  # 控制台只输出 INFO 及以上
console_handler.setFormatter(formatter)

# 4. 配置代码文件 Handler (FileHandler) ->看 DEBUG
file_handler = logging.FileHandler("./logs/hk_data_download.log", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)    # 文件记录 DEBUG 及以上 (详细)
file_handler.setFormatter(formatter)

# 5. 把 Handler 添加到 Logger
# 先清空可能存在的旧 handler (防止重复打印)
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

# ===================== 信号处理 =====================
def handle_interrupt(signum, frame):
    logger.warning("\n捕获到Ctrl+C中断信号，正在保存缓存...")
    save_empty_code_cache()
    logger.info("缓存保存完成，程序退出")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_interrupt)

# ===================== 缓存系统 =====================
def init_cache_folder():
    cache_dir = os.path.dirname(CACHE_FILE_PATH)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

def load_empty_code_cache_once():
    global EMPTY_CODE_CACHE
    init_cache_folder()
    if not os.path.exists(CACHE_FILE_PATH):
        EMPTY_CODE_CACHE = {}
        return
    try:
        with open(CACHE_FILE_PATH, "r", encoding="utf-8") as f:
            # 简单加载，不做复杂过期校验，由人工定期清理或遇到错误清理
            EMPTY_CODE_CACHE = json.load(f)
        logger.info(f"缓存加载完成：有效缓存{len(EMPTY_CODE_CACHE)}个")
    except Exception as e:
        logger.error(f"加载缓存异常: {e}", exc_info=True)  # 新增：记录异常堆栈
        EMPTY_CODE_CACHE = {}

def save_empty_code_cache():
    global NEED_SAVE_CACHE
    if not NEED_SAVE_CACHE or not EMPTY_CODE_CACHE:
        return
    init_cache_folder()
    try:
        with open(CACHE_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(EMPTY_CODE_CACHE, f, ensure_ascii=False, indent=2)
        NEED_SAVE_CACHE = False
        logger.debug("缓存保存成功")  # 新增：DEBUG级日志记录保存结果
    except Exception as e:
        logger.error(f"保存缓存失败：{e}", exc_info=True)  # 新增：记录异常堆栈

def add_empty_code_to_cache(code, reason="no_data"):
    global EMPTY_CODE_CACHE, NEED_SAVE_CACHE
    if code not in EMPTY_CODE_CACHE:
        EMPTY_CODE_CACHE[code] = {
            "reason": reason,
            "update_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        NEED_SAVE_CACHE = True
        logger.debug(f"[{code}] 加入无数据缓存，原因：{reason}")  # 新增：记录缓存添加

def is_code_in_cache(code):
    return code in EMPTY_CODE_CACHE

# ===================== 工具函数 =====================
def ensure_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        logger.debug(f"创建目录：{dir_path}")  # 新增：记录目录创建

def extract_hk_raw_code(normalized_code):
    """从 hk.00700 提取 00700"""
    raw_code = normalized_code.split(".")[-1]
    logger.debug(f"[{normalized_code}] 提取原始代码：{raw_code}")  # 新增：DEBUG日志
    return raw_code

def get_date_str(dt_obj):
    return dt_obj.strftime("%Y%m%d")

def get_check_start_date(last_date_str):
    """
    根据最后日期，回推 CHECK_DAYS 天，作为校验开始日期
    """
    dt = datetime.datetime.strptime(last_date_str, "%Y%m%d")
    check_date = dt - datetime.timedelta(days=CHECK_DAYS)
    check_date_str = check_date.strftime("%Y%m%d")
    logger.debug(f"最后日期{last_date_str}，回推{CHECK_DAYS}天得到校验起始日：{check_date_str}")  # 新增
    return check_date_str

# ===================== 数据获取与处理 =====================
def fetch_data_from_em(code, is_index, start_date, end_date):
    """
    统一封装东财接口调用
    返回值：(df, reason)
        df: 成功返回数据框，无数据返回空框，失败返回None
        reason: None(成功) / "retry_failed"(重试失败) / "empty_data"(接口返回空)
    """
    raw_code = extract_hk_raw_code(code)
    df = None
    error_reason = None
    
    for attempt in range(RETRY_MAX_ATTEMPTS):
        try:
            logger.debug(f"[{code}] 第{attempt+1}次调用接口，is_index={is_index}，时间范围[{start_date}, {end_date}]")
            if is_index:
                # 指数接口不支持时间范围，全量拉取
                # 东财的数据，close 叫 latest，而且没有 volume，改用新浪的
                # df = ak.stock_hk_index_daily_em(symbol=raw_code)
                df = ak.stock_hk_index_daily_sina(symbol=raw_code)
            else:
                # 个股接口：前复权 (qfq)
                df = ak.stock_hk_hist(
                    symbol=raw_code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
            # 接口调用成功，检查是否为空数据
            if df is None or df.empty:
                error_reason = "empty_data"
                logger.debug(f"[{code}] 接口调用成功但返回空数据")
            else:
                error_reason = None
            break
        except Exception as e:
            error_detail = traceback.format_exc()  # 捕获完整异常堆栈
            logger.debug(f"[{code}] 第{attempt+1}次调用接口异常：{e}\n{error_detail}")
            if attempt < RETRY_MAX_ATTEMPTS - 1:
                sleep_time = SLEEP_BASE * (attempt + 1)
                logger.debug(f"[{code}] 休眠{sleep_time}秒后重试")
                time.sleep(sleep_time)
            else:
                error_reason = "retry_failed"
                logger.debug(f"[{code}] 接口重试{RETRY_MAX_ATTEMPTS}次均失败", exc_info=True)

    if df is not None and not df.empty:
        # 标准化列名
        df = df.rename(columns=EM_COL_MAPPING)
        if "date" not in df.columns:
            logger.debug(f"[{code}] 数据无date列，判定为空数据")
            df = pd.DataFrame()
            error_reason = "empty_data"
        else:
            # 格式化日期
            df["date"] = pd.to_datetime(df["date"])
            
            # 过滤无效列并补全
            for col in TARGET_COLUMNS:
                if col not in df.columns:
                    df[col] = 0.0
                    logger.debug(f"[{code}] 补全缺失列{col}为0.0")
            
            # 类型转换
            for col in CHECK_FIELDS:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    logger.debug(f"[{code}] 转换列{col}为数值类型")
                    
            df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
            df = df[TARGET_COLUMNS]
            logger.debug(f"[{code}] 数据处理完成，有效行数：{len(df)}")
    return df, error_reason

def check_consistency_and_download(code, is_index, last_trading_day_str):
    """
    核心逻辑：
    1. 检查本地是否有文件
    2. 无文件 -> 全量下载
    3. 有文件 -> 计算回溯日期(T-15) -> 下载重叠段
    4. 校验重叠段一致性：
       - 不一致 -> 触发全量下载 (复权变化)
       - 一致 -> 截取新增段 -> 追加写入
    返回：(status, msg)
        status: success / failed / skipped / no_data
        msg: 详细说明
    """
    file_path = os.path.join(HK_DATA_PATH, f"{code}.csv")
    raw_code = extract_hk_raw_code(code)
    logger.debug(f"[{code}] 开始处理，文件路径：{file_path}，最新交易日：{last_trading_day_str}")

    if "hki." in code and code not in ("hki.HSI", "hki.HSTECH"):
        return "no_data", "接口无数据"
    
    # === Case 1: 全新下载 ===
    if not os.path.exists(file_path):
        logger.debug(f"[{code}] 本地文件不存在，执行全量下载")
        df_new, reason = fetch_data_from_em(code, is_index, START_DATE, last_trading_day_str)
        
        # 区分：无数据(empty_data) / 下载失败(retry_failed) / 成功
        if reason == "empty_data":
            add_empty_code_to_cache(code, "empty_from_start")
            return "no_data", "接口无数据"
        elif reason == "retry_failed":
            return "failed", "接口调用失败"
        elif df_new is None:
            return "failed", "未知错误导致数据为空"
        
        # 下载成功，写入文件
        df_new["date"] = df_new["date"].dt.strftime("%Y-%m-%d")
        df_new.to_csv(file_path, index=False, encoding="utf-8-sig")
        logger.debug(f"[{code}] 全量初始化完成，写入{len(df_new)}条数据")
        return "success", f"全量初始化 {len(df_new)} 条"

    # === Case 2: 增量校验与下载 ===
    try:
        # 读取本地最后的数据用于校验
        # 只需要读最后 30 行足够覆盖 15 天
        df_local_all = pd.read_csv(file_path)
        logger.debug(f"[{code}] 读取本地文件，总行数：{len(df_local_all)}")
        
        if df_local_all.empty:
            # 文件损坏或为空，重新全量
            raise ValueError("Local file empty")
        
        df_local_all['date'] = pd.to_datetime(df_local_all['date'])
        local_max_date_dt = df_local_all['date'].max()
        local_max_date_str = local_max_date_dt.strftime("%Y%m%d")
        logger.debug(f"[{code}] 本地最新日期：{local_max_date_str}")

        # 如果本地已经是最新
        if local_max_date_str >= last_trading_day_str:
            return "skipped", "已是最新"

        # 计算校验开始时间 (回溯 CHECK_DAYS 天)
        check_start_str = get_check_start_date(local_max_date_str)
        
        # 远程请求：从 check_start_str 到 最新交易日
        df_remote, reason = fetch_data_from_em(code, is_index, check_start_str, last_trading_day_str)
        
        # 处理远程请求结果
        if reason == "retry_failed":
            return "failed", "远程数据获取失败"
        if reason == "empty_data" or (df_remote is None or df_remote.empty):
            return "skipped", "远程无新数据"

        # --- 一致性校验 ---
        # 1. 提取本地在校验窗口内的数据
        check_start_dt = pd.to_datetime(check_start_str)
        df_local_check = df_local_all[df_local_all['date'] >= check_start_dt].copy()
        logger.debug(f"[{code}] 本地校验数据行数：{len(df_local_check)} (时间范围：{check_start_str}~{local_max_date_str})")
        
        # 2. 提取远程在本地已有范围内的数据（重叠部分）
        df_remote_check = df_remote[df_remote['date'] <= local_max_date_dt].copy()
        logger.debug(f"[{code}] 远程校验数据行数：{len(df_remote_check)} (重叠时间范围：{check_start_str}~{local_max_date_str})")

        # 3. 寻找交集日期
        common_dates = pd.merge(df_local_check[['date']], df_remote_check[['date']], on='date')
        logger.debug(f"[{code}] 校验交集日期数：{len(common_dates)}")
        
        is_consistent = True
        if not common_dates.empty:
            # 只比对交集日期的数据
            df_local_compare = df_local_check[df_local_check['date'].isin(common_dates['date'])].sort_values('date').set_index('date')
            df_remote_compare = df_remote_check[df_remote_check['date'].isin(common_dates['date'])].sort_values('date').set_index('date')
            
            # 比对关键字段
            for field in CHECK_FIELDS:
                # 需处理 NaN
                arr_local = df_local_compare[field].fillna(0).values
                arr_remote = df_remote_compare[field].fillna(0).values
                
                # 使用 numpy 的 isclose 进行浮点比对
                if not np.allclose(arr_local, arr_remote, atol=FLOAT_TOLERANCE):
                    # 记录详细的比对差异
                    diff_idx = ~np.isclose(arr_local, arr_remote, atol=FLOAT_TOLERANCE)
                    diff_local = arr_local[diff_idx]
                    diff_remote = arr_remote[diff_idx]
                    logger.warning(f"[{code}] 校验失败 | 字段: {field} | 差异行数：{len(diff_local)} | 本地值示例：{diff_local[:5]} | 远程值示例：{diff_remote[:5]}")
                    logger.warning(f"[{code}] 发现复权/数据变化，触发全量下载")
                    is_consistent = False
                    break
        else:
            # 只有当本地很久没更新，且check_start到local_max之间本来就没交易日时，可能无交集
            logger.debug(f"[{code}] 无校验交集日期，无需全量，直接追加")
            pass

        # === 决策执行 ===
        if not is_consistent:
            # 校验失败 -> 全量重下
            logger.debug(f"[{code}] 校验不一致，执行全量重下")
            df_full, reason = fetch_data_from_em(code, is_index, START_DATE, last_trading_day_str)
            if reason == "retry_failed" or df_full is None:
                return "failed", "全量修正失败"
            if reason == "empty_data":
                add_empty_code_to_cache(code, "empty_after_check")
                return "no_data", "全量重下后接口无数据"
            
            df_full["date"] = df_full["date"].dt.strftime("%Y-%m-%d")
            df_full.to_csv(file_path, index=False, encoding="utf-8-sig")
            logger.debug(f"[{code}] 全量修正完成，写入{len(df_full)}条数据")
            return "success", f"校验不一致，已全量修正 {len(df_full)} 条"
        
        else:
            # 校验成功 -> 增量追加
            # 筛选出日期 > local_max_date_dt 的数据
            df_append = df_remote[df_remote['date'] > local_max_date_dt].copy()
            logger.debug(f"[{code}] 校验通过，增量数据行数：{len(df_append)}")
            if df_append.empty:
                return "skipped", "校验通过但无新数据"
            
            df_append["date"] = df_append["date"].dt.strftime("%Y-%m-%d")
            # 追加写入 (mode='a', header=False)
            df_append.to_csv(file_path, mode='a', header=False, index=False, encoding="utf-8-sig")
            return "success", f"增量追加 {len(df_append)} 条"

    except Exception as e:
        error_detail = traceback.format_exc()
        logger.error(f"[{code}] 处理异常：{e}\n{error_detail}", exc_info=True)
        logger.debug(f"[{code}] 异常后尝试全量兜底")
        # 异常兜底：尝试全量覆盖
        try:
            df_full, reason = fetch_data_from_em(code, is_index, START_DATE, last_trading_day_str)
            if reason is None and df_full is not None and not df_full.empty:
                df_full["date"] = df_full["date"].dt.strftime("%Y-%m-%d")
                df_full.to_csv(file_path, index=False, encoding="utf-8-sig")
                logger.debug(f"[{code}] 异常后全量恢复完成，写入{len(df_full)}条数据")
                return "success", f"异常后全量恢复 {len(df_full)} 条"
            elif reason == "empty_data":
                add_empty_code_to_cache(code, "empty_after_exception")
                return "no_data", "异常后全量恢复但接口无数据"
            else:
                return "failed", "异常后全量恢复失败"
        except Exception as e2:
            logger.error(f"[{code}] 全量兜底也异常：{e2}", exc_info=True)
            return "failed", f"异常后全量恢复失败: {str(e2)}"


# ===================== 主流程 =====================
def main():
    # 1. 环境初始化
    ensure_dir(HK_DATA_PATH)
    ensure_dir("./logs")
    load_empty_code_cache_once()
    
    # 2. 获取截止日期
    last_day_raw = trading_day.get_last_trading_day()
    if not last_day_raw:
        logger.error("无法获取最新交易日")
        return
    last_trading_day_str = last_day_raw.replace("-", "")
    logger.info(f"目标截止日期: {last_trading_day_str} (校验回溯: {CHECK_DAYS}天)")

    # 3. 获取清单
    logger.info("获取全量证券列表...")
    hk_list_df = get_hk_stock_list()
    if hk_list_df.empty:
        logger.error("证券列表为空，退出程序")
        return
    logger.debug(f"获取到证券列表总行数：{len(hk_list_df)}")

    # 4. 构建优先级队列 (参考 Request 2)
    # 4.1 指数
    index_df = hk_list_df[hk_list_df["证券类型"] == "指数"]
    idx_codes = index_df["证券代码"].drop_duplicates().tolist()
    
    # 4.2 股票
    stock_df = hk_list_df[hk_list_df["证券类型"] == "股票"]
    
    # 分离 港股通 vs 其他港股
    if "所属市场及板块" in stock_df.columns:
        ggt_df = stock_df[stock_df["所属市场及板块"].astype(str).str.contains("港股通")]
        other_df = stock_df[~stock_df["所属市场及板块"].astype(str).str.contains("港股通")]
    else:
        ggt_df = pd.DataFrame()
        other_df = stock_df

    ggt_codes = ggt_df["证券代码"].drop_duplicates().tolist()
    other_codes = other_df["证券代码"].drop_duplicates().tolist()

    # 4.3 合并任务列表 (保持顺序：指数 -> 港股通 -> 其他)
    # 结构: (code, is_index, tag)
    task_list = []
    task_list.extend([(c, True, "指数") for c in idx_codes])
    task_list.extend([(c, False, "港股通") for c in ggt_codes])
    task_list.extend([(c, False, "其他港股") for c in other_codes])
    
    # 去重 (防止某些代码既在港股通又在其他列表)
    seen = set()
    unique_task_list = []
    for t in task_list:
        if t[0] not in seen:
            unique_task_list.append(t)
            seen.add(t[0])
            
    total_count = len(unique_task_list)
    logger.info(f"任务统计: 总数 {total_count} | 指数 {len(idx_codes)} | 港股通 {len(ggt_codes)} | 其他 {len(other_codes)}")
    logger.debug(f"去重后任务列表长度：{total_count}")

    # 5. 执行循环
    success_cnt = 0
    failed_cnt = 0
    no_data_cnt = 0
    skipped_cnt = 0
    
    for i, (code, is_index, tag) in enumerate(unique_task_list, 1):
        if is_code_in_cache(code):
            # 仅每100个打印一次跳过日志，保持清爽
            if i % 100 == 0: 
                logger.info(f"进度 {i}/{total_count} | {code} | 跳过 (缓存无数据)")
            no_data_cnt += 1
            continue
            
        # 状态指示
        prefix = f"[{tag}] {i}/{total_count} {code}"
        
        status, msg = check_consistency_and_download(code, is_index, last_trading_day_str)
        
        # 统计计数
        if status == "success":
            success_cnt += 1
            logger.info(f"{prefix} | 成功 | {msg}")
            # 稍微休眠，东财对并发有限制
            time.sleep(random.uniform(0.2, 0.5))
        elif status == "failed":
            failed_cnt += 1
            logger.error(f"{prefix} | 失败 | {msg}")
        elif status == "no_data":
            no_data_cnt += 1
            logger.warning(f"{prefix} | 无数据 | 加入缓存")
        else:  # skipped
            skipped_cnt += 1
            if i % 50 == 0:
                logger.info(f"{prefix} | 无更新 | {msg}")

        # 批次休眠
        if i % 20 == 0:
            logger.debug(f"批次休眠：已处理{i}个任务，休眠1秒")
            time.sleep(1)

    # 6. 收尾
    save_empty_code_cache()
    # 新增：统计汇总日志
    logger.info(f"""所有任务结束. 
    累计成功下载/更新: {success_cnt} 个
    下载失败: {failed_cnt} 个
    无数据(加入缓存): {no_data_cnt} 个
    无更新(跳过): {skipped_cnt} 个
    总任务数: {total_count} 个""")

if __name__ == "__main__":
    main()