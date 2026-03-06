"""
港股数据批量下载脚本
功能说明：
1. 批量下载港股（港股通+其他港股）、港股指数的日频历史数据
2. 适配东财(em)/新浪(sina)双接口，交替重试（东财败换新浪/新浪败换东财），成功后缓存最优接口优先级
3. 数据输出为UTF-8-SIG编码的CSV，统一英文表头，格式：date open high low close volume amount amplitude pctChg chgAmt turn
4. 自动跳过已下载的证券代码，按指数→港股通→其他港股优先级下载
5. 全链路校验空数据/缺核心列，彻底解决新浪接口兼容问题，输出精细化日志便于排查
"""
import akshare as ak
import pandas as pd
import time
import os
import logging
import random
import traceback
import sys
from datetime import datetime
from requests.exceptions import ConnectionError, Timeout, ReadTimeout
from http.client import RemoteDisconnected

# 导入港股清单获取函数
from get_hk_list import get_hk_stock_list  
# 交易日模块
import trading_day  

# ===================== 全局配置项 =====================
BASE_DATA_PATH = "./data"
HK_DATA_PATH = os.path.join(BASE_DATA_PATH, "hk")
START_DATE = "20140101"
# 优化休眠时间：提升基础间隔，降低风控概率（解决东财RemoteDisconnected）
SLEEP_BASE = 1
SLEEP_RANDOM = 2
# 全局接口优先级缓存（成功后自动更新，后续所有同类型均优先使用）
PREFERRED_API = "em"  # 个股优先接口
PREFERRED_INDEX_API = "em"  # 指数优先接口

# ===================== 日志配置（核心优化：分层输出） =====================
# 1. 创建日志器
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # 根日志器设为DEBUG，确保所有handler能接收到
logger.handlers.clear()  # 清空默认handler

# 2. 控制台Handler（保持原有输出：INFO级别+简洁格式）
console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
sys.stdout.reconfigure(encoding='utf-8')

# 3. 文件Handler（增强输出：DEBUG级别+详细格式+完整上下文）
file_handler = logging.FileHandler("./logs/hk_data_download.log", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
# 新增：模块、函数、行号、进程ID，便于定位问题
file_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(process)d - %(message)s"
)
file_handler.setFormatter(file_formatter)

# 4. 添加handler到日志器
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# ===================== 工具函数 =====================
def ensure_dir(dir_path):
    """确保目录存在，不存在则创建"""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        logger.info(f"【目录初始化】创建目录: {dir_path}")
        logger.debug(f"【目录初始化-详情】目录绝对路径：{os.path.abspath(dir_path)} | 父目录存在性：{os.path.exists(os.path.dirname(dir_path))}")

def get_last_trading_day_str():
    """获取最新交易日，返回YYYYMMDD格式"""
    last_trade_day = trading_day.get_last_trading_day()
    logger.debug(f"【交易日获取-原始值】{last_trade_day} | 类型：{type(last_trade_day)}")
    if isinstance(last_trade_day, datetime):
        return last_trade_day.strftime("%Y%m%d")
    return last_trade_day.replace("-", "") if isinstance(last_trade_day, str) else ""

def extract_hk_raw_code(normalized_code):
    """从标准化代码提取纯代码（hk.00001→00001；hki.CESM→CESM）"""
    try:
        code_str = str(normalized_code).strip()
        logger.debug(f"【代码提取-原始输入】{normalized_code} | 处理后字符串：{code_str}")
        if "." in code_str:
            raw_code = code_str.split(".")[-1]
            if code_str.startswith("hk.") and len(raw_code) == 5 and raw_code.isdigit():
                logger.debug(f"【代码提取-成功】个股代码：{normalized_code} → {raw_code}")
                return raw_code
            elif code_str.startswith("hki."):
                logger.debug(f"【代码提取-成功】指数代码：{normalized_code} → {raw_code}")
                return raw_code
        logger.debug(f"【代码提取-无匹配规则】{normalized_code}")
        return ""
    except Exception as e:
        logger.warning(f"【代码提取失败】{normalized_code} | {str(e)[:50]}")
        logger.debug(f"【代码提取失败-完整堆栈】{traceback.format_exc()}")
        return ""

def single_api_call(func, *args, date_cols=None, **kwargs):
    """
    单次API调用封装（核心优化：日志增强+完整堆栈+请求/返回快照）
    关键优化：
    1. 记录接口调用的完整入参（args/kwargs）
    2. 返回数据增加快照（前3行+列名），便于排查数据格式问题
    3. 异常时输出完整堆栈到日志文件
    4. 控制台仅输出简洁信息，日志文件保留全量上下文
    """
    global PREFERRED_API, PREFERRED_INDEX_API
    try:
        # 1. 记录接口调用的完整入参（DEBUG级别，仅日志文件可见）
        logger.debug(f"【API调用-入参】函数：{func.__name__} | args：{args} | kwargs：{kwargs} | 日期列校验列表：{date_cols}")
        # 2. 调用接口
        result = func(*args, **kwargs)
        logger.info(f"【API调用执行】接口名称：{func.__name__}")

        # 3. 第一层校验：是否为有效DataFrame
        if not isinstance(result, pd.DataFrame):
            logger.warning(f"【API无效返回】{func.__name__} | 非DataFrame类型：{type(result)}")
            logger.debug(f"【API无效返回-快照】返回值：{str(result)[:500]}")  # 记录返回值快照
            return None
        
        # 4. 第二层校验：是否为空数据
        if result.empty:
            logger.warning(f"【API空数据返回】{func.__name__} | 无任何数据行")
            logger.debug(f"【API空数据返回-详情】列名：{list(result.columns)} | 索引：{list(result.index)}")
            return None
        
        # 5. 第三层校验：日期列
        if date_cols and isinstance(date_cols, list):
            has_valid_date_col = any(col in result.columns for col in date_cols)
            if not has_valid_date_col:
                logger.warning(f"【API无日期列】{func.__name__} | 无任何有效日期列 | 现有列：{list(result.columns)[:10]}")
                logger.debug(f"【API无日期列-详情】待校验列：{date_cols} | 全部返回列：{list(result.columns)}")
                return None

        # 6. 记录返回数据快照（仅日志文件可见）
        logger.debug(f"【API调用成功-数据快照】函数：{func.__name__} | 行数：{len(result)} | 列名：{list(result.columns)}")
        logger.debug(f"【API调用成功-前3行】\n{result.head(3).to_string()}")

        # 7. 更新全局接口优先级
        if func == ak.stock_hk_hist:
            PREFERRED_API = "em"
            logger.info(f"【个股优先级更新】全局优先接口改为：em（东财）")
        elif func == ak.stock_hk_daily:
            PREFERRED_API = "sina"
            logger.info(f"【个股优先级更新】全局优先接口改为：sina（新浪）")
        elif func == ak.stock_hk_index_daily_em:
            PREFERRED_INDEX_API = "em"
            logger.info(f"【指数优先级更新】全局优先接口改为：em（东财）")
        elif func == ak.stock_hk_index_daily_sina:
            PREFERRED_INDEX_API = "sina"
            logger.info(f"【指数优先级更新】全局优先接口改为：sina（新浪）")

        logger.info(f"【API调用成功】{func.__name__} | 有效数据行：{len(result)}")
        return result

    # 分类捕获异常，增强日志
    except (ConnectionError, Timeout, ReadTimeout, RemoteDisconnected) as e:
        logger.warning(f"【API网络异常】{func.__name__} | {str(e)[:60]}")
        logger.debug(f"【API网络异常-完整信息】异常类型：{type(e)} | 完整消息：{str(e)}")
        logger.debug(f"【API网络异常-完整堆栈】{traceback.format_exc()}")
        return None
    except TypeError as e:
        logger.error(f"【API参数错误】{func.__name__} | {str(e)[:100]}")
        logger.debug(f"【API参数错误-入参快照】args：{args} | kwargs：{kwargs}")
        logger.debug(f"【API参数错误-完整堆栈】{traceback.format_exc()}")
        return None
    except Exception as e:
        logger.error(f"【API未知异常】{func.__name__} | {str(e)[:60]}")
        logger.debug(f"【API未知异常-完整信息】异常类型：{type(e)} | 完整消息：{str(e)}")
        logger.debug(f"【API未知异常-完整堆栈】{traceback.format_exc()}")
        return None

def get_downloaded_codes():
    """获取已下载的标准化代码列表，避免重复下载"""
    downloaded_codes = []
    if os.path.exists(HK_DATA_PATH):
        for file_name in os.listdir(HK_DATA_PATH):
            if file_name.endswith(".csv"):
                downloaded_codes.append(file_name.replace(".csv", ""))
    return downloaded_codes

def download_single_security(normalized_code, start_date, end_date):
    """
    单证券下载核心函数（核心优化：失败场景全量上下文日志）
    """
    global PREFERRED_API, PREFERRED_INDEX_API
    # 1. 提取纯代码并校验
    raw_code = extract_hk_raw_code(normalized_code)
    logger.debug(f"【单证券下载-初始化】标准化代码：{normalized_code} | 纯代码：{raw_code} | 时间范围：{start_date}~{end_date}")
    if not raw_code:
        logger.warning(f"【下载跳过】标准化代码无效：{normalized_code}")
        return None
    
    # 核心配置
    TARGET_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount", "amplitude", "pctChg", "chgAmt", "turn"]
    COL_MAPPING = {
        "trade_date": "date", "latest": "close",
        "成交额": "amount", "振幅": "amplitude", "涨跌幅": "pctChg",
        "涨跌额": "chgAmt", "换手率": "turn"
    }
    DATE_COLS = ["date", "trade_date"]

    is_index = normalized_code.startswith("hki.")
    df = None
    logger.info(f"【{'指数' if is_index else '个股'}下载启动】{normalized_code} | 全局优先接口：{PREFERRED_INDEX_API if is_index else PREFERRED_API}")

    # ========== 指数下载 ==========
    if is_index:
        # 优先接口调用
        if PREFERRED_INDEX_API == "em":
            df = single_api_call(ak.stock_hk_index_daily_em, raw_code, date_cols=DATE_COLS)
            if df is None:
                logger.info(f"【指数交替重试】{normalized_code} | 东财em失败，切新浪sina接口")
                df = single_api_call(ak.stock_hk_index_daily_sina, raw_code, date_cols=DATE_COLS)
        else:
            df = single_api_call(ak.stock_hk_index_daily_sina, raw_code, date_cols=DATE_COLS)
            if df is None:
                logger.info(f"【指数交替重试】{normalized_code} | 新浪sina失败，切东财em接口")
                df = single_api_call(ak.stock_hk_index_daily_em, raw_code, date_cols=DATE_COLS)

    # ========== 个股下载 ==========
    else:
        if PREFERRED_API == "em":
            df = single_api_call(
                ak.stock_hk_hist, raw_code, "daily", start_date, end_date, "qfq",
                date_cols=DATE_COLS
            )
            if df is None:
                logger.info(f"【个股交替重试】{normalized_code} | 东财em失败，切新浪sina接口（延迟1秒）")
                time.sleep(1)
                df = single_api_call(ak.stock_hk_daily, raw_code, "qfq", date_cols=DATE_COLS)
        else:
            df = single_api_call(ak.stock_hk_daily, raw_code, "qfq", date_cols=DATE_COLS)
            if df is None:
                logger.info(f"【个股交替重试】{normalized_code} | 新浪sina失败，切东财em接口（延迟1秒）")
                time.sleep(1)
                df = single_api_call(
                    ak.stock_hk_hist, raw_code, "daily", start_date, end_date, "qfq",
                    date_cols=DATE_COLS
                )

    # 交替重试后仍无有效数据
    if df is None or df.empty:
        logger.error(f"【下载失败】{normalized_code} | 交替重试后所有接口均无有效数据")
        logger.debug(f"【下载失败-上下文】标准化代码：{normalized_code} | 纯代码：{raw_code} | 是否指数：{is_index} | 优先接口：{PREFERRED_INDEX_API if is_index else PREFERRED_API} | 时间范围：{start_date}~{end_date}")
        return None

    # ========== 数据标准化处理（增强日志） ==========
    try:
        logger.debug(f"【数据处理-原始数据】{normalized_code} | 行数：{len(df)} | 列名：{list(df.columns)}")
        
        # 列名映射
        valid_mapping = {k: v for k, v in COL_MAPPING.items() if k in df.columns}
        logger.debug(f"【数据处理-列映射】有效映射：{valid_mapping} | 原始列名：{list(df.columns)}")
        if valid_mapping:
            df.rename(columns=valid_mapping, inplace=True)
            logger.debug(f"【数据处理-列映射后】列名：{list(df.columns)}")
        
        # 日期列校验
        if "date" not in df.columns:
            logger.error(f"【处理失败】{normalized_code} | 无有效date列")
            logger.debug(f"【处理失败-列详情】所有列：{list(df.columns)}")
            return None

        # 日期标准化
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["date"])
        logger.debug(f"【数据处理-日期标准化】行数（去无效日期后）：{len(df)} | 日期列示例：{df['date'].head(3).tolist()}")
        
        # 时间范围筛选
        df = df[
            (pd.to_datetime(df["date"]) >= pd.to_datetime(start_date)) &
            (pd.to_datetime(df["date"]) <= pd.to_datetime(end_date))
        ]
        logger.debug(f"【数据处理-时间筛选】行数（筛选后）：{len(df)} | 时间范围：{start_date}~{end_date}")

        if df.empty:
            logger.warning(f"【处理失败】{normalized_code} | 时间范围过滤后无有效数据")
            return None

        # 补充缺失列
        missing_cols = [col for col in TARGET_COLUMNS if col not in df.columns]
        for col in missing_cols:
            df[col] = pd.NA
            logger.debug(f"【新浪接口兼容】{normalized_code} | 补充缺失列：{col} → NA")
        logger.debug(f"【数据处理-补充列后】列名：{list(df.columns)} | 缺失列：{missing_cols}")

        # 最终数据整理
        df = df.sort_values("date") \
               .drop_duplicates(subset=["date"], keep="last") \
               [TARGET_COLUMNS] \
               .reset_index(drop=True)
        logger.debug(f"【数据处理-最终结果】{normalized_code} | 行数：{len(df)} | 列名：{list(df.columns)} | 前3行：\n{df.head(3).to_string()}")

        logger.info(f"【处理完成】{normalized_code} | 最终有效数据行：{len(df)} | 新浪接口兼容处理完成")
        return df
    except Exception as e:
        logger.error(f"【数据处理异常】{normalized_code} | {str(e)[:80]}")
        logger.debug(f"【数据处理异常-完整堆栈】{traceback.format_exc()}")
        logger.debug(f"【数据处理异常-上下文】标准化代码：{normalized_code} | 原始数据行数：{len(df)} | 原始列名：{list(df.columns)}")
        return None

def save_security_data(df, normalized_code):
    """保存数据（增强日志：记录保存路径+文件大小）"""
    if df is None or df.empty:
        logger.debug(f"【保存跳过】{normalized_code} | 数据为空")
        return False
    csv_path = os.path.join(HK_DATA_PATH, f"{normalized_code}.csv")
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        # 记录文件大小（仅日志文件可见）
        file_size = os.path.getsize(csv_path) / 1024  # KB
        logger.info(f"【保存成功】{normalized_code} | {csv_path}")
        logger.debug(f"【保存成功-详情】路径：{os.path.abspath(csv_path)} | 文件大小：{file_size:.2f}KB | 数据行数：{len(df)}")
        return True
    except Exception as e:
        logger.error(f"【保存失败】{normalized_code} | {str(e)[:50]}")
        logger.debug(f"【保存失败-完整堆栈】{traceback.format_exc()}")
        logger.debug(f"【保存失败-上下文】路径：{os.path.abspath(csv_path)} | 数据行数：{len(df) if df is not None else 'None'}")
        return False

# ===================== 主流程（日志增强） =====================
def main():
    """主执行流程"""
    global PREFERRED_API, PREFERRED_INDEX_API
    PREFERRED_API = "em"
    PREFERRED_INDEX_API = "em"

    # 初始化目录+获取结束日期
    ensure_dir(HK_DATA_PATH)
    end_date = get_last_trading_day_str()
    if not end_date:
        logger.error("❌ 无法获取最新交易日，程序退出")
        return

    # 启动信息
    logger.info("=" * 60)
    logger.info("【港股数据批量下载任务 - 正式启动】")
    logger.info(f"📅 下载时间范围：{START_DATE} ~ {end_date}")
    logger.info(f"📁 数据存储路径：{os.path.abspath(HK_DATA_PATH)}")
    logger.info(f"🔧 初始全局优先级 - 个股：{PREFERRED_API} | 指数：{PREFERRED_INDEX_API}")
    logger.info(f"⚙️  接口请求配置 - 基础休眠：{SLEEP_BASE}s | 随机偏移：{SLEEP_RANDOM}s")
    logger.info(f"📋 输出表头：{' '.join(TARGET_COLUMNS)}")
    logger.info("=" * 60)
    logger.debug(f"【主流程-初始化】BASE_DATA_PATH：{BASE_DATA_PATH} | HK_DATA_PATH：{HK_DATA_PATH} | START_DATE：{START_DATE} | END_DATE：{end_date}")

    # 获取港股清单
    logger.info("\n【步骤1：获取港股统一清单】")
    hk_list_df = get_hk_stock_list()
    if hk_list_df.empty:
        logger.error("❌ 港股清单获取失败，无数据可下载，程序退出")
        logger.debug(f"【清单获取失败-详情】返回DataFrame是否为空：{hk_list_df.empty} | 列名：{list(hk_list_df.columns) if not hk_list_df.empty else '无'}")
        return
    logger.info(f"✅ 清单获取成功 | 总证券数：{len(hk_list_df)}")
    logger.debug(f"【清单获取成功-详情】前5行：\n{hk_list_df.head(5).to_string()} | 列名：{list(hk_list_df.columns)}")

    # 拆分清单
    index_codes = hk_list_df[hk_list_df["证券类型"] == "指数"]["证券代码"].drop_duplicates().tolist()
    ggt_codes = hk_list_df[(hk_list_df["证券类型"] == "股票") & (hk_list_df["所属市场及板块"] == "港股通")]["证券代码"].drop_duplicates().tolist()
    normal_hk_codes = hk_list_df[(hk_list_df["证券类型"] == "股票") & (hk_list_df["所属市场及板块"] == "港股")]["证券代码"].drop_duplicates().tolist()

    # 清单统计
    logger.info("\n【清单分类统计】")
    logger.info(f"📊 指数：{len(index_codes)} 只 | 港股通个股：{len(ggt_codes)} 只 | 其他港股：{len(normal_hk_codes)} 只")
    downloaded_codes = get_downloaded_codes()
    total_to_download = len(index_codes+ggt_codes+normal_hk_codes) - len(downloaded_codes)
    logger.info(f"📌 已下载证券数：{len(downloaded_codes)} 只 | 本次待下载：{total_to_download} 只")
    logger.debug(f"【清单分类-详情】指数代码列表：{index_codes[:10]}... | 港股通代码列表：{ggt_codes[:10]}... | 其他港股代码列表：{normal_hk_codes[:10]}...")
    logger.debug(f"【已下载代码-详情】数量：{len(downloaded_codes)} | 前10个：{downloaded_codes[:10]}...")

    # 初始化下载统计
    download_stats = {
        "index_success": 0, "index_fail": 0,
        "ggt_success": 0, "ggt_fail": 0,
        "normal_hk_success": 0, "normal_hk_fail": 0
    }

    # 阶段1：下载指数
    logger.info("\n" + "-" * 50)
    logger.info("【阶段1：下载指数数据（优先级最高）】")
    logger.info("-" * 50)
    for idx, code in enumerate(index_codes, 1):
        progress = (idx / len(index_codes)) * 100
        if code in downloaded_codes:
            logger.info(f"📌 进度{idx}/{len(index_codes)} ({progress:.1f}%) | {code} | 已下载，跳过")
            download_stats["index_success"] += 1
            continue
        df_data = download_single_security(code, START_DATE, end_date)
        if save_security_data(df_data, code):
            download_stats["index_success"] += 1
        else:
            download_stats["index_fail"] += 1
            logger.debug(f"【指数下载失败-统计】代码：{code} | 进度：{idx}/{len(index_codes)} | 成功率：{download_stats['index_success']/(idx)*100 if idx>0 else 0:.1f}%")
        time.sleep(random.uniform(SLEEP_BASE, SLEEP_BASE + SLEEP_RANDOM))

    # 阶段2：下载港股通个股
    logger.info("\n" + "-" * 50)
    logger.info("【阶段2：下载港股通个股数据（优先级中等）】")
    logger.info("-" * 50)
    for idx, code in enumerate(ggt_codes, 1):
        progress = (idx / len(ggt_codes)) * 100
        if code in downloaded_codes:
            logger.info(f"📌 进度{idx}/{len(ggt_codes)} ({progress:.1f}%) | {code} | 已下载，跳过")
            download_stats["ggt_success"] += 1
            continue
        df_data = download_single_security(code, START_DATE, end_date)
        if save_security_data(df_data, code):
            download_stats["ggt_success"] += 1
        else:
            download_stats["ggt_fail"] += 1
            logger.debug(f"【港股通下载失败-统计】代码：{code} | 进度：{idx}/{len(ggt_codes)} | 成功率：{download_stats['ggt_success']/(idx)*100 if idx>0 else 0:.1f}%")
        time.sleep(random.uniform(SLEEP_BASE, SLEEP_BASE + SLEEP_RANDOM))

    # 阶段3：下载其他港股个股
    logger.info("\n" + "-" * 50)
    logger.info("【阶段3：下载其他港股个股数据（优先级最低）】")
    logger.info("-" * 50)
    for idx, code in enumerate(normal_hk_codes, 1):
        progress = (idx / len(normal_hk_codes)) * 100
        if code in downloaded_codes:
            logger.info(f"📌 进度{idx}/{len(normal_hk_codes)} ({progress:.1f}%) | {code} | 已下载，跳过")
            download_stats["normal_hk_success"] += 1
            continue
        df_data = download_single_security(code, START_DATE, end_date)
        if save_security_data(df_data, code):
            download_stats["normal_hk_success"] += 1
        else:
            download_stats["normal_hk_fail"] += 1
            logger.debug(f"【其他港股下载失败-统计】代码：{code} | 进度：{idx}/{len(normal_hk_codes)} | 成功率：{download_stats['normal_hk_success']/(idx)*100 if idx>0 else 0:.1f}%")
        time.sleep(random.uniform(SLEEP_BASE, SLEEP_BASE + SLEEP_RANDOM))

    # 下载完成：输出最终统计
    logger.info("\n" + "=" * 60)
    logger.info("【港股数据批量下载任务 - 执行完成】")
    logger.info(f"📊 指数：尝试{len(index_codes)} | 成功{download_stats['index_success']} | 失败{download_stats['index_fail']}")
    logger.info(f"📊 港股通个股：尝试{len(ggt_codes)} | 成功{download_stats['ggt_success']} | 失败{download_stats['ggt_fail']}")
    logger.info(f"📊 其他港股：尝试{len(normal_hk_codes)} | 成功{download_stats['normal_hk_success']} | 失败{download_stats['normal_hk_fail']}")
    total_try = len(index_codes) + len(ggt_codes) + len(normal_hk_codes)
    total_success = download_stats['index_success'] + download_stats['ggt_success'] + download_stats['normal_hk_success']
    success_rate = total_success / total_try * 100 if total_try > 0 else 0.0
    logger.info(f"📈 整体成功率：{success_rate:.1f}% ({total_success}/{total_try})")
    logger.info("=" * 60)
    logger.debug(f"【任务完成-全量统计】{download_stats} | 整体成功率：{success_rate:.1f}% | 总尝试数：{total_try} | 总成功数：{total_success}")

# 全局目标列
TARGET_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount", "amplitude", "pctChg", "chgAmt", "turn"]

if __name__ == "__main__":
    main()