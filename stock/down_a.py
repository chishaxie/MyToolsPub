import baostock as bs
import pandas as pd
import os
import datetime
import logging
import warnings
import json
import signal

from get_a_list import get_a_stock_list
from trading_day import get_last_trading_day

from bt.bs_connection import login_baostock, logout_baostock, check_baostock_login

warnings.filterwarnings('ignore')

# ===================== 配置项 =====================
START_DATE = "2014-01-01"
# 修改点：只保留一个核心数据文件夹，存储完整数据
DATA_FOLDER = "data/a" 

# 缓存配置
CACHE_FILE_PATH = "./data/__down_a__.json"
CACHE_EXPIRE_DAYS = 1  # 缓存有效期（天）

# 修改点：直接使用完整字段列表
FIELDS = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST"

# 复权与校验配置
ADJUST_FLAG = "2" # 前复权
CHECK_DAYS = 15
# 校验时用来对比的字段（这些字段必须包含在 FIELDS 中）
CHECK_FIELDS = ["open", "high", "low", "close", "volume"]
FLOAT_TOLERANCE = 1e-6

# ===================== 全局变量 =====================
# 内存缓存字典
EMPTY_CODE_CACHE = {}
# 标记是否需要保存缓存
NEED_SAVE_CACHE = False

# ===================== 日志配置 =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ===================== 信号处理 =====================
def handle_interrupt(signum, frame):
    """处理Ctrl+C中断，保存缓存后退出"""
    logger.warning("\n捕获到Ctrl+C中断信号，正在保存缓存...")
    save_empty_code_cache()
    logger.info("缓存保存完成，程序退出")
    exit(0)

signal.signal(signal.SIGINT, handle_interrupt)

# ===================== 缓存相关函数 =====================
def init_cache_folder():
    """初始化缓存文件夹"""
    cache_dir = os.path.dirname(CACHE_FILE_PATH)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

def load_empty_code_cache_once():
    """加载缓存到内存"""
    global EMPTY_CODE_CACHE
    init_cache_folder()
    
    if not os.path.exists(CACHE_FILE_PATH):
        EMPTY_CODE_CACHE = {}
        return
    
    try:
        with open(CACHE_FILE_PATH, "r", encoding="utf-8") as f:
            cache_data = json.load(f)
        
        current_time = datetime.datetime.now()
        valid_cache = {}
        for code, info in cache_data.items():
            try:
                cache_time = datetime.datetime.strptime(info["cache_time"], "%Y-%m-%d %H:%M:%S")
                if (current_time - cache_time).days < CACHE_EXPIRE_DAYS:
                    valid_cache[code] = info
            except Exception:
                pass
        
        EMPTY_CODE_CACHE = valid_cache
        logger.info(f"缓存加载完成：有效缓存{len(EMPTY_CODE_CACHE)}个")
    except Exception as e:
        logger.warning(f"加载缓存文件失败：{e}，初始化空缓存")
        EMPTY_CODE_CACHE = {}

def save_empty_code_cache():
    """保存内存缓存到文件"""
    global NEED_SAVE_CACHE
    if not NEED_SAVE_CACHE or not EMPTY_CODE_CACHE:
        return
    
    init_cache_folder()
    try:
        with open(CACHE_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(EMPTY_CODE_CACHE, f, ensure_ascii=False, indent=2)
        logger.info(f"缓存保存成功：共{len(EMPTY_CODE_CACHE)}条记录")
        NEED_SAVE_CACHE = False
    except Exception as e:
        logger.error(f"保存缓存文件失败：{e}")

def add_empty_code_to_cache(code, empty_type="no_interface_data"):
    """新增无接口数据代码到内存缓存"""
    global EMPTY_CODE_CACHE, NEED_SAVE_CACHE
    if code not in EMPTY_CODE_CACHE:
        EMPTY_CODE_CACHE[code] = {
            "empty_type": empty_type,
            "cache_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        NEED_SAVE_CACHE = True

def is_code_in_empty_cache(code):
    """检查是否在缓存中"""
    return code in EMPTY_CODE_CACHE and EMPTY_CODE_CACHE[code]["empty_type"] == "no_interface_data"

# ===================== 核心逻辑函数 =====================
def create_folder():
    """创建数据文件夹"""
    if not os.path.exists(DATA_FOLDER): 
        os.makedirs(DATA_FOLDER)
    init_cache_folder()

def get_existing_max_date(code):
    """获取本地文件中的最大日期"""
    file_path = os.path.join(DATA_FOLDER, f"{code}.csv")
    if not os.path.exists(file_path):
        return START_DATE
    try:
        # 只读取date列以加快速度
        df = pd.read_csv(file_path, usecols=['date'], parse_dates=['date'])
        if df.empty: return START_DATE
        return df['date'].max().strftime("%Y-%m-%d")
    except Exception:
        return START_DATE

def get_check_date_range(existing_max_date):
    """计算校验日期范围"""
    if existing_max_date == START_DATE:
        return START_DATE, existing_max_date
    try:
        max_date_dt = datetime.datetime.strptime(existing_max_date, "%Y-%m-%d")
        check_start_dt = max_date_dt - datetime.timedelta(days=CHECK_DAYS)
        check_start_date = check_start_dt.strftime("%Y-%m-%d")
        if check_start_date < START_DATE: check_start_date = START_DATE
        return check_start_date, existing_max_date
    except Exception:
        return START_DATE, existing_max_date

def get_baostock_data(code, start_date, end_date):
    """
    获取baostock数据（完整字段）
    """
    rs = bs.query_history_k_data_plus(
        code=code,
        fields=FIELDS, # 使用完整字段
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag=ADJUST_FLAG
    )
    
    if rs.error_code != '0':
        raise RuntimeError(f"Baostock API Error | Code: {rs.error_code} | Msg: {rs.error_msg}")
    
    df = rs.get_data()
    
    if df.empty:
        return None
    
    df['date'] = pd.to_datetime(df['date']).dt.strftime("%Y-%m-%d")
    # 转换数值类型以便校验和计算
    for field in CHECK_FIELDS:
        df[field] = pd.to_numeric(df[field], errors='coerce')
    
    df = df.drop_duplicates(subset=['date'], keep='last')
    return df

def check_data_consistency(code, existing_max_date):
    """校验数据一致性"""
    if existing_max_date == START_DATE:
        return None, False
    
    check_start, check_end = get_check_date_range(existing_max_date)
    if check_start == check_end:
        return None, False
    
    file_path = os.path.join(DATA_FOLDER, f"{code}.csv")
    
    try:
        # 读取本地数据用于校验
        local_df = pd.read_csv(file_path, parse_dates=['date'])
        local_df['date'] = local_df['date'].dt.strftime("%Y-%m-%d")
        
        # 筛选出校验时间段的数据
        local_check_df = local_df[
            (local_df['date'] >= check_start) & (local_df['date'] <= check_end)
        ][['date'] + CHECK_FIELDS].copy()
        
        for field in CHECK_FIELDS:
            local_check_df[field] = pd.to_numeric(local_check_df[field], errors='coerce')
        
        if local_check_df.empty: return None, False
    except Exception:
        # 读取失败视为不一致，重新下载
        return None, False
    
    # 获取远程数据进行比对
    remote_df = get_baostock_data(code, check_start, check_end)
    
    if remote_df is None: 
        return None, False
        
    remote_check_df = remote_df[['date'] + CHECK_FIELDS].copy()
    
    merge_df = pd.merge(
        local_check_df, remote_check_df,
        on='date', suffixes=('_local', '_remote'),
        how='inner'
    )
    if merge_df.empty: return None, False
    
    inconsistent_fields = []
    for field in CHECK_FIELDS:
        local_col = f"{field}_local"
        remote_col = f"{field}_remote"
        diff = abs(merge_df[local_col] - merge_df[remote_col])
        if (diff > FLOAT_TOLERANCE).any():
            inconsistent_fields.append(field)
    
    if inconsistent_fields:
        logger.warning(f"{code}数据不一致（字段：{inconsistent_fields}），触发全量更新")
        return False, True
    return True, False

def download_single_security(code, start_date, end_date, is_full_download=False):
    """
    下载单个证券数据并保存到 data/a
    """
    # 获取数据（异常由外部捕获）
    full_df = get_baostock_data(code, start_date, end_date)
    
    # 情况A：接口正常，但真的没有数据 -> 缓存
    if full_df is None:
        return "no_interface", "无接口数据"
    
    # 情况B：DataFrame为空（双重检查）
    if full_df.empty:
        return "empty", "无增量数据"
    
    # 情况C：有数据，执行保存
    file_path = os.path.join(DATA_FOLDER, f"{code}.csv")
    new_count = len(full_df)
    
    # 如果是全量下载，或者文件不存在，直接写入
    if is_full_download or not os.path.exists(file_path):
        full_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        msg = f"全量/新建：{new_count}条"
        return "success", msg
    
    # 增量下载：读取现有文件，追加新日期
    try:
        existing_df = pd.read_csv(file_path, usecols=['date'])
        existing_dates = set(existing_df['date'].tolist())
        
        # 过滤掉已存在的日期
        new_df = full_df[~full_df['date'].isin(existing_dates)]
        
        if not new_df.empty:
            new_df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig')
            msg = f"增量下载：新增{len(new_df)}条"
        else:
            msg = "日期重叠，无实际新增"
            return "empty", msg
            
    except Exception as e:
        # 如果读取旧文件失败，回退到覆盖写入
        logger.warning(f"{code} 增量合并失败（{e}），回退到全量覆盖")
        full_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        msg = f"覆盖写入：{new_count}条"

    return "success", msg

# ===================== 主函数 =====================
def main():
    global NEED_SAVE_CACHE
    create_folder()
    load_empty_code_cache_once()
    
    try:
        last_trading_day = get_last_trading_day()
        logger.info(f"下载任务开始 | 截止日期：{last_trading_day}")
    except Exception as e:
        logger.error(f"初始化失败：{e}")
        return

    # 登录
    try:
        login_baostock()
        check_baostock_login()
    except Exception as e:
        logger.error(f"登录异常：{e}")
        return

    # 获取列表
    try:
        stock_list_df = get_a_stock_list()
        all_codes = stock_list_df['证券代码'].drop_duplicates().tolist()
        total_count = len(all_codes)
        logger.info(f"待处理总数：{total_count}")
    except Exception as e:
        logger.error(f"获取证券列表失败：{e}")
        return

    success_count = 0
    empty_count = 0
    failed_list = []

    for idx, code in enumerate(all_codes, 1):
        # 1. 检查缓存（无需网络）
        if is_code_in_empty_cache(code):
            empty_count += 1
            logger.info(f"[{idx}/{total_count}] {code} | 跳过 (缓存无数据)")
            continue
        
        # 2. 检查本地日期（无需网络）
        existing_max_date = get_existing_max_date(code)
        if existing_max_date >= last_trading_day:
            empty_count += 1
            logger.info(f"[{idx}/{total_count}] {code} | 跳过 (已是最新 {existing_max_date})")
            continue
        
        # 3. 核心处理流程（包含网络请求）
        try:
            # 3.1 一致性校验（可能触发API调用）
            is_consistent, need_full_download = check_data_consistency(code, existing_max_date)
            
            if need_full_download:
                download_start = START_DATE
                is_full = True
            else:
                download_start = (datetime.datetime.strptime(existing_max_date, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                is_full = False
            
            # 3.2 下载数据
            status, msg = download_single_security(code, download_start, last_trading_day, is_full)
            
            # 3.3 结果处理
            if status == "no_interface":
                add_empty_code_to_cache(code)
                empty_count += 1
                logger.info(f"[{idx}/{total_count}] {code} | 无数据 (已缓存)")
            
            elif status == "success":
                success_count += 1
                logger.info(f"[{idx}/{total_count}] {code} | 成功 | {msg}")
            
            elif status == "empty":
                empty_count += 1
                logger.info(f"[{idx}/{total_count}] {code} | 无增量")
            
            else:
                raise RuntimeError(msg)

        except Exception as e:
            err_msg = str(e).replace('\n', ' ')[:100]
            failed_list.append((code, err_msg))
            logger.error(f"[{idx}/{total_count}] {code} | 失败 | {err_msg}")
        
    save_empty_code_cache()

    logger.info(f"\n===== 任务摘要 =====")
    logger.info(f"总数：{total_count} | 成功：{success_count} | 无需更新：{empty_count} | 失败：{len(failed_list)}")
    if failed_list:
        logger.info(f"失败列表（部分）：{failed_list[:5]}")

    # 退出清理
    try:
        bs.logout()
    except Exception:
        pass

if __name__ == "__main__":
    main()