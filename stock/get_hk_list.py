import akshare as ak
import pandas as pd
import os
import logging
from datetime import date, datetime
from requests.exceptions import ConnectionError, Timeout, ReadTimeout
from http.client import RemoteDisconnected

# ===================== 全局配置（新增港股通兜底文件路径） =====================
BASE_DATA_PATH = "./data"  # 与A股清单同目录
HK_LIST_PATH = os.path.join(BASE_DATA_PATH, "HK_List.csv")  # 统一输出文件命名
GGT_XLS_PATH = os.path.join(BASE_DATA_PATH, "GGTBDZQMD.xls")  # 港股通兜底静态清单路径

# 全局配置1：证券类型映射（与A股脚本一致）
STOCK_TYPE_MAP = {
    "股票": "股票",
    "指数": "指数"
}

# ===================== 日志配置（修复Windows编码问题，显式指定UTF-8） =====================
def config_logger():
    """配置日志输出：时间+等级+内容，分级输出（INFO/WARNING/ERROR），修复Windows编码问题"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # 避免重复添加处理器
    if logger.handlers:
        logger.handlers.clear()
    
    # 日志格式（与A股脚本完全一致）
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    
    # 1. 文件处理器：保存日志到文件，指定UTF-8编码
    file_handler = logging.FileHandler("./logs/hk_list_get.log", encoding="utf-8")
    file_handler.setFormatter(fmt)
    # 2. 控制台处理器：输出到终端，指定UTF-8编码（修复Windows GBK编码问题）
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    # 添加工厂
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

# 初始化全局日志对象（与A股脚本风格一致）
logger = config_logger()

# ===================== 工具函数（保留所有优化，新增港股通相关适配） =====================
def normalize_hk_code(code, is_index=False):
    """
    标准化港股代码（核心：区分个股/指数前缀，hk.（个股）/hki.（指数））
    - 个股（is_index=False）：纯数字→补零到5位+hk.前缀（如1→hk.00001、06680→hk.06680）
    - 指数（is_index=True）：字母+数字/纯字母→保留原始格式+hki.前缀（如CES100→hki.CES100）
    :param code: 原始港股代码（个股/指数/港股通）
    :param is_index: 是否为指数代码，False=个股/港股通，True=指数
    :return: 标准化代码，空值/异常返回""
    """
    try:
        code_str = str(code).strip()
        # 第一步：清理常见冗余后缀/前缀（统一预处理，适配港股通接口代码）
        code_str = code_str.replace("HK", "").replace(".HK", "").replace("-HK", "").replace(" ", "")
        # 空值直接返回
        if not code_str:
            return ""
        
        # 第二步：区分个股/指数，拼接不同前缀
        prefix = "hki." if is_index else "hk."
        if code_str.isdigit():
            # 个股/港股通：纯数字→补零到5位+对应前缀（适配港股通接口的06680/1347等格式）
            normalized_code = code_str.zfill(5)
            return f"{prefix}{normalized_code}"
        else:
            # 指数：含字母/特殊字符→保留原始格式+对应前缀
            return f"{prefix}{code_str}"
    except Exception as e:
        logger.warning(f"代码标准化失败 {code}: {str(e)[:50]}")
        return ""

def single_api_call(func, *args, **kwargs):
    """单次API调用：捕获网络异常，适配高失败率接口，核心容错函数"""
    try:
        result = func(*args, **kwargs)
        logger.info(f"{func.__name__} 调用成功")
        return result
    except (ConnectionError, Timeout, ReadTimeout, RemoteDisconnected) as e:
        logger.warning(f"{func.__name__} 网络连接异常: {str(e)[:50]}")
        return None
    except Exception as e:
        logger.error(f"{func.__name__} 调用异常: {str(e)[:50]}")
        return None

def create_folder():
    """创建数据目录，不存在则建（复用A股脚本逻辑）"""
    if not os.path.exists(BASE_DATA_PATH):
        os.makedirs(BASE_DATA_PATH)
        logger.info(f"创建数据文件夹：{BASE_DATA_PATH}")

def save_to_csv(df: pd.DataFrame, file_path: str):
    """幂等性保存CSV文件（参考A股脚本）：自动创建目录，utf-8-sig编码避免中文乱码"""
    create_folder()
    if df.empty:
        logger.error("保存失败：数据为空")
        return False
    try:
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        logger.info(f"港股清单保存成功！")
        logger.info(f"保存路径：{os.path.abspath(file_path)}")
        logger.info(f"有效数据量：{len(df)} 条（含港股/港股通+指数）")
        return True
    except Exception as e:
        logger.error(f"港股清单保存失败：{str(e)[:50]}")
        return False

def is_today_file(file_path: str) -> bool:
    """判断文件是否存在且最后修改时间为当天（参考A股缓存逻辑，原封不动）"""
    if not os.path.exists(file_path):
        logger.info(f"目标文件{file_path}不存在，将重新获取数据")
        return False
    # 获取文件最后修改时间并转换为日期
    file_mtime = os.path.getmtime(file_path)
    file_date = datetime.fromtimestamp(file_mtime).date()
    today = date.today()
    if file_date == today:
        logger.info(f"目标文件{file_path}存在且为今日更新，直接读取本地文件")
        return True
    else:
        logger.info(f"目标文件{file_path}存在但非今日更新，将重新获取数据")
        return False

# ===================== 新增：港股通兜底清单读取（基于GGTBDZQMD.xls） =====================
def get_ggt_xls_components_set() -> set:
    """
    读取港股通兜底静态清单（GGTBDZQMD.xls），生成标准化代码集合（兜底逻辑）
    若文件不存在/读取失败/无数据，返回空集合；成功则返回标准化后的港股通代码集合
    :return: 港股通标准化代码集合（hk.XXXX格式），失败返回空set()
    """
    logger.info("===== 开始读取港股通兜底静态清单（GGTBDZQMD.xls） =====")
    # 检查兜底文件是否存在
    if not os.path.exists(GGT_XLS_PATH):
        logger.warning(f"港股通兜底清单文件不存在：{GGT_XLS_PATH}，跳过兜底逻辑")
        return set()
    
    # 读取xls文件，处理各类异常
    try:
        # 强制将证券代码读取为字符串（避免00001被解析成1），适配xls格式
        df_xls = pd.read_excel(GGT_XLS_PATH, dtype={"证券代码": str})
        if df_xls.empty:
            logger.warning("港股通兜底清单文件为空，跳过兜底逻辑")
            return set()
        
        # 提取证券代码并标准化
        ggt_xls_set = set()
        raw_count = len(df_xls)
        if "证券代码" in df_xls.columns:
            for code in df_xls["证券代码"]:
                if pd.isna(code):
                    continue
                # 标准化为个股格式（hk.前缀，补零5位），与接口数据格式对齐
                norm_code = normalize_hk_code(code, is_index=False)
                if norm_code:
                    ggt_xls_set.add(norm_code)
        
        # 统计兜底数据有效数量
        valid_count = len(ggt_xls_set)
        logger.info(f"港股通兜底清单解析完成：有效代码{valid_count}只 / 原始数据{raw_count}只")
        return ggt_xls_set
    except ImportError as e:
        # 缺少xlrd依赖（读取xls需要）的特殊异常
        logger.error(f"读取xls失败：缺少依赖库（需安装 xlrd），错误：{str(e)[:50]}")
        return set()
    except Exception as e:
        logger.error(f"读取港股通兜底清单异常：{str(e)[:50]}")
        return set()

# ===================== 核心：港股通成份股获取（接口+兜底双重保障） =====================
def get_hk_ggt_components_set() -> set:
    """
    获取港股通成份股的标准化代码集合（接口+兜底双重保障）
    1. 优先调用东方财富接口获取实时数据
    2. 接口失败/无数据时，自动读取本地GGTBDZQMD.xls兜底清单
    :return: 港股通标准化代码集合，失败/空数据返回空set()
    """
    # 第一步：调用接口获取实时港股通数据
    logger.info("===== 开始获取港股通成份股数据（东方财富接口） =====")
    df_ggt = single_api_call(ak.stock_hk_ggt_components_em)
    ggt_api_set = set()
    
    # 接口成功则解析数据
    if df_ggt is not None and not df_ggt.empty:
        raw_count = len(df_ggt)
        if "代码" in df_ggt.columns:
            for code in df_ggt["代码"]:
                if pd.isna(code):
                    continue
                norm_code = normalize_hk_code(code, is_index=False)
                if norm_code:
                    ggt_api_set.add(norm_code)
        valid_api_count = len(ggt_api_set)
        logger.info(f"港股通接口数据解析完成：{valid_api_count} 只 / {raw_count} 只")
    else:
        logger.warning("港股通成份股接口调用失败/返回空数据，启用兜底清单逻辑")
    
    # 第二步：读取兜底清单数据
    ggt_xls_set = get_ggt_xls_components_set()
    
    # 第三步：合并接口+兜底数据（去重，双重保障）
    ggt_code_set = ggt_api_set.union(ggt_xls_set)
    
    # 日志输出合并结果
    if ggt_api_set and ggt_xls_set:
        logger.info(f"港股通集合合并完成：接口{len(ggt_api_set)}只 + 兜底{len(ggt_xls_set)}只 = 总计{len(ggt_code_set)}只")
    elif ggt_api_set:
        logger.info(f"仅使用港股通接口数据：{len(ggt_api_set)}只")
    elif ggt_xls_set:
        logger.info(f"接口失败，使用港股通兜底数据：{len(ggt_xls_set)}只")
    else:
        logger.warning("港股通接口和兜底清单均无有效数据，跳过港股通判断")
    
    return ggt_code_set

# ===================== 核心：港股个股数据获取（保留处理前后数量统计） =====================
def get_hk_stock_basic():
    """获取港股个股基础数据：标准化为统一输出格式，新增处理前后数量统计"""
    logger.info("===== 开始获取港股个股数据（新浪接口） =====")
    df_sina = single_api_call(ak.stock_hk_spot)
    if df_sina is None or df_sina.empty:
        logger.error("新浪接口返回空数据，个股获取失败")
        return pd.DataFrame()
    
    # 统计处理前原始数据条数
    raw_count = len(df_sina)
    # 构建个股数据列表（适配统一输出字段）
    stock_list = []
    if "代码" in df_sina.columns and "中文名称" in df_sina.columns:
        for _, row in df_sina.iterrows():
            raw_code = row["代码"]
            raw_name = row["中文名称"]
            # 过滤空值
            if pd.isna(raw_code) or pd.isna(raw_name):
                continue
            # 标准化代码（个股，默认hk.前缀）
            norm_code = normalize_hk_code(raw_code)
            if not norm_code:
                continue
            # 按统一字段格式组装（与A股输出字段完全一致）
            stock_list.append({
                "证券类型": "股票",          # 个股标记为股票类型
                "证券代码": norm_code,      # hk.00001格式
                "证券名称": str(raw_name).strip(),
                "上市日期": "",             # 接口无该字段，留空
                "退市日期": "",             # 接口无该字段，留空
                "交易状态": "上市",         # 默认上市状态
                "所属市场及板块": "港股"    # 初始标注为港股，后续根据港股通结果修改
            })
    else:
        logger.error("新浪接口列名变化，未找到「代码」/「中文名称」列")
        return pd.DataFrame()
    
    # 转换为DataFrame并去重（按证券代码去重，避免重复数据）
    stock_df = pd.DataFrame(stock_list).drop_duplicates(subset=["证券代码"], keep="first")
    processed_count = len(stock_df)
    # 日志输出处理后/处理前的数量对比
    logger.info(f"港股个股标准化完成：{processed_count} 只 / {raw_count} 只")
    return stock_df

# ===================== 核心：港股指数数据获取（新浪+东财双接口，保留数量统计） =====================
def get_hk_index_basic():
    """获取港股指数基础数据：优先新浪接口，东财备选，保留处理前后数量统计"""
    logger.info("===== 开始获取港股指数数据（新浪+东财双接口） =====")
    # 优先调用新浪指数接口
    df_index_sina = single_api_call(ak.stock_hk_index_spot_sina)
    if df_index_sina is not None and not df_index_sina.empty:
        return _parse_index_data(df_index_sina, source="sina")
    
    # 新浪接口失败时调用东财接口
    logger.warning("新浪指数接口失败，尝试调用东财接口")
    df_index_em = single_api_call(ak.stock_hk_index_spot_em)
    if df_index_em is not None and not df_index_em.empty:
        return _parse_index_data(df_index_em, source="em")
    
    logger.error("所有指数接口均返回空数据，指数获取失败")
    return pd.DataFrame()

def _parse_index_data(df: pd.DataFrame, source: str):
    """解析指数数据：适配不同接口格式，指数用hki.前缀，保留处理前后数量统计"""
    # 统计处理前原始数据条数
    raw_count = len(df)
    index_list = []
    # 新浪接口字段映射
    if source == "sina" and "代码" in df.columns and "名称" in df.columns:
        for _, row in df.iterrows():
            raw_code = row["代码"]
            raw_name = row["名称"]
            if pd.isna(raw_code) or pd.isna(raw_name):
                continue
            # 标准化代码（指数，指定is_index=True，hki.前缀）
            norm_code = normalize_hk_code(raw_code, is_index=True)
            if not norm_code:
                continue
            index_list.append({
                "证券类型": "指数",          # 指数标记为指数类型
                "证券代码": norm_code,      # hki.CES100格式
                "证券名称": str(raw_name).strip(),
                "上市日期": "",             # 接口无该字段，留空
                "退市日期": "",             # 接口无该字段，留空
                "交易状态": "上市",         # 默认上市状态
                "所属市场及板块": "港股"    # 指数固定标注为港股，不参与港股通判断
            })
    # 东财接口字段映射
    elif source == "em" and "代码" in df.columns and "名称" in df.columns:
        for _, row in df.iterrows():
            raw_code = row["代码"]
            raw_name = row["名称"]
            if pd.isna(raw_code) or pd.isna(raw_name):
                continue
            # 标准化代码（指数，指定is_index=True，hki.前缀）
            norm_code = normalize_hk_code(raw_code, is_index=True)
            if not norm_code:
                continue
            index_list.append({
                "证券类型": "指数",
                "证券代码": norm_code,
                "证券名称": str(raw_name).strip(),
                "上市日期": "",
                "退市日期": "",
                "交易状态": "上市",
                "所属市场及板块": "港股"
            })
    else:
        logger.error(f"{source}接口列名变化，未找到「代码」/「名称」列")
        return pd.DataFrame()
    
    # 转换为DataFrame并去重（按证券代码去重）
    index_df = pd.DataFrame(index_list).drop_duplicates(subset=["证券代码"], keep="first")
    processed_count = len(index_df)
    # 日志输出处理后/处理前的数量对比
    logger.info(f"港股指数标准化完成：{processed_count} 条 / {raw_count} 条")
    return index_df

# ===================== 核心对外接口：整合所有逻辑（兼容原缓存） =====================
def get_hk_stock_list() -> pd.DataFrame:
    """
    核心对外接口（与A股get_a_stock_list完全对齐）：
    - 缓存逻辑：今日文件存在则读取本地，否则重新从akshare获取
    - 核心新增：港股通接口+兜底xls双重判断，接口失败不影响原逻辑
    - 数据整合：港股/港股通个股+指数统一格式输出
    - 字段规范：与A股清单完全一致的7个核心字段
    返回：包含7个指定字段的DataFrame，异常时返回空DataFrame（带字段名）
    """
    # 目标输出字段（与A股清单完全一致，固定顺序）
    target_columns = ['证券类型', '证券代码', '证券名称', '上市日期', '退市日期', '交易状态', '所属市场及板块']

    # 第一步：缓存逻辑（优先读取今日本地文件，避免重复调用接口）
    if is_today_file(HK_LIST_PATH):
        try:
            df = pd.read_csv(HK_LIST_PATH, encoding='utf-8-sig')
            # 校验字段是否完整，避免文件损坏导致的业务错误
            if all(col in df.columns for col in target_columns):
                return df
            else:
                logger.warning("本地文件字段不完整，将重新获取并生成数据")
        except Exception as e:
            logger.error(f"读取本地文件失败：{str(e)}，将重新获取并生成数据")

    # 第二步：重新获取数据（个股+指数），港股通双重保障
    try:
        # 1. 基础数据获取：个股+指数（原逻辑不变）
        stock_df = get_hk_stock_basic()
        index_df = get_hk_index_basic()
        
        # 2. 港股通判断：接口+兜底双重保障，失败返回空集合不影响原数据
        ggt_code_set = get_hk_ggt_components_set()
        # 若港股通集合非空，修改匹配个股的所属市场及板块为「港股通」
        if ggt_code_set and not stock_df.empty:
            match_count = stock_df[stock_df['证券代码'].isin(ggt_code_set)].shape[0]
            stock_df.loc[stock_df['证券代码'].isin(ggt_code_set), '所属市场及板块'] = '港股通'
            logger.info(f"港股通标的匹配完成，共{match_count}只个股标注为「港股通」")
        
        # 3. 整合个股+指数数据，忽略索引避免重复
        all_df = pd.concat([stock_df, index_df], ignore_index=True)
        # 4. 筛选指定字段并按固定顺序排列，去重避免个股/指数代码冲突
        final_df = all_df[target_columns].drop_duplicates(subset=["证券代码"], keep="first")
        # 5. 幂等性保存至本地CSV（自动创建目录，utf-8-sig避免中文乱码）
        save_to_csv(final_df, HK_LIST_PATH)
        return final_df
    except Exception as e:
        logger.error(f"调用akshare获取/处理港股数据失败：{str(e)}", exc_info=True)
        return pd.DataFrame(columns=target_columns)  # 异常返回空DF（带字段）

# ===================== 主函数：脚本入口（新增港股通数量统计，便于验证） =====================
def main():
    """主函数：直接运行脚本时的入口，调用核心接口完成港股清单全流程处理"""
    try:
        # 调用核心接口获取数据（自动走缓存逻辑，无需额外处理）
        hk_list_df = get_hk_stock_list()
        # 输出执行结果和示例数据，新增港股/港股通/指数数量统计
        logger.info(f"===== 港股清单处理完成 =====")
        if not hk_list_df.empty:
            total_count = len(hk_list_df)
            stock_count = len(hk_list_df[hk_list_df['证券类型'] == '股票'])
            index_count = len(hk_list_df[hk_list_df['证券类型'] == '指数'])
            ggt_count = len(hk_list_df[hk_list_df['所属市场及板块'] == '港股通'])
            normal_hk_count = stock_count - ggt_count
            # 总数量+分类数量统计
            logger.info(f"最终获取到{total_count}条港股有效记录 | 普通港股：{normal_hk_count}只 | 港股通：{ggt_count}只 | 指数：{index_count}条")
            # 分别输出港股通、普通港股、指数示例，便于验证
            if ggt_count > 0:
                ggt_sample = hk_list_df[hk_list_df['所属市场及板块'] == '港股通'].head(5)
                logger.info(f"前5条港股通数据示例：\n{ggt_sample.to_string(index=False)}")
            normal_hk_sample = hk_list_df[hk_list_df['所属市场及板块'] == '港股'].head(5)
            logger.info(f"前5条普通港股数据示例：\n{normal_hk_sample.to_string(index=False)}")
            index_sample = hk_list_df[hk_list_df['证券类型'] == '指数'].head(5)
            logger.info(f"前5条指数数据示例（hki.前缀）：\n{index_sample.to_string(index=False)}")
    except Exception as e:
        logger.error(f"港股清单脚本整体执行失败：{str(e)}", exc_info=True)

if __name__ == "__main__":
    main()