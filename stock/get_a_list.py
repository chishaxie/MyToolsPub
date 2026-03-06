import baostock as bs
import pandas as pd
import os
import logging
from datetime import date, datetime

from bt.bs_connection import login_baostock, check_baostock_login

# 全局配置1：证券类型数字编码→中文名称映射（baostock官方规则）
STOCK_TYPE_MAP = {
    '1': '股票',
    '2': '指数',
    '3': '基金',
    '4': '可转债',
    '5': 'ETF'
}

# 全局配置2：代码前缀→基础市场映射（优先判断，解决转债/指数未知问题）
MARKET_PREFIX_MAP = {
    'sh': '沪市',
    'sz': '深市',
    'bj': '北交所'
}

# 全局配置3：股票板块划分规则（仅对股票类型生效，key=(基础市场, 数字前缀), value=细分板块）
STOCK_BOARD_RULES = {
    # 沪市细分
    ('沪市', '60'): '沪市主板',
    ('沪市', '68'): '沪市科创板',
    # 深市细分
    ('沪市', '78'): '沪市科创板',
    ('深市', '00'): '深市主板',
    ('深市', '30'): '深市创业板',
    # 北交所无细分，统一为北交所
    ('北交所', '80'): '北交所'
}

def config_logger():
    """配置日志输出：时间+等级+内容，分级输出（INFO/WARNING/ERROR）"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(__name__)

# 初始化全局日志对象
logger = config_logger()

def init_baostock():
    """初始化baostock连接，幂等性设计"""
    login_baostock()
    check_baostock_login()
    logger.info("baostock连接初始化成功")

def get_all_stock_basic() -> pd.DataFrame:
    """获取所有证券基础信息（6个核心字段）"""
    rs = bs.query_stock_basic(code="")
    
    data_list = []
    while (rs.error_code == '0') and rs.next():
        data_list.append(rs.get_row_data())
    
    if not data_list:
        return pd.DataFrame()
    
    columns = ['证券代码', '证券名称', '上市日期', '退市日期', '证券类型', '交易状态']
    result_df = pd.DataFrame(data_list, columns=columns)
    logger.info(f"成功获取证券基础数据，共{len(result_df)}条记录")
    return result_df

def convert_stock_type(code: str) -> str:
    """
    将证券类型数字编码转换为可读中文名称
    :param code: 证券类型数字字符串（如'1'/'2'）
    :return: 可读中文名称，无匹配则返回'未知类型'
    """
    stock_type = STOCK_TYPE_MAP.get(code, '未知类型')
    if stock_type == '未知类型':
        logger.warning(f"无匹配的证券类型编码：{code}，标记为未知类型")
    return stock_type

def parse_stock_code(raw_code: str) -> tuple:
    """
    解析baostock证券代码，提取基础市场+纯数字代码
    :param raw_code: 原始代码（如sh.600000、sz.300059、bj.800001）
    :return: (基础市场, 纯数字代码)，解析失败返回('未知', '')
    """
    try:
        # 按.分割为前缀和数字部分，兼容无.的异常格式
        code_parts = raw_code.split('.')
        if len(code_parts) >= 2:
            prefix = code_parts[0].lower()  # 转小写避免大小写问题
            pure_code = code_parts[1]
            # 根据前缀获取基础市场
            base_market = MARKET_PREFIX_MAP.get(prefix, '未知')
            return (base_market, pure_code)
        else:
            # 无.的代码，直接标记为未知市场
            logger.warning(f"证券代码{raw_code}无市场前缀（无.分隔），解析失败")
            return ('未知', raw_code)
    except Exception as e:
        logger.warning(f"证券代码{raw_code}格式异常，解析失败：{str(e)}")
        return ('未知', '')

def get_stock_board(base_market: str, pure_code: str, stock_type: str) -> str:
    """
    根据基础市场+纯数字代码+证券类型，获取精细化的市场&板块名称
    规则：仅股票类型细分板块，非股票类型仅返回基础市场
    """
    # 非股票类型，直接返回基础市场
    if stock_type != '股票':
        return base_market
    
    # 股票类型，按前缀匹配细分板块
    for (market, code_prefix), board in STOCK_BOARD_RULES.items():
        if base_market == market and pure_code.startswith(code_prefix):
            return board
    
    # 股票类型但无匹配规则，返回基础市场并打警告
    logger.warning(f"股票类型-{base_market}-纯代码{pure_code}无匹配板块规则，仅标注基础市场")
    return base_market

def process_market_and_board(stock_df: pd.DataFrame) -> pd.DataFrame:
    """
    核心处理函数：转换证券类型为中文 + 解析代码补充【所属市场及板块】字段
    解决转债/指数交易所未知问题，同时对股票精细化划分科创/创业板/主板/北交所
    """
    new_df = stock_df.copy()
    # 初始化精细化市场板块字段（替换原交易所字段）
    new_df['所属市场及板块'] = ""
    total = len(new_df)
    logger.info(f"开始处理{total}只证券：转换类型+匹配市场板块，每500条输出一次进度（提升效率）")

    for idx, row in new_df.iterrows():
        raw_code = row['证券代码']
        type_code = row['证券类型']

        # 步骤1：转换证券类型为可读中文名称
        stock_type_cn = convert_stock_type(type_code)
        new_df.loc[idx, '证券类型'] = stock_type_cn

        # 步骤2：解析原始代码，获取基础市场+纯数字代码
        base_market, pure_code = parse_stock_code(raw_code)

        # 步骤3：根据市场+代码+类型，获取精细化市场板块
        market_board = get_stock_board(base_market, pure_code, stock_type_cn)
        new_df.loc[idx, '所属市场及板块'] = market_board

        # 进度提示：每500条输出一次，减少日志刷屏
        if (idx + 1) % 500 == 0:
            logger.info(f"处理进度：{idx+1}/{total} | 示例：{raw_code} → {stock_type_cn} → {market_board}")

    logger.info("证券类型转换+市场板块匹配完成，无更多未知市场")
    return new_df

def save_to_csv(df: pd.DataFrame, file_path: str):
    """幂等性保存CSV文件，自动创建目录，编码utf-8-sig避免中文乱码"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    logger.info(f"数据已成功保存至文件：{file_path}，共{len(df)}条记录")

def is_today_file(file_path: str) -> bool:
    """判断文件是否存在，且文件最后修改时间为当天"""
    if not os.path.exists(file_path):
        logger.info(f"目标文件{file_path}不存在，将重新获取数据")
        return False
    # 获取文件最后修改时间（本地时间），转换为日期对象
    file_mtime = os.path.getmtime(file_path)
    file_date = datetime.fromtimestamp(file_mtime).date()
    today = date.today()
    if file_date == today:
        logger.info(f"目标文件{file_path}存在且为今日更新，直接读取本地文件")
        return True
    else:
        logger.info(f"目标文件{file_path}存在但非今日更新，将重新获取数据")
        return False

def get_a_stock_list() -> pd.DataFrame:
    """
    核心对外接口：获取全类型证券清单（带今日缓存逻辑）
    逻辑：文件存在+今日更新 → 读取本地文件；否则baostock获取→处理→保存→返回
    返回：包含7个指定字段的DataFrame（证券类型/代码/名称/上市/退市/交易状态/所属市场及板块）
    """
    file_path = "./data/A_List.csv"
    # 目标输出字段（固定顺序，替换原交易所为【所属市场及板块】）
    target_columns = ['证券类型', '证券代码', '证券名称', '上市日期', '退市日期', '交易状态', '所属市场及板块']

    # 缓存逻辑：今日文件存在则直接读取
    if is_today_file(file_path):
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            # 校验字段是否完整，避免文件损坏导致的业务错误
            if all(col in df.columns for col in target_columns):
                return df
            else:
                logger.warning("本地文件字段不完整，将重新获取并生成数据")
        except Exception as e:
            logger.error(f"读取本地文件失败：{str(e)}，将重新获取并生成数据")

    # 非今日文件/文件损坏/文件不存在 → 调用baostock接口获取数据
    try:
        init_baostock()
        # 1. 获取基础原始数据
        basic_df = get_all_stock_basic()
        if basic_df.empty:
            raise Exception("未获取到任何证券基础数据，终止流程")
        # 2. 转换证券类型+匹配精细化市场板块
        processed_df = process_market_and_board(basic_df)
        # 3. 筛选指定字段并按固定顺序排列
        final_df = processed_df[target_columns].copy()
        # 4. 保存至本地CSV
        save_to_csv(final_df, file_path)
        return final_df
    except Exception as e:
        logger.error(f"调用baostock获取/处理数据失败：{str(e)}", exc_info=True)
        raise  # 抛出异常，让调用方感知错误
    finally:
        # 无论是否成功，最终都关闭baostock连接，避免连接泄漏
        # bs.logout()
        logger.info("baostock连接已关闭")

def main():
    """主函数：直接运行脚本时的入口，调用核心接口完成数据处理"""
    try:
        # 调用核心接口获取数据（自动走缓存逻辑）
        stock_list_df = get_a_stock_list()
        # 输出部分示例数据，验证效果
        logger.info(f"脚本执行完成，最终获取到{len(stock_list_df)}条全类型证券有效记录")
        logger.info(f"前5条数据示例：\n{stock_list_df.head().to_string(index=False)}")
    except Exception as e:
        logger.error(f"脚本整体执行失败：{str(e)}", exc_info=True)

if __name__ == "__main__":
    main()