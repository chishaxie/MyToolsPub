# bs_connection.py：统一管理baostock连接状态，避免子模块重复登录/登出
import baostock as bs

# 全局状态标记：记录是否已登录（替代不存在的bs.is_login()）
BAOSTOCK_LOGGED_IN = False

def login_baostock() -> bool:
    """
    唯一登录入口：确保全局只登录一次
    返回：登录成功返回True，失败返回False
    """
    global BAOSTOCK_LOGGED_IN
    if BAOSTOCK_LOGGED_IN:
        return True
    
    try:
        lg = bs.login()
        if lg.error_code == '0':
            BAOSTOCK_LOGGED_IN = True
            return True
        else:
            print(f"baostock登录失败：{lg.error_msg}")
            return False
    except Exception as e:
        print(f"baostock登录异常：{str(e)}")
        return False

def logout_baostock() -> None:
    """
    唯一登出入口：确保全局只登出一次
    """
    global BAOSTOCK_LOGGED_IN
    if BAOSTOCK_LOGGED_IN:
        bs.logout()
        BAOSTOCK_LOGGED_IN = False

def check_baostock_login() -> None:
    """
    检查登录状态：未登录则抛异常，供子模块调用
    """
    if not BAOSTOCK_LOGGED_IN:
        raise ConnectionError(
            "baostock未登录！请先在主模块执行 bs_connection.login_baostock() 完成登录"
        )