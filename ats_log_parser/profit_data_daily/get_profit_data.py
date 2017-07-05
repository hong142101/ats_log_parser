# -*- coding: utf-8 -*-
import os
import re
import pandas as pd
from .columns_struct import *
from .parse_others import ParseSymbolTree
from .parse_others import get_new_profit_data, clean_profit_data
from .connect_mysql import *


# ----------------------------------------------------------------------------------------------------------------------
def connect_database(settle_info):
    db = TradeDatabase()
    db.connect(ip=settle_info.dbase_ip,
               user=settle_info.dbase_acc,
               password=settle_info.dbase_pw,
               database=settle_info.database_name)
    md = MarketDatabase()
    md.connect(ip=settle_info.tick_dbase_ip,
               user=settle_info.tick_dbase_acc,
               password=settle_info.tick_dbase_pw,
               port=settle_info.tick_port,
               database=settle_info.tick_database_name)
    ci = ContractInfo()
    ci.connect(ip=settle_info.contract_dbase_ip,
               user=settle_info.contract_dbase_acc,
               password=settle_info.contract_dbase_pw,
               port=settle_info.contract_port,
               database=settle_info.contract_database_name)
    return db, md, ci


# ----------------------------------------------------------------------------------------------------------------------
def get_position_profit_data(accounts_info, settle_info):
    """
    遍历各个账户的信息并得到结果
    
    :param accounts_info: 账户信息
    :param settle_info: 取初始值类里的数据库信息
    :return: 
    """
    db, md, ci = connect_database(settle_info)
    pst = ParseSymbolTree()
    account = accounts_info['account']
    file_path = accounts_info['file_path']
    trade_date_list = db.get_trade_date_list(account)
    try:
        old_profit_data = pd.read_csv(file_path).values.tolist()
        old_last_date = old_profit_data[-1][0]
    except:
        old_profit_data = []
        old_last_date = '2000-01-01'

    # 得到需要更新的成交和持仓数据
    new_trades_position_data = db.get_new_trades_position_data(account, old_last_date, trade_date_list)
    # 计算成交收益和持仓收益
    new_profit_data = get_new_profit_data(md, ci, pst, account, new_trades_position_data)
    # 旧的权益数据拼上新的数据
    old_profit_data.extend(new_profit_data)
    # 清洗掉全0的结算日
    whole_profit_data = clean_profit_data(old_profit_data)
    whole_profit_data = pd.DataFrame(whole_profit_data, columns=PROFIT_DATA_COLUMNS)
    whole_profit_data.to_csv(file_path, index=False)

    return


# ----------------------------------------------------------------------------------------------------------------------
# cta持仓，成交以及权益的每日结算
def get_profit_data(settle_info):
    """
    每日更新各个账户的策略持仓和成交盈亏
    通过文件夹内csv名称来提取账户名
    由csv内最后一行的日期开始更新最新的持仓和成交
    
    如要添加新账户
    在文件夹内创建一个带列名的空csv
    并在account.py内将账户名加入
    """
    accounts_info = {'account': settle_info.account_name,
                     'file_path': os.path.join(settle_info.profit_data_folder, settle_info.account_name + '.csv')}

    get_position_profit_data(accounts_info, settle_info)

    if os.path.exists(accounts_info['file_path']):
        return True
    else:
        return False


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
