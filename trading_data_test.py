import logging
import ats_log_parser as alp

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    sinfo = alp.SettleInformation()
    # 交易记录服务器
    sinfo.database_name = ''
    sinfo.dbase_ip = ''
    sinfo.dbase_acc = ''
    sinfo.dbase_pw = ''
    sinfo.port = ''

    # 实时行情数据库
    sinfo.tick_database_name = ''
    sinfo.tick_dbase_ip = ''
    sinfo.tick_dbase_acc = ''
    sinfo.tick_dbase_pw = ''
    sinfo.tick_port = ''

    # 合约信息数据库
    sinfo.contract_database_name = ''
    sinfo.contract_dbase_ip = ''
    sinfo.contract_dbase_acc = ''
    sinfo.contract_dbase_pw = ''
    sinfo.contract_port = ''

    # 分析交易日志
    sinfo.parse_log = True
    sinfo.log_file_folder = r'.\test'
    sinfo.position_csv_folder = r'.\test\trading_position'
    # 检查理论持仓和实际持仓
    sinfo.check_position = True
    sinfo.strategy_config_folder = r'.\test\strategy_config'
    # 推数据库
    sinfo.push_into_database = True
    # 计算持仓浮盈
    sinfo.holding_position_profit = True
    sinfo.holding_profit_folder = r'.\test\holding_positions_profit'
    # 按每日收盘价计算账户理论权益
    sinfo.profit_data = True
    sinfo.profit_data_folder = r'.\test'
    # 记录账户当期实际权益
    sinfo.account_balance = True
    sinfo.account_balance_folder = r'.\test'

    alp.run_settlement(['test'], sinfo)
