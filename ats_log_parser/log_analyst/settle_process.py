# coding=utf8
import os
import logging
import datetime
from ..db_connector.mysql_operations import TradeDatabase, ContractInfo
from ..db_connector.position_csv import PositionCsv
from .future_baseinfo import get_multiplier, get_commission, get_close_commission
from .update_position import update_positions_with_trades, format_unfilled_signal
from .reappear_trades import parse_trades_from_logfile, amalgamated_trades


# 一般每日交易结算按如下流程进行
# 1.1.从数据库同步最新持仓信息
# 2.解析最新的交易时段日志,提取订单和策略执行信息
# 3.按给定规则匹配开平仓交易
# 4.将最新持仓与平仓交易信息输出到csv供人工查验
# 5.将最新持仓信息提交到数据库
# 6.将新增已平仓交易提交到数据库
# 7.将ats_log提交到数据库

class SettleInformation:
    def __init__(self):
        # 结算账户名称
        self.account_name = None

        # 从数据库查询持仓信息所用的日期
        # None则表示查询数据库内,account_name名下最新的持仓信息
        # 数据类型为datetime.date
        # e.g. datetime.date(2016, 1, 6)
        self.position_date = None

        # 结算时使用的标识日期,会填充至持仓表的settle_date列
        # 同时用于查找对应的ats_log文件, 数据类型为datetime.date
        self.settle_date = None
        self.closed_date = None
        self.closed_datetime = None
        # 标识当前结算的是日盘还是夜盘, 接受的值为[True/False/None]
        # True表示结算日盘,将过滤日志文件内所有当日16点以后的日志,同时标识intraday_seq为900
        # False表示结算夜盘,过滤日志文件内所有当日20点以前的日志,同时标识intraday_seq为2100
        # None表示不分日夜盘直接结算,对日志不做过滤,同时标识intraday_seq为0
        self.is_daytime = None

        # 交易记录数据库名称
        self.database_name = None
        # 交易记录数据库ip地址及端口
        self.dbase_ip = None
        # 交易记录数据库登录账户名
        self.dbase_acc = None
        # 交易记录数据库登录密码
        self.dbase_pw = None
        # 交易记录数据库端口
        self.port = None

        # 实时行情数据库的库名称
        self.tick_database_name = None
        # 实时行情数据库ip地址及端口
        self.tick_dbase_ip = None
        # 实时行情数据库登录账户名
        self.tick_dbase_acc = None
        # 实时行情数据库登录密码
        self.tick_dbase_pw = None
        # 实时行情数据库ip端口
        self.tick_port = None

        # 合约信息数据库的库名称
        self.contract_database_name = None
        # 合约信息数据库ip地址及端口
        self.contract_dbase_ip = None
        # 合约信息数据库登录账户名
        self.contract_dbase_acc = None
        # 合约信息数据库登录密码
        self.contract_dbase_pw = None
        # 实时行情数据库ip端口
        self.contract_port = None

        # 是否分析交易日志
        self.parse_log = False
        # ats日志根目录
        self.log_file_folder = './trade_logs'
        # 交易记录中间表缓存位置
        self.position_csv_folder = './positions'

        # 是否推送交易记录数据库
        self.push_into_database = False

        # 是否检查交易配置文件的持仓和即将入库的持仓
        self.check_position = False
        # 持仓策略配置文件的位置
        self.strategy_config_folder = r'./strategy_config'

        # 是否需要按照持仓计算持仓收益
        self.holding_position_profit = False
        # 持仓记录的位置
        self.holding_profit_folder = './holding_positions_profit'

        # 是否需要计算该账户理论净值
        self.profit_data = False
        # 计算账户理论净值生成的csv文件的位置
        self.profit_data_folder = r'.\profit_data'

        # 是否计算账户实际净值变化
        self.record_account_balance = False
        # 账户实际净值情况的csv记录文件
        self.account_balance_folder = r'.\account_balance'
        return


# ----------------------------------------------------------------------------------------------------------------------
def __parse_intraday_seq(settle_info):
    intraday_seq = 0
    handle_from_dt = None
    handle_until_dt = None
    # 无限制
    if settle_info.is_daytime is None:
        pass
    # 只解析至白天早上8点到下午4点
    elif settle_info.is_daytime:
        intraday_seq = 900
        handle_until_dt = datetime.datetime.combine(settle_info.settle_date,
                                                    datetime.time(16, 0))
        handle_from_dt = datetime.datetime.combine(settle_info.settle_date,
                                                   datetime.time(8, 0))
    # 只解析当日夜间20点之后和次日
    elif not settle_info.is_daytime:
        intraday_seq = 2100
        handle_from_dt = datetime.datetime.combine(settle_info.settle_date,
                                                   datetime.time(20, 0))
    return intraday_seq, handle_from_dt, handle_until_dt


# ----------------------------------------------------------------------------------------------------------------------
def __get_log_file_name(settle_info):
    filename = (
        'Transactions_%s.log'
        % settle_info.settle_date.strftime('%Y%m%d')
    )
    filename = os.path.join(settle_info.log_file_folder,
                            filename)
    return filename


# ----------------------------------------------------------------------------------------------------------------------
def __get_position_csv_name(settle_info):
    seq, _1, _2 = __parse_intraday_seq(settle_info)
    filename = ('positions_%s_%s_%d.csv'
                % (settle_info.account_name,
                   settle_info.settle_date.strftime('%Y%m%d'),
                   seq)
                )

    return os.path.join(settle_info.position_csv_folder, filename)


# ----------------------------------------------------------------------------------------------------------------------
def summary_daily_trades(settle_info, write_unfilled_signals=True):
    # connect
    database = TradeDatabase()
    database.connect(ip=settle_info.dbase_ip,
                     user=settle_info.dbase_acc,
                     password=settle_info.dbase_pw,
                     database=settle_info.database_name)

    # parse intraday_seq
    intraday_seq, from_dt, until_dt = __parse_intraday_seq(settle_info)

    # fetch holdings
    position = database.fetch_holding_positions(settle_info.account_name,
                                                settle_info.position_date,
                                                intraday_seq,
                                                settle_info.settle_date)
    # position = database.if_database_is_wrong_then_get_holding_positions_from_csv()
    # parse log
    log_file = __get_log_file_name(settle_info)
    open_tds, close_tds, uf_opens, uf_closes = parse_trades_from_logfile(log_file, from_dt, until_dt)
    # handle position
    holding_pos, closed_trades, odd_closes = update_positions_with_trades(settle_info.account_name,
                                                                          position,
                                                                          open_tds,
                                                                          close_tds)

    if 'amalgamated' in settle_info.account_name:
        holding_pos, closed_trades, odd_closes = amalgamated_trades(holding_pos, closed_trades, odd_closes)
    # write csv
    pc = PositionCsv()
    for r in closed_trades:
        pc.add_row(r)
    for r in holding_pos:
        pc.add_row(r)
    for r in odd_closes:
        pc.add_row(r)
        # logging.warning('%s has odd close trade:\n%s\n'
        # % (settle_info.account_name, str(r)))
    if write_unfilled_signals:
        # format unfilled signals
        ufilled_signals = list()
        for trade in uf_opens:
            format_unfilled_signal(ufilled_signals, trade, True)
        for trade in uf_closes:
            format_unfilled_signal(ufilled_signals, trade, False)
        for r in ufilled_signals:
            pc.add_row(r)
            # logging.warning('%s has unfilled signals\n%s\n'
            # % (settle_info.account_name, str(r)))

    # output
    if not os.path.exists(settle_info.position_csv_folder):
        os.makedirs(settle_info.position_csv_folder)
    pc.save_to_file(__get_position_csv_name(settle_info))
    return True


# ----------------------------------------------------------------------------------------------------------------------
def __fill_holding_positions_fields(positions, settle_info, commission_rate, contract_base_info):
    intraday_seq, _1, _2 = __parse_intraday_seq(settle_info)
    for p in positions:
        p['intraday_seq'] = intraday_seq
        p['settle_date'] = settle_info.settle_date
        p['open_commission'] = get_commission(settle_info.account_name,
                                              p['contract'],
                                              p['open_deal_price'],
                                              p['quantity'])
    return


# ----------------------------------------------------------------------------------------------------------------------
def __fill_closed_trades_fields(closed_trades, settle_info, commission_rate, contract_base_info):
    for t in closed_trades:
        # netprofit and commission
        point = (t['close_deal_price'] - t['open_deal_price']) * t['dir'] * t['quantity']
        mlt = get_multiplier(t['contract'])
        t['open_commission'] = get_commission(settle_info.account_name,
                                              t['contract'],
                                              t['open_deal_price'],
                                              t['quantity'])
        t['close_commission'] = get_commission(settle_info.account_name,
                                               t['contract'],
                                               t['open_deal_price'],
                                               t['quantity'])
        # t['close_commission'] = get_close_commission(settle_info.account_name,
        #                                              t['contract'],
        #                                              t['close_deal_price'],
        #                                              t['quantity'],
        #                                              t['open_deal_time'],
        #                                              t['close_deal_time'])
        net_profit = point * mlt - t['open_commission'] - t['close_commission']
        t['net_profit'] = round(net_profit, 2)
    return


# ----------------------------------------------------------------------------------------------------------------------
def push_positions_and_trades_into_database(settle_info):
    # load postions and closed_trades csv
    pc = PositionCsv()
    pc.load_file(__get_position_csv_name(settle_info))
    positions = pc.get_rows()
    # 取合约信息和合约手续费
    settle_date = settle_info.settle_date.strftime("%Y%m%d")
    ci = ContractInfo(ip=settle_info.contract_dbase_ip,
                      user=settle_info.contract_dbase_acc,
                      password=settle_info.contract_dbase_pw,
                      database=settle_info.contract_database_name,
                      port=settle_info.contract_port)
    commission_rate = ci.commission_rate(closed_date=settle_date)
    contract_base_info = ci.contract_base_info(closed_date=settle_date)
    # classify
    closed_trades = list()
    holding_positions = list()
    for pos in positions:
        if pos['open_deal_time'] is None:
            raise Exception('cant handle odd record row:\n'
                            '%s' % str(pos))
        # holding
        if pos['close_deal_time'] is None:
            holding_positions.append(pos)
        # closed
        else:
            closed_trades.append(pos)
    # fill net profit and commissions
    __fill_holding_positions_fields(holding_positions, settle_info, commission_rate, contract_base_info)
    __fill_closed_trades_fields(closed_trades, settle_info, commission_rate, contract_base_info)
    # connect mysql
    database = TradeDatabase()
    database.connect(settle_info.dbase_ip,
                     user=settle_info.dbase_acc,
                     password=settle_info.dbase_pw,
                     database=settle_info.database_name,
                     port=settle_info.port)
    # seq
    intraday_seq, _1, _2 = __parse_intraday_seq(settle_info)
    # insert raw log
    database.insert_ats_raw_logs(settle_info.account_name,
                                 settle_info.settle_date,
                                 intraday_seq,
                                 __get_log_file_name(settle_info))
    # insert
    database.insert_closed_trades(settle_info.account_name, closed_trades)
    database.insert_holding_positions(settle_info.account_name, holding_positions)
    return True
