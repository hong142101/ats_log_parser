import datetime
import logging

from .log_analyst.settle_process import SettleInformation
from .log_analyst.settle_process import __get_position_csv_name
from .log_analyst.settle_process import push_positions_and_trades_into_database
from .log_analyst.settle_process import summary_daily_trades
from .check_and_settle.check_position import check_position
from .check_and_settle.holding_profit_qxy import holding_position_mysql_profit
from .check_and_settle.record_account_balance import record_account_balance
from .profit_data_daily.get_profit_data import get_profit_data

DEFAULT_TAR_DATE = datetime.date.today()

GUIDE_DESCRIPTION00 = '''
input settle date, eg. 2016-04-20
default: %s
settle date:
''' % str(DEFAULT_TAR_DATE)

GUIDE_DESCRIPTION01 = '''
input closed date, eg. 2016-04-20
default: %s
closed date:
''' % str(DEFAULT_TAR_DATE)

GUIDE_DESCRIPTION1 = '''
settle daytime trades or nighttime?
options: daytime / nighttime / none
input:
'''

GUIDE_DESCRIPTION2 = '''
===============================================
#####     trading settlement process     ######
<task types>
parse  :: append ats log into positions
settle :: update positions&trades into database
===============================================
select working:'''


def print_settle_info(settle_info, accounts):
    info_str = ('>>>>>>'
                'settle settings\n'
                'account        : %s\n'
                'position date  : %s\n'
                'settle date    : %s\n'
                'closed date    : %s\n'
                'settle daytime : %s\n'
                '--\n'
                'datebase       : %s\n'
                'db_user        : %s\n'
                '--\n'
                'log path       : %s\n'
                'position path  : %s\n'
                '>>>>>>'
                % (str(settle_info.account_name),
                   str(settle_info.position_date),
                   str(settle_info.settle_date),
                   str(settle_info.closed_date),
                   str(settle_info.is_daytime),
                   str(settle_info.database_name),
                   str(settle_info.dbase_acc),
                   str(settle_info.log_file_folder),
                   str(settle_info.position_csv_folder))
                )
    print(info_str)
    print('\naccounts: ', str(accounts))
    return


# # 根据策略配置文件生成分策略持仓，并和交易log分析文件的持仓做对比
# def check_position(settle_info):
#     position_csv_path = __get_position_csv_name(settle_info)
#     strategy_positions = parse_strategy_config(settle_info)
#     csv_positions = parse_position_csv(position_csv_path)
#
#     if csv_positions == strategy_positions:
#         logging.info("mysql_positions == strategy_positions")
#         return True
#     else:
#         logging.info('strategy_positions')
#         for i in strategy_positions:
#             logging.info(str(i))
#         logging.info('holding_csv_position')
#         for i in csv_positions:
#             logging.info(str(i))
#         logging.info("mysql_positions != strategy_positions")
#         return False


def _run_settle_process_on_account(accounts, settle_info):
    # 结算日期
    s_date = input(GUIDE_DESCRIPTION00)
    if not s_date:
        settle_info.settle_date = DEFAULT_TAR_DATE
    else:
        settle_info.settle_date = datetime.datetime.strptime(s_date, '%Y-%m-%d').date()
    # 收盘价日期
    s_date = input(GUIDE_DESCRIPTION01)
    if not s_date:
        settle_info.closed_date = DEFAULT_TAR_DATE
    else:
        settle_info.closed_date = datetime.datetime.strptime(s_date, '%Y-%m-%d').date()
    # 结算阶段(日盘/夜盘/全天)
    s_section = input(GUIDE_DESCRIPTION1)
    if not s_section or s_section.lower() == 'none':
        settle_info.is_daytime = None
    elif s_section.lower() == 'daytime':
        settle_info.is_daytime = True
    elif s_section.lower() == 'nighttime':
        settle_info.is_daytime = False
    else:
        raise Exception('unrecognized intraday section parameter')

    # print info setting details
    print_settle_info(settle_info, accounts)
    print('\n')
    for acc in accounts:
        logging.info('\n#--------------------#\naccount: %s\n' % acc)
        settle_info.account_name = acc

        # 分析交易日志
        if settle_info.parse_log is True:
            if summary_daily_trades(settle_info, write_unfilled_signals=False) is True:
                logging.info('\nsummary_daily_trades SUCCESSFULLY!')
            else:
                logging.info('\nsummary_daily_trades Failed!')

        # 检查策略.strat的持仓与未载入数据库的持仓
        if settle_info.check_position is True:
            result = check_position(settle_info)
        else:
            result = True
        input()
        if result is False:
            continue

        if settle_info.push_into_database is True:
            if push_positions_and_trades_into_database(settle_info) is True:
                logging.info('\npush_positions_and_trades SUCCESSFULLY!')
            else:
                logging.info('\npush_positions_and_trades Failed!')

        # 生成持仓收益文件
        if settle_info.holding_position_profit is True:
            if holding_position_mysql_profit(settle_info):
                logging.info('\nholding_position_mysql_profit SUCCESSFULLY!')
            else:
                logging.info('\nholding_position_mysql_profit Failed!')

        # 按日生成账户当日理论权益
        if settle_info.profit_data is True:
            if get_profit_data(settle_info):
                logging.info('\nget_profit_data SUCCESSFULLY!')
            else:
                logging.info('\nget_profit_data Failed!')

        # 记录账户当期实际权益
        if settle_info.record_account_balance is True:
            record_account_balance(settle_info)

    return


def run_settlement(accounts, settle_info):
    # sinfo = SettleInformation()
    # sinfo.settle_date = None
    # sinfo.account_name = 'ly_jinqu_1'
    # sinfo.database_name = 'trade_records'
    # sinfo.dbase_ip = '172.18.93.153'
    # sinfo.dbase_acc = 'qxy'
    # sinfo.dbase_pw = ''
    # sinfo.log_file_folder = r'.\trade_logs'
    # sinfo.position_csv_folder = r'.\positions'

    try:
        _run_settle_process_on_account(accounts, settle_info)

    except Exception as e:
        logging.fatal(str(e))

    input('press enter to quit ...')
