# coding=utf8
import pandas as pd
import datetime as dt
import os
import logging
import mysql.connector

try:
    from .record_struct import CLOSED_TD_COLUMNS, HOLDING_POS_COLUMNS, RAW_LOG_COLUMNS
except:
    from record_struct import CLOSED_TD_COLUMNS, HOLDING_POS_COLUMNS, RAW_LOG_COLUMNS


def to_sql_val(py_item):
    if type(py_item) is str:
        return '"%s"' % py_item
    elif py_item is None:
        return 'null'
    elif type(py_item) is dt.datetime:
        val = py_item.strftime('%Y-%m-%d %H:%M:%S.%f')
        return to_sql_val(val)
    elif type(py_item) is dt.date:
        val = py_item.strftime('%Y-%m-%d')
        return to_sql_val(val)
    else:
        return to_sql_val(str(py_item))


def get_insert_cmd_str(database, columns):
    ins_cmd = 'insert into %s ' % database
    for i, name in enumerate(columns):
        if i > 0:
            ins_cmd += ',%s' % name
        else:
            ins_cmd += '(%s' % name
    ins_cmd += ') '
    ins_cmd += 'values (' + '%s,' * len(columns)
    return ins_cmd.rstrip(',') + ')'


########################################################################################################################
class TradeDatabase:
    def __init__(self):
        self.cnx = None
        return

    # ------------------------------------------------------------------------------------------------------------------
    def __del__(self):
        if self.cnx:
            self.cnx.close()

    # ------------------------------------------------------------------------------------------------------------------
    def connect(self, ip, user, password, database, port='3306'):
        try:
            self.cnx = mysql.connector.connect(user=user, password=password, host=ip, port=port)
            self.cnx.database = database
        except Exception as err:
            logging.fatal('connecting to mysql error\n%s\n' % str(err))
        return

    # ------------------------------------------------------------------------------------------------------------------
    def get_last_closed_trade_date(self, account_name):

        cur = self.cnx.cursor()

        cur.execute("select max(close_deal_time) from "
                    "closed_trades "
                    "where account = %s" % to_sql_val(account_name))

        last_closed_trade_time = dt.datetime.min
        for item in cur:
            for i, value in enumerate(item):
                last_closed_trade_time = value

        if last_closed_trade_time is not None:

            if last_closed_trade_time.hour < 3:
                last_closed_trade_time = dt.datetime(last_closed_trade_time.year,
                                                     last_closed_trade_time.month,
                                                     (last_closed_trade_time.day - 1),
                                                     21)
            elif last_closed_trade_time.hour >= 21:
                last_closed_trade_time = dt.datetime(last_closed_trade_time.year,
                                                     last_closed_trade_time.month,
                                                     last_closed_trade_time.day,
                                                     21)
            else:
                last_closed_trade_time = dt.datetime(last_closed_trade_time.year,
                                                     last_closed_trade_time.month,
                                                     last_closed_trade_time.day,
                                                     9)

        cur.close()

        return last_closed_trade_time

    # ------------------------------------------------------------------------------------------------------------------
    def get_last_holding_date(self, account_name):

        cur = self.cnx.cursor()

        cur.execute("select max(intraday_seq) from "
                    "holding_positions "
                    "where account = %s" % to_sql_val(account_name))

        intraday_seq = ''
        for item in cur:
            for i, value in enumerate(item):
                intraday_seq = value

        cur.execute("select max(settle_date) from "
                    "holding_positions "
                    "where account = %s" % to_sql_val(account_name))

        settle_date = dt.datetime.min
        for item in cur:
            for i, value in enumerate(item):
                settle_date = value

        if settle_date is not None:
            if intraday_seq == 900:
                settle_date = dt.datetime(settle_date.year,
                                          settle_date.month,
                                          settle_date.day,
                                          9)
            elif intraday_seq == 2100:
                settle_date = dt.datetime(settle_date.year,
                                          settle_date.month,
                                          settle_date.day,
                                          21)
            elif intraday_seq == 0:
                settle_date = dt.datetime(settle_date.year,
                                          settle_date.month,
                                          settle_date.day,
                                          21)

        cur.execute("select max(open_deal_time) from "
                    "holding_positions "
                    "where account = %s" % to_sql_val(account_name))

        last_open_date = ''
        for item in cur:
            for i, value in enumerate(item):
                last_open_date = value

        if last_open_date is not None:
            if last_open_date.hour < 3:
                last_open_date = dt.datetime(last_open_date.year,
                                             last_open_date.month,
                                             last_open_date.day - 1,
                                             21)
            elif last_open_date.hour >= 21:
                last_open_date = dt.datetime(last_open_date.year,
                                             last_open_date.month,
                                             last_open_date.day,
                                             21)
            else:
                last_open_date = dt.datetime(last_open_date.year,
                                             last_open_date.month,
                                             last_open_date.day,
                                             9)

        if (last_open_date is not None) and (settle_date is not None):
            if last_open_date > settle_date:
                last_holding_trade = last_open_date
            else:
                last_holding_trade = settle_date
        else:
            last_holding_trade = None

        cur.close()
        return last_holding_trade

    # ------------------------------------------------------------------------------------------------------------------
    def fetch_holding_positions(self, account_name, position_date=None, intraday_seq=None, settle_date=None):
        """
        :param account_name: document name str
        :param position_date: datetime or date only use date part,
                            if None return last settle date positions
        :param intraday_seq: intraday record sequence number
        :return: records dict
        """
        cur = self.cnx.cursor()
        # get column names
        column_names = list()
        cur.execute('show columns from holding_positions')
        for item in cur:
            column_names.append(item[0])
        # fetch

        last_closed_trade = self.get_last_closed_trade_date(account_name)
        print('last_closed_trade', last_closed_trade)
        last_holding_trade = self.get_last_holding_date(account_name)
        print('last_holding_trade', last_holding_trade)

        holdings = list()
        if (last_closed_trade is None) and (last_holding_trade is None):
            print('last_closed_trade is None\n')
        else:
            if (last_closed_trade is not None) and (last_holding_trade is not None):
                if last_closed_trade > last_holding_trade:
                    print('last_closed_trade > last_holding_trade\n')
                else:
                    query = self.fetch_holding_positions_query(account_name)
                    cur.execute(query)
                    print('last_closed_trade <= last_holding_trade\n')
                    # convert
                    for item in cur:
                        rec = dict()
                        for i, value in enumerate(item):
                            rec[column_names[i]] = value
                        holdings.append(rec)
                    # close
                    cur.close()
            else:
                query = self.fetch_holding_positions_query(account_name)
                cur.execute(query)
                print('last_closed_trade <= last_holding_trade\n')
                # convert
                for item in cur:
                    rec = dict()
                    for i, value in enumerate(item):
                        rec[column_names[i]] = value
                    holdings.append(rec)
                # close
                cur.close()

        logging.info('%s fetch holding positions: %d  @  %s - %d\n'
                     % (account_name, len(holdings), str(position_date), intraday_seq))

        for i in range(len(holdings)):
            if 'amalgamated' in holdings[i]['account']:
                holdings[i]['open_strategy'] = 'holding'

        return holdings

    # ------------------------------------------------------------------------------------------------------------------
    def insert_holding_positions(self, account_name, positions):
        """
        :param account_name: document name str
        :param positions: [{position1}, {position2} ...]
        :return: bool
        """
        holdings = list()
        for item in positions:
            pos = dict()
            for h in HOLDING_POS_COLUMNS:
                if h in item:
                    pos[h] = item[h]
                else:
                    pos[h] = None
            pos['account'] = account_name
            pos['record_update_time'] = dt.datetime.now()
            holdings.append(pos)

        # insert
        insert_cmd = get_insert_cmd_str('holding_positions',
                                        HOLDING_POS_COLUMNS)

        n_success = 0
        cur = self.cnx.cursor()
        for pos in holdings:
            try:
                values = list()
                for h in HOLDING_POS_COLUMNS:
                    if h in pos:
                        values.append(to_sql_val(pos[h]))
                    else:
                        values.append('null')
                cur.execute(insert_cmd % tuple(values))
                n_success += 1
            except Exception as e:
                logging.error('insert holding position failed\n'
                              '%s\n%s\n%s\n'
                              % (str(e), account_name, str(pos)))
        self.cnx.commit()
        cur.close()

        logging.info('%s insert holding positions, total:%d, success:%d'
                     % (account_name, len(positions), n_success)
                     )
        return

    # ------------------------------------------------------------------------------------------------------------------
    def insert_closed_trades(self, account_name, trades_list):
        """
        :param account_name: document name str
        :param trades_list: [{trade1}, {trade2} ...]
        :return:
        """
        # append info
        cl_trades = list()
        for item in trades_list:
            td = dict()
            for h in CLOSED_TD_COLUMNS:
                if h in item:
                    td[h] = item[h]
                else:
                    td[h] = None
            td['account'] = account_name
            td['record_update_time'] = dt.datetime.now()
            cl_trades.append(td)

        # insert
        n_success = 0
        insert_cmd = get_insert_cmd_str('closed_trades',
                                        CLOSED_TD_COLUMNS)
        cur = self.cnx.cursor()
        for td in cl_trades:
            try:
                values = list()
                for h in CLOSED_TD_COLUMNS:
                    if h in td:
                        values.append(to_sql_val(td[h]))
                    else:
                        values.append('null')
                cur.execute(insert_cmd % tuple(values))
                n_success += 1
            except Exception as e:
                logging.error('insert closed trade failed\n'
                              '%s\n%s\n%s\n'
                              % (str(e), account_name, str(td)))

        self.cnx.commit()
        cur.close()

        logging.info('%s insert closed trades, total:%d, success:%d'
                     % (account_name, len(trades_list), n_success)
                     )
        return

    # ------------------------------------------------------------------------------------------------------------------
    def insert_ats_raw_logs(self, account_name, settle_date, intraday_seq, raw_log_file):
        if not os.path.exists(raw_log_file):
            logging.warning("TradeDatabase::insert_ats_raw_logs::"
                            "log file does not exist\n%s" % raw_log_file)
            return

        insert_cmd = get_insert_cmd_str('ats_raw_logs',
                                        RAW_LOG_COLUMNS)

        with open(raw_log_file) as fid:
            text = fid.read()
            cur = self.cnx.cursor()

            try:
                cur.execute(insert_cmd % (to_sql_val(account_name),
                                          to_sql_val(settle_date),
                                          to_sql_val(intraday_seq),
                                          to_sql_val(text))
                            )
            except Exception as e:
                logging.warning('insert raw logs error\n'
                                '%s\n%s\n%s\n'
                                % (str(e), account_name, raw_log_file))
            else:
                self.cnx.commit()

            cur.close()
        return

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def fetch_holding_positions_query(account_name):
        query = ('select * from holding_positions '
                 'where '
                 'settle_date = (select max(settle_date) from holding_positions '
                 '               where '
                 '               account = %s) '
                 'and '
                 'intraday_seq = (select max(intraday_seq) from holding_positions '
                 '                where '
                 '                settle_date = (select max(settle_date) from holding_positions '
                 '                               where '
                 '                               account = %s)'
                 '                and '
                 '                account = %s)'
                 'and '
                 'account = %s'
                 % (to_sql_val(account_name),
                    to_sql_val(account_name),
                    to_sql_val(account_name),
                    to_sql_val(account_name)))

        return query

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def if_database_is_wrong_then_get_holding_positions_from_csv():
        data = pd.read_csv(r'.\positions_lyq\positions_ly_jinqu_1_lyq_20170105_0 - 副本.csv', keep_default_na=False)
        holding = data.to_dict(orient='records')

        for i in holding:
            for key in i.keys():
                if key == 'open_deal_time':
                    i[key] = dt.datetime.strptime(i[key], "%Y-%m-%d %H:%M:%S.%f")
                elif key == 'open_order_time':
                    i[key] = dt.datetime.strptime(i[key], "%Y-%m-%d %H:%M:%S")

        return holding


########################################################################################################################
class ContractInfo:
    def __init__(self, ip, user, password, database, port='3306'):
        self.cnx = None
        self.connect(ip=ip,
                     user=user,
                     password=password,
                     database=database,
                     port=port)

    # ------------------------------------------------------------------------------------------------------------------
    def __del__(self):
        if self.cnx:
            self.cnx.close()

    # ------------------------------------------------------------------------------------------------------------------
    def connect(self, ip, user, password, database, port='3306'):
        try:
            self.cnx = mysql.connector.connect(user=user, password=password, host=ip, port=port)
            self.cnx.database = database
        except Exception as err:
            logging.fatal('connecting to mysql error\n%s\n' % str(err))
        return

    # ------------------------------------------------------------------------------------------------------------------
    def commission_rate(self, closed_date=None):
        cur = self.cnx.cursor(cursor_class=mysql.connector.cursor.MySQLCursorDict)
        cur.execute('select * from instrument_commission_rate '
                    'where '
                    'TradingDay = {closed_date}'.format(closed_date=closed_date))
        contract_info = list(cur)

        commission_rate = dict()
        for i in contract_info:
            if i['InvestorID'] not in commission_rate:
                commission_rate[i['InvestorID']] = dict()
            if i['InstrumentID'] not in commission_rate[i['InvestorID']]:
                commission_rate[i['InvestorID']][i['InstrumentID']] = dict()
            commission_rate[i['InvestorID']][i['InstrumentID']]['OpenRatioByMoney'] = float(i['OpenRatioByMoney'])
            commission_rate[i['InvestorID']][i['InstrumentID']]['OpenRatioByVolume'] = float(i['OpenRatioByVolume'])
            commission_rate[i['InvestorID']][i['InstrumentID']]['CloseRatioByMoney'] = float(i['CloseRatioByMoney'])
            commission_rate[i['InvestorID']][i['InstrumentID']]['CloseRatioByVolume'] = float(i['CloseRatioByVolume'])
            commission_rate[i['InvestorID']][i['InstrumentID']]['CloseTodayRatioByMoney'] = \
                float(i['CloseTodayRatioByMoney'])
            commission_rate[i['InvestorID']][i['InstrumentID']]['CloseTodayRatioByVolume'] = \
                float(i['CloseTodayRatioByVolume'])

        return commission_rate

    # ------------------------------------------------------------------------------------------------------------------
    def contract_base_info(self, closed_date='20000101'):
        cur = self.cnx.cursor(cursor_class=mysql.connector.cursor.MySQLCursorDict)
        cur.execute('select * from codetable '
                    'where '
                    'TradingDay = {closed_date}'.format(closed_date=closed_date))
        contract_info = list(cur)

        contract_base_info = dict()
        for i in contract_info:
            if i['InstrumentID'] not in contract_base_info:
                contract_base_info[i['InstrumentID']] = dict()
            contract_base_info[i['InstrumentID']]['VolumeMultiple'] = float(i['VolumeMultiple'])
            contract_base_info[i['InstrumentID']]['PriceTick'] = float(i['PriceTick'])

        return contract_base_info


########################################################################################################################
if __name__ == "__main__":
    commission_rate = ContractInfo().commission_rate(closed_date='20170605')
    for i in commission_rate.keys():
        for j in commission_rate[i].keys():
            print(i, j, commission_rate[i][j])
