# -*- coding: utf-8 -*-
import logging
import datetime as dt
import pandas as pd
import mysql.connector

from .columns_struct import CLOSED_TD_COLUMNS, HOLDING_POS_COLUMNS


def date_to_datetime(settle_date):
    date = dt.datetime.strptime(settle_date, "%Y-%m-%d")
    date_time = dt.datetime(date.year, date.month, date.day, 15, 0, 0)

    return date_time


########################################################################################################################
def get_different_query(account, settle_date, closed_deal_time):
    """ 
    通过账户名区分不同的mysql语句
    
    :param account: 账户
    :param settle_date: 结算日期
    :param closed_deal_time: 每日截止的成交时间戳
    :return: 从始至今所有成交记录, 当日结算日的持仓记录
    """

    ct_query = ("SELECT * FROM closed_trades "
                "WHERE "
                "close_deal_time <= '%s' "
                "AND "
                "account = '%s'" % (closed_deal_time, account))

    hp_query = ("SELECT * FROM holding_positions "
                "WHERE "
                "settle_date = '%s'"
                "AND "
                "(intraday_seq = 0 or intraday_seq = 900) "
                "AND "
                "account = '%s'" % (settle_date, account))

    return ct_query, hp_query


########################################################################################################################
class TradeDatabase:
    """
    接交易数据库
    用于抓取成交和持仓数据
    
    目前数据输出格式为df
    有空会将数据库游标模式改为
    cur = cnx.cursor(cursor_class=mysql.connector.cursor.MySQLCursorDict)
    用以输出列名为键的dict
    """

    # ------------------------------------------------------------------------------------------------------------------
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
    def select_holding_date(self, account):
        """取有持仓的日期数据"""
        cur = self.cnx.cursor()
        query = ("SELECT settle_date FROM holding_positions "
                 "WHERE "
                 "account = '%s'" % account)
        cur.execute(query)

        holding_date = list()
        for i, value in enumerate(cur):
            holding_date.append(value[0].strftime("%Y-%m-%d"))

        cur.close()
        return holding_date

    # ------------------------------------------------------------------------------------------------------------------
    def select_closed_date(self, account):
        """取有成交的日期数据"""
        cur = self.cnx.cursor()
        query = ("SELECT close_deal_time FROM closed_trades "
                 "WHERE "
                 "account = '%s'" % account)
        cur.execute(query)

        closed_date = list()
        for i, value in enumerate(cur):
            closed_date.append(value[0].strftime("%Y-%m-%d"))

        cur.close()
        return closed_date

    # ------------------------------------------------------------------------------------------------------------------
    def get_closed_trades(self, query):
        """取从始至今的总的成交数据"""
        cur = self.cnx.cursor()
        cur.execute(query)

        data = list()
        for i, value in enumerate(cur):
            data.append(value)
        data = pd.DataFrame(data, columns=CLOSED_TD_COLUMNS)

        cur.close()
        return data

    # ------------------------------------------------------------------------------------------------------------------
    def get_holding_positions(self, query):
        """取当日结算的持仓数据"""
        cur = self.cnx.cursor()
        cur.execute(query)

        data = list()
        for i, value in enumerate(cur):
            data.append(value)
        data = pd.DataFrame(data, columns=HOLDING_POS_COLUMNS)

        cur.close()
        return data

    # ------------------------------------------------------------------------------------------------------------------
    def get_trade_date_list(self, account):
        """获取有持仓或成交的日期列"""
        holding_date = self.select_holding_date(account)
        closed_date = self.select_closed_date(account)
        trade_date_list = list(set(holding_date) | set(closed_date))
        trade_date_list.sort()

        return trade_date_list

    # ------------------------------------------------------------------------------------------------------------------
    def get_new_trades_position_data(self, account, old_last_date, trade_date_list):
        """获取新的结算日，成交记录和持仓记录"""
        new_trades_position_data = []
        for settle_date in trade_date_list:
            if settle_date > old_last_date:
                market_close_time = date_to_datetime(settle_date)
                ct_query, hp_query = get_different_query(account, settle_date, market_close_time)

                closed_trades = self.get_closed_trades(ct_query)
                holding_positions = self.get_holding_positions(hp_query)

                new_trades_position_data.append(
                    {'settle_date': settle_date,
                     'closed_trades': closed_trades,
                     'holding_positions': holding_positions}
                )
            else:
                pass
        return new_trades_position_data


########################################################################################################################
class MarketDatabase:
    """
    连接实时行情数据库
    通过合约和结算日和收盘时间来找到收盘前最后一根tick
    然后找到last_price用以计算持仓收益
    """

    def __init__(self):
        self.cnx = None

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
    def get_close_price(self, contract, settle_close_datetime):
        """
        取当日结算日的最后一根tick的价格数据
        :param contract: 合约号
        :param settle_close_datetime: 该合约的结算收盘时间
        :return: 
        """
        cur = self.cnx.cursor()
        query = ("SELECT max(datetime) FROM {contract} "
                 "WHERE "
                 "datetime < '{settle_close_datetime}'".format(contract=contract,
                                                               settle_close_datetime=settle_close_datetime))
        cur.execute(query)
        close_datetime = list(cur)[0][0]

        query = ("SELECT last_price FROM {contract} "
                 "WHERE "
                 "datetime = '{close_datetime}'".format(contract=contract,
                                                        close_datetime=close_datetime))
        cur.execute(query)
        close_price = list(cur)[0][0]

        cur.close()
        return close_price


########################################################################################################################
class ContractInfo:
    """
    连接实时行情数据库
    通过合约和结算日来找到当日合约信息
    """

    def __init__(self):
        self.cnx = None

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
    def get_contract_info(self, contract, settle_date):
        cur = self.cnx.cursor(cursor_class=mysql.connector.cursor.MySQLCursorDict)
        query = ("SELECT * FROM codetable "
                 "WHERE "
                 "InstrumentID = '{contract}' "
                 "AND "
                 "TradingDay = '{settle_date}'".format(contract=contract,
                                                       settle_date=settle_date))
        cur.execute(query)
        contract_info = list(cur)[0]

        cur.close()
        return contract_info


########################################################################################################################
if __name__ == "__main__":
    # db = TradeDatabase()
    # db.connect('td_records_cta')
    # holding_date = db.select_holding_date('ly_jinqu_1')
    # closed_date = db.select_closed_date('ly_jinqu_1')
    #
    # query = ("SELECT * FROM holding_positions")
    # holding_data = db.get_holding_positions(query)
    #
    # query = ("SELECT * FROM closed_trades")
    # closed_data = db.get_closed_trades(query)
    #
    # for value in holding_date:
    #     print(value)

    ci = ContractInfo()
    a = ci.get_contract_info('hc1710', '20170605')
    print(a)
