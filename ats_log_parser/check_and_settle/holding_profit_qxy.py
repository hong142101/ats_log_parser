# -*- coding: utf-8 -*-
import os
import logging
import datetime as dt
import pandas as pd
from WindPy import *
import mysql.connector

column = [
    'account',
    'settle_date',
    'intraday_seq',
    'contract',
    'dir',
    'quantity',
    'open_strategy',
    'open_order_time',
    'open_deal_time',
    'open_trigger_price',
    'open_order_price',
    'open_deal_price',
    'mul',
    'closed_price',
    'profit']


########################################################################################################################
class HoldingPositionProfit:
    def __init__(self):
        self.cnx = None
        return

    # ------------------------------------------------------------------------------------------------------------------
    def __del__(self):
        self.cnx.close()
        self.cnx_info.close()
        self.cnx_price.close()

    # ------------------------------------------------------------------------------------------------------------------
    def connect(self, settle_info):
        self.cnx = mysql.connector.connect(user=settle_info.dbase_acc,
                                           password=settle_info.dbase_pw,
                                           host=settle_info.dbase_ip,
                                           port=settle_info.port,
                                           database=settle_info.database_name)

        self.cnx_info = mysql.connector.connect(user=settle_info.contract_dbase_acc,
                                                password=settle_info.contract_dbase_pw,
                                                host=settle_info.contract_dbase_ip,
                                                port=settle_info.contract_port,
                                                database=settle_info.contract_database_name)

        self.cnx_price = mysql.connector.connect(user=settle_info.tick_dbase_acc,
                                                 password=settle_info.tick_dbase_pw,
                                                 host=settle_info.tick_dbase_ip,
                                                 port=settle_info.tick_port,
                                                 database=settle_info.tick_database_name)

    # ------------------------------------------------------------------------------------------------------------------
    def get_holding_position(self, settle_date, intraday_seq):
        cursor = self.cnx.cursor(cursor_class=mysql.connector.cursor.MySQLCursorDict)
        query = ("SELECT * FROM holding_positions "
                 "where "
                 "settle_date = '%s' "
                 "and "
                 "intraday_seq = %s" % (settle_date, intraday_seq))
        cursor.execute(query)
        data = list(cursor)

        return data

    # ------------------------------------------------------------------------------------------------------------------
    def get_volume_multiple(self, contract, TradingDay):
        cursor = self.cnx_info.cursor()
        query = ("select VolumeMultiple from codetable "
                 "where "
                 "TradingDay = '{last_date}' "
                 "and "
                 "InstrumentID = '{contract}'".format(last_date=TradingDay, contract=contract)
                 )
        cursor.execute(query)
        volume_multiple = list(cursor)[0][0]

        return volume_multiple

    # ------------------------------------------------------------------------------------------------------------------
    def get_close_price(self, contract, settle_date, closed_time):
        cursor = self.cnx_price.cursor()
        query = ("select max(datetime) from {contract} "
                 "where "
                 "datetime < '{settle_date} {closed_time}'".format(contract=contract,
                                                                   settle_date=settle_date,
                                                                   closed_time=closed_time)
                 )
        cursor.execute(query)
        last_datetime = list(cursor)[0][0]

        query = ("select last_price from {contract} "
                 "where "
                 "datetime = '{last_datetime}'".format(contract=contract, last_datetime=last_datetime)
                 )
        cursor.execute(query)
        last_price = list(cursor)[0][0]

        return last_price


# ----------------------------------------------------------------------------------------------------------------------
def holding_position_mysql_profit(settle_info):
    hpp = HoldingPositionProfit()
    hpp.connect(settle_info)

    settle_date = settle_info.settle_date
    is_daytime = settle_info.is_daytime
    closed_date = settle_info.closed_date
    holding_profit_folder = settle_info.holding_profit_folder
    TradingDay = settle_date.strftime("%Y%m%d")

    if is_daytime is True:
        intraday_seq = '0900'
        closed_time = '15:00:00'
    elif is_daytime is False:
        intraday_seq = '2100'
        closed_time = '02:30:00'
    elif is_daytime is None:
        intraday_seq = '0'
        closed_time = '15:00:00'
    else:
        raise Exception('unrecognized intraday section parameter')

    holding_positions = hpp.get_holding_position(settle_date, intraday_seq)

    holding_profit_data = list()
    for i, piece in enumerate(holding_positions):
        contract = str(piece['contract'])
        open_price = int(piece['open_deal_price'])
        direction = int(piece['dir'])
        quantity = int(piece['quantity'])
        open_strategy = str(piece['open_strategy'])

        volume_multiple = hpp.get_volume_multiple(contract, TradingDay)
        close_price = hpp.get_close_price(contract, closed_date, closed_time)

        if not type(close_price) == float:
            close_price = 0

        net_profit = (close_price - open_price) * direction * quantity * volume_multiple

        holding_profit_data.append(
            [piece['account'],
             piece['settle_date'],
             piece['intraday_seq'],
             piece['contract'],
             piece['dir'],
             piece['quantity'],
             piece['open_strategy'],
             piece['open_order_time'],
             piece['open_deal_time'],
             piece['open_trigger_price'],
             piece['open_order_price'],
             piece['open_deal_price'],
             volume_multiple,
             close_price,
             net_profit])

        # print(open_strategy, contract, piece['open_deal_price'], close_price, net_profit)

    holding_profit_file = 'holding_profit_%s_%s.csv' % (settle_date, intraday_seq)
    holding_path = os.path.join(holding_profit_folder, holding_profit_file)

    holding_profit_data = pd.DataFrame(holding_profit_data, columns=column)
    holding_profit_data.to_csv(holding_path, index=False)

    if os.path.exists(holding_path):
        return True
    else:
        return False
