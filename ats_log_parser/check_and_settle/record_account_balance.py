# -*- coding: utf-8 -*-
import re
import os
import logging
import pandas as pd
import mysql.connector


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
    def connect(self, settle_info):
        self.cnx = mysql.connector.connect(
            user=settle_info.dbase_acc,
            password=settle_info.dbase_pw,
            host=settle_info.dbase_ip,
            port=settle_info.port,
            database=settle_info.database_name)

    # ------------------------------------------------------------------------------------------------------------------
    def get_closed_trades(self, settle_info):
        """取从始至今的总的成交数据"""
        self.connect(settle_info)
        cur = self.cnx.cursor()
        query = ("SELECT sum(net_profit) FROM closed_trades "
                 "WHERE "
                 "close_deal_time <= '{close_deal_time}' "
                 "AND "
                 "account = '{account_name}'".format(close_deal_time=settle_info.close_datetime,
                                                     account_name=settle_info.account_name))
        cur.execute(query)
        data = list(cur)[0][0]

        cur.close()
        return data


def parse_ats_daytime_log(ats_log_path, is_daytime):
    f = open(ats_log_path, 'r')
    for line in f:
        b_line = line.encode(encoding="utf-8")
        if b_line != b'\n':
            datetime = re.findall(r'\[(\d{4}/\d{2}/\d{2}) (\d{2}:\d{2}:\d{2}\.\d{3})\](.*$)', line)
            if (datetime[0][1] > "09:00:00.000") and ("Trading Account Detail" in datetime[0][2]):
                line = f.__next__()
                while not re.findall(r'\[(\d{4}/\d{2}/\d{2}) (\d{2}:\d{2}:\d{2}\.\d{3})\](.*$)', line):
                    line = f.__next__()
                    if "Balance" in line:
                        balance = re.findall(r'Balance: ([0-9]+\.[0-9]+)', line)[0]
                        return balance


def record_account_balance(settle_info):
    ats_log_filename = "{date}.log".format(date=settle_info.settle_date.strftime("%Y%m%d"))
    ats_log_path = os.path.join(settle_info.log_file_folder, settle_info.account_name)
    summary_log_path = os.path.join(ats_log_path, ats_log_filename)
    intraday_seq = None
    closed_date = settle_info.closed_date.strftime("%Y-%m-%d")
    if settle_info.is_daytime is True:
        intraday_seq = '0900'
        settle_info.close_datetime = ' '.join([closed_date, "15:15:00.000000"])
    elif settle_info.is_daytime is False:
        intraday_seq = '2100'
        settle_info.close_datetime = ' '.join([closed_date, "02:30:00.000000"])
    else:
        logging.error("Can't set intraday_seq = 0 in this account")
        input()
    # end_real_balance = parse_ats_daytime_log(summary_log_path, settle_info.is_daytime)

    holding_profit_file = "holding_profit_{date}_{intraday_seq}.csv".format(date=str(settle_info.settle_date),
                                                                            intraday_seq=intraday_seq)
    holding_profit_path = os.path.join(settle_info.holding_profit_folder, holding_profit_file)
    end_theoretic_holding_balance = sum(pd.read_csv(holding_profit_path).to_dict('dict')['profit'].values())
    end_theoretic_closed_balance = TradeDatabase().get_closed_trades(settle_info)
    print("end_theoretic_balance", end_theoretic_holding_balance + end_theoretic_closed_balance)


if __name__ == "__main__":
    f = open(r'\\172.18.93.131\users\hongxing\ly_duichong_3_300_qxy\20170613.log', 'r')
    beginning_balance = 0
    for line in f:
        b_line = line.encode(encoding="utf-8")
        if b_line != b'\n':
            datetime = re.findall(r'\[(\d{4}/\d{2}/\d{2}) (\d{2}:\d{2}:\d{2}\.\d{3})\](.*$)', line)
            if (datetime[0][1] < "09:00:00.000") and ("Trading Account Detail" in datetime[0][2]):
                line = f.__next__()
                while not re.findall(r'\[(\d{4}/\d{2}/\d{2}) (\d{2}:\d{2}:\d{2}\.\d{3})\](.*$)', line):
                    line = f.__next__()
                    if "Balance" in line:
                        balance = re.findall(r'Balance: ([0-9]+\.[0-9]+)', line)
                        print(balance)
                        exit()
