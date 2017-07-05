#coding=utf8
import datetime as DT
import logging
import os

import pymongo as MGD
from bson.son import SON
from record_struct import SIDE_TRADE_FIELDS

from trade_log_parser.outdated.gen_feature_md5 import add_position_features
from trade_log_parser.outdated.gen_feature_md5 import add_trades_features


#JSON of holding_position
#{
#    "_id" : ObjectId("5710834dbee4d6c7bab4ac19"),
#    "account" : "test_qxy",
#    "settle_date" : ISODate("1970-01-01T00:00:00.000+0000"),
#    "contract" : "ru0000",
#    "open_strategy" : "db_test",
#    "open_order_time" : ISODate("1970-01-01T00:00:00.000+0000"),
#    "open_deal_time" : ISODate("1970-01-01T00:00:00.300+0000"),
#    "dir" : "buy",
#    "quantity" : NumberInt(20),
#    "open_trigger_price" : 1000.25,
#    "open_order_price" : 1001.25,
#    "open_deal_price" : 1002.25,
#    "open_commission" : 0.0,
#    "open_tag" : "",
#    "feature_md5" : "-",
#    "record_update_time" : ISODate("1970-01-01T05:36:00.322+0000")
#}
#JSON of closed_trade
#{
#    "_id" : ObjectId("57108263bee4d6c7bab4ac17"),
#    "account" : "test_qxy",
#    "contract" : "ru0000",
#    "dir" : "buy",
#    "quantity" : NumberInt(20),
#    "open_strategy" : "db_test",
#    "open_order_time" : ISODate("1970-01-01T00:00:00.000+0000"),
#    "open_deal_time" : ISODate("1970-01-01T00:00:00.300+0000"),
#    "open_trigger_price" : 1000.25,
#    "open_order_price" : 1001.25,
#    "open_deal_price" : 1002.25,
#    "open_commission" : 0.0,
#    "open_tag" : "",
#    "close_strategy" : "db_test",
#    "close_order_time" : ISODate("1970-01-01T00:00:00.000+0000"),
#    "close_deal_time" : ISODate("1970-01-01T00:00:00.300+0000"),
#    "close_trigger_price" : 1000.25,
#    "close_order_price" : 1001.25,
#    "close_deal_price" : 1002.25,
#    "close_commission" : 0.0,
#    "close_tag" : "",
#    "net_profit" : 0.0,
#    "feature_md5" : "-",
#    "record_update_time" : ISODate("1970-01-01T05:36:00.322+0000")
#}

def _expend_db_date_range(settle_date, days=1):
    if type(settle_date) == DT.datetime:
        settle_date = settle_date.date()
    #convert date range
    try:
        settle_date = DT.datetime.combine(settle_date, DT.time(0))
        dt_fm = DT.datetime.combine(settle_date, DT.time(0))
        dt_to = dt_fm + DT.timedelta(days=days)
        return dt_fm, dt_to
    except Exception as e:
        logging.fatal('expend database date range fail\n%s' % str(e))
        raise Exception(e)

#====================================================================

class TradeDatabase:
    def __init__(self):
        self.ip      = None
        self.port    = None
        self.user    = None
        self.pw      = None
        self.client  = None
        self.db_name = None
        return

    def __del__(self):
        pass

    def connect(self, ip, user=None, password=None, database=None, port=27017):
        self.ip = ip
        self.port = port
        self.user = user
        self.pw = password
        if database:
            self.db_name = database
        else:
            self.db_name = 'trade_records'
        #connect
        self.client = MGD.MongoClient(self.ip, self.port)
        if user and password:
            self.client['admin'].authenticate(self.user, self.pw)
        return

    def __find_last_positions_settle_date(self, account_name):
        doc = self.client[self.db_name]['holding_positions']
        pipeline = [{'$match': {'account': account_name}},
                    {'$sort': SON([('settle_date', -1)])},
                    {'$limit': 1}]
        settle_date = list(doc.aggregate(pipeline))
        return settle_date[0]['settle_date'] if settle_date else None

    def fetch_holding_positions(self, account_name, settle_date=None):
        """
        :param account_name: document name str
        :param settle_date: datetime or date only use date part,
                            if None return last settle date positions
        :return: records dict
        """
        #fetch last holding positions
        if settle_date is None:
            settle_date = self.__find_last_positions_settle_date(account_name)
            #hasnt any records
            if settle_date is None:
                return list()
        dt_fm, dt_to = _expend_db_date_range(settle_date)
        doc = self.client[self.db_name]['holding_positions']
        records = doc.find({'account': account_name,
                            'settle_date': {'$gte': dt_fm, '$lt': dt_to}})
        return list(records)

    def rewrite_holding_positions(self, account_name, settle_date, positions):
        """
        :param account_name: document name str
        :param settle_date: datetime or date only use date part
        :param positions: [{position1}, {position2} ...]
        :return: bool
        """
        dt_fm, dt_to = _expend_db_date_range(settle_date)
        #confirm settle date
        for p in positions:
            if p['settle_date'] < dt_fm or dt_to <= p['settle_date']:
                return False
        #delete obsoletes
        doc = self.client[self.db_name]['holding_positions']
        rtn = doc.delete_many({'account': account_name,
                               'settle_date': {'$gte': dt_fm, '$lt': dt_to}})
        return self.insert_holding_positions(account_name, positions)

    def insert_holding_positions(self, account_name, positions):
        """
        :param account_name: document name str
        :param positions: [{position1}, {position2} ...]
        :return: bool
        """
        holdings = list()
        for item in positions:
            pos = dict()
            for f in SIDE_TRADE_FIELDS:
                if f in item:
                    pos[f] = item[f]
            pos['account'] = account_name
            pos['record_update_time'] = DT.datetime.now()
            holdings.append(pos)
        add_position_features(holdings)
        doc = self.client[self.db_name]['holding_positions']
        for pos in holdings:
            try:
                doc.insert_one(pos)
            except Exception as e:
                logging.error('insert holding position failed\n'
                              '%s\n%s\n%s\n'
                              % (str(e), account_name, str(pos)))
        return

    def insert_closed_trades(self, account_name, trades_list):
        """
        :param account_name: document name str
        :param trades_list: [{trade1}, {trade2} ...]
        :return:
        """
        #write insert datetime
        for d in trades_list:
            d['account'] = account_name
            d['record_update_time'] = DT.datetime.now()
        add_trades_features(trades_list)
        #insert
        doc = self.client[self.db_name]['closed_trades']
        for trade in trades_list:
            try:
                doc.insert_one(trade)
            except Exception as e:
                logging.error('insert closed trade failed\n'
                              '%s\n%s\n%s\n'
                              % (str(e), account_name, str(trade)))
        return

    def insert_ats_raw_logs(self, account_name, settle_date, raw_log_file):
        """
        :param account_name: document name str
        :param raw_logs: list
        :return:
        """
        if not os.path.exists(raw_log_file):
            return
        if type(settle_date) is DT.datetime:
            settle_date = settle_date.date()
        settle_date = DT.datetime.combine(settle_date, DT.time(0, 0, 0))
        with open(raw_log_file) as fid:
            text = fid.read()
            try:
                doc = self.client[self.db_name]['ats_raw_logs']
                doc.update_one({'account': account_name, 'log_date': settle_date},
                               {'$set':{'content': text}}, upsert=True)
            except Exception as e:
                logging.warning('insert raw logs error\n'
                                '%s\n%s\n%s\n'
                                % (str(e), account_name, raw_log_file))
        return

#==========================================================================
if __name__ == '__main__':
    account_name = 'qxy_test'

    d = TradeDatabase()
    d.connect('127.0.0.1', user='qxy', password='123456')

    h = d.fetch_holding_positions(account_name)
    for i in h:
        print(i)

    pos1 = {
    "settle_date" : DT.datetime(2016,4,18),
    "contract" : "rb0000",
    "open_strategy" : "db_test",
    "open_order_time" : DT.datetime(1970,1,1),
    "open_deal_time" : DT.datetime(1970,1,1),
    "dir" : "buy",
    "quantity" : 21,
    "open_trigger_price" : 1000.25,
    "open_order_price" : 1001.25,
    "open_deal_price" : 1002.25,
    "open_commission" : 0.0,
    "open_tag" : "qxy_try_pymongo",
    "record_update_time" : DT.datetime(1970,1,1)
    }

    d.rewrite_holding_positions(account_name, DT.datetime(2016,4,18), [pos1])
    d.insert_ats_raw_logs(account_name, DT.datetime(2016,4,18), './trade_logs/Transactions_20160415.log')
