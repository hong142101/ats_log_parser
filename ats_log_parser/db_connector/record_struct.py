# coding=utf8
import datetime

RAW_LOG_COLUMNS = ['account', 'date', 'intraday_seq', 'text']

HOLDING_POS_COLUMNS = [
    'account',
    'settle_date',
    'intraday_seq',
    'symbol',
    'contract',
    'dir',
    'quantity',
    'open_strategy',
    'open_order_time',
    'open_deal_time',
    'open_trigger_price',
    'open_order_price',
    'open_deal_price',
    'open_commission',
    'open_tag',
    'record_update_time']

CLOSED_TD_COLUMNS = [
    'account',
    'symbol',
    'contract',
    'dir',
    'quantity',
    'open_strategy',
    'open_order_time',
    'open_deal_time',
    'open_trigger_price',
    'open_order_price',
    'open_deal_price',
    'open_commission',
    'open_tag',
    'close_strategy',
    'close_order_time',
    'close_deal_time',
    'close_trigger_price',
    'close_order_price',
    'close_deal_price',
    'close_commission',
    'close_tag',
    'net_profit',
    'record_update_time']

CLOSED_TRADE_FIELDS = [
    'symbol',
    "contract",
    "dir",
    "quantity",
    # open side
    "open_strategy",
    "open_order_time",
    "open_deal_time",
    "open_trigger_price",
    "open_order_price",
    "open_deal_price",
    "open_commission",
    "open_tag",
    # close side
    "close_strategy",
    "close_order_time",
    "close_deal_time",
    "close_trigger_price",
    "close_order_price",
    "close_deal_price",
    "close_commission",
    "close_tag",
    #
    "net_profit"
]

CLOSED_TRADE_FIELDS_TYPES = {
    "symbol": str,
    "contract": str,
    "dir": int,
    "quantity": int,
    # open side
    "open_strategy": str,
    "open_order_time": datetime.datetime,
    "open_deal_time": datetime.datetime,
    "open_trigger_price": float,
    "open_order_price": float,
    "open_deal_price": float,
    "open_commission": float,
    "open_tag": str,
    # close side
    "close_strategy": str,
    "close_order_time": datetime.datetime,
    "close_deal_time": datetime.datetime,
    "close_trigger_price": float,
    "close_order_price": float,
    "close_deal_price": float,
    "close_commission": float,
    "close_tag": str,
    #
    "net_profit": float
}
