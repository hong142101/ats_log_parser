# -*- coding: utf-8 -*-
"""
mysql数据库中提取数据用到的列名
"""

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
    'record_update_time'
]

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
    'record_update_time'
]

PROFIT_DATA_COLUMNS = [
    'date',
    'profit',
    'closed_trades',
    'holding_positions'
]
