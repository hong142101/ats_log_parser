#coding=utf8
import hashlib

#calculate this MD5 code to prevent duplicate insertion
#"feature_md5" should be an unique index in document
def _insert_features_md5(data_list, features):
    """
    :param data_list: list[dict1{}, dict2{}, ...]
    :param features: list[feature_str1, feature_str2, ...]
    :return: bool
    """
    for d in data_list:
        feature_str = ''
        for f in features:
            feature_str += str(d[f])
        bcodes = bytes(feature_str, 'utf8')
        md5 = hashlib.md5()
        md5.update(bcodes)
        d['feature_md5'] = md5.hexdigest()
    return True


POSITION_FEATURES = [
        'account', 'settle_date', 'contract', 'open_strategy',
        'open_order_time', 'open_deal_time', 'dir',
        'quantity', 'open_trigger_price', 'open_order_price',
        'open_deal_price', 'open_tag'
    ]

CLOSED_TRADE_FEATURES = [
        'account', 'contract', 'dir', 'quantity',
        'open_strategy', 'open_order_time', 'open_deal_time',
        'open_trigger_price', 'open_order_price', 'open_deal_price',
        'open_tag',
        'close_strategy', 'close_order_time', 'close_deal_time',
        'close_trigger_price', 'close_order_price', 'close_deal_price',
        'close_tag'
    ]


def add_position_features(positions):
    return _insert_features_md5(positions, POSITION_FEATURES)

def add_trades_features(closed_trades):
    return _insert_features_md5(closed_trades, CLOSED_TRADE_FEATURES)