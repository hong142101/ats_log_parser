# coding=utf8
from ..db_connector.record_struct import CLOSED_TRADE_FIELDS


def __has_opposite_dir(dir1, dir2):
    return dir1 + dir2 == 0


def __open_new_position(position_list, trade):
    pos = dict()
    pos['symbol'] = trade['symbol']
    pos['contract'] = trade['contract']
    pos['dir'] = trade['dir']
    pos['quantity'] = trade['quantity']
    pos['open_strategy'] = trade['strategy']
    pos['open_order_time'] = trade['order_time']
    pos['open_deal_time'] = trade['deal_time']
    pos['open_trigger_price'] = trade['trigger_price']
    pos['open_order_price'] = trade['order_price']
    pos['open_deal_price'] = trade['deal_price']
    pos['open_commission'] = trade['commission']
    pos['open_tag'] = trade['tag']
    for header in CLOSED_TRADE_FIELDS:
        if header not in pos:
            pos[header] = None
    position_list.append(pos)
    return


def __close_holding_position(position, close_trade):
    if position['quantity'] > close_trade['quantity']:
        position['quantity'] = close_trade['quantity']
    position['close_strategy'] = close_trade['strategy']
    position['close_order_time'] = close_trade['order_time']
    position['close_deal_time'] = close_trade['deal_time']
    position['close_trigger_price'] = close_trade['trigger_price']
    position['close_order_price'] = close_trade['order_price']
    position['close_deal_price'] = close_trade['deal_price']
    position['close_commission'] = close_trade['commission']
    position['close_tag'] = close_trade['tag']
    return position['quantity']


def __odd_closes_to_side_trades(odd_closes_list):
    odd_trades = list()
    for close_trade in odd_closes_list:
        position = dict()
        position['contract'] = close_trade['contract']
        position['dir'] = close_trade['dir']
        position['quantity'] = close_trade['quantity']
        position['close_strategy'] = close_trade['strategy']
        position['close_order_time'] = close_trade['order_time']
        position['close_deal_time'] = close_trade['deal_time']
        position['close_trigger_price'] = close_trade['trigger_price']
        position['close_order_price'] = close_trade['order_price']
        position['close_deal_price'] = close_trade['deal_price']
        position['close_commission'] = close_trade['commission']
        position['close_tag'] = close_trade['tag']
        for header in CLOSED_TRADE_FIELDS:
            if header not in position:
                position[header] = None
        odd_trades.append(position)
    return odd_trades


def __position_readability_sort(pos):
    return (pos['contract'],
            pos['open_strategy'],
            pos['open_order_time'])


def __position_match_rule(name, position, close_trade):
    if not position['quantity'] > 0:
        return False
    if not position['contract'] == close_trade['contract']:
        return False
    if 'amalgamated' not in name:
        if not position['open_strategy'] == close_trade['strategy']:
            return False
    if not __has_opposite_dir(position['dir'], close_trade['dir']):
        return False
    if not position['open_order_time'] <= close_trade['order_time']:
        return False
    return True


def format_unfilled_signal(uf_signal_list, signal, is_open_sig):
    pos = dict()
    for header in CLOSED_TRADE_FIELDS:
        pos[header] = None
    # public part
    pos['symbol'] = signal['symbol']
    pos['contract'] = signal['contract']
    pos['dir'] = signal['dir']
    pos['quantity'] = signal['quantity']
    # open
    if is_open_sig:
        pos['open_strategy'] = signal['strategy']
        pos['open_order_time'] = signal['order_time']
        pos['open_deal_time'] = signal['deal_time']
        pos['open_trigger_price'] = signal['trigger_price']
        pos['open_order_price'] = signal['order_price']
        pos['open_deal_price'] = signal['deal_price']
        pos['open_commission'] = signal['commission']
        pos['open_tag'] = signal['tag']
    # close
    else:
        pos['close_strategy'] = signal['strategy']
        pos['close_order_time'] = signal['order_time']
        pos['close_deal_time'] = signal['deal_time']
        pos['close_trigger_price'] = signal['trigger_price']
        pos['close_order_price'] = signal['order_price']
        pos['close_deal_price'] = signal['deal_price']
        pos['close_commission'] = signal['commission']
        pos['close_tag'] = signal['tag']
    # append
    uf_signal_list.append(pos)
    return


def update_positions_with_trades(name, holding_positions, open_trades, close_trades):
    """
    :param holding_positions:
    :param open_trades:
    :param close_trades:
    :return:
    """
    # append open trades

    for trade in open_trades:
        __open_new_position(holding_positions, trade)
    # sort by time
    holding_positions.sort(key=lambda item: item['open_order_time'])
    close_trades.sort(key=lambda item: item['order_time'])
    # settle close trades
    closed_trades = list()
    odd_closes = list()
    for trade in close_trades:
        for hold_pos in holding_positions:
            if not __position_match_rule(name, hold_pos, trade):
                continue
            # match position
            tar_pos = dict(hold_pos)
            close_qty = __close_holding_position(tar_pos, trade)
            closed_trades.append(tar_pos)
            # minus used part
            trade['quantity'] -= close_qty
            hold_pos['quantity'] -= close_qty
            if trade['quantity'] < 1:
                break
        # unsuitable closes
        if trade['quantity'] > 0:
            odd_closes.append(trade)
    # collect remained holdings
    rem_holdings = list()
    for hold_pos in holding_positions:
        if hold_pos['quantity'] > 0:
            rem_holdings.append(hold_pos)
    # format
    odd_close_trades = __odd_closes_to_side_trades(odd_closes)
    rem_holdings.sort(key=__position_readability_sort)
    closed_trades.sort(key=__position_readability_sort)
    odd_close_trades.sort(key=__position_readability_sort)
    return rem_holdings, closed_trades, odd_close_trades


########################################################################################################################
if __name__ == "__main__":
    pass
