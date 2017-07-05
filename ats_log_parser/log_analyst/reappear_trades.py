# coding=utf8
import logging, datetime
from .raw_log_parser import parse_log_file
from .future_baseinfo import get_symbol


def __raw_signal_to_dict(signals):
    side_trades = list()
    for s in signals:
        sig = dict()
        sig['symbol'] = get_symbol(s.instrument)
        sig['contract'] = s.instrument
        sig['dir'] = s.dir
        sig['quantity'] = s.quantity
        sig['strategy'] = s.strategy
        sig['order_time'] = None
        sig['deal_time'] = None
        sig['trigger_price'] = s.price
        sig['order_price'] = None
        sig['deal_price'] = None
        sig['commission'] = None
        sig['tag'] = s.tag
        # addtional
        sig['startup'] = s.startup
        sig['runtime'] = s.runtime
        sig['signal'] = s.signal
        sig['cpu_tick'] = s.cpu_tick
        side_trades.append(sig)
    return side_trades


def __raw_orders_to_dict(orders):
    order_list = list()
    for o in orders:
        order = dict()
        order['symbol'] = get_symbol(o.instrument)
        order['contract'] = o.instrument
        order['dir'] = o.dir
        order['quantity'] = o.quantity
        order['traded'] = 0
        order['order_time'] = o.datetime
        order['deal_time'] = None
        order['order_price'] = o.price
        order['deal_price'] = None
        # addtional
        order['startup'] = o.startup
        order['runtime'] = o.runtime
        order['signal'] = o.signal
        order['order'] = o.order
        order['cpu_tick'] = o.cpu_tick
        order_list.append(order)
    return order_list


def __fill_raw_trades_into_order(order_list, raw_trades):
    for t in raw_trades:
        for o in order_list:
            if o['quantity'] == o['traded']:
                continue
            if not t.startup == o['startup']:
                continue
            if not t.runtime == o['runtime']:
                continue
            if not t.order == o['order']:
                continue
            # match
            if o['traded'] > 0:
                cost = o['traded'] * o['deal_price']
                cost += t.quantity * t.price
                o['traded'] += t.quantity
                o['deal_price'] = cost / o['traded']
            else:
                o['traded'] += t.quantity
                o['deal_price'] = t.price
            # update last traded time
            trade_delay = t.cpu_tick - o['cpu_tick']
            trade_delay = datetime.timedelta(microseconds=trade_delay)
            o['deal_time'] = o['order_time'] + trade_delay
            # fully deal
            if o['quantity'] < o['traded']:
                raise Exception('order quantity over filled\n'
                                '%s' % str(o))
            break
        else:
            logging.warning('trade cant find suitable order:\n'
                            'rt:%d, ord:%d, qty:%d, pri:%.4f, dt:%s'
                            % (t.runtime, t.order, t.quantity,
                               t.price, str(t.datetime)))
    return


def __combine_signal_and_order(signal_list, order_list):
    side_trades = list()
    for o in order_list:
        if o['traded'] < 1:
            continue
        for i in range(len(signal_list)):
            s = signal_list[i]
            if not s['quantity'] > 0:
                continue
            if not o['startup'] == s['startup']:
                continue
            if not o['runtime'] == s['runtime']:
                continue
            if not o['signal'] == s['signal']:
                continue
            if not o['dir'] == s['dir']:
                continue
            # create side_trade per order
            info = dict(s)
            info['order_time'] = o['order_time']
            info['deal_time'] = o['deal_time']
            info['order_price'] = o['order_price']
            info['deal_price'] = o['deal_price']
            # check validation
            if o['traded'] <= s['quantity']:
                s['quantity'] -= o['traded']
                info['quantity'] = o['traded']
                side_trades.append(info)
            else:
                raise Exception('order quantity overtake signal quantity\n'
                                'signal - %s\norder - %s\n' %
                                (str(s), str(o))
                                )
            break
        else:
            logging.warning('order cant find suitable signal\n'
                            '%s' % str(o))

    unfilled_side_trades = list()
    for s in signal_list:
        if s['quantity'] > 0:
            unfilled_side_trades.append(dict(s))

    return side_trades, unfilled_side_trades


def parse_trades_from_logfile(filename, parse_from_dt=None, parse_until_dt=None):
    """
    :param filename: 日志文件名
    :param parse_from_dt: 解析log时间戳大于等于此时间的日志
    :param parse_until_dt: 解析log时间戳小于此时间的日志
    :return:
    """

    raw_signals, raw_orders, raw_trades = parse_log_file(filename, parse_from_dt, parse_until_dt)
    # convert signals
    signal_list = __raw_signal_to_dict(raw_signals)
    order_list = __raw_orders_to_dict(raw_orders)
    # fill trade into order
    __fill_raw_trades_into_order(order_list, raw_trades)
    # fill order into side trade
    side_trades, unfilled = __combine_signal_and_order(signal_list, order_list)
    # classify
    open_trades = list()
    close_trade = list()
    for t in side_trades:
        if t['dir'] == 'BUY':
            t['dir'] = 1
            open_trades.append(t)
        elif t['dir'] == 'SELLSHORT':
            t['dir'] = -1
            open_trades.append(t)
        elif t['dir'] == 'SELL':
            t['dir'] = -1
            close_trade.append(t)
        elif t['dir'] == 'BUYTOCOVER':
            t['dir'] = 1
            close_trade.append(t)
        else:
            logging.warning('trade with unknown dir:\n%s' % str(t))
    unfilled_opens = list()
    unfilled_closes = list()
    for t in unfilled:
        if t['dir'] == 'BUY':
            t['dir'] = 1
            unfilled_opens.append(t)
        elif t['dir'] == 'SELLSHORT':
            t['dir'] = -1
            unfilled_opens.append(t)
        elif t['dir'] == 'SELL':
            t['dir'] = -1
            unfilled_closes.append(t)
        elif t['dir'] == 'BUYTOCOVER':
            t['dir'] = 1
            unfilled_closes.append(t)
        else:
            logging.warning('trade with unknown dir:\n%s' % str(t))
    return open_trades, close_trade, unfilled_opens, unfilled_closes


def amalgamated_trades(holding_pos_unamalgamated, closed_trades_unamalgamated, odd_closes_unamalgamated):
    holding_pos = list()
    closed_trades = list()
    odd_closes = list()

    temp = []
    temp_val = {}
    temp_qua = {}
    for i in list(range(len(holding_pos_unamalgamated)))[::-1]:

        name = holding_pos_unamalgamated[i]['contract'] + \
               str(holding_pos_unamalgamated[i]['dir']) + \
               holding_pos_unamalgamated[i]['open_strategy']
        if name in temp:
            temp_val[name] = temp_val[name] + holding_pos_unamalgamated[i]['quantity'] * holding_pos_unamalgamated[i]['open_deal_price']
            temp_qua[name] = temp_qua[name] + holding_pos_unamalgamated[i]['quantity']
        else:
            temp.append(name)
            temp_val[name] = holding_pos_unamalgamated[i]['quantity'] * holding_pos_unamalgamated[i]['open_deal_price']
            temp_qua[name] = holding_pos_unamalgamated[i]['quantity']
            holding_pos.append(holding_pos_unamalgamated[i])

    for i in range(len(holding_pos)):
        name = holding_pos[i]['contract'] + \
               str(holding_pos[i]['dir']) + \
               holding_pos[i]['open_strategy']
        holding_pos[i]['quantity'] = temp_qua[name]
        holding_pos[i]['open_deal_price'] = temp_val[name] / temp_qua[name]
        holding_pos[i]['open_strategy'] = 'holding'

    temp = []
    temp_val = {}
    temp_qua = {}
    for i in list(range(len(closed_trades_unamalgamated)))[::-1]:

        name = closed_trades_unamalgamated[i]['contract'] + \
               str(closed_trades_unamalgamated[i]['dir']) + \
               closed_trades_unamalgamated[i]['close_strategy']
        if name in temp:
            temp_val[name] = temp_val[name] + closed_trades_unamalgamated[i]['quantity'] * \
                                              closed_trades_unamalgamated[i]['close_deal_price']
            temp_qua[name] = temp_qua[name] + closed_trades_unamalgamated[i]['quantity']
        else:
            temp.append(name)
            temp_val[name] = closed_trades_unamalgamated[i]['quantity'] * closed_trades_unamalgamated[i][
                'close_deal_price']
            temp_qua[name] = closed_trades_unamalgamated[i]['quantity']
            closed_trades.append(closed_trades_unamalgamated[i])

    for i in range(len(closed_trades)):
        name = closed_trades[i]['contract'] + \
               str(closed_trades[i]['dir']) + \
               closed_trades[i]['close_strategy']
        closed_trades[i]['quantity'] = temp_qua[name]
        closed_trades[i]['close_deal_price'] = temp_val[name] / temp_qua[name]

    temp = []
    temp_val = {}
    temp_qua = {}
    for i in list(range(len(odd_closes_unamalgamated)))[::-1]:

        name = odd_closes_unamalgamated[i]['contract'] + \
               str(odd_closes_unamalgamated[i]['dir']) + \
               odd_closes_unamalgamated[i]['close_strategy']
        if name in temp:
            temp_val[name] = temp_val[name] + odd_closes_unamalgamated[i]['quantity'] * odd_closes_unamalgamated[i][
                'close_deal_price']
            temp_qua[name] = temp_qua[name] + odd_closes_unamalgamated[i]['quantity']
        else:
            temp.append(name)
            temp_val[name] = odd_closes_unamalgamated[i]['quantity'] * odd_closes_unamalgamated[i]['close_deal_price']
            temp_qua[name] = odd_closes_unamalgamated[i]['quantity']
            odd_closes.append(odd_closes_unamalgamated[i])

    for i in range(len(odd_closes)):
        name = odd_closes[i]['contract'] + \
               str(odd_closes[i]['dir']) + \
               odd_closes[i]['close_strategy']
        odd_closes[i]['quantity'] = temp_qua[name]
        odd_closes[i]['close_deal_price'] = temp_val[name] / temp_qua[name]

    return holding_pos, closed_trades, odd_closes
