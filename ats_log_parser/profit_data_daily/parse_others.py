# -*- coding: utf-8 -*-
import re
import os
import datetime as dt
import xml.etree.ElementTree as ElementTree


########################################################################################################################
class ParseSymbolTree:
    def __init__(self):
        if os.path.exists(r'.\module\symbol_tree.xml'):
            self.file = r'.\module\symbol_tree.xml'
        else:
            self.file = 'symbol_tree.xml'
        self.tree = ElementTree.parse(self.file)
        self.root = self.tree.getroot()

    # ----------------------------------------------------------------------------------------------------------------------
    def trade_time_from_to(self):
        # 得到symbol tree内的所有信息
        whole_time_from_to = []
        for area in self.root:
            for exchange in area:
                for derivative in exchange:
                    for contract in derivative:
                        for time_rule in contract:
                            for stage in time_rule:
                                whole_time_from_to.append(
                                    [str(area.tag),
                                     str(exchange.tag),
                                     str(derivative.tag),
                                     str(contract.tag),
                                     int(re.findall(r'(^[0-9]+)T.*', time_rule.get('from'))[0]),
                                     int(re.findall(r'(^[0-9]+)T.*', time_rule.get('to'))[0]),
                                     int(stage.get('from')),
                                     int(stage.get('to'))])
        return whole_time_from_to

    # ----------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_time_from_to(whole_time_from_to, futures, date):
        # 某个品种在某日的交易时间规则
        time_from_to = []
        for i in range(len(whole_time_from_to)):
            if (futures == whole_time_from_to[i][3]) and \
                    ((date >= whole_time_from_to[i][4]) and (date < whole_time_from_to[i][5])):
                time_from_to.append(whole_time_from_to[i])
        return time_from_to


########################################################################################################################
def get_new_profit_data(md, ci, pst, account, new_trades_position_data):
    """
    遍历单账户内需要更新的多日成交和持仓
    并计算单账户的成交和持仓收益
    """
    new_profit_data = []
    for daily_position in new_trades_position_data:
        settle_date = daily_position['settle_date']
        closed_trades = daily_position['closed_trades']
        holding_positions = daily_position['holding_positions']

        # 当日成交收益
        closed_trades_profit = round(sum(closed_trades['net_profit']), 2)
        # 当日持仓结算，按照收盘价来计算
        holding_positions_profit = round(parse_every_day_holding(md, ci, pst, settle_date, holding_positions), 2)
        whole_profit = round(closed_trades_profit + holding_positions_profit, 2)

        new_profit_data.append(
            [settle_date,
             whole_profit,
             closed_trades_profit,
             holding_positions_profit]
        )
        print(account, settle_date)
    return new_profit_data


# ----------------------------------------------------------------------------------------------------------------------
def parse_every_day_holding(md, ci, pst, settle_date, holding_positions):
    """计算单账户下的单日持仓收益"""
    # futures_info = pd.read_csv(r'.\futures.csv')
    date = int(dt.datetime.strptime(settle_date, "%Y-%m-%d").strftime("%Y%m%d"))
    futures_holding_profit = list()
    whole_time_from_to = pst.trade_time_from_to()
    for j in holding_positions.index:
        # 获取持仓信息内的各种key
        futures = holding_positions['symbol'][j]
        contract = holding_positions['contract'][j]
        if futures is None:
            futures = re.findall(r'(^[a-zA-Z]+)[0-9]+$', contract)[0]
        mul = int(ci.get_contract_info(contract, str(date))['VolumeMultiple'])
        time_from_to = pst.get_time_from_to(whole_time_from_to, futures, date)
        close_time = 0
        for piece in time_from_to:
            if piece[7] <= 200000:
                if piece[7] > close_time:
                    close_time = piece[7]
        close_datetime = ' '.join((settle_date, str(close_time)))
        close_datetime = dt.datetime.strptime(close_datetime, "%Y-%m-%d %H%M%S").strftime("%Y-%m-%d %H:%M:%S")

        # 抓取wind和实时行情数据的last_price
        close_price = md.get_close_price(contract, close_datetime)

        if not type(close_price) is str:
            profit = (close_price - holding_positions['open_deal_price'][j]) * \
                     holding_positions['dir'][j] * \
                     holding_positions['quantity'][j] * mul
        else:
            print("ERROR", futures, settle_date, close_price)
            profit = 0

        futures_holding_profit.append(profit)

    return sum(futures_holding_profit)


def clean_profit_data(profit_data):
    """清理权益全为0的垃圾结算日"""
    for i in list(range(len(profit_data)))[::-1]:
        profit = profit_data[i][1] == 0
        closed = profit_data[i][2] == 0
        hold = profit_data[i][3] == 0
        if profit and closed and hold:
            profit_data.pop(i)

    return profit_data


if __name__ == "__main__":
    print(ParseSymbolTree().trade_time_from_to())
