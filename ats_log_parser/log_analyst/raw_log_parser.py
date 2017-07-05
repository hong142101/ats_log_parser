# coding=utf8
import re
import os
import logging
import datetime


class SignalInfo:
    def __init__(self):
        self.log_datetime = None
        self.startup = None
        self.strategy = None
        self.runtime = None
        self.signal = None
        self.instrument = None
        self.quantity = None
        self.price = None
        self.dir = None
        self.datetime = None
        self.cpu_tick = None
        self.tag = None


class OrderCreationInfo:
    def __init__(self):
        self.log_datetime = None
        self.startup = None
        self.runtime = None
        self.signal = None
        self.order = None
        self.instrument = None
        self.quantity = None
        self.price = None
        self.dir = None
        self.datetime = None
        self.cpu_tick = None


class OrderTradedInfo:
    def __init__(self):
        self.log_datetime = None
        self.startup = None
        self.runtime = None
        self.order = None
        self.quantity = None
        self.price = None
        self.datetime = None
        self.cpu_tick = None


def __parse_signal_row(row):
    try:
        stg = re.findall('stg:([^,]+),', row)
        rt = re.findall('rt:(\d+)', row)
        sig = re.findall('sig:(\d+)', row)
        ins = re.findall('ins:([a-zA-Z]+\d+)', row)
        qty = re.findall('qty:(\d+)', row)
        pri = re.findall('pri:([0-9.]+)', row)
        dr = re.findall('dir:([A-Z]+)', row)
        dt = re.findall('dt:(\d+T\d+)', row)
        ctk = re.findall('ctk:(\d+)', row)
        tag = re.findall('msg:(.*)', row)

        sinfo = SignalInfo()
        sinfo.strategy = stg[0]
        sinfo.runtime = int(rt[0])
        sinfo.signal = int(sig[0])
        sinfo.instrument = ins[0]
        sinfo.quantity = int(qty[0])
        sinfo.price = float(pri[0])
        sinfo.dir = dr[0]
        if len(dt[0]) == 13:
            temp = dt[0].replace("T", "T00")
            sinfo.datetime = datetime.datetime.strptime(temp, "%Y%m%dT%H%M%S")
        elif len(dt[0]) == 14:
            temp = dt[0].replace("T", "T0")
            sinfo.datetime = datetime.datetime.strptime(temp, "%Y%m%dT%H%M%S")
        else:
            sinfo.datetime = datetime.datetime.strptime(dt[0], "%Y%m%dT%H%M%S")
        sinfo.cpu_tick = int(ctk[0])
        sinfo.tag = tag[0]

        return sinfo

    except Exception as e:
        logging.warning('parse_signal_row fail\nerror: %s\nrow: %s'
                        % (str(e), row))
    return None


def __parse_order_creation_row(row):
    try:
        rt = re.findall('rt:(\d+)', row)
        sig = re.findall('sig:(\d+)', row)
        od = re.findall('ord:(\d+)', row)
        ins = re.findall('ins:([a-zA-Z]+\d+)', row)
        qty = re.findall('qty:(\d+)', row)
        pri = re.findall('pri:([0-9.]+)', row)
        dr = re.findall('dir:([A-Z]+)', row)
        dt = re.findall('dt:(\d+T\d+)', row)
        ctk = re.findall('ctk:(\d+)', row)

        sinfo = OrderCreationInfo()
        sinfo.runtime = int(rt[0])
        sinfo.signal = int(sig[0])
        sinfo.order = int(od[0])
        sinfo.instrument = ins[0]
        sinfo.quantity = int(qty[0])
        sinfo.price = float(pri[0])
        sinfo.dir = dr[0]
        if len(dt[0]) == 13:
            temp = dt[0].replace("T", "T00")
            sinfo.datetime = datetime.datetime.strptime(temp, "%Y%m%dT%H%M%S")
        elif len(dt[0]) == 14:
            temp = dt[0].replace("T", "T0")
            sinfo.datetime = datetime.datetime.strptime(temp, "%Y%m%dT%H%M%S")
        else:
            sinfo.datetime = datetime.datetime.strptime(dt[0], "%Y%m%dT%H%M%S")
        sinfo.cpu_tick = int(ctk[0])

        return sinfo

    except Exception as e:
        logging.warning('parse_order_creation_row fail\nerror: %s\nrow: %s'
                        % (str(e), row))
    return None


def __parse_order_traded_row(row):
    try:
        rt = re.findall('rt:(\d+)', row)
        od = re.findall('ord:(\d+)', row)
        qty = re.findall('qty:(\d+)', row)
        pri = re.findall('pri:([0-9.]+)', row)
        dt = re.findall('dt:(\d+T\d+)', row)
        ctk = re.findall('ctk:(\d+)', row)

        sinfo = OrderTradedInfo()
        sinfo.runtime = int(rt[0])
        sinfo.order = int(od[0])
        sinfo.quantity = int(qty[0])
        sinfo.price = float(pri[0])
        if len(dt[0]) == 13:
            temp = dt[0].replace("T", "T00")
            sinfo.datetime = datetime.datetime.strptime(temp, "%Y%m%dT%H%M%S")
        elif len(dt[0]) == 14:
            temp = dt[0].replace("T", "T0")
            sinfo.datetime = datetime.datetime.strptime(temp, "%Y%m%dT%H%M%S")
        else:
            sinfo.datetime = datetime.datetime.strptime(dt[0], "%Y%m%dT%H%M%S")
        sinfo.cpu_tick = int(ctk[0])

        return sinfo

    except Exception as e:
        logging.warning('parse_order_creation_row fail\nerror: %s\nrow: %s'
                        % (str(e), row))
    return None


def __parse_row(row):
    log_dt = re.findall('^\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]\[[A-Z]\]', row)
    if not log_dt or not log_dt[0]:
        return None

    try:
        log_dt = datetime.datetime.strptime(log_dt[0], "%Y/%m/%d %H:%M:%S.%f")
    except Exception as e:
        logging.warning('parse log datetime fail\nerror: %s\nrow: %s'
                        % (str(e), row))
        return None

    log_type = re.findall('<([a-z_]+)>', row)
    if not log_type or not log_type[0]:
        return None

    info = None
    if log_type[0] == 'signal':
        info = __parse_signal_row(row)
    elif log_type[0] == 'order_created':
        info = __parse_order_creation_row(row)
    elif log_type[0] == 'order_traded':
        info = __parse_order_traded_row(row)
    elif log_type[0] == 'order_canceled':
        pass
    elif log_type[0] == 'order_rejected':
        pass

    if info is not None:
        info.log_datetime = log_dt

    return info


def parse_log_file(filename, parse_from_dt=None, parse_until_dt=None):
    """
    :param filename: 日志文件名
    :param parse_from_dt: 解析log时间戳大于等于此时间的日志
    :param parse_until_dt: 解析log时间戳小于此时间的日志
    :return:
    """

    if parse_from_dt is None:
        parse_from_dt = datetime.datetime.min
    if parse_until_dt is None:
        parse_until_dt = datetime.datetime.max

    signals = list()
    orders = list()
    trades = list()
    if os.path.exists(filename):
        # split individual starts
        starts = list()
        lines = list()
        with open(filename) as fid:
            for line in fid:
                row = line.rstrip().lstrip()
                if not row:
                    continue
                spliter_line = re.findall(r'^======', row)
                if not spliter_line:
                    lines.append(line)
                else:
                    starts.append(list(lines))
                    lines.clear()
            if lines:
                starts.append(list(lines))
        # scan
        for n, lines in enumerate(starts):
            for line in lines:
                info = __parse_row(line)
                if info is None:
                    continue
                if info.log_datetime < parse_from_dt:
                    continue
                if info.log_datetime >= parse_until_dt:
                    continue
                # set startup id
                info.startup = n
                if type(info) == SignalInfo:
                    signals.append(info)
                elif type(info) == OrderCreationInfo:
                    orders.append(info)
                elif type(info) == OrderTradedInfo:
                    trades.append(info)

    return signals, orders, trades


# =========================================================================
def main():
    filename = r'C:\D\work_scripts\trade_log_parser\trade_logs\Transactions_20160420.log'
    s, o, t = parse_log_file(filename)

    for i in s:
        print(i)

    for i in o:
        print(i)

    for i in t:
        print(i)


# =========================================================================
if __name__ == '__main__':
    main()
