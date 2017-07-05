import re
import datetime as dt

CONTRACT_BASE_INFO = dict()

CONTRACT_BASE_INFO['IC'] = dict()
CONTRACT_BASE_INFO['IC']['minmove'] = 0.2
CONTRACT_BASE_INFO['IC']['multiplier'] = 200

CONTRACT_BASE_INFO['IH'] = dict()
CONTRACT_BASE_INFO['IH']['minmove'] = 0.2
CONTRACT_BASE_INFO['IH']['multiplier'] = 300

CONTRACT_BASE_INFO['T'] = dict()
CONTRACT_BASE_INFO['T']['minmove'] = 0.005
CONTRACT_BASE_INFO['T']['multiplier'] = 10000

CONTRACT_BASE_INFO['ER'] = dict()
CONTRACT_BASE_INFO['ER']['minmove'] = 1
CONTRACT_BASE_INFO['ER']['multiplier'] = 100

CONTRACT_BASE_INFO['JR'] = dict()
CONTRACT_BASE_INFO['JR']['minmove'] = 1
CONTRACT_BASE_INFO['JR']['multiplier'] = 20

CONTRACT_BASE_INFO['LR'] = dict()
CONTRACT_BASE_INFO['LR']['minmove'] = 1
CONTRACT_BASE_INFO['LR']['multiplier'] = 20

CONTRACT_BASE_INFO['MA'] = dict()
CONTRACT_BASE_INFO['MA']['minmove'] = 1
CONTRACT_BASE_INFO['MA']['multiplier'] = 10

CONTRACT_BASE_INFO['PM'] = dict()
CONTRACT_BASE_INFO['PM']['minmove'] = 1
CONTRACT_BASE_INFO['PM']['multiplier'] = 50

CONTRACT_BASE_INFO['RI'] = dict()
CONTRACT_BASE_INFO['RI']['minmove'] = 1
CONTRACT_BASE_INFO['RI']['multiplier'] = 20

CONTRACT_BASE_INFO['RO'] = dict()
CONTRACT_BASE_INFO['RO']['minmove'] = 2
CONTRACT_BASE_INFO['RO']['multiplier'] = 5

CONTRACT_BASE_INFO['RS'] = dict()
CONTRACT_BASE_INFO['RS']['minmove'] = 1
CONTRACT_BASE_INFO['RS']['multiplier'] = 10

CONTRACT_BASE_INFO['SF'] = dict()
CONTRACT_BASE_INFO['SF']['minmove'] = 2
CONTRACT_BASE_INFO['SF']['multiplier'] = 5

CONTRACT_BASE_INFO['SM'] = dict()
CONTRACT_BASE_INFO['SM']['minmove'] = 2
CONTRACT_BASE_INFO['SM']['multiplier'] = 5

CONTRACT_BASE_INFO['WH'] = dict()
CONTRACT_BASE_INFO['WH']['minmove'] = 1
CONTRACT_BASE_INFO['WH']['multiplier'] = 20

CONTRACT_BASE_INFO['WS'] = dict()
CONTRACT_BASE_INFO['WS']['minmove'] = 1
CONTRACT_BASE_INFO['WS']['multiplier'] = 10

CONTRACT_BASE_INFO['WT'] = dict()
CONTRACT_BASE_INFO['WT']['minmove'] = 1
CONTRACT_BASE_INFO['WT']['multiplier'] = 10

CONTRACT_BASE_INFO['ZC'] = dict()
CONTRACT_BASE_INFO['ZC']['minmove'] = 0.2
CONTRACT_BASE_INFO['ZC']['multiplier'] = 100

CONTRACT_BASE_INFO['fu'] = dict()
CONTRACT_BASE_INFO['fu']['minmove'] = 1
CONTRACT_BASE_INFO['fu']['multiplier'] = 50

CONTRACT_BASE_INFO['RI'] = dict()
CONTRACT_BASE_INFO['RI']['minmove'] = 1
CONTRACT_BASE_INFO['RI']['multiplier'] = 20

CONTRACT_BASE_INFO['hc'] = dict()
CONTRACT_BASE_INFO['hc']['minmove'] = 1
CONTRACT_BASE_INFO['hc']['multiplier'] = 10

CONTRACT_BASE_INFO['ni'] = dict()
CONTRACT_BASE_INFO['ni']['minmove'] = 10
CONTRACT_BASE_INFO['ni']['multiplier'] = 1

CONTRACT_BASE_INFO['pb'] = dict()
CONTRACT_BASE_INFO['pb']['minmove'] = 5
CONTRACT_BASE_INFO['pb']['multiplier'] = 5

CONTRACT_BASE_INFO['b'] = dict()
CONTRACT_BASE_INFO['b']['minmove'] = 1
CONTRACT_BASE_INFO['b']['multiplier'] = 10

CONTRACT_BASE_INFO['bb'] = dict()
CONTRACT_BASE_INFO['bb']['minmove'] = 0.05
CONTRACT_BASE_INFO['bb']['multiplier'] = 500

CONTRACT_BASE_INFO['al'] = dict()
CONTRACT_BASE_INFO['al']['minmove'] = 5
CONTRACT_BASE_INFO['al']['multiplier'] = 5

CONTRACT_BASE_INFO['bu'] = dict()
CONTRACT_BASE_INFO['bu']['minmove'] = 2
CONTRACT_BASE_INFO['bu']['multiplier'] = 10

CONTRACT_BASE_INFO['sn'] = dict()
CONTRACT_BASE_INFO['sn']['minmove'] = 10
CONTRACT_BASE_INFO['sn']['multiplier'] = 1

CONTRACT_BASE_INFO['wr'] = dict()
CONTRACT_BASE_INFO['wr']['minmove'] = 1
CONTRACT_BASE_INFO['wr']['multiplier'] = 10

CONTRACT_BASE_INFO['ru'] = dict()
CONTRACT_BASE_INFO['ru']['minmove'] = 5
CONTRACT_BASE_INFO['ru']['multiplier'] = 10

CONTRACT_BASE_INFO['cu'] = dict()
CONTRACT_BASE_INFO['cu']['minmove'] = 10
CONTRACT_BASE_INFO['cu']['multiplier'] = 5

CONTRACT_BASE_INFO['rb'] = dict()
CONTRACT_BASE_INFO['rb']['minmove'] = 1
CONTRACT_BASE_INFO['rb']['multiplier'] = 10

CONTRACT_BASE_INFO['au'] = dict()
CONTRACT_BASE_INFO['au']['minmove'] = 0.05
CONTRACT_BASE_INFO['au']['multiplier'] = 1000

CONTRACT_BASE_INFO['ag'] = dict()
CONTRACT_BASE_INFO['ag']['minmove'] = 1
CONTRACT_BASE_INFO['ag']['multiplier'] = 15

CONTRACT_BASE_INFO['zn'] = dict()
CONTRACT_BASE_INFO['zn']['minmove'] = 5
CONTRACT_BASE_INFO['zn']['multiplier'] = 5

CONTRACT_BASE_INFO['m'] = dict()
CONTRACT_BASE_INFO['m']['minmove'] = 1
CONTRACT_BASE_INFO['m']['multiplier'] = 10

CONTRACT_BASE_INFO['y'] = dict()
CONTRACT_BASE_INFO['y']['minmove'] = 2
CONTRACT_BASE_INFO['y']['multiplier'] = 10

CONTRACT_BASE_INFO['a'] = dict()
CONTRACT_BASE_INFO['a']['minmove'] = 1
CONTRACT_BASE_INFO['a']['multiplier'] = 10

CONTRACT_BASE_INFO['c'] = dict()
CONTRACT_BASE_INFO['c']['minmove'] = 1
CONTRACT_BASE_INFO['c']['multiplier'] = 10

CONTRACT_BASE_INFO['p'] = dict()
CONTRACT_BASE_INFO['p']['minmove'] = 2
CONTRACT_BASE_INFO['p']['multiplier'] = 10

CONTRACT_BASE_INFO['j'] = dict()
CONTRACT_BASE_INFO['j']['minmove'] = 0.5
CONTRACT_BASE_INFO['j']['multiplier'] = 100

CONTRACT_BASE_INFO['l'] = dict()
CONTRACT_BASE_INFO['l']['minmove'] = 5
CONTRACT_BASE_INFO['l']['multiplier'] = 5

CONTRACT_BASE_INFO['RM'] = dict()
CONTRACT_BASE_INFO['RM']['minmove'] = 1
CONTRACT_BASE_INFO['RM']['multiplier'] = 10

CONTRACT_BASE_INFO['IF'] = dict()
CONTRACT_BASE_INFO['IF']['minmove'] = 0.2
CONTRACT_BASE_INFO['IF']['multiplier'] = 300

CONTRACT_BASE_INFO['TF'] = dict()
CONTRACT_BASE_INFO['TF']['minmove'] = 0.005
CONTRACT_BASE_INFO['TF']['multiplier'] = 10000

CONTRACT_BASE_INFO['jd'] = dict()
CONTRACT_BASE_INFO['jd']['minmove'] = 1
CONTRACT_BASE_INFO['jd']['multiplier'] = 10

CONTRACT_BASE_INFO['i'] = dict()
CONTRACT_BASE_INFO['i']['minmove'] = 0.5
CONTRACT_BASE_INFO['i']['multiplier'] = 100

CONTRACT_BASE_INFO['bb'] = dict()
CONTRACT_BASE_INFO['bb']['minmove'] = 0.05
CONTRACT_BASE_INFO['bb']['multiplier'] = 500

CONTRACT_BASE_INFO['fb'] = dict()
CONTRACT_BASE_INFO['fb']['minmove'] = 0.05
CONTRACT_BASE_INFO['fb']['multiplier'] = 500

CONTRACT_BASE_INFO['TA'] = dict()
CONTRACT_BASE_INFO['TA']['minmove'] = 2
CONTRACT_BASE_INFO['TA']['multiplier'] = 5

CONTRACT_BASE_INFO['TC'] = dict()
CONTRACT_BASE_INFO['TC']['minmove'] = 0.2
CONTRACT_BASE_INFO['TC']['multiplier'] = 200

CONTRACT_BASE_INFO['SR'] = dict()
CONTRACT_BASE_INFO['SR']['minmove'] = 1
CONTRACT_BASE_INFO['SR']['multiplier'] = 10

CONTRACT_BASE_INFO['CF'] = dict()
CONTRACT_BASE_INFO['CF']['minmove'] = 5
CONTRACT_BASE_INFO['CF']['multiplier'] = 5

CONTRACT_BASE_INFO['FG'] = dict()
CONTRACT_BASE_INFO['FG']['minmove'] = 1
CONTRACT_BASE_INFO['FG']['multiplier'] = 20

CONTRACT_BASE_INFO['OI'] = dict()
CONTRACT_BASE_INFO['OI']['minmove'] = 2
CONTRACT_BASE_INFO['OI']['multiplier'] = 10

CONTRACT_BASE_INFO['ME'] = dict()
CONTRACT_BASE_INFO['ME']['minmove'] = 1
CONTRACT_BASE_INFO['ME']['multiplier'] = 50

CONTRACT_BASE_INFO['pp'] = dict()
CONTRACT_BASE_INFO['pp']['minmove'] = 1
CONTRACT_BASE_INFO['pp']['multiplier'] = 5

CONTRACT_BASE_INFO['v'] = dict()
CONTRACT_BASE_INFO['v']['minmove'] = 5
CONTRACT_BASE_INFO['v']['multiplier'] = 5

CONTRACT_BASE_INFO['jm'] = dict()
CONTRACT_BASE_INFO['jm']['minmove'] = 0.5
CONTRACT_BASE_INFO['jm']['multiplier'] = 60

CONTRACT_BASE_INFO['cs'] = dict()
CONTRACT_BASE_INFO['cs']['minmove'] = 1
CONTRACT_BASE_INFO['cs']['multiplier'] = 10

# ==============================================
# COMMISSION_ONE_SIDE = ['sr', 'fg', 'rm', 'tc', 'cu', 'ag', 'au']
# COMMISSION_DIF_SIDE = ['m', 'i', 'j', 'jm', 'bb', 'fb']

COMMISSION_RATE = dict()

# Exchange rate
COMMISSION_RATE['exchange'] = dict()
COMMISSION_RATE['exchange']['ag'] = 0.00005
COMMISSION_RATE['exchange']['al'] = 3
COMMISSION_RATE['exchange']['au'] = 10
COMMISSION_RATE['exchange']['bu'] = 0.0001
COMMISSION_RATE['exchange']['cu'] = 0.00005
COMMISSION_RATE['exchange']['fu'] = 0.00002
COMMISSION_RATE['exchange']['hc'] = 0.0001
COMMISSION_RATE['exchange']['ni'] = 6
COMMISSION_RATE['exchange']['pb'] = 0.00004
COMMISSION_RATE['exchange']['rb'] = 0.0001
COMMISSION_RATE['exchange']['ru'] = 0.000045
COMMISSION_RATE['exchange']['sn'] = 3
COMMISSION_RATE['exchange']['wr'] = 0.00004
COMMISSION_RATE['exchange']['zn'] = 3
COMMISSION_RATE['exchange']['a'] = 2
COMMISSION_RATE['exchange']['b'] = 2
COMMISSION_RATE['exchange']['bb'] = 0.0001
COMMISSION_RATE['exchange']['c'] = 1.2
COMMISSION_RATE['exchange']['cs'] = 1.5
COMMISSION_RATE['exchange']['fb'] = 0.0001
COMMISSION_RATE['exchange']['i'] = 0.00018
COMMISSION_RATE['exchange']['j'] = 0.00018
COMMISSION_RATE['exchange']['jd'] = 0.00015
COMMISSION_RATE['exchange']['jm'] = 0.00018
COMMISSION_RATE['exchange']['l'] = 2
COMMISSION_RATE['exchange']['m'] = 1.5
COMMISSION_RATE['exchange']['p'] = 2.5
COMMISSION_RATE['exchange']['pp'] = 0.00018
COMMISSION_RATE['exchange']['v'] = 2
COMMISSION_RATE['exchange']['y'] = 2.5
COMMISSION_RATE['exchange']['CF'] = 4.3
COMMISSION_RATE['exchange']['FG'] = 3
COMMISSION_RATE['exchange']['JR'] = 3
COMMISSION_RATE['exchange']['LR'] = 3
COMMISSION_RATE['exchange']['MA'] = 1.4
COMMISSION_RATE['exchange']['OI'] = 2.5
COMMISSION_RATE['exchange']['PM'] = 5
COMMISSION_RATE['exchange']['RI'] = 2.5
COMMISSION_RATE['exchange']['RM'] = 1.5
COMMISSION_RATE['exchange']['RS'] = 2
COMMISSION_RATE['exchange']['SF'] = 3
COMMISSION_RATE['exchange']['SM'] = 3
COMMISSION_RATE['exchange']['SR'] = 3
COMMISSION_RATE['exchange']['TA'] = 3
COMMISSION_RATE['exchange']['WH'] = 2.5
COMMISSION_RATE['exchange']['ZC'] = 4
COMMISSION_RATE['exchange']['IC'] = 0.000023
COMMISSION_RATE['exchange']['IF'] = 0.000023
COMMISSION_RATE['exchange']['IH'] = 0.000023
COMMISSION_RATE['exchange']['T'] = 3
COMMISSION_RATE['exchange']['TF'] = 3
COMMISSION_RATE['exchange']['IC'] = 0.0001
COMMISSION_RATE['exchange']['IF'] = 0.0001
COMMISSION_RATE['exchange']['IH'] = 0.0001

# default: 1.1 times Exchange rate
COMMISSION_RATE['default'] = dict()
for k in COMMISSION_RATE['exchange']:
    COMMISSION_RATE['default'][k] = 1.1 * COMMISSION_RATE['exchange'][k]


# ===============================================
def get_symbol(contract_code):
    symbol = re.findall(r'^([a-zA-Z]+)', contract_code)
    if symbol and symbol[0] in CONTRACT_BASE_INFO:
        return symbol[0]
    return ''


def get_minmove(code):
    if code not in CONTRACT_BASE_INFO:
        code = get_symbol(code)
    if code:
        return CONTRACT_BASE_INFO[code]['minmove']
    else:
        print('miss minmove data : %s' % code)
        CONTRACT_BASE_INFO[code]['minmove'] = input('add manually >>>')
        return CONTRACT_BASE_INFO[code]['minmove']


def get_multiplier(code):
    if code not in CONTRACT_BASE_INFO:
        code = get_symbol(code)
    if code:
        return CONTRACT_BASE_INFO[code]['multiplier']
    else:
        print('miss multiplier data : %s' % code)
        CONTRACT_BASE_INFO[code]['multiplier'] = input('add manually >>>')
        return CONTRACT_BASE_INFO[code]['multiplier']


def get_commission_info(code, account='default'):
    if account not in COMMISSION_RATE:
        account = 'default'
    return COMMISSION_RATE[account][code]


def get_commission(account, code, price, quantity):
    if code not in CONTRACT_BASE_INFO:
        code = get_symbol(code)
    fee_rate = get_commission_info(code, account)
    if fee_rate < 0.01:
        cost = price * quantity * fee_rate * get_multiplier(code)
    else:
        cost = quantity * fee_rate
    return round(cost, 2)


def nighttime_checkto_settle_date(deal_time, deal_date):
    """
    将夜盘成交的成交日期转化为结算日日期以判断是否是日内平仓
    :param deal_time: 
    :param deal_date: 
    :return: 
    """
    if deal_time >= dt.time(21, 0, 0):
        if deal_date.weekday() == 4:
            aDay = dt.timedelta(days=3)
            deal_date += aDay
        elif deal_date == 5:
            aDay = dt.timedelta(days=2)
            deal_date += aDay
        elif deal_date == 6:
            aDay = dt.timedelta(days=1)
            deal_date += aDay
        elif deal_date <= 3:
            aDay = dt.timedelta(days=1)
            deal_date += aDay
    return deal_date


def get_close_commission(account, code, price, quantity, open_deal_datetime, close_deal_datetime):
    open_deal_datetime = dt.datetime.strptime(open_deal_datetime, "%Y-%m-%d %H:%M:%S.%f")
    open_deal_date = open_deal_datetime.date()
    open_deal_time = open_deal_datetime.time()
    close_deal_datetime = dt.datetime.strptime(close_deal_datetime, "%Y-%m-%d %H:%M:%S.%f")
    close_deal_date = close_deal_datetime.date()
    close_deal_time = close_deal_datetime.time()

    open_deal_date = nighttime_checkto_settle_date(open_deal_time, open_deal_date)
    close_deal_date = nighttime_checkto_settle_date(close_deal_time, close_deal_date)

    if code not in CONTRACT_BASE_INFO:
        code = get_symbol(code)
    fee_rate = get_commission_info(code, account)
    if fee_rate < 0.01:
        cost = price * quantity * fee_rate * get_multiplier(code)
    else:
        cost = quantity * fee_rate
    return round(cost, 2)


'''
def trade_close_intraday(indate, outdate):
    return indate.date() == outdate.date()

def NightPeriod(indate,outdate):
    if indate!=outdate:
        d_in  = datetime.date(int(indate/10000),int(indate%10000/100),int(indate%100))
        d_out = datetime.date(int(outdate/10000),int(outdate%10000/100),int(outdate%100))
        return d_in==d_out-datetime.timedelta(days=1)
    else:
        return False
'''
