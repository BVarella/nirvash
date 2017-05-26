import ctypes as ct
import argparse
from collections import deque

class MomentStruct(ct.Structure):
    _fields_ = [("year", ct.c_ulonglong, 12),
                ("month", ct.c_ulonglong, 4),
                ("day", ct.c_ulonglong, 5),
                ("hour", ct.c_ulonglong, 5),
                ("minute", ct.c_ulonglong, 6),
                ("second", ct.c_ulonglong, 6),
                ("microssecond", ct.c_ulonglong, 20)]

class TradeStruct(ct.Structure):
    _fields_ = [("number", ct.c_ulonglong),
                ("price", ct.c_double),
                ("quantity", ct.c_ulonglong),
                ("moment", MomentStruct),
                ("indicator", ct.c_ubyte),
                ("buy_number", ct.c_ulonglong),
                ("sell_number", ct.c_ulonglong),
                ("next_type", ct.c_ubyte)]

class OrderStruct(ct.Structure):
    _fields_ = [("number", ct.c_ulonglong),
                ("side", ct.c_ubyte),
                ("type", ct.c_ubyte),
                ("moment", MomentStruct),
                ("priority", ct.c_ulonglong),
                ("price", ct.c_double),
                ("quantity", ct.c_ulonglong),
                ("traded", ct.c_ulonglong),
                ("datetime", MomentStruct),
                ("status", ct.c_char),
                ("agressor", ct.c_ubyte),
                ("member", ct.c_ulonglong),
                ("next_type", ct.c_ubyte)]

class HeaderStruct(ct.Structure):
    _fields_ = [("symbol", ct.c_char * 16),
                ("date", MomentStruct),
                ("next_type", ct.c_ubyte)]

def wildcard(symbol, date):
    def WDO(month, year):
        letters = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]
        letter = letters[month % 12]
        postfix = year % 100 + (1 if month == 12 else 0)
        return "WDO%s%s" % (letter, postfix)

    if "%" not in symbol:
        return symbol

    if symbol == "WDO%":
        return WDO(date.month, date.year)
    else:
        raise "Wildcard not implemented."

def load_date(text, moment):
    moment.year = int(text[:4])
    moment.month = int(text[5:7])
    moment.day = int(text[8:10])

def load_time(text, moment):
    moment.hour = int(text[:2])
    moment.minute = int(text[3:5])
    moment.second = int(text[6:8])

    if len(text) > 8:
        microssecond = text[9:]
        if len(microssecond) == 6:
            moment.microssecond = int(microssecond)
        else:
            moment.microssecond = int(microssecond) * int(1E3)
    else:
        moment.microssecond = 0

def load_datetime(text, moment):
    load_date(text[:10], moment)
    load_time(text[11:], moment)

def header_info(text):
    moment = MomentStruct()
    load_date(text[23:33], moment)
    amount = int(text[45:54]) - 2
    return moment, amount

def extend_moment(target, source):
    target.year = source.year
    target.month = source.month
    target.day = source.day
    target.hour = source.hour
    target.minute = source.minute
    target.second = source.second
    target.microssecond = source.microssecond

def symbol_name(text):
    return text[11:61].strip()

def parse_trade(text, moment):
    trade = TradeStruct()
    trade.number = int(text[62:72])
    trade.price = float(text[74:93])
    trade.quantity = int(text[94:112])
    extend_moment(trade.moment, moment)
    load_time(text[113:125], trade.moment)
    trade.indicator = int(text[126])
    trade.buy_number = int(text[139:154])
    trade.sell_number = int(text[184:199])
    return trade

def parse_order(text, moment):
    order = OrderStruct()
    order.number = int(text[64:79])
    order.side = int(text[62])
    order.type = int(text[96:99])
    extend_moment(order.moment, moment)
    load_time(text[100:115], order.moment)
    order.priority = int(text[116:126])
    order.price = float(text[128:147])
    order.quantity = int(text[148:166])
    order.traded = int(text[167:185])
    load_datetime(text[197:216], order.datetime)
    order.status = text[217].encode('utf8')
    order.agressor = int(text[219])
    order.member = int(text[221:])
    return order

def parse_file(filename, symbol, parser):
    elements = deque()
    with open(filename, "r") as file:
        date, amount = header_info(next(file))
        symbol = wildcard(symbol, date)
        for i in range(amount):
            text = next(file)
            if symbol_name(text) == symbol:
                elements.append(parser(text, date))
            if (i + 1) % 50000 == 0:
                print("%d parsed lines..." % (i + 1))
    return (date, symbol, elements)

def parse_trade_file(filename, symbol):
    return parse_file(filename, symbol, parse_trade)

def parse_order_file(filename, symbol):
    return parse_file(filename, symbol, parse_order)

def sort_key(event):
    def order_sort_key(order):
        return (
            order.moment.hour,
            order.moment.minute,
            order.moment.second,
            order.moment.microssecond,
            0,
            order.priority,
            order.number)

    def trade_sort_key(trade):
        return (
            trade.moment.hour,
            trade.moment.minute,
            trade.moment.second,
            trade.moment.microssecond,
            1,
            0,
            trade.number)

    if type(event) == TradeStruct:
        return trade_sort_key(event)
    else:
        return order_sort_key(event)

    return map(sort_tuple, trades)

def parse_events(tradepath, buypath, sellpath, symbol, targetpath):
    print("Parsing trades")
    date, symbol, trade_events = parse_trade_file(tradepath, symbol)
    print("Parsing buy orders")
    _, _, buy_events = parse_order_file(buypath, symbol)
    print("Parsing sell orders")
    _, _, sell_events = parse_order_file(sellpath, symbol)

    print("Sorting")
    events = deque()
    events.extend(trade_events)
    events.extend(buy_events)
    events.extend(sell_events)
    events = sorted(events, key=sort_key)

    print("Composing event file")
    header = HeaderStruct()
    header.moment = date
    header.symbol = symbol.encode("utf8")

    with open(targetpath, "wb") as file:
        last = header
        for i, event in enumerate(events):
            if type(event) == TradeStruct:
                last.next_type = 1
            else:
                last.next_type = 2
            file.write(last)

            if (i + 1) % 10000 == 0:
                print("%d events written" % (i + 1))

            last = event
        file.write(last)

    print("Done!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bovespa marketdata converter.")
    parser.add_argument(
        "symbol",
        type=str,
        help="the requested symbol to be extracted")
    parser.add_argument("tradepath", type=str, help="trades filepath")
    parser.add_argument("buypath", type=str, help="buy orders filepath")
    parser.add_argument("sellpath", type=str, help="sell orders filepath")

    args = parser.parse_args()
    parse_events(
        args.tradepath,
        args.buypath,
        args.sellpath,
        args.symbol,
        args.targetpath)
