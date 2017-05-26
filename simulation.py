import argparse
from collections import deque
from parsers import TradeStruct, OrderStruct, HeaderStruct

class Moment:
    def __init__(self, moment_struct):
        self.year = moment_struct.year
        self.month = moment_struct.month
        self.day = moment_struct.day
        self.hour = moment_struct.hour
        self.minute = moment_struct.minute
        self.second = moment_struct.second
        self.microssecond = moment_struct.microssecond

    def __str__(self):
        return "%d-%d-%d %d:%d:%d.%d" % (
            self.year, self.month, self.day,
            self.hour, self.minute, self.second, self.microssecond)

class Header:
    def __init__(self, header_struct):
        self.symbol = header_struct.symbol.decode()
        self.date = Moment(header_struct.date)

    def __str__(self):
        return "%s %s" % (self.symbol, self.date)

class Trade:
    def __init__(self, trade_struct):
        self.number = trade_struct.number
        self.price = trade_struct.price
        self.quantity = trade_struct.quantity
        self.moment = Moment(trade_struct.moment)
        self.indicator = trade_struct.indicator
        self.buy_number = trade_struct.buy_number
        self.sell_number = trade_struct.sell_number

    def __str__(self):
        return "%s TRADE %f x %d" % (self.moment, self.price, self.quantity)

    @property
    def is_cancelled(self):
        return self.indicator == 2

class Order:
    def __init__(self, order_struct):
        self.number = order_struct.number
        self.side = order_struct.side
        self.type = order_struct.type
        self.moment = Moment(order_struct.moment)
        self.priority = order_struct.priority
        self.price = order_struct.price
        self.quantity = order_struct.quantity
        self.traded = order_struct.traded
        self.datetime = Moment(order.datetime)
        self.status = order_struct.status.decode()
        self.agressor = order_struct.agressor
        self.member = order_struct.member

    def __str__(self):
        return "%s ORDER %f x [%d/%d]" % (
            self.moment, self.price, self.traded, self.quantity)

    @property
    def is_buy(self):
        return self.side == 1

    @property
    def is_sell(self):
        return self.side == 2

    @property
    def is_new_event(self):
        return self.type == 1

    @property
    def is_update_event(self):
        return self.type == 2

    @property
    def is_cancel_event(self):
        return self.type == 3

    @property
    def is_trade_event(self):
        return self.type == 4

    @property
    def is_reentry_event(self):
        return self.type == 5

    @property
    def is_new_stop_price_event(self):
        return self.type == 6

    @property
    def is_reject_event(self):
        return self.type == 7

    @property
    def is_remove_event(self):
        return self.type == 8

    @property
    def is_stop_price_triggered_event(self):
        return self.type == 9

    @property
    def is_expired_event(self):
        return self.type == 11

    @property
    def is_new(self):
        return self.status == "0"

    @property
    def is_traded_partially(self):
        return self.status == "1"

    @property
    def is_traded(self):
        return self.status == "2"

    @property
    def is_cancelled(self):
        return self.status == "4"

    @property
    def is_modified(self):
        return self.status == "5"

    @property
    def is_rejected(self):
        return self.status == "8"

    @property
    def is_expired(self):
        return self.status == "C"

    @property
    def is_neutral(self):
        return self.agressor == 0

    @property
    def is_agressor(self):
        return self.agressor == 1

    @property
    def is_receptor(self):
        return self.agressor == 2


with open("./tmp/WDO_20170517", "rb") as file:
    header = HeaderStruct()
    trade = TradeStruct()
    order = OrderStruct()

    file.readinto(header)
    current = header
    print(Header(header))

    while current.next_type > 0:
        if current.next_type == 1:
            current = trade
            file.readinto(current)
            print(Trade(current))
        else:
            current = order
            file.readinto(current)
            print(Order(current))
