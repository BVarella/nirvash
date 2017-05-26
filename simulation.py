import argparse, pdb
from collections import deque, OrderedDict
from bintrees import FastRBTree
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
        self.datetime = Moment(order_struct.datetime)
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

class OrderBook:
    def __init__(self):
        self.orders = dict()
        self.buy_book = FastRBTree()
        self.sell_book = FastRBTree()

    def _remove_order(self, order):
        book = self.buy_book if order.is_buy else self.sell_book

        del book[order.price][order.number]
        if len(book[order.price]) == 0:
            del book[order.price]

    def _add_order(self, order):
        if (order.is_traded
            or order.is_cancelled
            or order.is_rejected
            or order.is_expired):
            return

        book = self.buy_book if order.is_buy else self.sell_book
        if order.price not in book:
            book[order.price] = OrderedDict()

        book[order.price][order.number] = order

    def order(self, number):
        if number in self.orders:
            return self.orders[number]

        return None

    def process_order(self, order):
        if order.number in self.orders:
            old_order = self.orders[order.number]
            self._remove_order(old_order)
        self._add_order(order)
        self.orders[order.number] = order

class Simulator:
    def __init__(self, filepath, ontrade=None, onorder=None):
        self.filepath = filepath
        self.ontrade = ontrade
        self.onorder = onorder

    def _reset(self):
        self.last_trade = None
        self.last_order = None
        self.book = OrderBook()

    def _dispatch_trade(self):
        if self.ontrade:
            self.ontrade(self.last_trade, self.book)

    def _dispatch_order(self):
        if self.onorder:
            self.onorder(self.last_order, self.book)

    def run(self):
        self._reset()
        header = HeaderStruct()
        trade = TradeStruct()
        order = OrderStruct()

        with open(self.filepath, "rb") as eventfile:
            eventfile.readinto(header)
            next_type = header.next_type

            while next_type > 0:
                if next_type == 1:
                    eventfile.readinto(trade)
                    self.last_trade = Trade(trade)
                    self._dispatch_trade()
                    next_type = trade.next_type
                else:
                    eventfile.readinto(order)
                    self.last_order = Order(order)
                    self.book.process_order(self.last_order)
                    self._dispatch_order()
                    next_type = order.next_type
