from abc import ABC, abstractmethod
import numpy as np
from enum import Enum
from dataclasses import dataclass
import random


class OrderType(Enum):
    BUY = "buy"
    SELL = "sell"
    NOOP = "noop"


@dataclass
class Order:
    type: OrderType = OrderType.NOOP
    security: str = ""
    weight: float = 0.0


class Strategy(ABC):
    def __init__(self, portfolio, blacklist):
        self.portfolio = portfolio
        self.blacklist = blacklist

    @abstractmethod
    def get_order(self, target_security, cur_date):
        pass


class NoStrategy(Strategy):
    def __init__(self, portfolio, blacklist):
        super().__init__(portfolio, blacklist)

    def get_order(self, target_security, cur_date):
        return Order(OrderType.NOOP)


class RandomBuyAndSell(Strategy):
    def __init__(self, portfolio, blacklist):
        super().__init__(portfolio, blacklist)
        self.state = True
        self.target_security = "RUT"

    def get_order(self, target_security, cur_date):
        if target_security == self.target_security and random.random() > 0.7:
            if self.state:
                self.state = not (self.state)
                return Order(OrderType.BUY, target_security, 0.1)
            else:
                self.state = not (self.state)
                return Order(OrderType.SELL, target_security, 0.1)
        return Order(OrderType.NOOP)


class StopGainAndLoss(Strategy):
    def __init__(self, portfolio, blacklist):
        super().__init__(portfolio, blacklist)
        self.set_limit()

    def set_limit(self, gain_limit=1, loss_limit=1):
        self.gain_limit = gain_limit
        self.loss_limit = -abs(loss_limit)

    def get_order(self, target_security, cur_date, prev_rebalance_date):
        security_book = self.portfolio.security_book[target_security]
        cur_value = security_book[security_book["date"] == cur_date]["value"].iloc[0]
        if cur_value == 0:
            return Order(OrderType.NOOP)

        # previous rebalance date?
        # previous min/max day?
        start_value = security_book[security_book["date"] == prev_rebalance_date][
            "value"
        ].iloc[0]
        range_return = (cur_value - start_value) / start_value

        if range_return > self.gain_limit or range_return < self.loss_limit:
            weight = security_book[security_book["date"] == cur_date]["weight"].iloc[0]
            self.blacklist.append(target_security)
            if range_return > 0:
                print(f"{cur_date}: stop gain {target_security}")
            else:
                print(f"{cur_date}: stop loss {target_security}")
            return Order(OrderType.SELL, target_security, weight)
        return Order(OrderType.NOOP)
