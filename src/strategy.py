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


# TODO: strategy should be composable?
class Strategy:
    def __init__(self, portfolio):
        pass

    def get_order(self, target_security, cur_date):
        pass


class NoStrategy(Strategy):
    def __init__(self, portfolio):
        self.portfolio = portfolio

    def get_order(self, target_security, cur_date):
        return Order(OrderType.NOOP)


class RandomBuyAndSell(Strategy):
    def __init__(self, portfolio):
        self.portfolio = portfolio
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
    pass
