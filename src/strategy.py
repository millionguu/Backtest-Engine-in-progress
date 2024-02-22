from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass


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
    def get_order(self, security, iter_index):
        pass


class NoStrategy(Strategy):
    def __init__(self, portfolio, blacklist):
        super().__init__(portfolio, blacklist)

    def get_order(self, security, iter_index):
        return Order(OrderType.NOOP)


class StopGainAndLoss(Strategy):
    def __init__(self, portfolio, blacklist):
        super().__init__(portfolio, blacklist)
        self.set_limit()

    def set_limit(self, gain_limit=1, loss_limit=1):
        self.gain_limit = gain_limit
        self.loss_limit = -abs(loss_limit)

    def get_order(self, security, iter_index, prev_rebalance_index):
        cur_value = self.portfolio.get_security_value(security, iter_index)
        if cur_value == 0:
            return Order(OrderType.NOOP)

        # previous rebalance date
        start_value = self.portfolio.get_security_value(security, prev_rebalance_index)
        range_return = (cur_value - start_value) / start_value

        if range_return > self.gain_limit or range_return < self.loss_limit:
            if range_return > 0:
                print(
                    f"{self.portfolio.date_df.item(iter_index, 0)}: stop gain {security}"
                )
            else:
                print(
                    f"{self.portfolio.date_df.item(iter_index, 0)}: stop loss {security}"
                )
            self.blacklist.append(security)
            weight = self.portfolio.get_security_weight(security, iter_index)
            return Order(OrderType.SELL, security, weight)
        return Order(OrderType.NOOP)
