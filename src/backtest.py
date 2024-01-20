import numpy as np
from src.strategy import OrderType, StopGainAndLoss


class BackTest:
    def __init__(self, portfolio, strategy, market, rebalance):
        self.portfolio = portfolio
        self.strategy = strategy
        self.market = market
        self.cur_date = self.portfolio.start_date
        self.rebalance = rebalance
        self.prev_rebalance_date = self.portfolio.start_date

    def run(self):
        while self.cur_date < self.portfolio.end_date:
            self.iterate()
            self.cur_date = self.portfolio.get_next_market_date(self.cur_date)

    def iterate(self):
        # update daily return first
        for security in self.market.securities:
            daily_return = self.market.query_return(security, self.cur_date)
        self.portfolio.update_portfolio(self.cur_date)

        # apply strategy
        for security in self.market.securities:
            self.portfolio.update_security_value(security, self.cur_date, daily_return)
            order = self.strategy.get_order(
                security, self.cur_date, self.prev_rebalance_date
            )
            if order.type == OrderType.BUY:
                self.portfolio.add_security_weight(
                    order.security, order.weight, self.cur_date
                )
            elif order.type == OrderType.SELL:
                self.portfolio.reduce_security_weight(
                    order.security, order.weight, self.cur_date
                )
                # after stop gain/loss, run rebalance
                if isinstance(self.strategy, StopGainAndLoss):
                    self.prev_rebalance_date = self.cur_date
                    self.rebalance.run(self.cur_date)
            else:
                pass
        self.portfolio.update_portfolio(self.cur_date)

        # apply rebalance
        if (
            np.argmax(self.portfolio.date_df == self.cur_date) % self.rebalance.period
            == 0
        ):
            self.prev_rebalance_date = self.cur_date
            self.rebalance.run(self.cur_date)
            self.portfolio.update_portfolio(self.cur_date)
