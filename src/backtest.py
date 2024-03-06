from src.strategy import OrderType, StopGainAndLoss


class BackTest:
    def __init__(self, portfolio, strategy, market, rebalance):
        self.portfolio = portfolio
        self.date_df = self.portfolio.date_df.clone()
        # start trading from the second day
        self.iter_index = 1
        self.strategy = strategy
        self.market = market
        self.rebalance = rebalance
        self.prev_rebalance_index = self.iter_index

    def run(self):
        last_index = self.portfolio.value_book[-1]["index"]
        while self.iter_index <= last_index:
            self.cur_date = self.date_df.item(self.iter_index, 0)
            self.iterate()
            self.iter_index += 1
        self.portfolio.finish()

    def iterate(self):
        # update daily return first
        # security needs to have value in yesterday
        for security in self.portfolio.hold_securities(self.iter_index - 1):
            daily_return = self.market.query_return(security, self.cur_date)
            self.portfolio.update_security_value(
                security, self.iter_index, daily_return
            )
        self.portfolio.update_portfolio(self.iter_index)

        # apply strategy
        for security in self.portfolio.hold_securities(self.iter_index):
            order = self.strategy.get_order(
                security, self.iter_index, self.prev_rebalance_index
            )
            if order.type == OrderType.BUY:
                self.portfolio.add_security_weight(
                    order.security, order.weight, self.iter_index
                )
            elif order.type == OrderType.SELL:
                self.portfolio.reduce_security_weight(
                    order.security, order.weight, self.iter_index
                )
                # after stop gain/loss, run rebalance
                if isinstance(self.strategy, StopGainAndLoss):
                    self.prev_rebalance_index = self.iter_index
                    self.rebalance.run(self.iter_index)
            else:
                pass

        # apply rebalance
        if self.iter_index % self.rebalance.period == 0:
            self.rebalance.run(self.iter_index)
            self.prev_rebalance_index = self.iter_index
