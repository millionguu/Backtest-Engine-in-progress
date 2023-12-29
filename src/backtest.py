from strategy import OrderType


class BackTest:
    def __init__(self, portfolio, strategy, market):
        self.portfolio = portfolio
        self.strategy = strategy
        self.market = market
        self.cur_date = self.portfolio.start_date

    def run(self):
        while self.cur_date < self.portfolio.end_date:
            self.iterate()
            self.cur_date = self.portfolio.get_next_market_date(self.cur_date)

    def iterate(self):
        for security in self.market.securities:
            order = self.strategy.get_order(security, self.cur_date)
            if order.type == OrderType.BUY:
                self.portfolio.add_security_weight(
                    order.security, order.weight, self.cur_date
                )
            elif order.type == OrderType.SELL:
                self.portfolio.reduce_security_weight(
                    order.security, order.weight, self.cur_date
                )
            else:
                pass
            daily_return = self.market.query_return(security, self.cur_date)
            self.portfolio.update_security_value(security, self.cur_date, daily_return)
        self.portfolio.update_portfolio(self.cur_date)
