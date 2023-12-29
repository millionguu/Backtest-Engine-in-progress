from datetime import date, timedelta
from market import Market
from portfolio import Portfolio
from analysis import Analysis
from strategy import BuyAllAtFirstDay, OrderType, RandomBuyAndSell


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
                portfolio.add_security_weight(
                    order.security, order.weight, self.cur_date
                )
            elif order.type == OrderType.SELL:
                portfolio.reduce_security_weight(
                    order.security, order.weight, self.cur_date
                )
            else:
                pass
            daily_return = market.query_return(security, self.cur_date)
            portfolio.update_security_value(security, self.cur_date, daily_return)
        portfolio.update_portfolio(self.cur_date)


if __name__ == "__main__":
    start_date = date.fromisoformat("2022-01-01")
    end_date = date.fromisoformat("2022-12-01")
    securities = ["IXIC"]

    portfolio = Portfolio(1000.0, start_date, end_date)
    # strategy = BuyAllAtFirstDay(portfolio)
    strategy = RandomBuyAndSell(portfolio)
    market = Market(securities)
    backtest = BackTest(portfolio, strategy, market)

    backtest.run()

    analysis = Analysis(portfolio, "IXIC", start_date, end_date)
    analysis.draw()
