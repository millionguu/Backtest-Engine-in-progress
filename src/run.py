from datetime import date
from market import Market
from portfolio import Portfolio
from analysis import Analysis, Benchmark, Metric
from rebalance import Rebalance
from strategy import NoStrategy, StopGainAndLoss
from backtest import BackTest
from factor.factor import DummyFactor


start_date = date.fromisoformat("2023-01-01")
end_date = date.fromisoformat("2023-12-21")
security_universe = ["^SPX", "^IXIC", "^RUT", "QQQ"]

market = Market(security_universe)
factor = DummyFactor(security_universe)
portfolio = Portfolio(100.0, start_date, end_date)
factor.set_portfolio(portfolio)

blacklist = []
# strategy = NoStrategy(portfolio, blacklist)
strategy = StopGainAndLoss(portfolio, blacklist)
strategy.set_limit(0.3, 0.3)
rebalance = Rebalance(180, portfolio, factor, blacklist)

backtest = BackTest(portfolio, strategy, market, rebalance)
backtest.run()
benchmark = Benchmark("^SPX", start_date, end_date).get_performance()

metric = Metric(portfolio, benchmark)
print(f"portfolio annulized return: {metric.annualized_return()}")
print(
    f"portfolio annulized return relative to benchmark: {metric.annualized_return_relative_to_benchmark()}"
)
print(f"information ratio: {metric.information_ratio()}")

analysis = Analysis(portfolio, benchmark)
analysis.draw()
