from datetime import date
from market import Market
from portfolio import Portfolio
from analysis import Analysis, Benchmark, Metric
from strategy import NoStrategy, RandomBuyAndSell
from backtest import BackTest
from factor import DummyFactor


start_date = date.fromisoformat("2023-01-01")
end_date = date.fromisoformat("2023-12-21")
monitor_securities = ["SPX", "IXIC", "RUT"]

factor = DummyFactor()
portfolio = Portfolio(100.0, start_date, end_date)
factor.set_portfolio(portfolio)
strategy = NoStrategy(portfolio)
# strategy = RandomBuyAndSell(portfolio)
market = Market(monitor_securities)
backtest = BackTest(portfolio, strategy, market)

backtest.run()

benchmark = Benchmark("SPX", start_date, end_date).get_performance()

metric = Metric(portfolio, benchmark)
print(f"portfolio annulized return: {metric.annualized_return()}")
print(
    f"portfolio annulized return relative to benchmark: {metric.annualized_return_relative_to_benchmark()}"
)
print(f"information ratio: {metric.information_ratio()}")

analysis = Analysis(portfolio, benchmark)
analysis.draw()
