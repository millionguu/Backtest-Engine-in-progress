from datetime import date
from src.market import Market
from src.portfolio import Portfolio
from src.analysis import Analysis, Benchmark, Metric
from src.rebalance import Rebalance
from src.strategy import NoStrategy, StopGainAndLoss
from src.backtest import BackTest
from src.factor.base_factor import DummyFactor
from src.factor.gyf import SalesGrowthFactor
from src.factor.const import SECTOR_ETF


start_date = date.fromisoformat("2021-01-01")
end_date = date.fromisoformat("2022-12-21")
# security_universe = ["^SPX", "^IXIC", "^RUT", "QQQ"]
security_universe = SECTOR_ETF

market = Market(security_universe)
# factor = DummyFactor(security_universe, start_date, end_date)
factor = SalesGrowthFactor(security_universe, start_date, end_date)
long_position = factor.get_long_position(start_date)
portfolio = Portfolio(100.0, start_date, end_date)
factor.set_portfolio_at_start(portfolio, long_position)

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
