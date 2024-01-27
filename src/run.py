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

### Long factor
long_factor = SalesGrowthFactor(security_universe, start_date, end_date, "long")
long_position = long_factor.get_position(start_date)
long_portfolio = Portfolio(100.0, start_date, end_date)
long_factor.set_portfolio_at_start(long_portfolio, long_position)

blacklist = []
strategy = StopGainAndLoss(long_portfolio, blacklist)
strategy.set_limit(0.3, 0.3)
rebalance = Rebalance(180, long_portfolio, long_factor, blacklist)

backtest = BackTest(long_portfolio, strategy, market, rebalance)
backtest.run()


### Short factor
short_factor = SalesGrowthFactor(security_universe, start_date, end_date, "short")
short_position = short_factor.get_position(start_date)
short_portfolio = Portfolio(100.0, start_date, end_date)
short_factor.set_portfolio_at_start(short_portfolio, short_position)

blacklist = []
strategy = StopGainAndLoss(short_portfolio, blacklist)
strategy.set_limit(0.3, 0.3)
rebalance = Rebalance(180, short_portfolio, short_factor, blacklist)

backtest = BackTest(short_portfolio, strategy, market, rebalance)
backtest.run()

### plot
benchmark = Benchmark("^SPX", start_date, end_date).get_performance()

metric = Metric(long_portfolio, benchmark)
print(f"portfolio annulized return: {metric.annualized_return()}")
print(
    f"portfolio annulized return relative to benchmark: {metric.annualized_return_relative_to_benchmark()}"
)
print(f"information ratio: {metric.information_ratio()}")

analysis = Analysis(short_portfolio, benchmark)
analysis.draw()
