from datetime import date
from src.security_symbol import SecurityTicker
from src.market import Market
from src.portfolio import Portfolio
from src.analysis import Analysis, Benchmark
from src.metric import Metric, InformationCoefficient, HitRate
from src.rebalance import Rebalance
from src.strategy import StopGainAndLoss
from src.backtest import BackTest
from src.factor.sales_growth import SalesGrowthFactor
from src.factor.fifty_two_week_high import FiftyTwoWeekHighFactor
from src.factor.fifty_two_week_high_etf import FiftyTwoWeekHighEtfFactor
from src.fund_universe import ISHARE_SECTOR_ETF_TICKER

start_date = date(2013, 1, 1)
end_date = date(2023, 10, 31)
security_universe = ISHARE_SECTOR_ETF_TICKER
rebalance_period = 30

market = Market(security_universe, start_date, end_date)

### Long factor
long_factor = SalesGrowthFactor(security_universe, "long")
long_portfolio = Portfolio(100.0, start_date, end_date)
long_factor.set_portfolio_at_start(long_portfolio)

blacklist = []
strategy = StopGainAndLoss(long_portfolio, blacklist)
strategy.set_limit(1, 1)
rebalance = Rebalance(rebalance_period, long_portfolio, long_factor, blacklist)

backtest = BackTest(long_portfolio, strategy, market, rebalance)
backtest.run()

# print(long_portfolio.value_book)

### Short factor
short_factor = SalesGrowthFactor(security_universe, "short")
short_portfolio = Portfolio(100.0, start_date, end_date)
short_factor.set_portfolio_at_start(short_portfolio)

blacklist = []
strategy = StopGainAndLoss(short_portfolio, blacklist)
strategy.set_limit(1, 1)
rebalance = Rebalance(rebalance_period, short_portfolio, short_factor, blacklist)

backtest = BackTest(short_portfolio, strategy, market, rebalance)
backtest.run()


### plot
benchmark = Benchmark(SecurityTicker("^SPX", "index"), start_date, end_date)

benchmark_performance = benchmark.get_performance()

metric = Metric(long_portfolio, benchmark_performance)
print(f"portfolio annulized return: {metric.portfolio_annualized_return()}")
print(
    f"portfolio annulized return relative to benchmark: {metric.annualized_return_relative_to_benchmark()}"
)
print(f"information ratio: {metric.information_ratio()}")
print(f"average monthly turnover: {metric.avg_monthly_turnover()}")
print(f"sharpe ratio(with risk-free rate 0.04): {metric.sharpe_ratio()}")


analysis = Analysis(
    long_portfolio,
    short_portfolio,
    benchmark_performance,
    "SPX",
)
analysis.draw()

ie = InformationCoefficient(long_portfolio, long_factor, market, rebalance_period)
ie.get_information_coefficient()
ie.draw()

hr = HitRate(long_portfolio, long_factor, market, rebalance_period, benchmark)
hr.get_hit_rate()
hr.draw()
