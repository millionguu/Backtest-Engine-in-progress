import datetime

from src.analysis.plot import Plot
from src.backtest import BackTest
from src.benchmark import Benchmark
from src.factor.cape import CapeFactor
from src.factor.dividend_yield import DividendYieldFactor
from src.factor.fifty_two_week_high import FiftyTwoWeekHighFactor
from src.factor.fifty_two_week_high_etf import FiftyTwoWeekHighEtfFactor
from src.factor.roe import RoeFactor
from src.factor.sales_growth import SalesGrowthFactor
from src.factor.volume import VolumeFactor
from src.factor_aggregator.lasso_aggregator import LassoAggregator
from src.factor_aggregator.simple_average_aggregator import SimpleAverageAggregator
from src.factor_aggregator.weighted_average_aggregator import WeightedAverageAggregator
from src.fund_universe import INVESCO_SECTOR_ETF_TICKER, ISHARE_SECTOR_ETF_TICKER
from src.market import Market
from src.portfolio import Portfolio
from src.rebalance import Rebalance
from src.security_symbol import SecurityTicker
from src.strategy import StopGainAndLoss

# Setting
start_date = datetime.date(2013, 1, 31)
end_date = datetime.date(2023, 10, 31)
security_universe = INVESCO_SECTOR_ETF_TICKER
rebalance_period = 1
rebalance_interval = "1mo"
Factor = LassoAggregator
index_ticker = "^SPXEW" if security_universe == INVESCO_SECTOR_ETF_TICKER else "^SPX"
benchmark = Benchmark(SecurityTicker(index_ticker, "index"), start_date, end_date)
market = Market(security_universe, start_date, end_date)


### Long factor
long_factor = Factor(security_universe, "long")
long_portfolio = Portfolio(100.0, start_date, end_date)
long_factor.set_portfolio_at_start(long_portfolio)

blacklist = []
strategy = StopGainAndLoss(long_portfolio, blacklist)
strategy.set_limit(1, 1)
rebalance = Rebalance(
    rebalance_period, long_portfolio, long_factor, blacklist, rebalance_interval
)

backtest = BackTest(long_portfolio, strategy, market, rebalance)
backtest.run()


### Short factor
short_factor = Factor(security_universe, "short")
short_portfolio = Portfolio(100.0, start_date, end_date)
short_factor.set_portfolio_at_start(short_portfolio)

blacklist = []
strategy = StopGainAndLoss(short_portfolio, blacklist)
strategy.set_limit(1, 1)
rebalance = Rebalance(
    rebalance_period, short_portfolio, short_factor, blacklist, rebalance_interval
)

backtest = BackTest(short_portfolio, strategy, market, rebalance)
backtest.run()


### Mid factor
mid_factor = Factor(security_universe, "mid")
mid_portfolio = Portfolio(100.0, start_date, end_date)
mid_factor.set_portfolio_at_start(mid_portfolio)

blacklist = []
strategy = StopGainAndLoss(mid_portfolio, blacklist)
strategy.set_limit(1, 1)
rebalance = Rebalance(
    rebalance_period, mid_portfolio, mid_factor, blacklist, rebalance_interval
)

backtest = BackTest(mid_portfolio, strategy, market, rebalance)
backtest.run()


### plot
benchmark_performance = benchmark.get_performance()

plot = Plot(
    long_portfolio,
    short_portfolio,
    benchmark_performance,
    index_ticker[1:],
    mid_portfolio,
)
plot.draw()
