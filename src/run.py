from datetime import date
from market import Market
from portfolio import Portfolio
from analysis import Analysis
from strategy import NoStrategy, RandomBuyAndSell
from backtest import BackTest
from factor import DummyFactor


start_date = date.fromisoformat("2023-01-01")
end_date = date.fromisoformat("2023-12-01")
monitor_securities = ["SPX", "IXIC", "RUT"]

factor = DummyFactor()
portfolio = Portfolio(100.0, start_date, end_date)
factor.set_portfolio(portfolio)
# strategy = NoStrategy(portfolio)
strategy = RandomBuyAndSell(portfolio)
market = Market(monitor_securities)
backtest = BackTest(portfolio, strategy, market)

backtest.run()

analysis = Analysis(portfolio, "IXIC", start_date, end_date)
analysis.draw()
