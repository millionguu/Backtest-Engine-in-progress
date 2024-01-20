# Backtest-Engine-in-progress

backtest engine

## modules of the system

- data loader: readin fund performance data from file, do the cleanup and store
  unified format data in the database
- market: readin data from database, response with query
- factor: readin various fund data, output the selected fund and its weight
- strategy: readin portfoilio, output buy/sell decision
- rebalance: call factor periodically to rebalance the portfoilio
- portfolio: a pandas dataframe, hold various fund in different weight
- backtest: readin portfolio and strategy, modify the portfolio in place
  according to market
- analysis: readin portfolio, draw graph and output the metrics

## tables

- all the tickers available in yahoo finance
- top_holdings_{etf}
- us_market_open_date
- sales_growth_fy1_msci_usa
