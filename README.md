# Backtest-Engine-in-progress

backtest engine designed to run fund selection / sector rotation

## modules of the system

- data_loader: clean the data source and store the result in the parquet format
- factor: sort the selected fund based on some signals
- market: response query with daily return value
- strategy: primarily stop loss and stop gain
- portfolio: data structure that hold funds data for a period of time
- rebalance: call factor periodically to change the portfoilio holdings
- backtest:  iteratively call methods to update portfolio
- analysis: draw result graph and output the metrics
