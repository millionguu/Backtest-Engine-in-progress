import polars as pl

from src.market import Market


class Benchmark:
    def __init__(self, benchmark, start_date, end_date):
        self.benchmark = benchmark
        self.start_date = start_date
        self.end_date = end_date
        self.market = Market([self.benchmark], self.start_date, self.end_date)

    def get_performance(self):
        df = self.market.data[self.benchmark]
        df = df.filter(
            (pl.col("date") >= self.start_date) & (pl.col("date") <= self.end_date)
        ).rename({"adj close": "value"})
        first_day_value = df.get_column("value").head(1).item()
        df = df.select(
            pl.col("date"),
            (pl.col("value") / pl.lit(first_day_value) * pl.lit(100)).alias("value"),
        )
        return df

    def query_range_return(self, start_date, end_date):
        range_return = self.market.query_ticker_range_return(
            self.benchmark, start_date, end_date
        )
        return range_return
