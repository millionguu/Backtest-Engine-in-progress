import polars as pl
import matplotlib.pyplot as plt
from src.market import Market


class Analysis:
    def __init__(self, long_portfolio, short_portfolio, benchmark, benchmark_label):
        self.long_portfolio = long_portfolio
        self.short_portfolio = short_portfolio
        self.long_portfolio_value = long_portfolio.value_book.get_column("value")
        self.short_portfolio_value = short_portfolio.value_book.get_column("value")
        self.dates = long_portfolio.date_df.get_column("date")
        self.benchmark_value = benchmark.get_column("value")
        self.benchmark_label = benchmark_label
        _, self.ax = plt.subplots(1, 1, figsize=(10, 5))

    def draw(self):
        portfolios = [self.long_portfolio_value, self.short_portfolio_value]
        portfolio_labels = [
            f"LONG - {self.benchmark_label}",
            f"SHORT - {self.benchmark_label}",
        ]
        colors = ["tab:green", "tab:red"]
        for p, l, c in zip(portfolios, portfolio_labels, colors):
            relative_value = p - self.benchmark_value
            self.ax.plot(self.dates, relative_value, label=l, color=c)

        self.ax.plot(
            self.dates,
            self.benchmark_value - self.benchmark_value,
            label=f"{self.benchmark_label} - {self.benchmark_label}",
            color="tab:blue",
        )

        self.ax.plot(
            self.dates,
            self.long_portfolio_value - self.short_portfolio_value,
            label="LONG - SHORT",
            color="tab:pink",
        )
        step = self.dates.shape[0] // 30
        self.ax.set_xticks(
            ticks=self.dates[::step],
            labels=self.dates[::step],
            rotation=90,
        )
        self.ax.grid(True)
        self.ax.legend()
        self.ax.set_title("Portofolio Return Relative to Benchmark")
        plt.show()


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
