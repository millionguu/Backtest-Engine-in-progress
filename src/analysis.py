import numpy as np
import polars as pl
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from src.market import Market


class Analysis:
    def __init__(self, long_portfolio, short_portfolio, benchmark, benchmark_label):
        self.long_portfolio = long_portfolio
        self.short_portfolio = short_portfolio
        self.dates = pd.to_datetime(long_portfolio.value_book["date"])
        self.benchmark = benchmark.to_numpy().reshape(-1)
        self.benchmark_label = benchmark_label
        _, self.ax = plt.subplots(1, 1, figsize=(10, 5))

    def draw(self):
        portfolios = [self.long_portfolio, self.short_portfolio]
        portfolio_labels = [
            f"LONG - {self.benchmark_label}",
            f"SHORT - {self.benchmark_label}",
        ]
        colors = ["tab:green", "tab:red"]
        for p, l, c in zip(portfolios, portfolio_labels, colors):
            relative_value = p.value_book["value"] - self.benchmark
            self.ax.plot(self.dates, relative_value, label=l, color=c)

        self.ax.plot(
            self.dates,
            self.benchmark - self.benchmark,
            label=f"{self.benchmark_label} - {self.benchmark_label}",
            color="tab:blue",
        )

        self.ax.plot(
            self.dates,
            self.long_portfolio.value_book["value"]
            - self.short_portfolio.value_book["value"],
            label="LONG - SHORT",
            color="tab:pink",
        )
        fmt = mdates.DateFormatter("%Y-%m-%d")
        self.ax.xaxis.set_major_formatter(fmt)
        self.ax.set_xticks(self.dates[::30])
        self.ax.grid(True)
        self.ax.legend()
        self.ax.set_title("Portofolio Return Relative to Benchmark")
        plt.show()


class Benchmark:
    def __init__(self, benchmark, start_date, end_date):
        self.benchmark = benchmark if not benchmark.startswith("^") else benchmark[1:]
        self.start_date = start_date.strftime("%Y-%m-%d")
        self.end_date = end_date.strftime("%Y-%m-%d")

    def get_performance(self):
        market = Market([self.benchmark])
        df = market.data[self.benchmark]
        df = (
            df.filter(
                (pl.col("date") >= self.start_date) & (pl.col("date") <= self.end_date)
            )
            .rename({"adj close": "value"})
            .select("value")
        )
        first_day_value = df.get_column("value").head(1).item()
        df = df.select(pl.col("value") / pl.lit(first_day_value) * pl.lit(100))
        return df


class Metric:
    def __init__(self, portfolio, benchmark):
        self.portfolio = portfolio
        self.benchmark = benchmark
        self.value_book = pl.from_pandas(self.portfolio.value_book)
        self.num_dates = self.value_book.shape[0]
        self.ann_const = 252
        self.annualized_factor = self.num_dates / self.ann_const

    def annualized_return(self):
        total_return = (
            self.value_book.get_column("value").item(-1)
            - self.value_book.get_column("value").item(0)
        ) / self.value_book.get_column("value").item(0)
        return np.power(1 + total_return, 1 / self.annualized_factor) - 1

    def annualized_benchmark_return(self):
        benchmark_return = (
            self.benchmark.get_column("value").item(-1)
            - self.benchmark.get_column("value").item(0)
        ) / self.benchmark.get_column("value").item(0)
        return np.power(1 + benchmark_return, 1 / self.annualized_factor) - 1

    def annualized_return_relative_to_benchmark(self):
        return self.annualized_return() - self.annualized_benchmark_return()

    def information_ratio(self):
        relative_return_std = (
            self.value_book.get_column("value") - self.benchmark.get_column("value")
        ).std()
        ann_stddev = relative_return_std * np.sqrt(self.ann_const)
        return self.annualized_return_relative_to_benchmark() / ann_stddev

    def information_coefficient(self):
        pass
