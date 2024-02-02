import numpy as np
import polars as pl
import matplotlib.pyplot as plt
from src.market import Market


class Analysis:
    def __init__(self, portfolios, portfolio_labels, benchmark, benchmark_label):
        self.portfolios = portfolios
        self.portfolio_labels = portfolio_labels
        self.benchmark = benchmark
        self.benchmark_label = benchmark_label
        fig, ax = plt.subplots(1, 1)
        self.ax = ax

    def draw(self):
        colors = ["tab:red", "tab:green"]
        for p, l, c in zip(self.portfolios, self.portfolio_labels, colors):
            self.ax.plot(p.value_book["value"], label=l, color=c)

        self.ax.plot(self.benchmark, label=self.benchmark_label, color="tab:blue")
        self.ax.legend()
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
