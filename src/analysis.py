import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from market import Market


class Analysis:
    def __init__(self, portfolio, benchmark):
        self.portfolio = portfolio
        self.benchmark = benchmark
        fig, ax = plt.subplots(1, 1)
        self.ax = ax

    def draw(self):
        self.ax.plot(
            self.portfolio.value_book["value"], label="portfolio", color="tab:red"
        )
        self.ax.plot(self.benchmark, label="benchmark", color="tab:blue")
        self.ax.legend()
        plt.show()


class Benchmark:
    def __init__(self, benchmark, start_date, end_date):
        self.benchmark = benchmark if not benchmark.startswith("^") else benchmark[1:]
        self.start_date = start_date
        self.end_date = end_date

    def get_performance(self):
        market = Market([self.benchmark])
        df = market.data[self.benchmark]
        condition = (df["date"] >= self.start_date) & (df["date"] <= self.end_date)
        benchmark = df[condition]["adj close"].reset_index().drop(columns=["index"])
        benchmark = benchmark.rename(columns={"adj close": "value"})
        benchmark = benchmark / benchmark["value"].iloc[0] * 100
        return benchmark


class Metric:
    def __init__(self, portfolio, benchmark):
        self.portfolio = portfolio
        self.benchmark = benchmark
        self.value_book = self.portfolio.value_book
        self.num_dates = self.value_book.shape[0]
        self.ann_const = 252
        self.annualized_factor = self.num_dates / self.ann_const

    def annualized_return(self):
        total_return = (
            self.value_book["value"].iloc[-1] - self.value_book["value"].iloc[0]
        ) / (self.value_book["value"].iloc[0])
        return np.power(1 + total_return, 1 / self.annualized_factor) - 1

    def annualized_benchmark_return(self):
        benchmark_return = (
            self.benchmark["value"].iloc[-1] - self.benchmark["value"].iloc[0]
        ) / (self.benchmark["value"].iloc[0])
        return np.power(1 + benchmark_return, 1 / self.annualized_factor) - 1

    def annualized_return_relative_to_benchmark(self):
        return self.annualized_return() - self.annualized_benchmark_return()

    def information_ratio(self):
        relative_return = self.value_book["value"] - self.benchmark["value"]
        ann_stddev = np.std(relative_return) * np.sqrt(self.ann_const)
        return self.annualized_return_relative_to_benchmark() / ann_stddev

    def information_coefficient(self):
        pass
