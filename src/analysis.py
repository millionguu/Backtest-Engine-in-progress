import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from market import Market


class Analysis:
    def __init__(self, portfolio, benchmark, start_date, end_date):
        self.portfolio = portfolio
        self.benchmark = benchmark
        self.start_date = start_date
        self.end_date = end_date
        fig, ax = plt.subplots(1, 2)
        self.ax = ax

    def draw(self, draw_benchmark=True):
        for security in self.portfolio.hold_securities():
            self.ax[0].plot(self.portfolio.security_book[security]["value"])
        if draw_benchmark:
            self.draw_benchmark()
        plt.show()

    def draw_benchmark(self):
        market = Market([self.benchmark])
        df = market.data[self.benchmark]
        condition = (df["date"] >= self.start_date) & (df["date"] <= self.end_date)
        # twin_ax = self.ax.twinx()
        benchmark = df[condition]["adj close"].reset_index().drop(columns=["index"])
        # twin_ax.plot(benchmark)
        self.ax[1].plot(benchmark)
        plt.show()

    def get_metric(self):
        pass
