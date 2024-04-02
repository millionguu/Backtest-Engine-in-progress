import matplotlib.pyplot as plt
import polars as pl


class Analysis:
    def __init__(
        self,
        long_portfolio,
        short_portfolio,
        benchmark,
        benchmark_label,
        mid_portfolio=None,
    ):
        benchmark = benchmark.join(long_portfolio.value_book, on="date", how="semi")
        long_portfolio = long_portfolio.value_book.join(
            benchmark, on="date", how="semi"
        )
        short_portfolio = short_portfolio.value_book.join(
            benchmark, on="date", how="semi"
        )
        self.long_portfolio_value = long_portfolio.get_column("value")
        self.short_portfolio_value = short_portfolio.get_column("value")
        self.dates = benchmark.get_column("date")
        self.benchmark_value = benchmark.get_column("value")
        self.benchmark_label = benchmark_label
        _, self.ax = plt.subplots(1, 1, figsize=(10, 5))
        self.mid_portfolio_value = None
        if mid_portfolio:
            mid_portfolio = mid_portfolio.value_book.join(
                benchmark, on="date", how="semi"
            )
            self.mid_portfolio_value = mid_portfolio.get_column("value")

    def draw(self):
        self.ax.plot(
            self.dates,
            self.long_portfolio_value - self.benchmark_value,
            label=f"LONG - {self.benchmark_label}",
            color="tab:green",
        )
        self.ax.plot(
            self.dates,
            self.short_portfolio_value - self.benchmark_value,
            label=f"SHORT - {self.benchmark_label}",
            color="tab:red",
        )
        if self.mid_portfolio_value is not None:
            self.ax.plot(
                self.dates,
                self.mid_portfolio_value - self.benchmark_value,
                label=f"MID - {self.benchmark_label}",
                color="tab:brown",
            )
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
