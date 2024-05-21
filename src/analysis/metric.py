from datetime import timedelta

import numpy as np
import polars as pl


class Metric:
    def __init__(self, portfolio, benchmark):
        self.portfolio = portfolio
        self.benchmark = benchmark
        self.value_book = self.portfolio.value_book
        self.annualized_factor = (
            self.portfolio.end_date - self.portfolio.start_date
        ) / timedelta(days=365)

    def portfolio_annualized_return(self):
        total_return = (
            self.value_book.get_column("value").item(-1)
            - self.value_book.get_column("value").item(0)
        ) / self.value_book.get_column("value").item(0)
        return np.power(1 + total_return, 1 / self.annualized_factor) - 1

    def benchmark_annualized_return(self):
        benchmark_return = (
            self.benchmark.get_column("value").item(-1)
            - self.benchmark.get_column("value").item(0)
        ) / self.benchmark.get_column("value").item(0)
        return np.power(1 + benchmark_return, 1 / self.annualized_factor) - 1

    def annualized_return_relative_to_benchmark(self):
        return self.portfolio_annualized_return() - self.benchmark_annualized_return()

    def information_ratio(self):
        weekly_portofolio = (
            self.value_book.with_columns(pl.col("date").dt.week().alias("week"))
            .group_by(pl.col("week"))
            .agg(
                (
                    pl.when(pl.col("date") == pl.col("date").max())
                    .then(pl.col("value"))
                    .otherwise(0)
                )
                .max()
                .alias("last_day_of_week"),
                (
                    pl.when(pl.col("date") == pl.col("date").min())
                    .then(pl.col("value"))
                    .otherwise(0)
                )
                .max()
                .alias("first_day_of_week"),
            )
            .with_columns(
                (
                    (pl.col("last_day_of_week") - pl.col("first_day_of_week"))
                    / pl.col("first_day_of_week")
                ).alias("portfolio_weekly_return")
            )
            .select(pl.col("week"), pl.col("portfolio_weekly_return"))
        )

        weekly_benchmark = (
            self.benchmark.with_columns(pl.col("date").dt.week().alias("week"))
            .group_by(pl.col("week"))
            .agg(
                (
                    pl.when(pl.col("date") == pl.col("date").max())
                    .then(pl.col("value"))
                    .otherwise(0)
                )
                .max()
                .alias("last_day_of_week"),
                (
                    pl.when(pl.col("date") == pl.col("date").min())
                    .then(pl.col("value"))
                    .otherwise(0)
                )
                .max()
                .alias("first_day_of_week"),
            )
            .with_columns(
                (
                    (pl.col("last_day_of_week") - pl.col("first_day_of_week"))
                    / pl.col("first_day_of_week")
                ).alias("benchmark_weekly_return")
            )
            .select(pl.col("week"), pl.col("benchmark_weekly_return"))
        )

        merge = weekly_portofolio.join(
            weekly_benchmark, how="inner", on="week"
        ).with_columns(
            (
                pl.col("portfolio_weekly_return") - pl.col("benchmark_weekly_return")
            ).alias("weekly_relative_return")
        )

        tracking_error = (merge.get_column("weekly_relative_return")).std() * np.sqrt(
            52
        )
        return self.annualized_return_relative_to_benchmark() / tracking_error

    def avg_monthly_turnover(self):
        return (
            self.portfolio.value_book.get_column("turnover").sum()
            / self.annualized_factor
            / 12
        )

    def sharpe_ratio(self):
        risk_free_rate = np.power(1 + 0.04, 1 / 252) - 1
        values = self.value_book.get_column("value")
        returns = (values.to_numpy() - values.shift(1).to_numpy()) / values.shift(
            1
        ).to_numpy()
        returns = returns[~np.isnan(returns)]
        avg_return = np.mean(returns - risk_free_rate)
        std_return = np.std(returns)
        return (avg_return) / std_return
