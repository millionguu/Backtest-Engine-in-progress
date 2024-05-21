import datetime

import numpy as np
import polars as pl
from scipy import stats


class Metric:
    def __init__(self, portfolio: pl.DataFrame, benchmark: pl.DataFrame):
        self.portfolio = portfolio
        self.benchmark = benchmark
        self.annualized_factor = (
            self.portfolio.get_column("date").item(-1)
            - self.portfolio.get_column("date").item(0)
        ) / datetime.timedelta(days=365)

    def _portfolio_return_report(self, value_df, label, level="year"):
        if level == "year":
            match_date = value_df.group_by(pl.col("date").dt.year().alias("year")).agg(
                pl.col("date").max().alias("last_day"),
            )
        elif level == "month":
            match_date = value_df.group_by(
                (pl.col("date").dt.year() * 12 + pl.col("date").dt.month()).alias(
                    "month"
                )
            ).agg(
                pl.col("date").max().alias("last_day"),
            )
        else:
            raise NotImplementedError(f"no implementation for {level}")

        return_df = value_df.join(
            match_date,
            left_on=pl.col("date"),
            right_on=pl.col("last_day"),
            how="inner",
        ).select(pl.col(level), pl.col("value"))
        report = (
            return_df.join(
                return_df,
                left_on=pl.col(level),
                right_on=(pl.col(level) + 1),
                how="left",
                suffix="_previous",
            )
            .with_columns(
                pl.when(pl.col("value_previous").is_null())
                .then(pl.lit(100))
                .otherwise(pl.col("value_previous"))
                .alias("value_previous")
            )
            .with_columns(
                (pl.col("value") / pl.col("value_previous") - 1).alias("return")
            )
            .with_columns(
                pl.lit(label).alias("label"),
                (pl.col("return") * 100).alias("return"),
            )
            .select(pl.col(level), pl.col("return"), pl.col("label"))
        )
        return report

    def portfolio_annual_return_report(self, level="year"):
        portfolio_report = self._portfolio_return_report(
            self.portfolio, "portfolio_return", level
        )
        benchmark_report = self._portfolio_return_report(
            self.benchmark, "benchmark_return", level
        )
        report = pl.concat([portfolio_report, benchmark_report], how="vertical")
        pivot_report = report.pivot(index=level, columns="label", values="return")
        pivot_report = pivot_report.with_columns(
            (pl.col("portfolio_return") - pl.col("benchmark_return")).alias("diff")
        )
        return pivot_report

    def t_test_against_benchmark(self, level):
        if level == "day":
            portfolio_daily_return = (
                self.portfolio.get_column("value")
                / self.portfolio.get_column("value").shift(1)
                - 1
            ).drop_nulls()
            benchmark_daily_return = (
                self.benchmark.get_column("value")
                / self.benchmark.get_column("value").shift(1)
                - 1
            ).drop_nulls()
            test_res = stats.ttest_ind(
                portfolio_daily_return,
                benchmark_daily_return,
            )
        elif level == "month":
            pivot_report = self.portfolio_annual_return_report(level="month")
            test_res = stats.ttest_ind(
                pivot_report.get_column("portfolio_return"),
                pivot_report.get_column("benchmark_return"),
            )
        elif level == "year":
            pivot_report = self.portfolio_annual_return_report(level="year")
            test_res = stats.ttest_ind(
                pivot_report.get_column("portfolio_return"),
                pivot_report.get_column("benchmark_return"),
            )
        else:
            raise NotImplementedError(f"no implementation for {level}")
        return test_res

    def portfolio_annualized_return(self):
        total_return = (
            self.portfolio.get_column("value").item(-1)
            - self.portfolio.get_column("value").item(0)
        ) / self.portfolio.get_column("value").item(0)
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
            self.portfolio.with_columns(pl.col("date").dt.week().alias("week"))
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
            self.value_book.get_column("turnover").sum() / self.annualized_factor / 12
        )

    def sharpe_ratio(self):
        risk_free_rate = np.power(1 + 0.04, 1 / 252) - 1
        values = self.portfolio.get_column("value")
        returns = (values.to_numpy() - values.shift(1).to_numpy()) / values.shift(
            1
        ).to_numpy()
        returns = returns[~np.isnan(returns)]
        avg_return = np.mean(returns - risk_free_rate)
        std_return = np.std(returns)
        return (avg_return) / std_return
