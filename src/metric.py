from datetime import timedelta

import matplotlib.pyplot as plt
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


class InformationCoefficient:
    def __init__(self, portfolio, factor, market, rebalance_period) -> None:
        self.portofolio = portfolio
        self.factor = factor
        self.market = market
        self.rebalance_period = rebalance_period
        self.ie = None

    def get_information_coefficient(self):
        total_rows = []
        for dict in self.portofolio.value_book.select(["date", "index"]).to_dicts():
            if dict["index"] % self.rebalance_period != 0:
                continue
            rows = self.get_rows(dict["date"])
            total_rows.extend(rows)

        df = pl.from_dicts(total_rows)
        df = df.group_by("date").agg(
            pl.corr(pl.col("rank"), pl.col("return")).alias("ie")
        )
        self.ie = df
        return df

    def get_rows(self, date):
        # (date, sector, rank, rebalance_period_return)
        rows = []
        fund_list = self.factor.get_fund_list(date)
        end_date = date + timedelta(days=self.rebalance_period)
        for rank, fund in enumerate(reversed(fund_list)):
            range_return = self.market.query_range_return(fund, date, end_date)
            rows.append(
                {
                    "date": date,
                    "sector": fund.sector,
                    "rank": rank,
                    "return": range_return,
                }
            )
        return rows

    def draw(self):
        _, ax = plt.subplots(1, 1, figsize=(10, 5))

        ie = self.ie.sort(pl.col("date")).with_columns(
            pl.col("date").cast(pl.String).str.slice(0, 7).alias("date")
        )
        ax.bar(ie.get_column("date"), ie.get_column("ie"))
        step = max(ie.shape[0] // 30, 1)
        ax.set_xticks(
            ticks=ie.get_column("date").to_list()[::step],
            labels=ie.get_column("date").to_list()[::step],
            rotation=90,
        )
        ax.set_title("Information Coefficient")
        plt.show()


class HitRate:
    def __init__(self, portfolio, factor, market, rebalance_period, benchmark) -> None:
        self.portofolio = portfolio
        self.factor = factor
        self.market = market
        self.rebalance_period = rebalance_period
        self.benchmark = benchmark
        self.hr = None

    def get_hit_rate(self):
        total_rows = []
        for dict in self.portofolio.value_book.select(["date", "index"]).to_dicts():
            if dict["index"] % self.rebalance_period != 0:
                continue
            rows = self.get_rows(dict["date"])
            total_rows.extend(rows)

        df = pl.from_dicts(total_rows)
        df = df.group_by("date").agg(
            (
                (pl.when(pl.col("return") > 0).then(1).otherwise(0).sum())
                / (pl.col("return").count())
            ).alias("hr")
        )
        self.hr = df
        return df

    def get_rows(self, date):
        # (date, sector, rank, return relative to benchmark)
        rows = []
        fund_list = self.factor.get_position(date)
        fund_list = list(map(lambda t: t[0], fund_list))
        end_date = date + timedelta(days=self.rebalance_period)
        benchmark_return = self.benchmark.query_range_return(date, end_date)
        for fund in fund_list:
            range_return = self.market.query_range_return(fund, date, end_date)
            rows.append(
                {
                    "date": date,
                    "sector": fund.sector,
                    "return": range_return - benchmark_return,
                }
            )
        return rows

    def draw(self):
        _, ax = plt.subplots(1, 1, figsize=(10, 5))

        hr = self.hr.sort(pl.col("date")).with_columns(
            pl.col("date").cast(pl.String).str.slice(0, 7).alias("date")
        )
        ax.bar(hr.get_column("date"), hr.get_column("hr"))
        ax.axhline(y=0.5)

        step = max(hr.shape[0] // 30, 1)
        ax.set_xticks(
            ticks=hr.get_column("date").to_list()[::step],
            labels=hr.get_column("date").to_list()[::step],
            rotation=90,
        )
        ax.set_title("Hit Rate")
        plt.show()
