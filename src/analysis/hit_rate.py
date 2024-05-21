from datetime import timedelta

import matplotlib.pyplot as plt
import polars as pl


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
