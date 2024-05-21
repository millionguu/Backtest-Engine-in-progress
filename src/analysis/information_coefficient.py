from datetime import timedelta

import matplotlib.pyplot as plt
import polars as pl


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
