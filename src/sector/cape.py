from dateutil.relativedelta import relativedelta
import polars as pl

from src.database import engine
from src.sector.base_sector import BaseSector


class CapeSector(BaseSector):
    def __init__(self):
        self.eps_table = "msci_usa_eps_quarterly"
        self.price_table = "msci_usa_price_daily"
        self.sector_df = self.get_sector_construction()
        self.sector_signal_cache = {}

    def get_sector_list(self, observe_date):
        """
        1. construct sector
        2. generate sector signal
        3. sort the sector by z-score
        """
        total_df_list = []
        for delta in range(self.month_range):
            date = observe_date - relativedelta(months=delta)
            if date in self.sector_signal_cache:
                sector_signal_df = self.sector_signal_cache["date"]
            else:
                signal_df = self.get_signal_df(date)
                sector_signal_df = self.get_sector_signal(self.sector_df, signal_df)
                self.sector_signal_cache["date"] = sector_signal_df
            total_df_list.append(sector_signal_df)
        total_df = pl.concat(total_df_list)
        res = self.sort_sector_using_z_score(total_df, observe_date)
        return res

    def get_signal_df(self, date):
        """
        signal on the security level
        """
        prev_month, cur_month = self.get_last_month_bound(date)
        bound = f"where date between '{prev_month}' and '{cur_month}' \
            and eps is not null"
        eps_df = pl.read_database(
            f"select * from {self.eps_table} {bound}", engine.connect()
        )
        price_df = pl.read_database("select * from msci_usa_price_daily")
        return

    def get_sector_signal(self, sector_df, signal_df):
        """
        signal on the sector level
        input parameter signal_df should have a column named signal
        """
        signal_df = signal_df.with_columns(
            pl.col("date").cast(pl.String).str.slice(0, 7).alias("ym")
        )
        sector_df = sector_df.with_columns(
            pl.col("date").cast(pl.String).str.slice(0, 7).alias("ym")
        )
        sector_signal_df = (
            signal_df.filter(pl.col("signal").is_not_null())
            .join(sector_df, on=["sedol7", "ym"], how="left")
            .group_by(["sector", "date"])
            .agg(
                pl.col("signal").mean().alias("simple_signal"),
                (
                    (pl.col("signal") * pl.coalesce(pl.col("weight"), 0)).sum()
                    / (pl.col("weight")).sum()
                ).alias("weighted_signal"),
                ((pl.col("signal") * pl.coalesce(pl.col("weight"), 0)))
                .sum()
                .alias("debug_signal"),
            )
            .filter(pl.col("sector") != "--")
        )
        return sector_signal_df

    def sort_sector_using_z_score(self, total_df, observe_date):
        prev_month, cur_month = self.get_last_month_bound(observe_date)
        cur_signal = total_df.filter(pl.col("date") > prev_month).clone()
        assert len(cur_month) < 15

        total_df = total_df.group_by(["sector"]).agg(
            (pl.col("weighted_signal").std().alias("std")),
            (pl.col("weighted_signal").mean().alias("mean")),
        )

        merge = (
            cur_signal.join(total_df, on="sector", how="inner")
            .with_columns(
                ((pl.col("weighted_signal") - pl.col("mean")) / pl.col("std")).alias(
                    "z-score"
                )
            )
            .sort("z-score", descending=True)
        )
        # print(merge)
        ordered_sector = merge.get_column("sector").to_list()
        return ordered_sector
