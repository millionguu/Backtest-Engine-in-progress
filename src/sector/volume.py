import datetime

import polars as pl

from src.sector.base_sector import BaseSector


class VolumeSector(BaseSector):
    def __init__(self) -> None:
        self.table = f"parquet/volume/us_security_volume_daily.parquet"
        self.sector_df = self.get_sector_construction()

    def get_sector_list(self, observe_date):
        """
        1. construct sector
        2. generate sector signal
        """
        signal_df = self.get_security_signal(observe_date)
        sector_signal_df = self.get_sector_signal(self.sector_df, signal_df, True)
        sector_list = (
            sector_signal_df.sort(pl.col("weighted_signal"), descending=True)
            .get_column("sector")
            .to_list()
        )
        return sector_list

    def get_security_signal(self, date):
        """
        use the average volume of current month divided by
        the average volume of the past 3 months as the signal
        """
        three_month_ago = (
            datetime.date(date.year, date.month - 3, 1)
            if date.month > 3
            else datetime.date(date.year - 1, date.month + 12 - 3, 1)
        )
        volume_df = (
            pl.scan_parquet(self.table)
            .filter(pl.col("volume").is_not_null())
            .filter(pl.col("volume") > 0)
            .filter((pl.col("date").dt.year() >= three_month_ago.year))
            .filter((pl.col("date").dt.year() <= date.year))
            .collect()
        )
        volume_df = volume_df.with_columns(
            (
                pl.lit(date.year * 12 + date.month)
                - pl.col("date").dt.year() * 12
                - pl.col("date").dt.month()
            ).alias("month_diff")
        )
        current_month_df = (
            volume_df.filter(pl.col("month_diff") == 0)
            .group_by("sedol7")
            .agg(pl.col("volume").mean().alias("cur_avg_volume"))
        )
        past_three_month_df = (
            volume_df.filter(pl.col("month_diff") > 0)
            .filter(pl.col("month_diff") <= 3)
            .group_by("sedol7")
            .agg(pl.col("volume").mean().alias("hist_avg_volume"))
        )
        signal_df = current_month_df.join(
            past_three_month_df, on="sedol7", how="inner"
        ).with_columns(
            (pl.col("cur_avg_volume") / pl.col("hist_avg_volume")).alias("signal")
        )
        signal_df = signal_df.with_columns(pl.lit(date).alias("date"))
        return signal_df
