import datetime

import polars as pl

from src.sector.base_sector import BaseSector


class FiftyTwoWeekHighSector(BaseSector):
    def __init__(self) -> None:
        self.price_table = "parquet/base/us_security_price_daily.parquet"

    def get_security_signal(self, date):
        if date.month == 2 and date.day == 29:
            date = datetime.date(date.year, 2, 28)
        one_year_ago = datetime.date(
            date.year - 1, date.month, date.day
        ) - datetime.timedelta(days=7)
        price_df = (
            pl.scan_parquet(self.price_table)
            .filter(pl.col("date") >= one_year_ago)
            .filter(pl.col("date") <= date)
            .filter(pl.col("price").is_not_null())
            .collect()
        )
        max_price_df = (
            price_df.with_columns(
                ((date - pl.col("date")) / datetime.timedelta(days=7))
                .cast(pl.Int8)
                .alias("week_diff")
            )
            .filter(pl.col("week_diff") <= 52)
            .groupby(pl.col("sedol7"))
            .agg(pl.col("price").max().alias("max_price"))
        )
        latest_price_date = (
            price_df.select(pl.col("date").max()).get_column("date").item(0)
        )
        lastest_price_df = price_df.filter(pl.col("date") == latest_price_date)

        assert (
            lastest_price_df.group_by("sedol7")
            .agg(pl.col("price").n_unique().alias("cnt"))
            .select(pl.col("cnt").max())
            .get_column("cnt")
            .item(0)
            == 1
        )

        signal_df = (
            lastest_price_df.join(max_price_df, how="inner", on="sedol7")
            .with_columns((pl.col("price") / pl.col("max_price")).alias("signal"))
            .select(pl.col("sedol7"), pl.col("date"), pl.col("signal"))
        )

        return signal_df

    def get_sector_list(self, observe_date):
        sector_df = self.get_sector_construction()
        security_signal_df = self.get_security_signal(observe_date)
        sector_signal_df = self.get_sector_signal(sector_df, security_signal_df)
        assert len(sector_signal_df) > 0
        sector_list = (
            sector_signal_df.sort("simple_avg_signal", descending=True)
            .get_column("sector")
            .to_list()
        )
        return sector_list
