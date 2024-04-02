import datetime

import polars as pl

from src.sector.base_sector import BaseSector
from src.security_symbol import SecurityTicker


class FiftyTwoWeekHighEtfSector(BaseSector):
    def __init__(self, security_universe, date) -> None:
        price_base_table = "parquet/ticker/"

        one_year_ago = datetime.date(
            date.year - 1, date.month, date.day
        ) - datetime.timedelta(days=7)
        # only support SecurityTicker for now
        assert type(security_universe[0]) == SecurityTicker
        price_list = []
        for security in security_universe:
            price_df = (
                pl.scan_parquet(price_base_table + security.ticker + ".parquet")
                .filter(pl.col("date") >= one_year_ago)
                .filter(pl.col("date") <= date)
                .rename({"adj close": "price"})
                .filter(pl.col("price").is_not_null())
                .collect()
                .with_columns(pl.lit(security.ticker).alias("ticker"))
            )
            price_list.append(price_df)
        self.price_df = pl.concat(price_list, how="vertical")

    def get_security_signal(self, date):
        max_price_df = (
            self.price_df.with_columns(
                ((date - pl.col("date")) / datetime.timedelta(days=7))
                .cast(pl.Int8)
                .alias("week_diff")
            )
            .filter(pl.col("week_diff") <= 52)
            .groupby(pl.col("ticker"))
            .agg(pl.col("price").max().alias("max_price"))
        )
        latest_price_date = (
            self.price_df.select(pl.col("date").max()).get_column("date").item(0)
        )
        lastest_price_df = self.price_df.filter(pl.col("date") == latest_price_date)

        assert (
            lastest_price_df.group_by("ticker")
            .agg(pl.col("price").n_unique().alias("cnt"))
            .select(pl.col("cnt").max())
            .get_column("cnt")
            .item(0)
            == 1
        )

        signal_df = (
            lastest_price_df.join(max_price_df, how="inner", on="ticker")
            .with_columns((pl.col("price") / pl.col("max_price")).alias("signal"))
            .select(pl.col("ticker"), pl.col("date"), pl.col("signal"))
        )
        return signal_df

    def get_sector_list(self, observe_date):
        sector_signal_df = self.get_security_signal(observe_date)
        assert len(sector_signal_df) > 0
        ticker_list = (
            sector_signal_df.sort("signal", descending=True)
            .get_column("ticker")
            .to_list()
        )
        return ticker_list
