import datetime

import polars as pl

from src.sector.base_sector import BaseSector


class SalesGrowthSector(BaseSector):
    def __init__(self, category="ntm") -> None:
        # hyper parameter: generate z-score using data in the last n years
        self.z_score_year_range = 10
        # category could be {ntm|fy1|ttm}
        self.table = f"parquet/sales_growth/us_sales_growth_{category}.parquet"
        self.sector_df = self.get_sector_construction()
        # key is (year, month)
        self.sector_signal_cache = {}

    def impl_sector_signal(self, observe_date):
        """
        1. construct sector
        2. generate sector signal
        3. sort the sector by z-score
        """
        total_df_list = []
        if observe_date.month == 2 and observe_date.day == 29:
            observe_date = datetime.date(observe_date.year, 2, 28)
        for delta in range(self.z_score_year_range):
            date = datetime.date(
                observe_date.year - delta, observe_date.month, observe_date.day
            )
            cache_key = (date.year, date.month)
            if cache_key in self.sector_signal_cache:
                sector_signal_df = self.sector_signal_cache[cache_key]
            else:
                security_signal_df = self.impl_security_signal(date)
                sector_signal_df = self.agg_to_sector_signal(
                    self.sector_df, security_signal_df, True
                )
                self.sector_signal_cache[cache_key] = sector_signal_df
            total_df_list.append(sector_signal_df)
        total_signal_df = pl.concat(total_df_list)
        z_score_df = self.get_sector_z_score(total_signal_df)
        return z_score_df

    def impl_security_signal(self, date):
        cur_month = datetime.date(date.year, date.month, 1)
        signal_df = (
            pl.scan_parquet(self.table)
            .filter(pl.col("growth").is_not_null())
            .filter(pl.col("date").dt.year() == date.year)
            .filter(pl.col("date").dt.month() == date.month)
            .collect()
        )
        # rewrite the date column to unify the date in the same month
        signal_df = signal_df.with_columns(pl.lit(cur_month).alias("date"))
        signal_df = signal_df.rename({"growth": "signal"})
        return signal_df
