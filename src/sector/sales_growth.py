from datetime import timedelta
import polars as pl

from src.sector.base_sector import BaseSector


class SalesGrowthSector(BaseSector):
    def __init__(self, category="ntm") -> None:
        # hyper parameter: generate z-score using data in the last n months
        self.z_score_month_range = 12
        # category of sales growth, could be {fy1|ttm|ntm}
        self.table = f"us_sales_growth_{category}.parquet"
        self.sector_df = self.get_sector_construction()
        # key is (year, month)
        self.sector_signal_cache = {}

    def get_sector_list(self, observe_date):
        """
        1. construct sector
        2. generate sector signal
        3. sort the sector by z-score
        """
        total_df_list = []
        for delta in range(self.z_score_month_range):
            # note that 30 should be the rebalance period
            date = observe_date - timedelta(days=delta * 30)
            cache_key = (date.year, date.month)
            if cache_key in self.sector_signal_cache:
                sector_signal_df = self.sector_signal_cache[cache_key]
            else:
                signal_df = self.get_security_signal(date)
                sector_signal_df = self.get_sector_signal(self.sector_df, signal_df)
                self.sector_signal_cache[cache_key] = sector_signal_df
            total_df_list.append(sector_signal_df)
        total_signal_df = pl.concat(total_df_list)
        sector_list = self.sort_sector_using_z_score(total_signal_df)
        return sector_list

    def get_security_signal(self, date):
        # TODO: adjust based on annocement date
        lastest_date = (
            pl.scan_parquet(f"parquet/sales_growth/{self.table}")
            .filter(pl.col("date") <= date)
            .select(pl.col("date").max())
            .collect()
            .get_column("date")
            .item(0)
        )
        signal_df = (
            pl.scan_parquet(f"parquet/sales_growth/{self.table}")
            .filter(pl.col("date") == lastest_date)
            .collect()
        )
        signal_df = signal_df.rename({"growth": "signal"})
        return signal_df
