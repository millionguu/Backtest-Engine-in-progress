from dateutil.relativedelta import relativedelta
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
            date = observe_date - relativedelta(months=delta)
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
        prev_month, cur_month = self.get_last_n_month_bound(date, 1)
        signal_df = (
            pl.scan_parquet(f"parquet/sales_growth/{self.table}")
            .filter(pl.col("date").dt.strftime("%Y-%m-%d") >= prev_month)
            .filter(pl.col("date").dt.strftime("%Y-%m-%d") <= cur_month)
            .collect()
        )
        signal_df = signal_df.rename({"growth": "signal"})
        return signal_df

    def get_sector_signal(self, sector_df, signal_df):
        """
        signal_df should have a column named signal
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
