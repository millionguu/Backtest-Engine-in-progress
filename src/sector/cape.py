from dateutil.relativedelta import relativedelta
import polars as pl

from src.sector.base_sector import BaseSector


class CapeSector(BaseSector):
    def __init__(self):
        # hyper parameter: generate z-score using data in the last n years
        self.year_range = 10
        self.eps_table = "parquet/cape/us_security_eps_quarterly.parquet"
        self.eps_table_backup = "parquet/cape/us_security_eps_annually.parquet"
        self.price_table = "parquet/cape/us_security_price_daily.parquet"
        self.cpi_table = "parquet/cape/us_cpi.parquet"
        self.sector_df = self.get_sector_construction()
        self.sector_signal_cache = {}

    def get_sector_list(self, observe_date):
        """
        1. construct sector securities and weight
        2. aggregate history data and calulate security signal
        3. aggregate security signal and calculate sector signal
        4. sort the sector sinal using z-score
        """
        total_df_list = []
        for delta in range(self.year_range):
            date = observe_date - relativedelta(year=delta)
            if date in self.sector_signal_cache:
                sector_signal_df = self.sector_signal_cache["date"]
            else:
                signal_df = self.get_security_signal(date)
                sector_signal_df = self.get_sector_signal(self.sector_df, signal_df)
                self.sector_signal_cache["date"] = sector_signal_df
            total_df_list.append(sector_signal_df)
        total_df = pl.concat(total_df_list)
        sector_list = self.sort_sector_using_z_score(total_df, observe_date)
        # less PE is better
        sector_list = list(reversed(sector_list))
        assert len(sector_list) > 0
        return sector_list

    def get_security_signal(self, date):
        """
        aggregate history eps data and calulate security PE
        """

        income_announcement_date = None

        prev_month, cur_month = self.get_last_n_month_bound(cur_date=date, n=10 * 12)
        # TODO: adjust eps based on CPI
        # adjust weight if missing cpi data
        eps_df = (
            pl.scan_parquet(f"parquet/cape/{self.eps_table}.parquet")
            .filter((pl.col("date").dt.strftime("%Y-%m-%d") >= prev_month))
            .filter((pl.col("date").dt.strftime("%Y-%m-%d") <= cur_month))
            .filter(pl.col("eps").is_not_null())
            .collect()
        )

        eps_df = pl.read_database(
            f"select * from {self.eps_table}  \
            where date between '{prev_month}' and '{cur_month}' \
            and eps is not null",
            engine.connect(),
        )
        price_df = pl.read_database(
            f"select * from msci_usa_price_daily \
             where date between '{prev_month}' and '{cur_month}' ",
            engine.connect(),
        )
        signal_df = eps_df.join(
            price_df,
            how="inner",
            left_on=["sedol7", "date"],
            right_on=["sedol7", "date"],
        )
        signal_df = signal_df.with_columns(
            (pl.col("price") / pl.col("eps")).alias("signal")
        )

        return signal_df
