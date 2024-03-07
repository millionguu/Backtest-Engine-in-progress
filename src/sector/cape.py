from datetime import timedelta
from dateutil.relativedelta import relativedelta
import polars as pl

from src.sector.base_sector import BaseSector


class CapeSector(BaseSector):
    def __init__(self):
        self.z_score_year_range = 10
        self.eps_quarterly_table = "parquet/cape/us_security_eps_quarterly.parquet"
        self.eps_annually_table = "parquet/cape/us_security_eps_annually.parquet"
        self.price_table = "parquet/cape/us_security_price_daily.parquet"
        self.cpi_table = "parquet/cape/us_cpi_yoy.parquet"
        self.report_announcement_table = (
            "parquet/cape/us_security_income_report_announcement_date.parquet"
        )
        self.sector_df = self.get_sector_construction()
        # the key of the cache is (year, month).
        # but the rebalance period is quarterly, so it's actually (year, quarter)
        # or could be simplified to year only?
        self.sector_signal_cache = {}

    def get_sector_list(self, observe_date):
        """
        1. construct sector securities and weight
        2. aggregate history data and calulate security signal
        3. aggregate security signal and calculate sector signal
        4. sort the sector sinal using z-score
        """
        total_df_list = []
        for delta in range(self.z_score_year_range):
            date = observe_date - relativedelta(year=delta)
            key = (date.year, date.month)
            if key in self.sector_signal_cache:
                sector_signal_df = self.sector_signal_cache[key]
            else:
                security_signal_df = self.get_security_signal(date)
                sector_signal_df = self.get_sector_signal(
                    self.sector_df, security_signal_df
                )
                self.sector_signal_cache[key] = sector_signal_df
            total_df_list.append(sector_signal_df)
        total_df = pl.concat(total_df_list)
        sector_list = self.sort_sector_using_z_score(total_df, observe_date)
        # less PE is better, thus we reverse the order
        sector_list = list(reversed(sector_list))
        assert len(sector_list) > 0
        return sector_list

    def get_security_signal(self, date):
        """
        aggregate history eps data and calulate security PE
        """

        eps_df = self.get_eps_construction(date)

        ten_year_ago = date - timedelta(days=10 * 365)
        # compound eps with the CPI data
        cpi_df = (
            pl.read_parquet(self.cpi_table)
            .filter(pl.col("year") >= ten_year_ago.year)
            .filter(pl.col("year") <= date.year)
            .select(pl.col("year"), pl.col("us_cpi_all").alias("cpi"))
            .with_columns((pl.col("cpi") + pl.lit(1)).alias("cpi"))
            .with_columns(pl.col("cpi").cum_prod())
        )

        eps_df = (
            eps_df.filter(pl.col("year") >= ten_year_ago.year)
            .join(cpi_df, how="inner", on="year")
            .with_columns((pl.col("annual_eps") * pl.col("cpi")).alias("annual_eps"))
        )

        print(eps_df.filter(pl.col("sedol7") == "2046251").sort(pl.col("year")))

        # aggregate eps on the last 10 years
        # note that when aggregating over the 10 year period,
        eps_df = eps_df.group_by("sedol7").agg(
            pl.col("annual_eps").sum().alias("agg_eps"),
            pl.col("num_quarter").sum().alias("num_quarter"),
        )

        # the num_quarter should range between [40, 43]
        # for those securities that have missing data in the past 10 years, we drop it
        eps_df = (
            eps_df.filter(pl.col("num_quarter") >= 40)
            .filter(pl.col("num_quarter") <= 43)
            .with_columns(
                (pl.col("agg_eps") / pl.col("num_quarter")).alias("avg_quarter_eps")
            )
        )

        price_df = (
            pl.scan_parquet(self.price_table).filter(pl.col("date") == date).collect()
        )

        signal_df = eps_df.join(
            price_df,
            how="inner",
            on="sedol7",
        )
        # annulize the PE, rather than using quarterly PE
        signal_df = signal_df.with_columns(
            (pl.col("price") / (pl.col("avg_quarter_eps") * pl.lit(4))).alias("signal")
        )

        print(signal_df.filter(pl.col("sedol7") == "2046251"))

        return signal_df

    def get_eps_construction(self, date):
        # note that we only use quarterly data whose report date is 3/31, 6/30, 9/30, 12/31
        eps_quaterly_df = (
            pl.scan_parquet(self.eps_quarterly_table)
            .filter(pl.col("eps").is_not_null())
            .filter(pl.col("date") < date)
            .filter(
                ((pl.col("date").dt.month() == 3) & (pl.col("date").dt.day() == 31))
                | ((pl.col("date").dt.month() == 6) & (pl.col("date").dt.day() == 30))
                | ((pl.col("date").dt.month() == 9) & (pl.col("date").dt.day() == 30))
                | ((pl.col("date").dt.month() == 12) & (pl.col("date").dt.day() == 31))
            )
            .collect()
        )

        # de-duplicate
        eps_quaterly_df = eps_quaterly_df.group_by(
            [
                pl.col("sedol7"),
                pl.col("date").dt.year().alias("year"),
                pl.col("date").dt.month().alias("month"),
            ]
        ).agg(pl.col("eps").max().alias("eps"))

        # aggregate to annual
        eps_quaterly_df = eps_quaterly_df.group_by(
            [pl.col("sedol7"), pl.col("year")]
        ).agg(
            pl.col("eps").sum().alias("agg_eps"),
            pl.col("eps").count().alias("num_quarter"),
        )

        # note that we only use those annual data whose report date is 12/31
        eps_annually_df = (
            pl.scan_parquet(self.eps_annually_table)
            .filter(pl.col("date") < date)
            .filter(pl.col("eps").is_not_null())
            .filter((pl.col("date").dt.month() == 12) & (pl.col("date").dt.day() == 31))
            .collect()
        )

        # de-duplicate
        eps_annually_df = (
            eps_annually_df.group_by(
                pl.col("sedol7"), pl.col("date").dt.year().alias("year")
            )
            .agg(pl.col("eps").max().alias("annual_eps"))
            .with_columns(pl.lit(4).alias("num_quarter_annually"))
        )

        # when missing a whole year data in quartely_df,
        # we try to replenish it using the annualy_df.
        # if still missing data after the replenish, we drop it.
        eps_df = eps_quaterly_df.join(
            eps_annually_df, how="outer", on=["sedol7", "year"]
        ).select(
            pl.col("sedol7"),
            pl.col("year"),
            pl.coalesce(pl.col("agg_eps"), pl.col("annual_eps")).alias("annual_eps"),
            pl.coalesce(pl.col("num_quarter"), pl.col("num_quarter_annually")).alias(
                "num_quarter"
            ),
        )
        return eps_df

    def get_report_announcement_date(self):
        # likewise, we only care about those report date in 3/31, 6/30, 9/30, 12/31
        # for those missing data, you need to manually lag the report date by 3 month when joining
        announcement_df = (
            pl.read_parquet(self.report_announcement_table)
            .filter(pl.col("announcement_date").is_not_null())
            .filter(
                (
                    (pl.col("report_date").dt.month() == 3)
                    & (pl.col("report_date").dt.day() == 31)
                )
                | (
                    (pl.col("report_date").dt.month() == 6)
                    & (pl.col("report_date").dt.day() == 30)
                )
                | (
                    (pl.col("report_date").dt.month() == 9)
                    & (pl.col("report_date").dt.day() == 30)
                )
                | (
                    (pl.col("report_date").dt.month() == 12)
                    & (pl.col("report_date").dt.day() == 31)
                )
            )
        )
        return announcement_df
