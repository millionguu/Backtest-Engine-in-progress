import datetime
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
        # the key of the cache is date.
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
            history_date = datetime.date(
                observe_date.year - delta, observe_date.month, observe_date.day
            )
            cache_key = history_date
            if cache_key in self.sector_signal_cache:
                sector_signal_df = self.sector_signal_cache[cache_key]
            else:
                security_signal_df = self.get_security_signal(history_date)
                sector_signal_df = self.get_sector_signal(
                    self.sector_df, security_signal_df
                )
                self.sector_signal_cache[cache_key] = sector_signal_df
                assert len(sector_signal_df) > 0
            total_df_list.append(sector_signal_df)
        total_signal_df = pl.concat(total_df_list)
        sector_list = self.sort_sector_using_z_score(total_signal_df)
        # less PE is better, thus we reverse the order
        sector_list = list(reversed(sector_list))
        assert len(sector_list) > 0
        return sector_list

    def get_security_signal(self, date):
        """
        aggregate history eps data and calulate security PE
        """

        eps_df = self.get_eps_construction(date)

        ten_year_ago = datetime.date(date.year - 11, date.month, date.day)
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

        assert (
            len(eps_df.filter(pl.col("sedol7") == "2046251").sort(pl.col("year"))) > 0
        )

        # aggregate eps on the last 10 years
        # note that when aggregating over the 10 year period,
        eps_df = eps_df.group_by("sedol7").agg(
            pl.col("annual_eps").sum().alias("agg_eps"),
            pl.col("num_quarter").sum().alias("num_quarter"),
        )

        # the num_quarter should range between [40, 44]
        # for those securities that have missing data in the past 10 years, we drop it
        eps_df = (
            eps_df.filter(pl.col("num_quarter") >= 40)
            .filter(pl.col("num_quarter") <= 44)
            .with_columns(
                (pl.col("agg_eps") / pl.col("num_quarter")).alias("avg_quarter_eps")
            )
        )

        # adjust date to the latest market open date, thus to have price data
        latest_market_open_date = (
            pl.scan_parquet(self.price_table)
            .filter(pl.col("date") <= date)
            .select(pl.col("date").max())
            .collect()
            .get_column("date")
            .item(0)
        )

        price_df = (
            pl.scan_parquet(self.price_table)
            .filter(pl.col("date") == latest_market_open_date)
            .collect()
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

        # assert len(signal_df.filter(pl.col("sedol7") == "2046251")) > 0
        return signal_df

    def get_eps_construction(self, date):

        eps_quarter_df = self.get_eps_quarter_df(date)

        # de-duplicate
        eps_quarter_df = eps_quarter_df.group_by(
            [
                pl.col("sedol7"),
                pl.col("date").dt.year().alias("year"),
                pl.col("date").dt.month().alias("month"),
            ]
        ).agg(pl.col("eps").max().alias("eps"))

        # aggregate to annual
        eps_quarter_df = eps_quarter_df.group_by(
            [
                pl.col("sedol7").alias("quarter_sedol7"),
                pl.col("year").alias("quarter_year"),
            ]
        ).agg(
            pl.col("eps").sum().alias("quarter_annual_eps"),
            pl.col("eps").count().alias("quarter_num_quarter"),
        )

        eps_annual_df = self.get_eps_annual_df(date)

        # de-duplicate
        eps_annual_df = (
            eps_annual_df.group_by(
                pl.col("sedol7").alias("annual_sedol7"),
                pl.col("date").dt.year().alias("annual_year"),
            )
            .agg(pl.col("eps").max().alias("annual_annual_eps"))
            .with_columns(pl.lit(4).alias("annual_num_quarter"))
        )

        # when missing a whole year data in quartely_df,
        # we try to replenish it using the annualy_df.
        # if still missing data after the replenish, we drop it.
        eps_df = eps_quarter_df.join(
            eps_annual_df,
            how="outer",
            left_on=["quarter_sedol7", "quarter_year"],
            right_on=["annual_sedol7", "annual_year"],
        ).select(
            pl.coalesce(pl.col("quarter_sedol7"), pl.col("annual_sedol7")).alias(
                "sedol7"
            ),
            pl.coalesce(pl.col("quarter_year"), pl.col("annual_year")).alias("year"),
            pl.coalesce(
                pl.col("quarter_annual_eps"), pl.col("annual_annual_eps")
            ).alias("annual_eps"),
            pl.coalesce(
                pl.col("quarter_num_quarter"), pl.col("annual_num_quarter")
            ).alias("num_quarter"),
        )
        return eps_df

    def get_eps_quarter_df(self, observe_date):
        """
        if date >= 3/31, 6/30, 9/30, 12/31 + 3 months,
        then the corresponding quarter data is all availabel.

        i.e.
        if date in [12/31, 3/31), we have all the 9/30 data and partly 12/31 data.
        if date in [3/31, 6/30), we have all the 12/31 data and partly 3/31 data.
        if date in [6/30, 9/30), we have all the 3/31 data and partly 6/30 data.
        if date in [9/30, 12/31), we have all the 6/30 data and partly 9/30 data.

        we use announcement_date to get the partly data.
        """

        year = observe_date.year
        if observe_date >= datetime.date(
            year - 1, 12, 31
        ) and observe_date < datetime.date(year, 3, 31):
            latest_full_report_date = datetime.date(year - 1, 9, 30)
            partly_report_date = datetime.date(year - 1, 12, 31)
        elif observe_date >= datetime.date(
            year, 3, 31
        ) and observe_date < datetime.date(year, 6, 30):
            latest_full_report_date = datetime.date(year - 1, 12, 31)
            partly_report_date = datetime.date(year, 3, 31)
        elif observe_date >= datetime.date(
            year, 6, 30
        ) and observe_date < datetime.date(year, 9, 30):
            latest_full_report_date = datetime.date(year, 3, 31)
            partly_report_date = datetime.date(year, 6, 30)
        elif observe_date >= datetime.date(
            year, 9, 30
        ) and observe_date < datetime.date(year, 12, 31):
            latest_full_report_date = datetime.date(year, 6, 30)
            partly_report_date = datetime.date(year, 9, 30)
        else:
            raise ValueError(f"unexpected date {observe_date}")

        # note that we only use quarterly data whose report date is 3/31, 6/30, 9/30, 12/31
        eps_quarter_df = (
            pl.scan_parquet(self.eps_quarterly_table)
            .filter(pl.col("eps").is_not_null())
            .filter(
                ((pl.col("date").dt.month() == 3) & (pl.col("date").dt.day() == 31))
                | ((pl.col("date").dt.month() == 6) & (pl.col("date").dt.day() == 30))
                | ((pl.col("date").dt.month() == 9) & (pl.col("date").dt.day() == 30))
                | ((pl.col("date").dt.month() == 12) & (pl.col("date").dt.day() == 31))
            )
            .select(pl.col("sedol7"), pl.col("date"), pl.col("eps"))
        )

        eps_quarter_full_df = (
            eps_quarter_df.filter(pl.col("date") <= latest_full_report_date)
            .collect()
            .with_columns(pl.col("date").dt.year().alias("year"))
        )

        # potentially missing data, we drop the whole year,
        # and try to use annually eps in the future
        missing_data_df = (
            eps_quarter_full_df.group_by(pl.col("sedol7"), pl.col("year"))
            .agg(pl.count("eps").alias("cnt"))
            .filter(pl.col("cnt") < 4)
            .select(pl.col("sedol7"), pl.col("year"))
        )

        eps_quarter_full_df = eps_quarter_full_df.join(
            missing_data_df, how="anti", on=["sedol7", "year"]
        ).select("sedol7", "date", "eps")

        eps_quarter_part_df = (
            eps_quarter_df.filter(pl.col("date") == partly_report_date)
            .with_columns(pl.lit(observe_date).alias("observe_date"))
            .collect()
            .sort(pl.col("observe_date"))
        )

        announcement_df = self.get_report_announcement_date(partly_report_date).sort(
            pl.col("announcement_date")
        )

        eps_quarter_part_df = (
            eps_quarter_part_df.join_asof(
                announcement_df,
                left_on="observe_date",
                right_on="announcement_date",
                strategy="backward",
            )
            .filter(pl.col("announcement_date").is_not_null())
            .select(pl.col("sedol7"), pl.col("date"), pl.col("eps"))
        )

        eps_quarter_df = pl.concat(
            [eps_quarter_full_df, eps_quarter_part_df], how="vertical"
        )
        return eps_quarter_df

    def get_eps_annual_df(self, observe_date):
        year = observe_date.year
        if observe_date >= datetime.date(year, 3, 31):
            latest_full_report_date = datetime.date(year - 1, 12, 31)
            partly_report_date = None
        else:
            latest_full_report_date = datetime.date(year - 2, 12, 31)
            partly_report_date = datetime.date(year - 1, 12, 31)

        # note that we only use those annual data whose report date is 12/31
        eps_annual_df = (
            pl.scan_parquet(self.eps_annually_table)
            .filter((pl.col("date").dt.month() == 12) & (pl.col("date").dt.day() == 31))
            .filter(pl.col("eps").is_not_null())
            .select(pl.col("sedol7"), pl.col("date"), pl.col("eps"))
        )
        eps_annual_full_df = eps_annual_df.filter(
            pl.col("date") <= latest_full_report_date
        ).collect()

        eps_annual_part_df = (
            eps_annual_df.filter(pl.col("date") == partly_report_date)
            .with_columns(pl.lit(observe_date).alias("observe_date"))
            .collect()
            .sort(pl.col("observe_date"))
        )

        announcement_df = self.get_report_announcement_date(partly_report_date).sort(
            pl.col("announcement_date")
        )

        eps_annual_part_df = (
            eps_annual_part_df.join_asof(
                announcement_df,
                left_on="observe_date",
                right_on="announcement_date",
                strategy="backward",
            )
            .filter(pl.col("announcement_date").is_not_null())
            .select(pl.col("sedol7"), pl.col("date"), pl.col("eps"))
        )

        eps_annual_df = pl.concat(
            [eps_annual_full_df, eps_annual_part_df], how="vertical"
        )
        return eps_annual_df

    def get_report_announcement_date(self, report_date):
        # likewise, we only care about those report date in 3/31, 6/30, 9/30, 12/31
        # we only consider the diff between announcement_date and report_date in range (0, 3] months is reasonable
        # for those missing data or gap larger than 3 months, we just fix it to be report_date + 3 months
        announcement_df = (
            pl.scan_parquet(self.report_announcement_table)
            .filter(pl.col("report_date") == report_date)
            .filter(pl.col("announcement_date").is_not_null())
            .filter(
                pl.col("announcement_date") - pl.col("report_date")
                > datetime.timedelta(days=0)
            )
            .filter(
                pl.col("announcement_date") - pl.col("report_date")
                <= datetime.timedelta(days=93)
            )
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
            .collect()
        )
        return announcement_df
