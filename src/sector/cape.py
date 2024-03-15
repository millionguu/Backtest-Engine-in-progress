import datetime
import polars as pl

from src.sector.base_sector import BaseSector


class CapeSector(BaseSector):
    def __init__(self):
        self.eps_quarterly_table = "parquet/cape/us_security_eps_quarterly.parquet"
        self.eps_annually_table = "parquet/cape/us_security_eps_annually.parquet"
        self.price_table = "parquet/cape/us_security_price_daily.parquet"
        self.cpi_table = "parquet/base/us_cpi.parquet"
        self.report_announcement_table = (
            "parquet/cape/us_security_income_report_announcement_date.parquet"
        )
        self.sector_df = self.get_sector_construction()

    def get_sector_list(self, observe_date):
        """
        1. construct sector securities and weight
        2. aggregate history data and calulate security signal
        3. aggregate security signal and calculate sector signal
        4. sort the sector sinal using z-score
        """
        # z-score range
        z_score_year_range = (
            10 if observe_date.year > 2010 else observe_date.year - 2001
        )
        total_df_list = []
        for delta in range(z_score_year_range):
            history_date = datetime.date(
                observe_date.year - delta, observe_date.month, observe_date.day
            )
            security_signal_df = self.get_security_signal(history_date)
            # we don't like negative PE
            security_signal_df = security_signal_df.filter(pl.col("signal") > 0)
            sector_signal_df = self.get_sector_signal(
                self.sector_df, security_signal_df
            )
            assert len(sector_signal_df) > 0
            total_df_list.append(sector_signal_df)
        total_signal_df = pl.concat(total_df_list)
        sector_list = self.sort_sector_using_z_score(total_signal_df)
        # less PE is better, thus we reverse the order
        sector_list = list(reversed(sector_list))
        assert len(sector_list) > 0
        return sector_list

    def get_sector_signal(
        self, sector_df: pl.DataFrame, signal_df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        use harmonic average
        """
        # should only have one date value
        assert len(signal_df.select(pl.col("date").unique())) == 1

        signal_df = signal_df.filter(pl.col("signal").is_not_null()).with_columns(
            pl.col("date").cast(pl.String).str.slice(0, 7).alias("ym")
        )

        sector_df = (
            sector_df.filter(pl.col("weight") > 0)
            .filter(pl.col("sector") != pl.lit("--"))
            .with_columns(pl.col("date").cast(pl.String).str.slice(0, 7).alias("ym"))
        )

        sector_signal_df = (
            signal_df.join(sector_df, on=["sedol7", "ym"], how="inner")
            .group_by(["sector", "date"])
            .agg(
                (pl.col("signal").mean()).alias("simple_avg_signal"),
                (pl.col("signal").count()).alias("signal_count"),
                (
                    1
                    / (
                        (1 / pl.col("signal"))
                        * (pl.coalesce(pl.col("weight"), 0) / (pl.col("weight")).sum())
                    ).sum()
                ).alias("weighted_signal"),
                (((pl.col("signal") * pl.coalesce(pl.col("weight"), 0))).sum()).alias(
                    "weighted_signal_numerator"
                ),
                (pl.col("weight").sum()).alias("weighted_signal_denominator"),
            )
        )
        # in some cases we don't like negtive value
        assert len(sector_signal_df.filter(pl.col("weighted_signal") < 0)) == 0
        # we only believe in those weight denominator are greater than 0.5
        sector_signal_df = sector_signal_df.filter(
            pl.col("weighted_signal_denominator") > 0.5
        )
        return sector_signal_df

    def get_security_signal(self, date):
        """
        aggregate history eps data and calulate security PE
        """

        eps_df = self.get_eps_construction(date)

        # make sure to have 120 months
        eleven_years_ago = date.year - 11

        # compound eps with the CPI data
        cpi_df = (
            pl.scan_parquet(self.cpi_table)
            .with_columns(pl.col("date").dt.year().alias("year"))
            .with_columns(pl.col("date").dt.month().alias("month"))
            .filter(pl.col("year") >= eleven_years_ago)
            .filter(pl.col("year") <= date.year)
            .select(
                pl.col("year"), pl.col("month"), pl.col("us_cpi_all").alias("cpi_index")
            )
            .collect()
        )

        latest_cpi_index = (
            cpi_df.select(pl.col("cpi_index").max()).get_column("cpi_index").item(0)
        )

        cpi_df = cpi_df.with_columns(
            (pl.lit(latest_cpi_index) / pl.col("cpi_index")).alias("cpi")
        )

        eps_df = (
            eps_df.filter(pl.col("year") >= eleven_years_ago)
            .join(cpi_df, how="inner", on=["year", "month"])
            .with_columns((pl.col("eps") * pl.col("cpi")).alias("eps"))
        )

        assert (
            len(eps_df.filter(pl.col("sedol7") == "2046251").sort(pl.col("year"))) > 0
        )

        # aggregate eps over the last 120 months
        eps_df = eps_df.with_columns(
            (pl.col("year") * 100 + pl.col("month"))
            .rank("ordinal", descending=True)
            .over("sedol7")
            .alias("row_number")
        ).filter(pl.col("row_number") <= 120)

        eps_df = eps_df.group_by("sedol7").agg(
            pl.col("eps").sum().alias("agg_eps"),
            pl.col("row_number").max().alias("max_row_number"),
        )

        # we only want those securities that has 120 months data
        eps_df = eps_df.filter(pl.col("max_row_number") == 120).with_columns(
            (pl.col("agg_eps") / pl.lit(10)).alias("avg_eps")
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
            .filter(pl.col("price").is_not_null())
            .collect()
        )

        signal_df = eps_df.join(
            price_df,
            how="inner",
            on="sedol7",
        )
        # annulize the PE, rather than using quarterly PE
        signal_df = signal_df.with_columns(
            (pl.col("price") / pl.col("avg_eps")).alias("signal")
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

        # explode to monthly
        eps_quarter_df = eps_quarter_df.with_columns(
            pl.lit(list(range(3))).alias("diff")
        )

        eps_quarter_df = (
            eps_quarter_df.explode("diff")
            .with_columns(pl.col("diff").cast(pl.Int8))
            .with_columns((pl.col("month") - pl.col("diff")).alias("month"))
            .with_columns((pl.col("eps") / 4).alias("eps"))
            .select(
                pl.col("sedol7").alias("a_sedol7"),
                pl.col("year").alias("a_year"),
                pl.col("month").alias("a_month"),
                pl.col("eps").alias("a_eps"),
            )
        )

        eps_annual_df = self.get_eps_annual_df(date)

        # de-duplicate
        eps_annual_df = eps_annual_df.group_by(
            pl.col("sedol7").alias("sedol7"),
            pl.col("date").dt.year().alias("year"),
        ).agg(pl.col("eps").max().alias("eps"))

        # explode to monthly
        eps_annual_df = eps_annual_df.with_columns(
            pl.lit(list(range(12))).alias("diff")
        )

        eps_annual_df = (
            eps_annual_df.explode("diff")
            .with_columns(pl.col("diff").cast(pl.Int8))
            .with_columns(pl.lit(12, dtype=pl.Int8).alias("month"))
            .with_columns((pl.col("month") - pl.col("diff")).alias("month"))
            .with_columns((pl.col("eps") / 12).alias("eps"))
            .select(
                pl.col("sedol7").alias("b_sedol7"),
                pl.col("year").alias("b_year"),
                pl.col("month").alias("b_month"),
                pl.col("eps").alias("b_eps"),
            )
        )

        # merge two data source
        eps_df = eps_quarter_df.join(
            eps_annual_df,
            how="outer",
            left_on=["a_sedol7", "a_year", "a_month"],
            right_on=["b_sedol7", "b_year", "b_month"],
        ).select(
            pl.coalesce(pl.col("a_sedol7"), pl.col("b_sedol7")).alias("sedol7"),
            pl.coalesce(pl.col("a_year"), pl.col("b_year")).alias("year"),
            pl.coalesce(pl.col("a_month"), pl.col("b_month")).alias("month"),
            pl.coalesce(pl.col("a_eps"), pl.col("b_eps")).alias("eps"),
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
        ) and observe_date <= datetime.date(year, 12, 31):
            latest_full_report_date = datetime.date(year, 6, 30)
            partly_report_date = datetime.date(year, 9, 30)
        else:
            raise ValueError(f"unexpected date {observe_date}")

        # note that we only use quarterly data whose report date is 3/31, 6/30, 9/30, 12/31
        eps_quarter_df = (
            pl.scan_parquet(self.eps_quarterly_table)
            .filter(pl.col("date").dt.year() >= latest_full_report_date.year - 11)
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
            .select(pl.col("sedol7"), pl.col("date"), pl.col("eps"))
        )

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

        if partly_report_date is None:
            return eps_annual_full_df
        else:
            eps_annual_part_df = (
                eps_annual_df.filter(pl.col("date") == partly_report_date)
                .with_columns(pl.lit(observe_date).alias("observe_date"))
                .collect()
                .sort(pl.col("observe_date"))
            )

            announcement_df = self.get_report_announcement_date(
                partly_report_date
            ).sort(pl.col("announcement_date"))

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
