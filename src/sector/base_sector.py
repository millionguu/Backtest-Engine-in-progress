import datetime
from abc import ABC, abstractmethod

import polars as pl


class BaseSector(ABC):
    def __init__(self) -> None:
        super().__init__()

    def get_sector_construction(self):
        """
        schema: "sedol7", "date", "sector", "weight"

        weight is adjusted based on the date and sector
        """
        sector_info = pl.read_parquet("parquet/base/us_sector_info.parquet").select(
            ["sedol7", "date", "sector"]
        )

        # originally, the weight is based on the all sectors
        sector_weight = pl.read_parquet("parquet/base/us_sector_weight.parquet").select(
            ["sedol7", "date", "weight"]
        )

        merge = sector_info.join(
            sector_weight, on=["sedol7", "date"], how="inner"
        ).select(["sedol7", "date", "sector", "weight"])

        # calculate the total weight for any particualr sector
        new_weight_base = merge.group_by(["date", "sector"]).agg(
            pl.col("weight").sum().alias("total_weight")
        )

        # new weight is based on any particular sector
        sector_weight_df = (
            merge.join(new_weight_base, on=["date", "sector"], how="left")
            .with_columns((pl.col("weight") / pl.col("total_weight")).alias("weight"))
            .select(["sedol7", "date", "sector", "weight"])
        )
        return sector_weight_df

    @abstractmethod
    def get_security_signal(self, date):
        """
        signal on the security level

        should have a column named signal

        aggregate on security's history data to get the sinal
        """
        pass

    @abstractmethod
    def get_sector_list(self, observe_date):
        """
        1. construct sector
        2. generate sector signal
        3. sort the sector
        """
        pass

    def get_sector_signal(
        self, sector_df: pl.DataFrame, signal_df: pl.DataFrame, allow_neg_signal=False
    ) -> pl.DataFrame:
        """
        signal on the sector level

        aggregated from the security level

        simple_signal: simple average over the security signals on the same sector

        weighted_signal: weighted average over the security signals on the same sector,
                         weight is given by the S&P 500 index weight

        input parameter signal_df should have a column named signal
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
                    (pl.col("signal") * pl.coalesce(pl.col("weight"), 0)).sum()
                    / (pl.col("weight")).sum()
                ).alias("weighted_signal"),
                (((pl.col("signal") * pl.coalesce(pl.col("weight"), 0))).sum()).alias(
                    "weighted_signal_numerator"
                ),
                (pl.col("weight").sum()).alias("weighted_signal_denominator"),
            )
        )
        # in some cases we don't like negtive value
        if not allow_neg_signal:
            assert len(sector_signal_df.filter(pl.col("weighted_signal") < 0)) == 0
        # we only believe in those weight denominator are greater than 0.5
        sector_signal_df = sector_signal_df.filter(
            pl.col("weighted_signal_denominator") > 0.5
        )
        return sector_signal_df

    def sort_sector_using_z_score(self, total_signal_df):
        """
        sort weighted signal in descending order
        """
        latest_month = (
            total_signal_df.select(pl.col("date").max()).get_column("date").item(0)
        )
        latest_signal_df = total_signal_df.filter(pl.col("date") == latest_month)

        total_signal_df = (
            total_signal_df
            # .filter(pl.col("date") != latest_month)
            .filter(pl.col("weighted_signal").is_not_null())
            .filter(pl.col("weighted_signal").is_not_nan())
            .group_by(["sector"])
            .agg(
                (pl.col("weighted_signal").std().alias("std")),
                (pl.col("weighted_signal").mean().alias("mean")),
            )
            .filter(pl.col("std").is_not_null())
        )

        assert len(total_signal_df.filter(pl.col("std").is_null())) == 0
        assert len(total_signal_df.filter(pl.col("mean").is_null())) == 0

        merge_df = latest_signal_df.join(
            total_signal_df, on="sector", how="inner"
        ).with_columns(
            ((pl.col("weighted_signal") - pl.col("mean")) / pl.col("std")).alias(
                "z-score"
            )
        )
        # print(merge)
        ordered_sector = (
            merge_df.sort("z-score", descending=True).get_column("sector").to_list()
        )
        return ordered_sector

    def get_report_announcement_date(self, report_date):
        # we only care about those report date in 3/31, 6/30, 9/30, 12/31
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
