from abc import ABC, abstractmethod
from dateutil.relativedelta import relativedelta
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

    def get_last_n_month_bound(self, cur_date, n=3):
        prev_month = (cur_date - relativedelta(months=n)).strftime("%Y-%m")
        cur_month = cur_date.strftime("%Y-%m")
        return (prev_month, cur_month)

    @abstractmethod
    def get_security_signal(self, date):
        """
        signal on the security level

        should have a column named signal

        aggregate on security's history data to get the sinal
        """
        pass

    def get_sector_signal(
        self, sector_df: pl.DataFrame, signal_df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        signal on the sector level

        aggregated from the security level

        simple_signal: simple average over the security signals on the same sector

        weighted_signal: weighted average over the security signals on the same sector,
                         weight is given by the S&P 500 index weight

        input parameter signal_df should have a column named signal
        """
        signal_df = signal_df.with_columns(
            pl.col("date").cast(pl.String).str.slice(0, 7).alias("ym")
        )
        sector_df = sector_df.with_columns(
            pl.col("date").cast(pl.String).str.slice(0, 7).alias("ym")
        )

        # TODO: deal with missing data
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

    def sort_sector_using_z_score(self, total_df, observe_date):
        """sort in descending order"""
        prev_month, cur_month = self.get_last_month_bound(observe_date)
        cur_signal = total_df.filter(pl.col("date") > prev_month).clone()
        assert len(cur_month) < 15

        total_df = total_df.group_by(["sector"]).agg(
            (pl.col("weighted_signal").std().alias("std")),
            (pl.col("weighted_signal").mean().alias("mean")),
        )

        merge = cur_signal.join(total_df, on="sector", how="inner").with_columns(
            ((pl.col("weighted_signal") - pl.col("mean")) / pl.col("std")).alias(
                "z-score"
            )
        )
        # print(merge)
        ordered_sector = (
            merge.sort("z-score", descending=True).get_column("sector").to_list()
        )
        return ordered_sector
