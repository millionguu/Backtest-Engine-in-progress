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
        # should only have one date value
        assert len(signal_df.select(pl.col("date").unique())) == 1

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

    def sort_sector_using_z_score(self, total_signal_df):
        """
        sort weighted signal in descending order
        """
        latest_month = (
            total_signal_df.select(pl.col("date").max()).get_column("date").item(0)
        )
        latest_signal_df = total_signal_df.filter(pl.col("date") == latest_month)

        total_signal_df = (
            total_signal_df.filter(pl.col("date") != latest_month)
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
