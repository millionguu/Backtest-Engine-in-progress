import polars as pl

from src.database import engine


class Sector:
    @staticmethod
    def get_sector_construction():
        sector_info = pl.read_database(
            "select * from msci_usa_sector_info", engine.connect()
        ).select(["sedol7", "date", "sector"])

        sector_weight = pl.read_database(
            "select * from msci_usa_sector_weight", engine.connect()
        ).select(["sedol7", "date", "weight"])

        merge = sector_info.join(
            sector_weight, on=["sedol7", "date"], how="inner"
        ).select(["sedol7", "date", "sector", "weight"])

        new_weight_base = merge.group_by(["date", "sector"]).agg(
            pl.col("weight").sum().alias("total_weight")
        )

        sector_weight_df = (
            merge.join(new_weight_base, on=["date", "sector"], how="left")
            .with_columns((pl.col("weight") / pl.col("total_weight")).alias("weight"))
            .select(["sedol7", "date", "sector", "weight"])
        )
        return sector_weight_df

    @staticmethod
    def get_sector_signal(signal_df):
        """
        signal_df should have a column named signal
        """
        signal_df = signal_df.with_columns(
            pl.col("date").cast(pl.String).str.slice(0, 7).alias("ym")
        )
        sector_df = Sector.get_sector_construction().with_columns(
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
        )
        return sector_signal_df
