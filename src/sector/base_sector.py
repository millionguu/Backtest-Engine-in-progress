from abc import ABC, abstractmethod
from dateutil.relativedelta import relativedelta
import polars as pl

from src.database import engine


class BaseSector(ABC):
    def __init__(self) -> None:
        super().__init__()

    def get_sector_construction(self):
        """
        schema: "sedol7", "date", "sector", "weight"

        weight is adjusted based on the date and sector
        """
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

    def get_last_month_bound(self, date):
        prev_month = (date - relativedelta(months=1)).strftime("%Y-%m")
        cur_month = date.strftime("%Y-%m")
        return (prev_month, cur_month)
