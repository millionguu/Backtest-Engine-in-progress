import polars as pl

from src.factor_aggregator.factor_aggregator import FactorAggregator
from src.sector.roe import RoeSector
from src.sector.sales_growth import SalesGrowthSector
from src.sector.volume import VolumeSector


class SimpleAverageAggregator(FactorAggregator):
    def __init__(self, security_universe, factor_type):
        super().__init__(security_universe, factor_type)

    def get_internal_sectors(self):
        roe = RoeSector()
        volume = VolumeSector()
        sales_growth = SalesGrowthSector()
        return [roe, volume, sales_growth]

    def get_fund_list(self, date):
        sector_score_df = self.get_normalized_score(date)
        sector_score_df = sector_score_df.group_by("sector").agg(
            pl.col("z-score").mean()
        )
        sector_list = (
            sector_score_df.sort(pl.col("z-score"), descending=True)
            .get_column("sector")
            .to_list()
        )
        fund_list = []
        for sector in sector_list:
            for security in self.security_universe:
                if security.sector == sector:
                    fund_list.append(security)
        return fund_list

    def get_normalized_score(self, date):
        original_sector_score_df: pl.DataFrame = self.get_sector_scores(date)
        sector_score_df = original_sector_score_df.with_columns(
            pl.when(pl.col("class_name") == "RoeSector")
            .then(pl.col("z-score"))
            .when(pl.col("class_name") == "VolumeSector")
            .then(1 / pl.col("z-score"))  # reversed signal
            .when(pl.col("class_name") == "SalesGrowthSector")
            .then(1 / pl.col("z-score"))  # reversed signal
            .otherwise(None)
        )

        # normalize for different class
        stat_df = sector_score_df.group_by("class_name").agg(
            pl.col("z-score").mean().alias("mean"),
            pl.col("z-score").std().alias("std"),
        )
        sector_score_df = sector_score_df.join(
            stat_df, on="class_name", how="inner"
        ).with_columns(
            ((pl.col("z-score") - pl.col("mean")) / pl.col("std")).alias("z-score")
        )
        return sector_score_df
