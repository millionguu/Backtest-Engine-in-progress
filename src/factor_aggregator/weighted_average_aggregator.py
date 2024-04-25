import polars as pl

from src.factor_aggregator.simple_average_aggregator import SimpleAverageAggregator


class WeightedAverageAggregator(SimpleAverageAggregator):
    def __init__(self, security_universe, factor_type):
        super().__init__(security_universe, factor_type)

    def get_fund_list(self, date):
        sector_score_df: pl.DataFrame = self.get_sector_scores(
            date, ["RoeSector"], ["VolumeSector", "SalesGrowthSector"]
        )

        sector_score_df = sector_score_df.with_columns(
            pl.when(pl.col("class_name") == "RoeSector")
            .then(pl.col("z-score") * 4)
            .when(pl.col("class_name") == "VolumeSector")
            .then(pl.col("z-score"))
            .when(pl.col("class_name") == "SalesGrowthSector")
            .then(pl.col("z-score") * 2)
            .otherwise(None)
        )

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
