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
        sector_score_df = self.get_sector_scores(
            date, ["RoeSector"], ["VolumeSector", "SalesGrowthSector"]
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
