import datetime
import polars as pl
from sklearn.linear_model import Lasso

from src.factor_aggregator.factor_aggregator import FactorAggregator
from src.sector.roe import RoeSector
from src.sector.sales_growth import SalesGrowthSector
from src.sector.volume import VolumeSector


class LassoAggregator(FactorAggregator):
    def __init__(self, security_universe, factor_type):
        super().__init__(security_universe, factor_type)
        self.lasso_model = None

    def get_internal_sectors(self):
        roe = RoeSector()
        volume = VolumeSector()
        sales_growth = SalesGrowthSector()
        return [roe, volume, sales_growth]

    def get_fund_list(self, date):
        assert self.lasso_model is not None
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


class LassoModel:
    def __init__(self, security_universe, start_date, end_date) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.security_universe = security_universe
        self.model = Lasso(alpha=0.1)
        self.X = pl.DataFrame(
            pl.date_range(
                start=datetime.date(start_date.year, start_date.month, 1),
                end=datetime.date(end_date.year, end_date.month, 1),
                interval="1mo",
                eager=True,
            ).alias("start_date")
        )
        self.X = self.X.with_columns(
            pl.col("start_date").dt.month_end().alias("end_date")
        )
        for fund in security_universe:
            self.X = self.X.with_columns(pl.lit(0).alias(str(fund.sector)))
        self.X = self.X.with_columns(pl.lit(0).alias("forward_1mo_return"))
        self.X = self.X.filter(pl.col("end_date") < end_date)

    def prepare_training_data(self):
        self.melt_X = self.X.melt(
            id_vars=["start_date", "end_date", "forward_1mo_return"],
            value_vars=[str(fund.sector) for fund in self.security_universe],
            variable_name="sector",
            value_name="z-score",
        )

    def tain(self):
        pass

    def eval(self):
        pass


if __name__ == "__main__":
    from src.fund_universe import ISHARE_SECTOR_ETF_TICKER

    start_date = datetime.date(2013, 12, 31)
    end_date = datetime.date(2023, 10, 31)
    security_universe = ISHARE_SECTOR_ETF_TICKER
    model = LassoModel(security_universe, start_date, end_date)
    print()
