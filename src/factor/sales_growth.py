from functools import cache
from dateutil.relativedelta import relativedelta
import polars as pl

from src.database import engine
from src.factor.base_factor import BaseFactor
from src.factor.sector import Sector
from src.factor.const import SECTOR_ETF_MAPPING, SECTOR_ETF


class SalesGrowthFactor(BaseFactor):
    def __init__(self, security_universe, start_date, end_date, factor_type):
        super().__init__(security_universe, start_date, end_date, factor_type)

    def set_portfolio_at_start(self, portfolio, position):
        for security, weight in position:
            portfolio.add_security_weight(security, weight, portfolio.start_date)

    @cache
    def get_security_list(self, date):
        sector_list = self.build_sector_history_z_score(date)
        etf_list = []
        for sector in sector_list:
            if sector in SECTOR_ETF_MAPPING:
                etf = SECTOR_ETF_MAPPING[sector]
                etf_list.append(etf)
            else:
                print(f"couln't find etf for {sector} sector")
        return etf_list

    def build_sector_history_z_score(self, observe_date):
        month_range = 12  # use data in the last 12 months
        total_df_list = []
        for delta in range(month_range):
            date = observe_date - relativedelta(months=delta)
            df = self.build_single_month_sector_factor(date)
            total_df_list.append(df)
        total_df = pl.concat(total_df_list)

        prev_month, cur_month = self.get_last_month_bound(observe_date)
        cur_signal = total_df.filter(pl.col("date") > prev_month).clone()
        assert len(cur_month) < 15

        total_df = total_df.group_by(["sector"]).agg(
            (pl.col("weighted_signal").std().alias("std")),
            (pl.col("weighted_signal").mean().alias("mean")),
        )

        merge = (
            cur_signal.join(total_df, on="sector", how="inner")
            .with_columns(
                ((pl.col("weighted_signal") - pl.col("mean")) / pl.col("std")).alias(
                    "z-score"
                )
            )
            .sort("z-score", descending=True)
        )
        print(merge)
        ordered_sector = merge.get_column("sector").to_list()
        return ordered_sector

    def build_single_month_sector_factor(self, date):
        prev_month, cur_month = self.get_last_month_bound(date)
        bound = f"where date between '{prev_month}' and '{cur_month}'"
        signal_df = pl.read_database(
            f"select * from msci_usa_sales_growth_ntm {bound}", engine.connect()
        )
        signal_df = signal_df.rename({"growth": "signal"})
        sector_signal_df = Sector.get_sector_signal(signal_df).filter(
            pl.col("sector") != "--"
        )
        return sector_signal_df

    @staticmethod
    def get_last_month_bound(date):
        prev_month = (date - relativedelta(months=1)).strftime("%Y-%m")
        cur_month = date.strftime("%Y-%m")
        return (prev_month, cur_month)


if __name__ == "__main__":
    from datetime import date

    start_date = date.fromisoformat("2022-01-01")
    end_date = date.fromisoformat("2022-02-15")
    factor = SalesGrowthFactor(SECTOR_ETF, start_date, end_date, "long")
    df = factor.build_single_month_sector_factor(end_date)
