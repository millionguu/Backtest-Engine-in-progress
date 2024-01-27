from functools import cache
import numpy as np
import pandas as pd

from src.database import engine
from src.factor.base_factor import BaseFactor
from src.factor.top_holding import get_top_holdings
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
        sector_list = self.build_sector_factor(date)
        etf_list = []
        for sector in sector_list:
            if sector in SECTOR_ETF_MAPPING:
                etf = SECTOR_ETF_MAPPING[sector]
                etf_list.append(etf)
            else:
                print(f"couln't find etf for {sector} sector")
        return etf_list

    def build_sector_factor(self, date):
        signal_df = pd.read_sql("select * from msci_usa_sales_growth_fy1", engine)
        signal_df = signal_df.rename(columns={"growth": "signal"})
        sector_signal_df = Sector.get_sector_signal(signal_df)
        closest_month_end = self.get_closest_month_end(date)
        sector_signal_df = sector_signal_df[
            sector_signal_df["date"] == closest_month_end
        ]
        sector_signal_df = sector_signal_df.sort_values(
            by="weighted_signal", ascending=False
        )
        sector_signal_df = sector_signal_df[sector_signal_df["sector"] != "--"]
        print("sector signal value:\n", sector_signal_df)
        sector_list = sector_signal_df["sector"].to_list()
        return sector_list


if __name__ == "__main__":
    from datetime import date

    start_date = date.fromisoformat("2022-01-01")
    end_date = date.fromisoformat("2022-12-21")
    factor = SalesGrowthFactor(SECTOR_ETF, start_date, end_date)
    df = factor.build_sector_factor(end_date)
    print()
