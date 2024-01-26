import datetime
import numpy as np
import pandas as pd
import duckdb

from src.database import engine
from src.factor.base_factor import Factor
from src.factor.top_holding import get_top_holdings
from src.factor.sector import Sector
from src.factor.const import SECTOR_ETF


class SalesGrowthFactor(Factor):
    def __init__(self, security_universe, start_date, end_date):
        super().__init__(security_universe, start_date, end_date)

    def get_position(self, date):
        # TODO: transfer sector to ETF
        security_list = self.build_factor(date)
        first_quintile = self.get_first_quintile(security_list)
        weight = 1 / len(first_quintile)
        return [(s, weight) for s in first_quintile]

    def set_portfolio_at_start(self, portfolio):
        for security, weight in self.get_position(self.start_date):
            portfolio.add_security_weight(security, weight, portfolio.start_date)

    def get_security_list(self, date):
        pass

    def build_sector_factor(self, date):
        signal_df = pd.read_sql("select * from msci_usa_sales_growth_ttm", engine)
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
        res = sector_signal_df["sector"].to_list()
        return res


if __name__ == "__main__":
    from datetime import date

    start_date = date.fromisoformat("2022-01-01")
    end_date = date.fromisoformat("2022-12-21")
    factor = SalesGrowthFactor(SECTOR_ETF, start_date, end_date)
    df = factor.build_factor(end_date)
    print("")
    # position = factor.get_position(date.fromisoformat("2023-03-31"))
