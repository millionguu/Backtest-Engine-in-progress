from src.factor.base_factor import Factor
from src.factor.top_holding import get_top_holdings
import numpy as np
import pandas as pd
from src.database import engine

# source: https://www.cnbc.com/sector-etfs/
SECTOR_ETF = [
    "XLE",
    "XLF",
    "XLU",
    "XLI",
    "GDX",
    "XLK",
    "XLV",
    "XLY",
    "XLP",
    "XLB",
    "XOP",
    "IYR",
    "XHB",
    "ITB",
    "VNQ",
    "GDXJ",
    "IYE",
    "OIH",
    "XME",
    "XRT",
    "SMH",
    "IBB",
    "KBE",
    "KRE",
    "XTL",
]


class SalesGrowthFactor(Factor):
    def __init__(self, security_universe, start_date, end_date):
        super().__init__(security_universe, start_date, end_date)

    def get_position(self, date):
        security_list = self.build_factor(date)
        first_quintile = self.get_first_quintile(security_list)
        weight = 1 / len(first_quintile)
        return [(s, weight) for s in first_quintile]

    def set_portfolio_at_start(self, portfolio):
        for security, weight in self.get_position(self.start_date):
            portfolio.add_security_weight(security, weight, portfolio.start_date)

    def build_factor(self, date):
        res = []
        for ticker in self.security_universe:
            avg_sales_grwoth = self.get_ticker_sales_growth(ticker, date)
            res.append((ticker, avg_sales_grwoth))
        res = sorted(res, key=lambda x: x[1], reverse=True)
        print("build factor value:", res)
        security_list = list(map(lambda x: x[0], res))
        return security_list

    def get_ticker_sales_growth(self, ticker, date):
        top_holdings = get_top_holdings(ticker, "US", True)
        holding_list = (
            top_holdings["company"].str.split().apply(lambda x: x[0]).to_list()
        )
        sales_growth = pd.read_sql("select * from sales_growth_fy1_msci_usa", engine)
        # TODO: join condition not robust
        test_contains = lambda row: any(
            map(
                lambda c: c in row.split()
                and "Class B" not in row
                and "Class C" not in row,
                holding_list,
            )
        )
        condition = sales_growth["company"].apply(test_contains)
        df = sales_growth[condition]
        df = df.drop(columns=["sedol7"]).set_index(["company"]).stack().reset_index()
        df = df.rename(columns={"level_1": "date", 0: "value"})
        df = pd.pivot_table(
            df, values="value", index="date", columns="company", aggfunc="max"
        ).sort_index()
        df.index = pd.to_datetime(df.index).date
        if len(df) > 0:
            return np.average(df[df.index < date].iloc[-1].dropna().values)
        else:
            print("couldnt find sales growth data for", ticker)
            return -100


if __name__ == "__main__":
    from datetime import date

    start_date = date.fromisoformat("2023-01-01")
    end_date = date.fromisoformat("2023-12-21")
    factor = SalesGrowthFactor(SECTOR_ETF, start_date, end_date)
    position = factor.get_position(date.fromisoformat("2023-03-31"))
