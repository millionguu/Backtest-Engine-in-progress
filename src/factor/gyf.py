from factor import Factor
from top_holding import get_top_holdings
import pandas as pd
from database import engine

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


class Factor1(Factor):
    def __init__(self):
        pass

    def get_position(self):
        weight = 0.9 / 3
        return [("SPX", weight), ("IXIC", weight), ("RUT", weight)]

    def set_portfolio(self, portfolio):
        for security, weight in self.get_position():
            portfolio.add_security_weight(security, weight, portfolio.start_date)

    def build_factor(self):
        top_holdings = get_top_holdings("SPY", "US", True)
        self.sales_growth = pd.read_sql(
            "select * from sales_growth_fy1_msci_usa", engine
        )
