# source: https://www.cnbc.com/sector-etfs/
from src.security_symbol import SecurityTicker, SecurityLipper
import polars as pl


sector_etf_ticker_mapping = {
    "Consumer Discretionary": "XLY",
    "Energy": "XLE",
    "Real Estate": "VNQ",
    "Materials": "XLB",
    "Utilities": "XLU",
    "Information Technology": "XLK",
    "Communication Services": "XTL",
    "Health Care": "XLV",
    "Industrials": "XLI",
    "Consumer Staples": "XLP",
    "Financials": "XLF",
}

sector_etf_lipper_mapping = {
    "Consumer Discretionary": "40056100",
    "Energy": "40197404",  # not from black rock
    "Real Estate": "40056109",
    "Materials": "40056098",
    "Utilities": "40056106",
    "Information Technology": "40056080",
    "Communication Services": "40056094",
    "Health Care": "40056104",
    "Industrials": "40056105",
    "Consumer Staples": "40056101",
    "Financials": "40056093",
}


SECTOR_ETF_TICKER = [SecurityTicker(v, k) for k, v in sector_etf_ticker_mapping.items()]
SECTOR_ETF_LIPPER = [SecurityLipper(v, k) for k, v in sector_etf_lipper_mapping.items()]


if __name__ == "__main__":
    data = pl.read_excel("data/US Sector ETF Info")

    data.filter(pl.col("Technical Indicator\nBenchmark").str.starts_with("S&P")).filter(
        pl.col("Fund Management\nCompany Name").str.starts_with("BlackRock")
    ).select(pl.col("Name"), pl.col("Schemes"), pl.col("Launch Date")).sort(
        pl.col("Schemes")
    )
