# source: https://www.cnbc.com/sector-etfs/
SECTOR_ETF_MAPPING = {
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


SECTOR_ETF = list(SECTOR_ETF_MAPPING.values())


# data.select(pl.all()).select(pl.col("Schemes")).unique().map_rows(lambda row: row[0][14:])

# data.filter(pl.col("Technical Indicator\nBenchmark").str.starts_with("S&P")) \ .filter(pl.col("Fund Management\nCompany Name").str.starts_with("BlackRock")) \
# .select(pl.col("Name"), pl.col("Schemes"), pl.col("Launch Date")) \
# .sort(pl.col("Schemes"))