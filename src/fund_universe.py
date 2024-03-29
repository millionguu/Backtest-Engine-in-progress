from src.security_symbol import SecurityTicker, SecurityLipper


# source: https://www.cnbc.com/sector-etfs/
cnbc_ticker_sector_etf = {
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

invesco_sp500_sector_etf = {
    "Consumer Discretionary": "40111945",
    "Energy": "40205516",  # not from invesco
    "Real Estate": "40212980",
    "Materials": "40111951",
    "Utilities": "40111953",
    "Information Technology": "40111952",
    "Communication Services": "40225234",
    "Health Care": "40111949",
    "Industrials": "40111950",
    "Consumer Staples": "40111946",
    "Financials": "40111948",
}

invesco_sp500_ticker_sector_etf = {
    "Consumer Discretionary": "RSPD",
    # "Energy": "ENFR",  # not from invesco
    "Real Estate": "RSPR",
    "Materials": "RSPM",
    "Utilities": "RSPU",
    "Information Technology": "RSPT",
    "Communication Services": "RSPC",
    "Health Care": "RSPH",
    "Industrials": "RSPN",
    "Consumer Staples": "RSPS",
    "Financials": "RSPF",
}

ishare_ticker_sector_etf = {
    "Consumer Discretionary": "IYC",
    # "Energy": "ENFR",  # not from invesco
    "Real Estate": "IYR",
    "Materials": "IYM",
    "Utilities": "IDU",
    "Information Technology": "IYW",
    "Communication Services": "IYZ",
    "Health Care": "IYH",
    "Industrials": "IYJ",
    "Consumer Staples": "IYK",
    "Financials": "IYF",
}

INVESCO_SECTOR_ETF_LIPPER = [
    SecurityLipper(v, k) for k, v in invesco_sp500_sector_etf.items()
]

ISHARE_SECTOR_ETF_TICKER = [
    SecurityTicker(v, k) for k, v in ishare_ticker_sector_etf.items()
]
CNBC_SECTOR_ETF_TICKER = [
    SecurityTicker(v, k) for k, v in cnbc_ticker_sector_etf.items()
]
INVESCO_SECTOR_ETF_TICKER = [
    SecurityTicker(v, k) for k, v in invesco_sp500_ticker_sector_etf.items()
]
