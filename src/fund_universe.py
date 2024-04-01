from src.security_symbol import SecurityTicker, SecurityLipper


invesco_sp500_sector_etf = {
    "Consumer Discretionary": "40111945",
    # "Energy": "",
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
    "Energy": "RSPG",
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
    "Energy": "IYE",
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

# market weight ETF
ISHARE_SECTOR_ETF_TICKER = [
    SecurityTicker(v, k) for k, v in ishare_ticker_sector_etf.items()
]

# equal weight ETF
INVESCO_SECTOR_ETF_TICKER = [
    SecurityTicker(v, k) for k, v in invesco_sp500_ticker_sector_etf.items()
]
