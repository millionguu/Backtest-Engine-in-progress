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
    "Energy": "ENFR",  # not from invesco
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


# SECTOR_ETF_TICKER = [SecurityTicker(v, k) for k, v in cnbc_ticker_sector_etf.items()]
SECTOR_ETF_TICKER = [
    SecurityTicker(v, k) for k, v in invesco_sp500_ticker_sector_etf.items()
]
SECTOR_ETF_LIPPER = [SecurityLipper(v, k) for k, v in invesco_sp500_sector_etf.items()]


# generate factor -> buy top 100 fund
# read file 5000 lipperid   ->  100
# equal weight buy 100 fund, each fund 0.001 weight  -> portofolio
# cash 100  -> fund 0.1 yuan

# wait 30 days and rebalance -> new 100 fund -> diff
# sold unwanted fund  buy want -> new target
# list of SecurityLipperId


# rebalance total:3/100
# every fund -> 1/3
# 1. new fund  sold unwanteda buy want   
# 2. no new fund  -> 33   35/total_value  30/total_value  40/total_value  value> 1  total_weight == 1