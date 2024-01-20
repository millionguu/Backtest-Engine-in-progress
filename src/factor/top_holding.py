# https://github.com/ranaroussi/yfinance/discussions/1761
# https://gist.github.com/bot-unit/ae757e68fc3616f8c35c8521fad51d83

import time
import requests
import bs4
import pandas as pd
from src.database import engine
import sqlalchemy

headers = {"User-agent": "Mozilla/5.0"}


def search_etf(ticker, region="UK", is_etf=True):
    search_site = f"https://markets.ft.com/data/funds/ajax/search"
    params = {"region": region, "isETF": "true" if is_etf else "false", "query": ticker}
    r = requests.get(search_site, params=params, headers=headers)
    return r.json()


def get_holders(holders_url):
    r = requests.get(holders_url, headers=headers)
    soup = bs4.BeautifulSoup(r.text, "html.parser")
    top_holding_section = soup.find_all("section", {"class": "mod-main-content"})
    if len(top_holding_section) == 0:
        return pd.DataFrame()
    top_holding_div = top_holding_section[0].find_all(
        "div", {"data-f2-app-id": "mod-top-ten"}
    )
    if len(top_holding_div) == 0:
        return pd.DataFrame()
    top_holding_table = top_holding_div[0].find_all("table", {"class": "mod-ui-table"})
    if len(top_holding_table) < 2:
        return pd.DataFrame()
    table = top_holding_table[1]
    heads = table.find_all("th")
    heads = [head.text.strip() for head in heads]
    heads.insert(1, "Ticker")
    columns = []
    rows = table.find_all("tr")
    for row in rows:
        # print(row)
        cols = row.find_all("td")
        if cols[0].find("a") is None:
            continue
        name = cols[0].find("a").text
        ticker = cols[0].find("span").text
        cols = [ele.text for ele in cols[1:]]
        cols.insert(0, name)
        cols.insert(1, ticker)
        columns.append(cols)
    top_holding_table = pd.DataFrame(columns=heads, data=columns)
    top_holding_table.drop(columns=["Long allocation"], inplace=True)
    return top_holding_table


def get_top_holdings(ticker, region="US", is_etf=True):
    table = f"top_holdings_{ticker}"
    if not sqlalchemy.inspect(engine).has_table(table):
        etfs = search_etf(ticker, region, is_etf)
        if isinstance(etfs, dict) and "data" in etfs and len(etfs["data"]) > 0:
            etf = etfs["data"][0]
            url = etf["url"].replace("~", "https://markets.ft.com/data")
            url = url.replace("summary", "holdings")
            df = get_holders(url)
            df.columns = df.columns.str.lower()
            df.to_sql(table, con=engine, chunksize=1000, index=False)
            time.sleep(0.1)
        else:
            raise ValueError("error occured while fetching top holdings.")
    return pd.read_sql(f"select * from {table}", engine)


if __name__ == "__main__":
    df = get_top_holdings("SPY")
    print(df)
