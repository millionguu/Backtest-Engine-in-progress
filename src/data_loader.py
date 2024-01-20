from datetime import datetime
import numpy as np
import pandas as pd
from sqlalchemy import text
import yfinance
from src.database import engine


def write_excel_data(engine, filename, table):
    data = pd.read_excel(f"../data/{filename}.xlsx", index_col=False)
    data = data.rename(columns={"Unnamed: 1": "company"})
    data.columns = data.columns.str.lower()
    convert_columns = [c for c in data.columns if c not in ["sedol7", "company"]]
    for c in convert_columns:
        data[c] = pd.to_numeric(data[c], errors="coerce")
    data.to_sql(table, con=engine, chunksize=1000, index=False)


def write_yahoo_finance_source_to_sqlite(engine, filename, table):
    data = pd.read_csv(f"../data/{filename}.csv", index_col=False)
    data["return"] = np.divide(
        data["Adj Close"] - data["Adj Close"].shift(1), data["Adj Close"].shift(1)
    )
    data.columns = data.columns.str.lower()
    data.to_sql(table, con=engine, chunksize=1000, index=False)


def write_market_open_date(engine):
    today = datetime.today().strftime("%Y-%m-%d")
    proxy = "http://127.0.0.1:1080"
    data = yfinance.download("^DJI", start="2000-01-01", end=today, proxy=proxy)
    data["date"] = pd.to_datetime(data.index).date
    data["date"].to_sql("us_market_open_date", con=engine, chunksize=1000, index=False)


def get_market_open_date(engine, start_date, end_date):
    df = pd.read_sql(f"select date from us_market_open_date", engine)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    condition = (df["date"] >= start_date) & (df["date"] <= end_date)
    return df[condition]["date"].values


if __name__ == "__main__":
    with engine.connect() as conn, conn.begin():
        # conn.execute(text(f"create table if not exists meta(id int(10));"))
        # conn.execute(text(f"drop table if exists us_market_open_date;"))
        # write_market_open_date(engine)

        filename = "Sales Gth FY1_MSCI USA_20001231-20231130"
        table = "sales_growth_fy1_msci_usa"
        conn.execute(text(f"drop table if exists {table};"))
        write_excel_data(engine, filename, table)
