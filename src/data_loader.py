from datetime import datetime
import numpy as np
import pandas as pd
from sqlalchemy import text
import yfinance
from src.database import engine


def write_sector_weight(engine):
    filename = "Weight_MSCI USA_20001229_20231130.xlsx"
    table = "msci_usa_sector_weight"
    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"drop table if exists {table};"))
    data = pd.read_excel("data/" + filename)
    data = data.rename(columns={"Unnamed: 1": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"], var_name="date", value_name="weight"
    )
    data["date"] = pd.to_datetime(data["date"].apply(lambda x: str(x))).dt.date
    data.to_sql(table, con=engine, chunksize=1000, index=False)


def write_sector_info(engine):
    filename = "GICS_MSCI USA_20001229_20231130.xlsx"
    table = "msci_usa_sector_info"
    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"drop table if exists {table};"))
    data = pd.read_excel("data/" + filename)
    data = data.rename(columns={"Unnamed: 1": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"], var_name="date", value_name="sector"
    )
    data["date"] = pd.to_datetime(data["date"]).dt.date
    data.to_sql(table, con=engine, chunksize=1000, index=False)


def write_sales_growth_data(engine):
    file_names = [
        "Sales Gth FY1_MSCI USA_20001231-20231130.xlsx",
        "Sales Gth NTM_MSCI USA_20001231-20231130.xlsx",
        "Sales Gth TTM_MSCI USA_20001231-20231130.xlsx",
    ]
    tables = [
        "msci_usa_sales_growth_fy1",
        "msci_usa_sales_growth_ntm",
        "msci_usa_sales_growth_ttm",
    ]
    with engine.connect() as conn, conn.begin():
        for table in tables:
            conn.execute(text(f"drop table if exists {table};"))
    for file_name, table in zip(file_names, tables):
        data = pd.read_excel(f"data/{file_name}", index_col=False)
        data = data.rename(columns={"Unnamed: 1": "company", "SEDOL7": "sedol7"})
        convert_columns = [c for c in data.columns if c not in ["sedol7", "company"]]
        for c in convert_columns:
            data[c] = pd.to_numeric(data[c], errors="coerce")
        data = data.melt(
            id_vars=["sedol7", "company"], var_name="date", value_name="growth"
        )
        data["date"] = pd.to_datetime(data["date"]).dt.date
        data.to_sql(table, con=engine, chunksize=1000, index=False)


def write_market_open_date(engine):
    table = "us_market_open_date"
    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"drop table if exists {table};"))
    today = datetime.today().strftime("%Y-%m-%d")
    data = yfinance.download("^DJI", start="2000-01-01", end=today)
    data["date"] = pd.to_datetime(data.index).date
    data["date"].to_sql(table, con=engine, chunksize=1000, index=False)


def get_market_open_date(engine, start_date, end_date):
    df = pd.read_sql(f"select date from us_market_open_date", engine)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    condition = (df["date"] >= start_date) & (df["date"] <= end_date)
    return df[condition]["date"].values


if __name__ == "__main__":
    write_sales_growth_data(engine)
