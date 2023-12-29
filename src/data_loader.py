import os
import pathlib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


def write_yahoo_finance_source_to_sqlite(engine, filename, table):
    with open(f"../data/{filename}.csv") as f:
        data = pd.read_csv(f, index_col=False)
    data["return"] = np.divide(
        data["Adj Close"] - data["Adj Close"].shift(1), data["Adj Close"].shift(1)
    )
    data.columns = data.columns.str.lower()
    data.to_sql(table, con=engine, chunksize=1000, index=False)


def write_market_open_date(engine, filename, table):
    with open(f"../data/{filename}.csv") as f:
        data = pd.read_csv(f, index_col=False)
    data.columns = data.columns.str.lower()
    data["date"].to_sql(table, con=engine, chunksize=1000, index=False)


def get_market_open_date(engine, table, start_date, end_date):
    df = pd.read_sql(f"select date from {table}", engine)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    condition = (df["date"] >= start_date) & (df["date"] <= end_date)
    return df[condition]["date"].values


if __name__ == "__main__":
    path = os.path.join(pathlib.Path(__file__).parent.parent, "data.db")
    engine = create_engine("sqlite:///" + path)
    with engine.connect() as conn, conn.begin():
        # write_market_open_date(engine, filename, "us_market_date")

        filename = "^RUT"
        table = "RUT"
        conn.execute(text(f"drop table if exists {table};"))
        write_yahoo_finance_source_to_sqlite(engine, filename, table)
        pd.read_sql(f"select * from {table}", engine)
