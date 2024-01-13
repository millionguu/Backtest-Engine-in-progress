import os
import pathlib
import time
import numpy as np
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
import sqlalchemy


class Market:
    def __init__(self, securities):
        path = os.path.join(pathlib.Path(__file__).parent.parent, "data.db")
        self.engine = create_engine("sqlite:///" + path)
        self.securities = securities
        self.data = dict()
        for security in self.securities:
            table = security if not security.startswith("^") else security[1:]
            if not sqlalchemy.inspect(self.engine).has_table(table):
                data = self.retrive_data_from_yfinance(security)
                data.to_sql(table, con=self.engine, chunksize=1000, index=False)
            self.data[table] = pd.read_sql(f"select * from {table}", self.engine)
            self.data[table]["date"] = pd.to_datetime(self.data[table]["date"]).dt.date
            time.sleep(3)

    def retrive_data_from_yfinance(self, security):
        # need proxy
        proxy = "http://127.0.0.1:1080"
        data = yf.download(security, start="2000-01-01", end="2023-12-31", proxy=proxy)
        data["return"] = np.divide(
            data["Adj Close"] - data["Adj Close"].shift(1), data["Adj Close"].shift(1)
        )
        data.columns = data.columns.str.lower()
        data["date"] = pd.to_datetime(data.index).date
        return data

    def query_return(self, security, date):
        table = security if not security.startswith("^") else security[1:]
        condition = self.data[table]["date"] == date
        if sum(condition) == 1:
            return self.data[table][condition]["return"].to_numpy()[0]
        else:
            # market close
            return 0


if __name__ == "__main__":
    from datetime import date
    from sqlalchemy import create_engine, text

    index = "^SPX"
    table = "SPX"
    path = os.path.join(pathlib.Path(__file__).parent.parent, "data.db")
    engine = create_engine("sqlite:///" + path)
    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"drop table if exists {table};"))

    market = Market([index])
    print(market.retrive_data_from_yfinance(index).iloc[1])
    print(market.query_return(index, date.fromisoformat("2023-01-04")))
