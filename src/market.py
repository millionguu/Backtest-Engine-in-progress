import datetime
import time
import numpy as np
import pandas as pd
import polars as pl
import sqlalchemy
import yfinance

from src.database import engine


class Market:
    def __init__(self, securities):
        self.securities = securities
        self.data = dict()
        for security in self.securities:
            table = security if not security.startswith("^") else security[1:]
            if not sqlalchemy.inspect(engine).has_table(table):
                data = self.retrive_data_from_yfinance(security)
                data.to_sql(table, con=engine, chunksize=1000, index=False)
                time.sleep(3)
            self.data[table] = pl.read_database(
                f"select * from {table}", engine.connect()
            )

    def retrive_data_from_yfinance(self, security):
        data = yfinance.download(security, start="2000-01-01", end="2023-12-31")
        data["return"] = np.divide(
            data["Adj Close"] - data["Adj Close"].shift(1), data["Adj Close"].shift(1)
        )
        data.columns = data.columns.str.lower()
        data["date"] = pd.to_datetime(data.index).date
        return data

    def query_return(self, security, date):
        if isinstance(date, type(datetime.date.today())):
            date = date.strftime("%Y-%m-%d")
        table = security if not security.startswith("^") else security[1:]
        res = self.data[table].filter(pl.col("date") == date)
        if len(res) == 1:
            return res.get_column("return").item(0)
        else:
            # possibly market close
            print(f"not found return value for {security} at {date}.")
            return 0


if __name__ == "__main__":
    from datetime import date

    # index = "^SPX"
    # table = "SPX"
    # with engine.connect() as conn, conn.begin():
    #     conn.execute(text(f"drop table if exists {table};"))
    # market = Market([index])
    # print(market.retrive_data_from_yfinance(index).iloc[1])

    market = Market(["XLI"])
    print(market.query_return("XLI", "2023-01-04"))
