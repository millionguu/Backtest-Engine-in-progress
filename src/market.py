import datetime
from pathlib import Path
import time
import numpy as np
import pandas as pd
import polars as pl
import sqlalchemy
import yfinance

from src.database import engine
from src.security_symbol import SecurityTicker, SecurityLipper


class Market:
    def __init__(self, securities, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.securities = securities
        self.data = dict()
        if isinstance(securities[0], SecurityTicker):
            self.load_ticker_return_data()
        elif isinstance(securities[0], SecurityLipper):
            self.load_lipper_return_data()

    def load_ticker_return_data(self):
        for security in self.securities:
            table = str(security)
            path = Path("parquet/ticker/" + str(table))
            if not path.exists():
                data = self.retrive_data_from_yfinance(security)
                data = pl.from_pandas(data)
                data.write_parquet(path)
                time.sleep(1)
            self.data[security] = (
                pl.scan_parquet(path)
                .filter(pl.col("date") >= self.start_date)
                .filter(pl.col("date") <= self.end_date)
                .collect()
            )
            earliest_date = self.data[security].get_column("date").item(0)
            if earliest_date < self.start_date:
                print(
                    f"note that {security} doesn't have enough history data, earliest date: {earliest_date}"
                )

    def load_lipper_return_data(self):
        # TODO:maybe filter on lipper_id
        table = "us_sector_fund_return_lipperid"
        self.data = pl.read_database(
            query=f"select * from {table} where end_date >= :start_date and end_date <= :end_date",
            connection=engine,
            execute_options={
                "parameters": {
                    "start_date": self.start_date,
                    "end_date": self.end_date,
                }
            },
        )

    def retrive_data_from_yfinance(self, security):
        if security.sector == "index":
            ticker = str(security)[1:]
        else:
            ticker = str(security)
        data = yfinance.download(ticker, start="2000-01-01", end="2023-12-31")
        data["return"] = np.divide(
            data["Adj Close"] - data["Adj Close"].shift(1), data["Adj Close"].shift(1)
        )
        data.columns = data.columns.str.lower()
        data["date"] = pd.to_datetime(data.index).date
        return data

    def query_return(self, security, date):
        if isinstance(security, SecurityTicker):
            return self.query_ticker_return(security, date)
        elif isinstance(security, SecurityLipper):
            return self.query_lipper_return(security, date)

    def query_ticker_return(self, security, date):
        res = self.data[security].filter(pl.col("date") == date)
        if len(res) == 1:
            return res.get_column("return").item(0)
        else:
            # print(f"not found return value for {security} at {date}.")
            return 0

    def query_lipper_return(self, security, date):
        res = self.data.filter(pl.col("end_date") == date).filter(
            pl.col("lipper_id").cast(pl.String) == str(security)
        )
        if len(res) == 1:
            return res.get_column("return").item(0) * 0.01
        else:
            # print(f"not found return value for {security} at {date}.")
            return 0


if __name__ == "__main__":
    from datetime import date

    # index = "^SPX"
    # table = "SPX"
    # with engine.connect() as conn, conn.begin():
    #     conn.execute(text(f"drop table if exists {table};"))
    # market = Market([index])
    # print(market.retrive_data_from_yfinance(index).iloc[1])
    start_date = date.fromisoformat("2000-01-01")
    end_date = date.fromisoformat("2004-10-01")
    s = SecurityLipper("40056080")
    market = Market([s], start_date, end_date)
    print(market.query_return(s, "2002-11-29"))
