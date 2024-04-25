import time
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import yfinance

from src.security_symbol import SecurityLipper, SecuritySedol, SecurityTicker


class Market:
    def __init__(self, securities, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.securities = securities
        self.data = dict()
        if isinstance(securities[0], SecurityTicker):
            return self.load_ticker_return_data()
        if isinstance(securities[0], SecurityLipper):
            return self.load_lipper_return_data()
        if isinstance(securities[0], SecuritySedol):
            return self.load_sedol_return_data()
        raise ValueError("secirity type not supported")

    def load_ticker_return_data(self):
        for security in self.securities:
            path = Path(f"parquet/ticker/{str(security)}.parquet")
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
        # TODO: maybe filter on lipper_id
        self.data = (
            pl.scan_parquet("parquet/fund_return/us_fund_daily_return_lipperid.parquet")
            .filter(pl.col("end_date") >= self.start_date)
            .filter(pl.col("end_date") <= self.end_date)
            .collect()
        )

    def load_sedol_return_data(self):
        # TODO: maybe filter on lipper_id
        self.data = (
            pl.scan_parquet(
                "parquet/fund_return/us_security_sedol_return_daily.parquet"
            )
            .filter(pl.col("date") >= self.start_date)
            .filter(pl.col("date") <= self.end_date)
            .collect()
        )

    def retrive_data_from_yfinance(self, security):
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
        elif isinstance(security, SecuritySedol):
            return self.query_sedol_return(security, date)
        else:
            raise ValueError(f"unexpected type: {security}")

    def query_range_return(self, security, start_date, end_date):
        if isinstance(security, SecurityTicker):
            return self.query_ticker_range_return(security, start_date, end_date)
        elif isinstance(security, SecurityLipper):
            return self.query_lipper_range_return(security, start_date, end_date)
        elif isinstance(security, SecuritySedol):
            return self.query_sedol_range_return(security, start_date, end_date)
        else:
            raise ValueError(f"unexpected type: {security}")

    def query_ticker_return(self, security, date):
        res = (
            self.data[security]
            .filter(pl.col("date") == date)
            .filter(pl.col("return").is_not_null())
        )
        if len(res) == 1:
            return res.get_column("return").item(0)
        else:
            # print(f"not found return value for {security} at {date}.")
            return 0

    def query_lipper_return(self, security, date):
        res = (
            self.data.filter(pl.col("end_date") == date)
            .filter(pl.col("lipper_id") == int(security.lipper_id))
            .filter(pl.col("return").is_not_null())
        )
        if len(res) == 1:
            return res.get_column("return").item(0) * 0.01
        else:
            # print(f"not found return value for {security} at {date}.")
            return 0

    def query_sedol_return(self, security, date):
        res = (
            self.data.filter(pl.col("sedol7") == security.sedol_id)
            .filter(pl.col("date") == date)
            .filter(pl.col("return").is_not_null())
        )
        if len(res) == 1 and abs(res.get_column("return").item(0)) < 0.5:
            return res.get_column("return").item(0)
        else:
            # print(f"not found return value for {security} at {date}.")
            return 0

    def query_ticker_range_return(self, security, start_date, end_date):
        res = (
            self.data[security]
            .filter(pl.col("date") >= start_date)
            .filter(pl.col("date") <= end_date)
            .filter(pl.col("return").is_not_null())
            .sort(pl.col("date"))
            .get_column("adj close")
        )
        range_return = res.item(-1) / res.item(0) - 1
        if len(res) > 1 and range_return < 1:
            return range_return
        else:
            return 0

    def query_sedol_range_return(self, security, start_date, end_date):
        res = (
            self.data.filter(pl.col("sedol7") == security.sedol_id)
            .filter(pl.col("date") >= start_date)
            .filter(pl.col("date") <= end_date)
            .filter(pl.col("return").is_not_null())
            .group_by("sedol7")
            .agg(pl.col("return").sum().alias("return"))
        )
        if len(res) == 1 and abs(res.get_column("return").item(0)) < 5:
            return res.get_column("return").item(0)
        else:
            return 0

    def query_lipper_range_return(self, security, start_date, end_date):
        raise NotImplementedError("no implementation for lipper id")


if __name__ == "__main__":
    from datetime import date

    start_date = date.fromisoformat("2010-01-01")
    end_date = date.fromisoformat("2014-10-01")
    s = SecurityLipper("40000039")
    market = Market([s], start_date, end_date)
    print(market.query_return(s, date.fromisoformat("2012-04-27")))
