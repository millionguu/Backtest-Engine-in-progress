import os
import pathlib
import pandas as pd
from sqlalchemy import create_engine


class Market:
    def __init__(self, securities):
        path = os.path.join(pathlib.Path(__file__).parent.parent, "data.db")
        engine = create_engine("sqlite:///" + path)
        self.securities = securities
        self.data = dict()
        for table in self.securities:
            self.data[table] = pd.read_sql(f"select * from {table}", engine)
            self.data[table]["date"] = pd.to_datetime(self.data[table]["date"]).dt.date

    def query_return(self, security, date):
        condition = self.data[security]["date"] == date
        if sum(condition) == 1:
            return self.data[security][condition]["return"].to_numpy()[0]
        else:
            # market close
            return 0


if __name__ == "__main__":
    from datetime import date

    market = Market(["IXIC"])
    print(market.query_return("IXIC", date.fromisoformat("2022-12-01")))
