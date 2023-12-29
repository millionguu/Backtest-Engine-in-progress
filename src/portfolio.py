from collections import defaultdict
import os
import pathlib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from data_loader import get_market_open_date


class Portfolio:
    def __init__(self, initial_cash, start_date, end_date):
        self.cash = initial_cash
        self.total_value = initial_cash
        self.security_book = defaultdict(self.empty_security_book)

        path = os.path.join(pathlib.Path(__file__).parent.parent, "data.db")
        engine = create_engine("sqlite:///" + path)
        self.date_df = get_market_open_date(
            engine, "us_market_date", start_date, end_date
        )
        self.start_date = self.date_df[0]
        self.end_date = self.date_df[-1]

    def empty_security_book(self):
        book = pd.DataFrame.from_dict(
            {
                "date": np.copy(self.date_df),
                "value": 0.0,
                "weight": 0.0,
            }
        )
        return book

    def hold_securities(self):
        return self.security_book.keys()

    def get_next_market_date(self, cur_date):
        idx = np.argmax(self.date_df == cur_date)
        return self.date_df[idx + 1]

    def add_security_weight(self, security, add_weight, date):
        add_value = self.total_value * add_weight
        if self.cash < add_value:
            raise ValueError("not enough cash to add")
        else:
            self.cash -= add_value

            condition = self.security_book[security]["date"] >= date
            self.security_book[security].loc[condition, "value"] += add_value
            self.security_book[security].loc[condition, "weight"] += add_weight

    def reduce_security_weight(self, security, reduce_weight, date):
        reduce_value = self.total_value * reduce_weight
        condition = self.security_book[security]["date"] >= date
        if (
            security not in self.hold_securities()
            or reduce_value > self.get_security_value(security, date)
        ):
            raise ValueError("not enough value to reduce")
        else:
            self.security_book[security].loc[condition, "value"] -= reduce_value
            self.security_book[security].loc[condition, "weight"] -= reduce_weight

    def get_security_value(self, security, date):
        condition = self.security_book[security]["date"] == date
        return self.security_book[security][condition]["value"].to_numpy()[0]

    def update_security_value(self, security, date, daily_return):
        condition = self.security_book[security]["date"] >= date
        self.security_book[security].loc[condition, "value"] *= 1 + daily_return

    def update_portfolio(self, date):
        total_value = self.cash
        for security in self.hold_securities():
            total_value += self.get_security_value(security, date)
        self.total_value = total_value

        condition = self.security_book[security]["date"] >= date
        for security in self.hold_securities():
            self.security_book[security].loc[condition, "weight"] = np.divide(
                self.security_book[security][condition]["value"], self.total_value
            )
