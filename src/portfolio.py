from collections import defaultdict
import numpy as np
import pandas as pd

from src.data_loader import get_market_open_date
from src.database import engine


class Portfolio:
    def __init__(self, initial_cash, start_date, end_date):
        self.date_df = get_market_open_date(engine, start_date, end_date)
        self.start_date = self.date_df[0]
        self.end_date = self.date_df[-1]

        self.security_book = defaultdict(self.empty_security_book)
        self.value_book = pd.DataFrame.from_dict(
            {
                "date": np.copy(self.date_df),
                "cash": initial_cash,
                "value": initial_cash,
            }
        )

    def empty_security_book(self):
        return pd.DataFrame.from_dict(
            {
                "date": np.copy(self.date_df),
                "value": 0.0,
                "weight": 0.0,
            }
        )

    def hold_securities(self):
        return self.security_book.keys()

    def get_next_market_date(self, cur_date):
        idx = np.argmax(self.date_df == cur_date)
        return self.date_df[idx + 1]

    def add_security_weight(self, security, add_weight, date):
        add_value = self.get_total_value(date) * add_weight
        if self.get_remain_cash(date) < add_value:
            raise ValueError("not enough cash to add")
        else:
            condition = self.value_book["date"] >= date
            self.value_book.loc[condition, "cash"] -= add_value

            condition = self.security_book[security]["date"] >= date
            self.security_book[security].loc[condition, "value"] += add_value
            self.security_book[security].loc[condition, "weight"] += add_weight

    def reduce_security_weight(self, security, reduce_weight, date):
        reduce_value = self.get_total_value(date) * reduce_weight
        if (
            security not in self.hold_securities()
            or reduce_value > self.get_security_value(security, date)
        ):
            raise ValueError("not enough value to reduce")
        else:
            condition = self.security_book[security]["date"] >= date
            self.security_book[security].loc[condition, "value"] -= reduce_value
            self.security_book[security].loc[condition, "weight"] -= reduce_weight

            condition = self.value_book["date"] >= date
            self.value_book.loc[condition, "cash"] += reduce_value

    def get_security_value(self, security, date):
        condition = self.security_book[security]["date"] == date
        return self.security_book[security][condition]["value"].to_numpy()[0]

    def get_remain_cash(self, date):
        condition = self.value_book["date"] == date
        return self.value_book[condition]["cash"].to_numpy()[0]

    def get_total_value(self, date):
        condition = self.value_book["date"] == date
        return self.value_book[condition]["value"].to_numpy()[0]

    def update_security_value(self, security, date, daily_return):
        """update security value"""
        condition = self.security_book[security]["date"] >= date
        self.security_book[security].loc[condition, "value"] *= 1 + daily_return

    def update_portfolio(self, date):
        """update security weight based on security value"""
        total_value = self.get_remain_cash(date)
        for security in self.hold_securities():
            total_value += self.get_security_value(security, date)
        condition = self.value_book["date"] >= date
        self.value_book.loc[condition, "value"] = total_value

        condition = self.security_book[security]["date"] >= date
        for security in self.hold_securities():
            self.security_book[security].loc[condition, "weight"] = np.divide(
                self.security_book[security][condition]["value"], total_value
            )
