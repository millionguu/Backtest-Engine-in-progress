from collections import defaultdict

import numpy as np
import polars as pl


class Portfolio:
    def __init__(self, initial_cash, start_date, end_date):
        self.date_df = self.get_market_open_date(start_date, end_date)
        self.start_date = self.date_df.item(0, 0)
        self.end_date = self.date_df.item(-1, 0)
        self.iter_index = 0

        self.security_book = defaultdict(self.empty_security_book)
        num = len(self.date_df)
        self.value_book = (
            pl.DataFrame(
                {
                    "date": self.date_df.to_series(),
                    "cash": pl.repeat(initial_cash, n=num, eager=True),
                    "value": pl.repeat(initial_cash, n=num, eager=True),
                    "turnover": pl.repeat(0, n=num, eager=True),
                    "sector": pl.repeat("", n=num, eager=True),
                }
            )
            .with_row_index()
            .to_dicts()
        )

    def get_market_open_date(self, start_date, end_date):
        df = (
            pl.scan_parquet("parquet/base/us_market_open_date.parquet")
            .filter(pl.col("date") >= start_date)
            .filter(pl.col("date") <= end_date)
            .collect()
        )
        return df

    def empty_security_book(self):
        return (
            pl.DataFrame(
                {
                    "date": self.date_df.to_series(),
                    "weight": 0.0,
                    "value": 0.0,
                }
            )
            .with_row_index()
            .to_dicts()
        )

    def hold_securities(self, iter_index):
        res = []
        for security in list(self.security_book.keys()):
            value = self.get_security_value(security, iter_index)
            if value > 0:
                res.append(security)
        return res

    def update_security_value(self, security, iter_index, daily_return):
        """
        1. update security value based on daily_return,
        security weight should be updated based on the value book
        """
        self.security_book[security][iter_index]["value"] = self.get_security_value(
            security, iter_index - 1
        ) * (1 + daily_return)

    def update_portfolio(self, iter_index):
        """
        2. update value book and security weight based on new security value
        """
        self.value_book[iter_index]["cash"] = self.get_remain_cash(iter_index - 1)
        total_value = self.get_remain_cash(iter_index)
        for security in self.hold_securities(iter_index):
            total_value += self.get_security_value(security, iter_index)
        self.value_book[iter_index]["value"] = total_value

        for security in self.hold_securities(iter_index):
            self.security_book[security][iter_index]["weight"] = np.divide(
                self.get_security_value(security, iter_index),
                self.get_total_value(iter_index),
            )

    def reduce_security_weight(self, security, reduce_weight, iter_index):
        """
        3. happens at the close price of the day, after daily return updated
        sold before buy
        won't change total value
        """
        self.security_book[security][iter_index]["weight"] = (
            self.get_security_weight(security, iter_index) - reduce_weight
        )
        if self.get_security_weight(security, iter_index) < 0:
            raise ValueError("not enough value to reduce")

        reduce_value = reduce_weight * self.get_total_value(iter_index)
        self.security_book[security][iter_index]["value"] = (
            self.get_security_value(security, iter_index) - reduce_value
        )
        self.value_book[iter_index]["cash"] = (
            self.get_remain_cash(iter_index) + reduce_value
        )

    def add_security_weight(self, security, add_weight, iter_index):
        """
        4. happens at the close price of the day, after daily return updated
        sold before buy
        won't change total value
        """
        add_value = self.get_total_value(iter_index) * add_weight
        self.value_book[iter_index]["cash"] = (
            self.get_remain_cash(iter_index) - add_value
        )
        if self.get_remain_cash(iter_index) < 0:
            raise ValueError("not enough cash to add")

        self.security_book[security][iter_index]["weight"] = (
            self.get_security_weight(security, iter_index) + add_weight
        )
        self.security_book[security][iter_index]["value"] = (
            self.get_security_value(security, iter_index) + add_value
        )

    def finish(self):
        self.value_book = pl.DataFrame(self.value_book)
        for security, book in self.security_book.items():
            self.security_book[security] = pl.DataFrame(book)

    def get_security_weight(self, security, iter_index):
        return self.security_book[security][iter_index]["weight"]

    def get_security_value(self, security, iter_index):
        return self.security_book[security][iter_index]["value"]

    def get_remain_cash(self, iter_index):
        return self.value_book[iter_index]["cash"]

    def get_total_value(self, iter_index):
        return self.value_book[iter_index]["value"]

    def print_snapshot(self, iter_index):
        total_value = self.get_total_value(iter_index)
        res = []
        for security in self.hold_securities(iter_index):
            value = self.get_security_value(security, iter_index)
            res.append(": ".join((security, value)))
        print(f"total value: {total_value}")
        print(". ".join(res))
