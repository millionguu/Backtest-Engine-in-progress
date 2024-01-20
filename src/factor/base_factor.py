from abc import ABC, abstractmethod
from datetime import date


class Factor(ABC):
    def __init__(self, security_universe, start_date, end_date):
        self.security_universe = security_universe
        self.start_date = start_date
        self.end_date = end_date

    @abstractmethod
    def get_position(self, date):
        pass

    @abstractmethod
    def set_portfolio_at_start(self, portfolio):
        pass

    def get_first_quintile(self, security_list):
        length = len(security_list)
        return security_list[: length // 5]

    def get_last_quintile(self, security_list):
        length = len(security_list)
        return security_list[length // 5 * 4 :]


class DummyFactor(Factor):
    def __init__(self, security_universe, start_date, end_date):
        super().__init__(security_universe, start_date, end_date)

    def get_position(self, date):
        weight = 1 / len(self.security_universe)
        weight = round(weight, 2)
        return [(security, weight) for security in self.security_universe]

    def set_portfolio_at_start(self, portfolio):
        for security, weight in self.get_position(portfolio.start_date):
            portfolio.add_security_weight(security, weight, portfolio.start_date)
