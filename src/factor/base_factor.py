from abc import ABC, abstractmethod
from datetime import timedelta


class BaseFactor(ABC):
    def __init__(self, security_universe, start_date, end_date, factor_type="long"):
        self.security_universe = security_universe
        self.start_date = start_date
        self.end_date = end_date
        self.factor_type = factor_type

    def get_position(self, date):
        if self.factor_type == "long":
            return self.get_long_position(date)
        elif self.factor_type == "short":
            return self.get_short_position(date)
        else:
            return None

    def get_long_position(self, date):
        security_list = self.get_security_list(date)
        first_quintile = self.get_quintile(security_list)
        weight = 1 / len(first_quintile)
        return [(s, weight) for s in first_quintile]

    def get_short_position(self, date):
        security_list = self.get_security_list(date)
        last_quintile = self.get_quintile(security_list, ordinal=4)
        weight = 1 / len(last_quintile)
        return [(s, weight) for s in last_quintile]

    @abstractmethod
    def get_security_list(self, date):
        pass

    @abstractmethod
    def set_portfolio_at_start(self, portfolio):
        pass

    @staticmethod
    def get_quintile(security_list, ordinal=1, base=5):
        gran = len(security_list) // base
        return security_list[(ordinal - 1) * gran : ordinal * gran]

    @staticmethod
    def get_closest_month_end(date):
        month_end = (date + timedelta(days=1)).replace(day=1) - timedelta(days=1)
        month_end = month_end.strftime("%Y-%m-%d")
        return month_end


class DummyFactor(BaseFactor):
    def __init__(self, security_universe, start_date, end_date):
        super().__init__(security_universe, start_date, end_date)

    def get_security_list(self, date):
        return self.security_universe

    def set_portfolio_at_start(self, portfolio, position):
        for security, weight in self.get_long_position(portfolio.start_date):
            portfolio.add_security_weight(security, weight, portfolio.start_date)
