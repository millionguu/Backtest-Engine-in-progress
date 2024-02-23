from abc import ABC, abstractmethod


class BaseFactor(ABC):
    def __init__(self, security_universe, start_date, end_date, factor_type):
        self.security_universe = security_universe
        self.start_date = start_date
        self.end_date = end_date
        self.factor_type = factor_type

    def get_position(self, date):
        security_list = self.get_security_list(date)
        target_security = self.get_target_security(security_list)
        weight = 1 / len(target_security)
        return [(s, weight) for s in target_security]

    @abstractmethod
    def get_security_list(self, date):
        pass

    @abstractmethod
    def set_portfolio_at_start(self, portfolio):
        pass

    def get_target_security(self, security_list, num=3):
        if self.factor_type == "long":
            return security_list[:num]
        else:
            return list(reversed(security_list))[:num]


class DummyFactor(BaseFactor):
    def __init__(self, security_universe, start_date, end_date):
        super().__init__(security_universe, start_date, end_date)

    def get_security_list(self, date):
        return self.security_universe

    def set_portfolio_at_start(self, portfolio, position):
        for security, weight in self.get_long_position(portfolio.start_date):
            portfolio.add_security_weight(security, weight, portfolio.start_date)
