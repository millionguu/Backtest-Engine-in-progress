from abc import ABC, abstractmethod


class BaseFactor(ABC):
    def __init__(self, security_universe, start_date, end_date, factor_type):
        self.security_universe = security_universe
        self.start_date = start_date
        self.end_date = end_date
        self.factor_type = factor_type

    def get_position(self, date):
        # hyperparameter, always return 3 funds in the fund selection
        num = 3
        security_list = self.get_security_list(date)
        if self.factor_type == "long":
            target_security = security_list[:num]
        else:
            target_security = list(reversed(security_list))[:num]
        weight = 1 / len(target_security)
        return [(s, weight) for s in target_security]

    @abstractmethod
    def get_security_list(self, date):
        pass

    @abstractmethod
    def set_portfolio_at_start(self, portfolio):
        pass


class DummyFactor(BaseFactor):
    def __init__(self, security_universe, start_date, end_date):
        super().__init__(security_universe, start_date, end_date)

    def get_security_list(self, date):
        return self.security_universe

    def set_portfolio_at_start(self, portfolio, position):
        for security, weight in self.get_long_position(portfolio.start_date):
            portfolio.add_security_weight(security, weight, portfolio.start_date)
