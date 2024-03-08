from abc import ABC, abstractmethod


class BaseFactor(ABC):
    def __init__(self, security_universe, factor_type):
        self.security_universe = security_universe
        self.factor_type = factor_type
        # hyperparameter, always return 3 funds in the sector rotation
        self.num = 3

    def set_portfolio_at_start(self, portfolio):
        position = self.get_position(portfolio.start_date)
        for security, weight in position:
            portfolio.add_security_weight(security, weight, 0)

    def get_position(self, date):
        security_list = self.get_fund_list(date)
        if self.factor_type == "long":
            target_security = security_list[: self.num]
        elif self.factor_type == "short":
            target_security = list(reversed(security_list))[: self.num]
        else:
            raise ValueError(f"no implementation for {self.factor_type}")
        weight = 1 / len(target_security)
        return [(s, weight) for s in target_security]

    @abstractmethod
    def get_fund_list(self, date):
        pass


class DummyFactor(BaseFactor):
    def __init__(self, security_universe, start_date, end_date):
        super().__init__(security_universe, start_date, end_date)

    def get_fund_list(self, date):
        return self.security_universe
