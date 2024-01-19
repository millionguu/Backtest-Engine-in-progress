from abc import ABC, abstractmethod


class Factor(ABC):
    @abstractmethod
    def __init__(self, security_universe):
        pass

    @abstractmethod
    def get_position(self):
        pass

    @abstractmethod
    def set_portfolio(self, portfolio):
        pass


class DummyFactor(Factor):
    def __init__(self, security_universe):
        self.security_universe = security_universe

    def get_position(self):
        weight = 1 / len(self.security_universe)
        weight = round(weight, 2)
        return [(security, weight) for security in self.security_universe]

    def set_portfolio(self, portfolio):
        for security, weight in self.get_position():
            portfolio.add_security_weight(security, weight, portfolio.start_date)
