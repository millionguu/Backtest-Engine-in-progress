from abc import ABC, abstractmethod


class Factor(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_position(self):
        pass

    @abstractmethod
    def set_portfolio(self, portfolio):
        pass


class DummyFactor(Factor):
    def __init__(self):
        pass

    def get_position(self):
        weight = 0.9 / 3
        return [("SPX", weight), ("IXIC", weight), ("RUT", weight)]

    def set_portfolio(self, portfolio):
        for security, weight in self.get_position():
            portfolio.add_security_weight(security, weight, portfolio.start_date)
