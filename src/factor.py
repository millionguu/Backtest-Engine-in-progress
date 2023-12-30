class Factor:
    def __init__(self):
        pass

    def get_position(self):
        pass

    def set_portfolio(self, portfolio):
        pass


class DummyFactor(Factor):
    def __init__(self):
        super().__init__()

    def get_position(self):
        return [("SPX", 0.1), ("IXIC", 0.8), ("RUT", 0.1)]

    def set_portfolio(self, portfolio):
        for security, weight in self.get_position():
            portfolio.add_security_weight(security, weight, portfolio.start_date)
