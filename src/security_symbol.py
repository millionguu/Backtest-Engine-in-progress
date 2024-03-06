class SecuritySymbol:
    pass


class SecurityTicker(SecuritySymbol):
    def __init__(self, ticker, sector=None):
        self.ticker = str(ticker)
        self.sector = sector

    def __str__(self) -> str:
        return self.ticker

    def display(self):
        if self.sector is not None:
            return f"{self.sector}({self.ticker})"
        return self.ticker

    def __hash__(self) -> int:
        return self.ticker.__hash__()

    def __eq__(self, __value: object) -> bool:
        return self.ticker.__eq__(__value.ticker)


class SecurityLipper(SecuritySymbol):
    def __init__(self, lipper_id, sector=None):
        self.lipper_id = str(lipper_id)
        self.sector = sector

    def __str__(self) -> str:
        return self.lipper_id

    def display(self):
        if self.sector is not None:
            return f"{self.sector}({self.lipper_id})"
        return self.lipper_id

    def __hash__(self) -> int:
        return self.lipper_id.__hash__()

    def __eq__(self, __value: object) -> bool:
        return self.lipper_id.__eq__(__value.lipper_id)


class SecuritySedol(SecuritySymbol):
    def __init__(self, sedol_id, sector=None):
        self.sedol_id = str(sedol_id)
        self.sector = sector

    def __str__(self) -> str:
        return self.sedol_id

    def display(self):
        if self.sector is not None:
            return f"{self.sector}({self.sedol_id})"
        return self.sedol_id

    def __hash__(self) -> int:
        return self.sedol_id.__hash__()

    def __eq__(self, __value: object) -> bool:
        return self.sedol_id.__eq__(__value.sedol_id)
