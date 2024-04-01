from src.factor.base_factor import BaseFactor
from src.sector.dividend_yield import DividendYieldSector


class DividendYieldFactor(BaseFactor):
    def __init__(self, security_universe, factor_type):
        super().__init__(security_universe, factor_type)

    def get_fund_list(self, date):
        """
        1. get the sorted sector based on the signal
        2. sort the fund by sector order
        """
        sector_list = list(DividendYieldSector("ntm").get_sector_list(date))
        fund_list = []
        for sector in sector_list:
            for security in self.security_universe:
                if security.sector == sector:
                    fund_list.append(security)
        return fund_list
