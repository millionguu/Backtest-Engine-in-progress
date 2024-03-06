from src.factor.base_factor import BaseFactor
from src.sector.sales_growth import SalesGrowthSector


class SalesGrowthFactor(BaseFactor):
    def __init__(self, security_universe, start_date, end_date, factor_type):
        super().__init__(security_universe, start_date, end_date, factor_type)

    def set_portfolio_at_start(self, portfolio):
        position = self.get_position(self.start_date)
        for security, weight in position:
            portfolio.add_security_weight(security, weight, 0)

    def get_fund_list(self, date):
        """
        1. get the sorted sector based on the signal
        2. sort the fund by sector order
        """
        sector_list = SalesGrowthSector("ntm").get_sector_list(date)
        fund_list = []
        for sector in sector_list:
            for security in self.security_universe:
                if security.sector == sector:
                    fund_list.append(security)
        return fund_list
