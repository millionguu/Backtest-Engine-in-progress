from src.factor.base_factor import BaseFactor
from src.sector.sales_growth import SalesGrowthSector
from src.fund_universe import SECTOR_ETF


class SalesGrowthFactor(BaseFactor):
    def __init__(self, security_universe, start_date, end_date, factor_type):
        super().__init__(security_universe, start_date, end_date, factor_type)

    def set_portfolio_at_start(self, portfolio):
        position = self.get_position(self.start_date)
        for security, weight in position:
            portfolio.add_security_weight(security, weight, 0)

    def get_security_list(self, date):
        """
        1. get the sorted sector based on the signal
        2. sort the fund by sector order
        """
        sector_list = SalesGrowthSector("ntm").get_sector_list(date)
        etf_list = []
        for sector in sector_list:
            for security in self.security_universe:
                if security.sector == sector:
                    etf_list.append(security)
        return etf_list


if __name__ == "__main__":
    from datetime import date

    start_date = date.fromisoformat("2022-01-01")
    end_date = date.fromisoformat("2022-02-15")
    factor = SalesGrowthFactor(SECTOR_ETF, start_date, end_date, "long")
    df = factor.build_single_month_sector_factor(end_date)
