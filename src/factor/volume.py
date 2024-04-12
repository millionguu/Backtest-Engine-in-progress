from src.factor.base_factor import BaseFactor
from src.sector.volume import VolumeSector


class VolumeFactor(BaseFactor):
    def __init__(self, security_universe, factor_type):
        super().__init__(security_universe, factor_type)

    def get_fund_list(self, date):
        sector_list = list(VolumeSector().get_sector_list(date))
        # reversed signal
        sector_list = list(reversed(sector_list))
        fund_list = []
        for sector in sector_list:
            for security in self.security_universe:
                if security.sector == sector:
                    fund_list.append(security)
        return fund_list
