from dataclasses import dataclass
from sector.base_sector import BaseSector
from src.factor.base_factor import BaseFactor


@dataclass
class SectorContext:
    sector: BaseSector
    rebalance_period: int
    rebalance_interval: str


class FactorAggregator(BaseFactor):
    def __init__(self, sector_contexts, security_universe, factor_type):
        self.sector_contexts = sector_contexts
        self.sectors = [sector_context.sector() for sector_context in sector_contexts]
        self.security_universe = security_universe
        self.factor_type = factor_type

    def get_aggregate_factor_scores(self):
        pass

    def get_fund_list(self, date):
        pass
