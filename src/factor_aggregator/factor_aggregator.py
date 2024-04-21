from abc import abstractmethod

import polars as pl

from src.factor.base_factor import BaseFactor


class FactorAggregator(BaseFactor):
    def __init__(self, security_universe, factor_type):
        super().__init__(security_universe, factor_type)
        self.sectors = self.get_internal_sectors()

    @abstractmethod
    def get_internal_sectors(self):
        raise NotImplementedError()

    @abstractmethod
    def get_fund_list(self, date):
        raise NotImplementedError()

    def get_sector_scores(self, observe_date, normal_signal, reversed_signal):
        sector_score_list = []
        for sector in self.sectors:
            sector_score_df = sector.impl_sector_signal(observe_date)
            sector_score_df = sector_score_df.with_columns(
                pl.lit(sector.__class__.__name__).alias("class_name")
            ).select(
                pl.col("sector"),
                pl.col("date"),
                pl.col("z-score"),
                pl.col("class_name"),
            )
            sector_score_list.append(sector_score_df)
        original_sector_score_df = pl.concat(sector_score_list)

        # normallize score across factor class
        sector_score_df = original_sector_score_df.with_columns(
            pl.when(pl.col("class_name").is_in(normal_signal))
            .then(pl.col("z-score"))
            .when(pl.col("class_name").is_in(reversed_signal))
            .then(-pl.col("z-score"))
            .otherwise(None)
        )

        # normalize for different class
        stat_df = sector_score_df.group_by("class_name").agg(
            pl.col("z-score").mean().alias("mean"),
            pl.col("z-score").std().alias("std"),
        )
        sector_score_df = sector_score_df.join(
            stat_df, on="class_name", how="inner"
        ).with_columns(
            ((pl.col("z-score") - pl.col("mean")) / pl.col("std")).alias("z-score")
        )
        return sector_score_df
