import numpy as np
import pandas as pd
import duckdb

from src.database import engine


class Sector:
    @staticmethod
    def get_sector_construction():
        sector_info = pd.read_sql("select * from msci_usa_sector_info", engine)
        sector_weight = pd.read_sql("select * from msci_usa_sector_weight", engine)
        merge = duckdb.sql(
            """
    select a.sedol7, a.company, a.date, a.sector, b.weight
    from (
        select sedol7, company, date, sector
        from sector_info
        ) as a
        join (
            select sedol7, company, date, weight
            from sector_weight
        ) as b 
        on a.sedol7 = b.sedol7
            and a.date = b.date
    """
        ).df()

        sector_df = duckdb.sql(
            """
    select
        base.sedol7, base.company, base.date, base.sector,
        base.weight / new_weight_base.total_weight as weight
    from (
        select *
        from merge) as base
        left outer join (
        select
            date, sector, sum(weight) as total_weight
        from merge
        group by
            date,
            sector) as new_weight_base 
        on base.date = new_weight_base.date
            and base.sector = new_weight_base.sector
    """
        ).df()
        return sector_df

    @staticmethod
    def get_sector_signal(signal_df):
        """
        signal_df should have a column named signal
        """
        sector_df = Sector.get_sector_construction()
        sector_signal_df = duckdb.sql(
            """
select
    sector.sector,
    sector.date,
    avg(signal.signal) as simply_signal,
    -- incase sum(weight) is not equal to 1
    sum(signal.signal * coalesce(sector.weight, 0)) / sum(sector.weight) as weighted_signal, 
    sum(signal.signal * coalesce(sector.weight, 0)) as debug_signal 
from (
    select
        *
    from
        signal_df
    where 
        signal is not null) as signal
    left outer join (
    select
        *
    from
        sector_df) as sector 
    on signal.sedol7 = sector.sedol7
        and signal.date = sector.date
group by
    sector.sector,
    sector.date
    """
        ).df()
        return sector_signal_df
