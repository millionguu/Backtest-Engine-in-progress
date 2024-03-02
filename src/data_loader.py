from datetime import datetime
import pandas as pd
import polars as pl
from sqlalchemy import text
import yfinance
from src.database import engine


def write_sector_weight(engine):
    filename = "Weight_MSCI USA_20001229_20231130.xlsx"
    table = "msci_usa_sector_weight"
    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"drop table if exists {table};"))
    data = pd.read_excel("data/" + filename)
    data = data.rename(columns={"Unnamed: 1": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"], var_name="date", value_name="weight"
    )
    data["date"] = pd.to_datetime(data["date"].apply(lambda x: str(x))).dt.date
    data.to_sql(table, con=engine, chunksize=1000, index=False)


def write_sector_info(engine):
    filename = "GICS_MSCI USA_20001229_20231130.xlsx"
    table = "msci_usa_sector_info"
    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"drop table if exists {table};"))
    data = pd.read_excel("data/" + filename)
    data = data.rename(columns={"Unnamed: 1": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"], var_name="date", value_name="sector"
    )
    data["date"] = pd.to_datetime(data["date"]).dt.date
    data.to_sql(table, con=engine, chunksize=1000, index=False)


def write_sales_growth_data(engine):
    file_names = [
        "Sales Gth FY1_MSCI USA_20001231-20231130.xlsx",
        "Sales Gth NTM_MSCI USA_20001231-20231130.xlsx",
        "Sales Gth TTM_MSCI USA_20001231-20231130.xlsx",
    ]
    tables = [
        "msci_usa_sales_growth_fy1",
        "msci_usa_sales_growth_ntm",
        "msci_usa_sales_growth_ttm",
    ]
    with engine.connect() as conn, conn.begin():
        for table in tables:
            conn.execute(text(f"drop table if exists {table};"))
    for file_name, table in zip(file_names, tables):
        data = pd.read_excel(f"data/{file_name}", index_col=False)
        data = data.rename(columns={"Unnamed: 1": "company", "SEDOL7": "sedol7"})
        convert_columns = [c for c in data.columns if c not in ["sedol7", "company"]]
        for c in convert_columns:
            data[c] = pd.to_numeric(data[c], errors="coerce")
        data = data.melt(
            id_vars=["sedol7", "company"], var_name="date", value_name="growth"
        )
        data["date"] = pd.to_datetime(data["date"]).dt.date
        data.to_sql(table, con=engine, chunksize=1000, index=False)


def write_market_open_date(engine):
    table = "us_market_open_date"
    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"drop table if exists {table};"))
    today = datetime.today().strftime("%Y-%m-%d")
    data = yfinance.download("^DJI", start="2000-01-01", end=today)
    data["date"] = pd.to_datetime(data.index).date
    data["date"].to_sql(table, con=engine, chunksize=1000, index=False)


def write_lipperid_return_data(engine):
    table = "us_sector_fund_return_lipperid"
    data = pl.read_excel("data/US Sector Fund and Benchmark Return TS.xlsx")
    data = (
        data.with_columns(
            pl.col("StartDate").str.to_date("%m-%d-%y"),
            pl.col("EndDate").str.to_date("%m-%d-%y"),
        )
        .rename(
            {
                "LipperID": "lipper_id",
                "StartDate": "start_date",
                "EndDate": "end_date",
                "Value": "return",
            }
        )
        .select(["lipper_id", "start_date", "end_date", "return"])
    )
    data.write_database(table, str(engine.url))


def write_sedol_ticker_mapping(engine):
    table = "sedol_ticker_mapping"
    data = pl.read_csv("data/SEDOL Tickers.csv")
    data.with_columns(
        pl.coalesce(pl.col("Ticker"), pl.col("Previous ticker")).alias("ticker")
    ).select(
        pl.col("SEDOL7").alias("sedol7"), pl.col("ticker"), pl.col("Name").alias("name")
    )

    data.write_database(table, str(engine.url))


def write_eps_data(engine):
    file_names = [
        "Reported EPS Qtr.xlsx",
        "Reported EPS Ann.xlsx",
    ]
    tables = [
        "msci_usa_eps_quarterly",
        "msci_usa_eps_annually",
    ]
    with engine.connect() as conn, conn.begin():
        for table in tables:
            conn.execute(text(f"drop table if exists {table};"))
    for file_name, table in zip(file_names, tables):
        data = pl.read_excel(f"data/{file_name}")
        data = data.rename({"": "company", "SEDOL7": "sedol7"})
        data = data.melt(
            id_vars=["sedol7", "company"], variable_name="date", value_name="eps"
        )
        data.with_columns(
            pl.col("date").str.to_date("%Y%m%d"), pl.col("eps").cast(pl.Float32)
        )
        data.write_database(table, str(engine.url))


def write_price_data(engine):
    file_name = "Price_MSCI USA_20001231-20231130.xlsx"
    table = "msci_usa_price_daily"

    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"drop table if exists {table};"))
    data = pl.read_excel(f"data/{file_name}")
    data = data.rename({"": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"], variable_name="date", value_name="price"
    )
    data.with_columns(
        pl.col("date").str.to_date("%Y%m%d"),
        pl.col("price").cast(pl.Float32, strict=False),
    )
    data.write_database(table, str(engine.url))


def write_cpi_data(engine):
    table = "us_cpi_mom"
    data = pl.read_csv("data/CPI Data.csv")
    data = data.rename(
        {
            "Mnemonic": "date",
            "US.CPIALL": "us_cpiall",
            "US.CPICORE": "us_cpicore",
            "BLSSUUR0000SA0": "blssuur",
            "CNPR7096764": "cnpr",
            "CN.CPICORE": "cn_cpicore",
        }
    ).select("date", "us_cpiall", "us_cpicore", "blssuur", "cnpr", "cn_cpicore")

    data = data.with_columns(
        pl.col("date").str.to_date("%Y/%m/%d"),
        pl.col("us_cpiall").cast(pl.Float32, strict=False),
        pl.col("us_cpicore").cast(pl.Float32, strict=False),
        pl.col("blssuur").cast(pl.Float32, strict=False),
        pl.col("cnpr").cast(pl.Float32, strict=False),
        pl.col("cn_cpicore").cast(pl.Float32, strict=False),
    ).sort(pl.col("date"))

    cpi_mom = (
        data.select(pl.all().exclude("date")).to_numpy()
        - data.select(pl.all().exclude("date")).shift(1).to_numpy()
    ) / data.select(pl.all().exclude("date")).shift(1).to_numpy()

    cpi_mom = pl.from_numpy(
        cpi_mom, schema=data.select(pl.all().exclude("date")).schema
    )
    cpi_mom = pl.concat([data.select(pl.col("date")), cpi_mom], how="horizontal")
    cpi_mom.write_database(table, str(engine.url))


if __name__ == "__main__":
    write_price_data(engine)
