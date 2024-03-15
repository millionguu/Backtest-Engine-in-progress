from datetime import datetime, timedelta
import polars as pl
import yfinance


def write_sector_weight():
    filename = "Weight_MSCI USA_20001229_20231130.xlsx"
    table = "us_sector_weight"
    data = pl.read_excel("data/" + filename)
    data = data.rename({"": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"], variable_name="date", value_name="weight"
    )
    data = data.with_columns(
        pl.col("date").str.split(".").list.get(0).str.to_date("%Y%m%d"),
        pl.col("weight").cast(pl.Float32),
    )
    data = data.sort("sedol7", "date")
    data.write_parquet(f"parquet/base/{table}.parquet")


def write_sector_info():
    filename = "GICS_MSCI USA_20001229_20231130.xlsx"
    table = "us_sector_info"
    data = pl.read_excel("data/" + filename)
    data = data.rename({"": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"], variable_name="date", value_name="sector"
    )
    data = data.with_columns(
        pl.col("date").str.split(".").list.get(0).str.to_date("%Y%m%d"),
    )
    data = data.sort("sedol7", "date")
    data.write_parquet(f"parquet/base/{table}.parquet")


def write_sales_growth_data():
    file_names = [
        "Sales Gth FY1_MSCI USA_20001231-20231130.xlsx",
        "Sales Gth NTM_MSCI USA_20001231-20231130.xlsx",
        "Sales Gth TTM_MSCI USA_20001231-20231130.xlsx",
    ]
    tables = [
        "us_sales_growth_fy1",
        "us_sales_growth_ntm",
        "us_sales_growth_ttm",
    ]
    for file_name, table in zip(file_names, tables):
        data = pl.read_excel(f"data/{file_name}")
        data = data.rename({"": "company", "SEDOL7": "sedol7"})
        data = data.melt(
            id_vars=["sedol7", "company"], variable_name="date", value_name="growth"
        )
        data = data.with_columns(
            pl.col("date").str.split(".").list.get(0).str.to_date("%Y%m%d"),
            pl.col("growth").cast(pl.Float32, strict=False),
        )
        data.write_parquet(f"parquet/sales_growth/{table}.parquet")


def write_market_open_date():
    table = "us_market_open_date"
    today = datetime.today().strftime("%Y-%m-%d")
    data = yfinance.download("^DJI", start="2000-01-01", end=today).reset_index()
    data = pl.from_pandas(data)
    data = data.with_columns(pl.col("Date").dt.date().alias("date")).select(
        pl.col("date")
    )
    data.write_parquet(f"parquet/base/{table}.parquet")


def write_us_fund_return_data():
    table = "us_fund_daily_return_lipperid"
    file_names = [
        "US Fund Return_2000-2005.xlsx",
        "US Fund Return_2005-2010.xlsx",
        "US Fund Return_2010-2015.xlsx",
        "US Fund Return_2015-2020.xlsx",
        "US Fund Return_2020-2023.xlsx",
    ]
    data = []
    for file_name in file_names:
        part = pl.read_excel(f"data/{file_name}")
        part = (
            part.with_columns(
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
        data.append(part)
    data = pl.concat(data, how="vertical")
    data = data.sort(["lipper_id", "end_date"])
    data.write_parquet(f"parquet/fund_return/{table}.parquet")


def write_sedol_ticker_mapping():
    table = "us_sedol_ticker_mapping"
    data = pl.read_csv("data/SEDOL Tickers.csv")
    data = data.with_columns(
        pl.coalesce(pl.col("Ticker"), pl.col("Previous ticker")).alias("ticker")
    ).select(
        pl.col("SEDOL7").alias("sedol7"), pl.col("ticker"), pl.col("Name").alias("name")
    )
    data.write_parquet(f"parquet/base/{table}.parquet")


def write_eps_data():
    file_names = [
        "Reported EPS Qtr.xlsx",
        "Reported EPS Ann.xlsx",
    ]
    tables = ["us_security_eps_quarterly", "us_security_eps_annually"]
    for file_name, table in zip(file_names, tables):
        data = pl.read_excel(f"data/{file_name}")
        data = data.rename({"Name": "company", "SEDOL7": "sedol7"})
        data = data.melt(
            id_vars=["sedol7", "company"], variable_name="date", value_name="eps"
        )
        data = data.with_columns(
            pl.col("date").str.to_date("%Y%m%d"), pl.col("eps").cast(pl.Float32)
        )
        data.write_parquet(f"parquet/cape/{table}.parquet")


def write_cape_us_price_data():
    file_name = "Price_MSCI USA_20001231-20231130.xlsx"
    table = "us_security_price_daily"

    data = pl.read_excel(f"data/{file_name}")
    data = data.rename({"": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"], variable_name="date", value_name="price"
    )
    data = (
        data.with_columns(
            pl.col("date").str.to_date("%Y%m%d"),
            pl.col("price").cast(pl.Float32, strict=False),
        )
        .select("sedol7", "date", "price")
        .sort(
            [
                "sedol7",
                "date",
            ]
        )
    )
    data.write_parquet(f"parquet/cape/{table}.parquet")


def write_cape_us_sedol_return_data():
    file_name = "Price_MSCI USA_20001231-20231130.xlsx"
    table = "us_security_sedol_return_daily"

    data = pl.read_excel(f"data/{file_name}")
    data = data.rename({"": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"], variable_name="date", value_name="price"
    )
    data = (
        data.with_columns(
            pl.col("date").str.to_date("%Y%m%d"),
            pl.col("price").cast(pl.Float32, strict=False),
        )
        .select("sedol7", "date", "price")
        .sort(
            [
                "sedol7",
                "date",
            ]
        )
    )
    prev_data = data.select(
        (pl.col("date") + timedelta(days=1)).alias("next_day"),
        pl.col("sedol7"),
        pl.col("price").alias("prev_price"),
    )
    data = (
        data.join(
            prev_data,
            how="inner",
            left_on=["sedol7", "date"],
            right_on=["sedol7", "next_day"],
        )
        .with_columns(
            ((pl.col("price") - pl.col("prev_price")) / pl.col("prev_price")).alias(
                "return"
            )
        )
        .filter(pl.col("return").is_not_null())
        .select("sedol7", "date", "return")
    )
    data.write_parquet(f"parquet/fund_return/{table}.parquet")


def write_cpi_data():
    table = "us_cpi"
    data = pl.read_csv("data/CPI Data.csv")
    data = data.rename(
        {
            "Mnemonic": "date",
            "US.CPIALL": "us_cpi_all",
            "US.CPICORE": "us_cpi_core",
            "BLSSUUR0000SA0": "us_chained_cpi",
            "CNPR7096764": "cn_cpi",
            "CN.CPICORE": "cn_cpi_core",
        }
    ).select(
        "date", "us_cpi_all", "us_cpi_core", "us_chained_cpi", "cn_cpi", "cn_cpi_core"
    )

    data = (
        data.with_columns(
            pl.col("date").str.to_date("%Y/%m/%d"),
            pl.col("us_cpi_all").cast(pl.Float32, strict=False),
            pl.col("us_cpi_core").cast(pl.Float32, strict=False),
            pl.col("us_chained_cpi").cast(pl.Float32, strict=False),
            pl.col("cn_cpi").cast(pl.Float32, strict=False),
            pl.col("cn_cpi_core").cast(pl.Float32, strict=False),
        )
        # .filter(pl.col("date").dt.month() == 12)
        .sort(pl.col("date"))
    )

    # cpi_yoy = (
    #     data.select(pl.all().exclude("date")).to_numpy()
    #     - data.select(pl.all().exclude("date")).shift(1).to_numpy()
    # ) / data.select(pl.all().exclude("date")).shift(1).to_numpy()

    # cpi_yoy = pl.from_numpy(
    #     cpi_yoy, schema=data.select(pl.all().exclude("date")).schema
    # )
    # cpi_yoy = pl.concat(
    #     [data.select(pl.col("date").dt.year().alias("year")), cpi_yoy], how="horizontal"
    # )
    # cpi_yoy.write_parquet(f"parquet/cape/{table}.parquet")
    data.write_parquet(f"parquet/base/{table}.parquet")


def write_income_report_date():
    table = "us_security_income_report_announcement_date"
    data = pl.read_excel("data/Income Report Dates.xlsx")
    data = data.rename({"Name": "company", "SEDOL7": "sedol7"})
    data = data.melt(
        id_vars=["sedol7", "company"],
        variable_name="report_date",
        value_name="announcement_date",
    )
    data = data.with_columns(
        pl.col("report_date").str.to_date("%Y%m%d"),
        pl.col("announcement_date").str.to_date("%Y%m%d"),
    )
    data.write_parquet(f"parquet/cape/{table}.parquet")


if __name__ == "__main__":
    write_cape_us_sedol_return_data()
