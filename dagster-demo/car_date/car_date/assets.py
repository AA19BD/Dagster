import dagster as dg
import polars as pl
import duckdb


CSV_URL = "https://raw.githubusercontent.com/Azure/carprice/refs/heads/master/dataset/carprice.csv"
CSV_PATH = "data/carprice.csv"

DUCKDB_PATH = "data/car_data.duckdb"
TABLE_NAME = "avg_price_per_brand"

@dg.asset(
    description="Загружает CSV-файл с данными о машинах, преобразует типы столбцов и сохраняет локально."
)
def car_data_file(context: dg.AssetExecutionContext) -> None:
    """Download the car data CSV file."""
    context.log.info(f"Downloading car data from {CSV_URL} to {CSV_PATH}")

    df = pl.read_csv(CSV_URL)
    df = df.with_columns([
        pl.col("normalized-losses").cast(pl.Float64, strict=False),
        pl.col("price").cast(pl.Float64, strict=False),
    ])
    df.write_csv(CSV_PATH)


@dg.asset(deps=[car_data_file], description="Создает таблицу DuckDB с усредненной ценой по брендам.")
def avg_price_table(context: dg.AssetExecutionContext) -> None:
    """Create a DuckDB table with the average price per brand."""
    context.log.info(f"Creating DuckDB table {TABLE_NAME} at {DUCKDB_PATH}")

    df = pl.read_csv(CSV_PATH)
    df = df.drop_nulls(["price"])
    avg_price_df = df.group_by("make").agg(pl.col("price").mean().alias("avg_price"))
    data = [(row["make"], row["avg_price"]) for row in avg_price_df.to_dicts()]

    # Connect to DuckDB and create the table
    with duckdb.connect(DUCKDB_PATH) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (make TEXT, avg_price DOUBLE)")
        conn.executemany(f"INSERT INTO {TABLE_NAME} VALUES (?, ?)", data)
