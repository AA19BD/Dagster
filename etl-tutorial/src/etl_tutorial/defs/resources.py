from dagster_duckdb import DuckDBResource

import dagster as dg

database_resource = DuckDBResource(database=r"C:\Users\user\Desktop\Dagster\jaffle_platform.duckdb")


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
        }
    )