from dagster import file_relative_path, get_dagster_logger
from dagster import AssetKey, EnvVar
from dagster_dbt import DagsterDbtTranslator, DbtCliResource
from dagster_duckdb import DuckDBResource


duckdb_database = file_relative_path(__file__, "../data/db/osmds.db")

duckdb_resource = DuckDBResource(
    database=duckdb_database,
)

logger = get_dagster_logger()
