from pathlib import Path

from dagster import EnvVar, get_dagster_logger
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource

duckdb_database = EnvVar("DUCK_DB_FILE")

duckdb_resource = DuckDBResource(
    database=duckdb_database,
)

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "dbt_project").resolve()

logger = get_dagster_logger()
dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
)

dbt_parse_invocation = dbt_resource.cli(["parse"], manifest={}).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
