from typing import Any, Mapping

from dagster import AssetKey, load_assets_from_package_module
from dagster_dbt import DagsterDbtTranslator

from . import events, google_hotel_ads

EVENTS = "events"
GOOGLE_HOTEL_ADS = "google_hotel_ads"


events_assets = load_assets_from_package_module(
    package_module=events, group_name=EVENTS
)

google_hotel_ads_assets = load_assets_from_package_module(
    package_module=google_hotel_ads, group_name=GOOGLE_HOTEL_ADS
)


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        return AssetKey(dbt_resource_props["name"])

    @classmethod
    def get_group_name(cls, dbt_resource_props) -> str:
        return "prepared"


# @dbt_assets(
#     manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
#     # io_manager_key="warehouse_io_manager",
#     dagster_dbt_translator=CustomDagsterDbtTranslator(),
# )
# def hotel_prices_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
#     yield from dbt.cli(["build"], context=context).stream()
