
import asyncio
import random

import pandas as pd
import pendulum
from dagster import (
    AssetExecutionContext,
    AssetIn,
    Backoff,
    RetryPolicy,
    TimeWindowPartitionsDefinition,
    asset,
)

from pipelines.assets.google_hotel_ads.scraper import (
    fetch_google_hotel_prices_desktop,
    fetch_google_hotel_prices_mobile,
)

retry_policy = RetryPolicy(
    max_retries=3,
    delay=0.2,  # 200ms
    backoff=Backoff.EXPONENTIAL,
)

# search_itineraries = SourceAsset(key=AssetKey("gsheets", "search_itineraries"))


@asset(
    partitions_def=TimeWindowPartitionsDefinition(
        start=pendulum.datetime(2024, 1, 21, 0, 0),
        cron_schedule="6 */6 * * *",
        fmt="%Y-%m-%d-%H-%M",
    ),
    ins={"search_itineraries": AssetIn(key="search_itineraries")},
    io_manager_key="duckdb_io_manager",
    metadata={"partition_expr": "run_at"},
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,  # 20s
        backoff=Backoff.EXPONENTIAL,
    ),
)
async def hotel_prices(
    context: AssetExecutionContext,
    search_itineraries,
) -> pd.DataFrame:
    """Captures price data for all hotels in search_itineraries. Both Mobile and Desktop Google hotel ads"""
    # only run for sample 10 rows
    inputs = search_itineraries[
        ["hotel_name", "checkin_date", "length_of_stay"]
    ].to_dict(orient="records")
    # context.log.info(f"Proxies: {https_proxies}")
    # add proxy to inputs
    # for inp in inputs:
    #   record = https_proxies.sample(1)
    #   server = f'https://{record["ip address"].values[0]}:{record["port"].values[0]}'
    #   context.log.info(f"Using proxy server {server}")
    #   inp['proxy_server'] = server
    desktop_tasks = [fetch_google_hotel_prices_desktop(**inp) for inp in inputs]
    mobile_tasks = [fetch_google_hotel_prices_mobile(**inp) for inp in inputs]
    random.shuffle(desktop_tasks)
    random.shuffle(mobile_tasks)
    # combine both mobile and desktop list
    tasks = desktop_tasks + mobile_tasks
    # shuffle tasks to avoid getting blocked
    random.shuffle(tasks)
    results = await asyncio.gather(*tasks)
    context.log.info(results)
    df = pd.concat([pd.DataFrame(result) for result in results])
    # add run date col with now datestring
    df["run_date"] = pendulum.now("UTC").to_date_string()
    # add run hour col with now hour
    df["run_at"] = pendulum.now("UTC").to_datetime_string()
    df["execution_at"] = context.asset_partition_key_for_output()
    context.log.info(f"Data: {df.head()}")
    return df[
        [
            "hotel_name",
            "checkin_date",
            "length_of_stay",
            "scrapped_url",
            "text",
            "run_date",
            "run_at",
            "execution_at",
        ]
    ]