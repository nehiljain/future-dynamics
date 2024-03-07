
import asyncio
import random

import gspread
import pandas as pd
import pendulum
from dagster import (
    AssetExecutionContext,
    Backoff,
    RetryPolicy,
    TimeWindowPartitionsDefinition,
    asset,
)

from . import google_hotel_prices

retry_policy = RetryPolicy(
    max_retries=3,
    delay=0.2,  # 200ms
    backoff=Backoff.EXPONENTIAL,
)



# @asset
# def cities(context: AssetExecutionContext) -> List[str]:
#     """
#     Pull in list of cities from makcorps.com api
#     """
#     return [
#         "New York",
#         "Toronto",
#     ]




# @asset
# def get_hotel_price_today(
#     context: AssetExecutionContext,
#     get_hotel_ids
# ) -> pd.DataFrame:
#     """
#     Pull in pricing data for all hotels in NYC from makcorps.com api
#     """
#     def parse_hotel_prices(json_data, checkin, checkout, hotel_id, now_timestamp):
#         """
#         Internal function to parse json from api and convert to df"""
#         # Extracting the comparison data
#         comparison_data = json_data.get('comparison', [[]])[0]

#         transformed_data = [
#           {
#               "vendor": item.get(f"vendor{i+1}"),
#               "price": item.get(f"price{i+1}"),
#               "tax": item.get(f"tax{i+1}"),
#           }
#           for i, item in enumerate(comparison_data)
#         ]
#         # Creating DataFrame
#         df = pd.DataFrame(transformed_data)
#         df['checkin'] = checkin
#         df['checkout'] = checkout
#         df['hotel_id'] = hotel_id
#         df['now_timestamp'] = now_timestamp
#         return df
#     url = "https://api.makcorps.com/hotel"
#     params = {
#         'rooms': 1,
#         'adults': 2,
#         'api_key': '65a063514bdc1a8110980170',
#     }
#     # checkin data april 12, 2024
#     checkin = pendulum.datetime(2024, 4, 12)

#     checkout = checkin.add(days=1)

#     checkin_out_combos = [
#         (checkin.add(days=i).to_date_string(), checkout.add(days=i).to_date_string())
#         for i in range(1, 10)
#     ]
#     output = []
#     for checkin, checkout in checkin_out_combos:
#       params['checkin'] = checkin
#       params['checkout'] = checkout
#       for hotel_id in get_hotel_ids:
#           context.log.info(f"Requesting data for hotel {hotel_id}")
#           params['hotelid'] = hotel_id
#           response = requests.get(url, params=params)
#           context.log.info(f"Response: {response.status_code}, {response.text}")
#           if response.status_code == 200:
#               json_data = response.json()
#               output.append(parse_hotel_prices(json_data, checkin, checkout, hotel_id, pendulum.now('UTC').to_datetime_string()))
#           else:
#               print(f"Error: {response.status_code}, {response.text}")
#               return None
#     return pd.concat(output)


# @asset
# def get_city_makcorps_ids(
#     context: AssetExecutionContext,
#     cities
# ) -> pd.DataFrame:
#     """
#     Pull in pricing data for all hotels in NYC from makcorps.com api
#     """
#     url = "https://api.makcorps.com/mapping"
#     data = []

#     for city in cities:
#         params = {
#             'api_key': '65a063514bdc1a8110980170',
#             'name': city
#         }
#         response = requests.get(url, params=params)

#         if response.status_code == 200:
#             json_data = response.json()
#             for item in json_data:
#                 if item['type'] == 'GEO':
#                     record = {
#                         'city': city,
#                         'id': item.get('document_id'),
#                         'scope': item.get('scope'),
#                         'name': item.get('name'),
#                         'data_type': item.get('data_type'),
#                         'coords': item.get('coords'),
#                         'place_type': item.get('details', {}).get('placetype'),
#                         'parent_name': item.get('details', {}).get('parent_name'),
#                         'grandparent_id': item.get('details', {}).get('grandparent_name'),
#                         'parent_id': item.get('details', {}).get('parent_id'),
#                         'grandparent_id': item.get('details', {}).get('grandparent_id'),
#                         'parent_place_type': item.get('details', {}).get('parent_place_type'),
#                         'highlighted_name': item.get('details', {}).get('highlighted_name'),
#                         'geo_name': item.get('details', {}).get('geo_name'),
#                         'address': item.get('details', {}).get('address')
#                     }
#                     data.append(record)
#         else:
#             context.log.warn(f"Error for {city}: {response.status_code}, {response.text}")

#     return pd.DataFrame(data)



@asset(
    group_name="raw_data",
    io_manager_key="duckdb_io_manager",
    retry_policy=retry_policy,
    # partitions_def=TimeWindowPartitionsDefinition(
    #     start=pendulum.datetime(2024, 1, 21, 0, 0),
    #     cron_schedule="6 */6 * * *",
    #     fmt="%Y-%m-%d-%H-%M",
    # ),
    # metadata={"partition_expr": "execution_at"},
)
def search_itineraries(
    context: AssetExecutionContext
) -> pd.DataFrame:
    """
    Pull in itineraries from google sheets
    """
    gc = gspread.service_account()
    sh = gc.open("B2Bed Hotel Pricing Data")
    worksheet = sh.get_worksheet(0)
    data = worksheet.get_all_records()
    def parse_date(x):
      try:
        return pendulum.from_format(x, "MMM D")
      except ValueError:
        return pendulum.from_format(x, "MMMM D")
    df = pd.DataFrame(data)
    df.rename(columns={'name': 'hotel_name'}, inplace=True)
    # parse checkin using pendulum pendulum.from_format(df['checkin'], "MMM D")
    df['clean_checkin'] = df['checkin'].apply(lambda x: parse_date(x))
    df['clean_checkout'] = df['checkout'].apply(lambda x: parse_date(x))
    df["checkin_date"] = df["checkin"].apply(lambda x: parse_date(x).to_date_string())
    df['length_of_stay'] = df.apply(lambda x: (x['clean_checkout'] - x['clean_checkin']).days, axis=1)
    df["run_at"] = pendulum.now("UTC").to_datetime_string()
    # df["execution_at"] = context.asset_partition_key_for_output()
    df["execution_at"] = pendulum.now("UTC").to_datetime_string()

    context.log.info("Data: df")
    return df


# @asset(partitions_def=HourlyPartitionsDefinition(start_date=pendulum.datetime(2024, 1, 21, 0, 0)),
#       group_name="raw_data",
#       retry_policy= RetryPolicy(
#         max_retries=5,
#         delay=5,  # 5s
#         backoff=Backoff.EXPONENTIAL,
#       ))
# def https_proxies(
#     context: AssetExecutionContext
# ) -> pd.DataFrame:
#     """Pull in https proxiesfrom free-proxy-list.net"""
#     return proxy_list.get_https_proxies()



@asset(partitions_def=TimeWindowPartitionsDefinition(start=pendulum.datetime(2024, 1, 21, 0, 0),
                                                     cron_schedule="6 */6 * * *",
                                                     fmt="%Y-%m-%d-%H-%M"),
       group_name="raw_data",
       io_manager_key="duckdb_io_manager",
       metadata={
          "partition_expr": "run_at"
       },
      retry_policy= RetryPolicy(
        max_retries=3,
        delay=60,  # 20s
        backoff=Backoff.EXPONENTIAL,
      ))
async def hotel_prices(
    context: AssetExecutionContext,
    search_itineraries,
    # https_proxies
) -> pd.DataFrame:
    """Captures price data for all hotels in search_itineraries. Both Mobile and Desktop Google hotel ads"""
    # only run for sample 10 rows
    latest_searches = search_itineraries[
        search_itineraries["execution_at"] == search_itineraries["execution_at"].max()
    ]
    latest_searches = latest_searches.drop_duplicates(
        subset=["hotel_name", "checkin_date", "length_of_stay", "created_at"]
    )
    inputs = latest_searches[["hotel_name", "checkin_date", "length_of_stay"]].to_dict(
        orient="records"
    )

    context.log.info(f"Inputs: {inputs}")
    # context.log.info(f"Proxies: {https_proxies}")
    # add proxy to inputs
    # for inp in inputs:
    #   record = https_proxies.sample(1)
    #   server = f'https://{record["ip address"].values[0]}:{record["port"].values[0]}'
    #   context.log.info(f"Using proxy server {server}")
    #   inp['proxy_server'] = server
    desktop_tasks = [google_hotel_prices.fetch_google_hotel_prices_desktop(**inp) for inp in inputs]
    mobile_tasks = [google_hotel_prices.fetch_google_hotel_prices_mobile(**inp) for inp in inputs]
    random.shuffle(desktop_tasks)
    random.shuffle(mobile_tasks)
    #combine both mobile and desktop list
    tasks = desktop_tasks + mobile_tasks
    # shuffle tasks to avoid getting blocked
    random.shuffle(tasks)
    results = await asyncio.gather(*tasks)
    context.log.info(results)
    df = pd.concat([pd.DataFrame(result) for result in results])
    # add run date col with now datestring
    df['run_date'] = pendulum.now('UTC').to_date_string()
    # add run hour col with now hour
    df['run_at'] = pendulum.now('UTC').to_datetime_string()
    df['execution_at'] = context.asset_partition_key_for_output()
    context.log.info(f"Data: {df.head()}")
    return df[['hotel_name', 'checkin_date', 'length_of_stay', 'scrapped_url', 'text', 'run_date', 'run_at', 'execution_at']]
