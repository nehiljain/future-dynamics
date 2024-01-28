from playwright.sync_api import sync_playwright
import pandas as pd
from dagster import get_dagster_logger

def get_https_proxies() -> pd.DataFrame:
  """Function to scrape https proxies from free-proxy-list.net"""
  logger = get_dagster_logger()
  with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://free-proxy-list.net/")
    table = page.locator('xpath=//*[@id="list"]/div/div[2]/div/table')
    rows = table.locator("tr").all_inner_texts()
    print(rows)
    logger.info(f"found table data {rows}")
    split_data = [row.split('\t') for row in rows]

    # Create a DataFrame, using the first row as the header, and the rest as data
    df = (
        pd.DataFrame(split_data[1:], columns=split_data[0])
        .applymap(lambda x: x.lower() if isinstance(x, str) else x)
        .rename(columns=str.lower)
        .query('`https` == "yes" and `country` == "united states"')
      )
    logger.info(f"found proxy {df}")

    # ---------------------
    context.close()
    browser.close()
    return df
