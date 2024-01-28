import asyncio
from playwright.async_api import async_playwright, Playwright, TimeoutError

import re
import pendulum
from typing import List
from pydantic import BaseModel, Field
from dagster import get_dagster_logger
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
# Add logger
semaphore = asyncio.Semaphore(3)
# logger = get_dagster_logger()


class Price(BaseModel):
    # Hotel rate text scrapped from the Meta website
    text: str = Field(description="Hotel rate text scrapped from the Meta website", example="Booking.com $110 USD")
    hotel_name: str = Field(description="Hotel name", example="Ace Hotel, NYC")
    checkin_date: str = Field(description="Checkin date", example="2024-04-12")
    length_of_stay: int = Field(description="Length of stay", example=1)
    scrapped_url: str = Field(description="URL of the scrapped page", example="https://www.google.com/travel/search")



async def fetch_google_hotel_prices_desktop(hotel_name, checkin_date, length_of_stay=1) -> List[dict]:
  async with semaphore:
    async with async_playwright() as playwright:
      prices = []
      print("HERE")
      # make playwright use iphone 14 plus browser
      iphone_14_plus = playwright.devices["iPhone 14 Plus"]
      browser = await playwright.chromium.launch(headless=False)
      context = await browser.new_context(**iphone_14_plus)
      page = await context.new_page()
      # chromium = playwright.chromium
      # browser = await iphone_14_plus.launch(headless=False)
      # page = await browser.new_page()
      logger.debug("Navigating to https://www.google.com/travel/search")
      try:
        await page.goto("https://www.google.com/travel/search")
        await page.get_by_role("combobox", name="Search for places, hotels and more").click()
        await page.get_by_role("combobox", name="Search for places, hotels and more").fill(hotel_name)
        await page.get_by_role("combobox", name="Search for places, hotels and more").press("Enter")
        logger.debug("Waiting for page load for hotel name %s checkin date %s and length of stay %s", hotel_name, checkin_date, length_of_stay)
        await asyncio.sleep(5)
        is_right_page = (await page.get_by_label("Reviews", exact=True).first.is_visible() and \
                await page.get_by_label("About", exact=True).first.is_visible() and \
                await page.get_by_label("Overview", exact=True).first.is_visible())
        is_list_item_shown = await page.get_by_label(hotel_name, exact=True).is_visible()
        logger.debug('Overview tab is visible: %s', await page.get_by_label("Overview", exact=True).first.is_visible())
        logger.debug('About tab is visible: %s', await page.get_by_label("About", exact=True).first.is_visible())
        logger.debug('Reviews tab is visible: %s', await page.get_by_label("Reviews", exact=True).first.is_visible())
        logger.debug("Right page shown: %s, List item shown: %s", is_right_page, is_list_item_shown)
        # while not (is_right_page):
        #   try:
        #       logger.debug(f"Trying to hit {hotel_name} label")
        #       # await page.get_by_label(hotel_name).click()
        #       for i in await page.get_by_label(re.compile(hotel_name, re.IGNORECASE)).all():
        #           logger.debug(await i.all_inner_texts())
        #       break
        #   except Exception as te:
        #       await asyncio.sleep(5)
        #       is_right_page = (await page.get_by_label("Reviews", exact=True).is_visible() and \
        #         await page.get_by_label("About", exact=True).is_visible() and \
        #         await page.get_by_label("Photos", exact=True).is_visible())


        checkin_picker = page.get_by_role("textbox", name=re.compile("Check-in", re.IGNORECASE))
        checkout_picker = page.get_by_role("textbox", name=re.compile("Check-out", re.IGNORECASE))
        orig_value = await checkin_picker.input_value()
        logger.debug("Original checkin date: %s", orig_value)
        dest_checkin_date = pendulum.parse(checkin_date)
        await checkin_picker.fill(dest_checkin_date.format('ddd, MMM D'))
        await asyncio.sleep(4)
        await checkout_picker.fill((dest_checkin_date + pendulum.duration(days=length_of_stay)).format('ddd, MMM D'))
        await checkout_picker.press("Enter")
        logger.debug("Right page shown: %s, List item shown: %s", is_right_page, is_list_item_shown)
        logger.debug("New checkin date: %s and checkout date: %s", await checkin_picker.input_value(), await checkout_picker.input_value())
        await asyncio.sleep(2)
        logger.debug("New checkin date: %s and checkout date: %s", await checkin_picker.input_value(), await checkout_picker.input_value())
        while not await page.get_by_role("button", name=re.compile("Fewer Options", re.IGNORECASE)).is_visible():
          await page.get_by_role("button", name=re.compile("View more options from \$\d+", re.IGNORECASE)).click()
          await asyncio.sleep(6)
        logger.debug("All prices loaded")
        all_prices = await page.get_by_role("link", name=re.compile("\w+(\s\w+)* \$\d+ \w+(\s\w+)*")).all()
        logger.debug("Found %s prices for hotel %s", len(all_prices), hotel_name)
        for price in all_prices:
          text = await price.all_inner_texts()
          # Rules to clean up the text
          text = re.sub(re.escape('View site'), '', text[0], flags=re.IGNORECASE)
          text = text.replace('\n',';;')
          price_obj = Price(text=text, hotel_name=hotel_name, checkin_date=checkin_date, length_of_stay=length_of_stay, scrapped_url=page.url)
          logger.debug("Price object: %s", dict(price_obj))
          prices.append(dict(price_obj))
        #TODO: Make this work at scale for screenshots
        # await page.screenshot(path="full_page_screenshot.png", full_page=True)
        await browser.close()
        logger.debug("Closed browser")
        return prices
      except TimeoutError:
         logger.error(f"Failed to load page for hotel {hotel_name} with checkin date {checkin_date} and length of stay {length_of_stay}")
         return [{
            'hotel_name': hotel_name,
            'checkin_date': checkin_date,
            'length_of_stay': length_of_stay,
            'scrapped_url': page.url,
            'text': 'Failed to load page'
         }]

async def get_google_hotel_prices_mobile(hotel_name, checkin_date, length_of_stay) -> List[dict]:
    async with semaphore:
      async with async_playwright() as playwright:
        prices = []
        iphone_14_plus = playwright.devices["iPhone 14 Plus"]
        browser = await playwright.chromium.launch(headless=False)
        context = await browser.new_context(**iphone_14_plus)
        page = await context.new_page()
        try:
            await page.goto("https://www.google.com/")
            await page.get_by_role("textbox", name="Google Search").click()
            await page.get_by_role("textbox", name="Google Search").fill(hotel_name)
            await page.get_by_role("option", name=re.compile(hotel_name, re.IGNORECASE)).first.click()
            await page.get_by_role("button", name="Check availability").click()
            await asyncio.sleep(5)
            cin_elements = await page.get_by_role("main").get_by_text(re.compile('(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', re.IGNORECASE)).first.all_inner_texts()
            original_checkin_date = cin_elements[0] if cin_elements else None
            cout_elements = await page.get_by_role("main").get_by_text(re.compile('(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', re.IGNORECASE)).nth(1).all_inner_texts()
            original_checkout_date = cout_elements[0] if cout_elements else None
            logger.debug(f"Original checkin date and checkout date: {original_checkin_date} and {original_checkout_date}")
            orig_checkin_date = pendulum.from_format(original_checkin_date, 'ddd, MMM D')
            orig_checkout_date = pendulum.from_format(original_checkout_date, 'ddd, MMM D')
            target_date = pendulum.parse(checkin_date)
            delta_days = (target_date - orig_checkin_date).days
            while delta_days > 0:
                await page.get_by_role("button", name="Set Check-in one day later").click()
                delta_days -= 1
                await asyncio.sleep(0.1)
            cin_elements = await page.get_by_role("main").get_by_text(re.compile('(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', re.IGNORECASE)).first.all_inner_texts()
            final_checkin_date = cin_elements[0] if cin_elements else None
            cout_elements = await page.get_by_role("main").get_by_text(re.compile('(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', re.IGNORECASE)).nth(1).all_inner_texts()
            final_checkout_date = cout_elements[0] if cout_elements else None
            logger.debug(f"Final checkin date and checkout date: {final_checkin_date} and {final_checkout_date}")
            itin_length_of_stay = (final_checkout_date - final_checkin_date).days
            assert itin_length_of_stay > 0
            while length_of_stay > itin_length_of_stay:
                await page.get_by_role("button", name="Set Check-out one day later").click()
                itin_length_of_stay += 1
                await asyncio.sleep(0.1)
            await asyncio.sleep(5)

            while not await page.get_by_role("button", name=re.compile("Fewer Options", re.IGNORECASE)).is_visible():
                await page.get_by_role("button", name=re.compile("View more options from \$\d+", re.IGNORECASE)).click()
                await asyncio.sleep(10)
            await page.screenshot(path="full_page_screenshot_iphone.png", full_page=True)
            all_prices = await page.get_by_role("link", name=re.compile("\w+(\s\w+)* \$\d+ \w+(\s\w+)*")).all()
            for price in all_prices:
                text = await price.all_inner_texts()
                # Rules to clean up the text
                text = re.sub(re.escape('View site'), '', text[0], flags=re.IGNORECASE)
                text = text.replace('\n',';;')
                price_obj = Price(text=text, hotel_name=hotel_name, checkin_date=checkin_date, length_of_stay=length_of_stay, scrapped_url=page.url)
                logger.debug("Price object: %s", dict(price_obj))
                prices.append(dict(price_obj))
            await context.close()
            await browser.close()
        except TimeoutError:
            price_obj = Price(text='Failed to load page', hotel_name=hotel_name, checkin_date=checkin_date, length_of_stay=length_of_stay, scrapped_url=page.url)
            logger.debug("Price object: %s", dict(price_obj))
            prices.append(dict(price_obj))

        return prices


async def main(inputs):
  results = await asyncio.gather(*(get_google_hotel_prices_mobile(hotel_name="chelsea hotel, toronto", checkin_date="2024-04-12", length_of_stay=1) for i in range(1)))
  return results

if __name__ == "__main__":
    inputs = [
      {
        "hotel_name": "hilton london kensington",
        "checkin_date": "2024-04-15",
        "length_of_stay": 1
      },
      {
        "hotel_name": "chelsea hotel, toronto",
        "checkin_date": "2024-04-20",
        "length_of_stay": 1
      }
    ]
    failed_inputs = [{'hotel_name': 'DoubleTree By Hilton New York Downtown',
      'checkin_date': '2024-02-09',
      'length_of_stay': 1},
    {'hotel_name': 'DoubleTree By Hilton New York Downtown',
      'checkin_date': '2024-02-09',
      'length_of_stay': 1},
    {'hotel_name': 'Candlewood Suites New York City-Times Square, An IHG Hotel',
      'checkin_date': '2024-02-09',
      'length_of_stay': 1},
    {'hotel_name': 'Hilton Toronto',
      'checkin_date': '2024-04-12',
      'length_of_stay': 1},
    {'hotel_name': 'Hilton Toronto',
      'checkin_date': '2024-04-13',
      'length_of_stay': 1},
    {'hotel_name': 'Boardwalk Beach Resort By Panhandle Getaways',
      'checkin_date': '2024-03-11',
      'length_of_stay': 1},
    {'hotel_name': 'Boardwalk Beach Resort By Panhandle Getaways',
      'checkin_date': '2024-03-12',
      'length_of_stay': 1},
    {'hotel_name': 'Boardwalk Beach Resort By Panhandle Getaways',
      'checkin_date': '2024-03-13',
      'length_of_stay': 1}]
    results = asyncio.run(main(inputs))
    print(f"Final Results: {results}")


