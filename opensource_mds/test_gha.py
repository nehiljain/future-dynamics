import asyncio
import re
import time
from pathlib import Path
from typing import List

import pendulum
from dagster import get_dagster_logger
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright as playwright
from pydantic import BaseModel, Field

load_dotenv()
SCREENSHOTS_DPATH = "/Users/nehiljain/code/data/future-dynamics/raw/"

# Add logger
semaphore = asyncio.Semaphore(3)
logger = get_dagster_logger()


class Price(BaseModel):
    # Hotel rate text scrapped from the Meta website
    text: str = Field(
        description="Hotel rate text scrapped from the Meta website",
        example="Booking.com $110 USD",
    )
    hotel_name: str = Field(description="Hotel name", example="Ace Hotel, NYC")
    checkin_date: str = Field(description="Checkin date", example="2024-04-12")
    length_of_stay: int = Field(description="Length of stay", example=1)
    scrapped_url: str = Field(
        description="URL of the scrapped page",
        example="https://www.google.com/travel/search",
    )


def fetch_google_hotel_prices_desktop(
    hotel_name, checkin_date, length_of_stay=1
) -> List[dict]:
    logger.debug(
        f"Starting desktop price fetch for hotel: {hotel_name}, check-in: {checkin_date}, stay length: {length_of_stay} days"
    )
    with playwright() as pw:
        prices = []
        chromium = pw.chromium
        browser = chromium.launch(
            headless=False,
        )
        page = browser.new_page()
        logger.debug("Navigating to https://www.google.com/travel/search")
        try:
            page.goto("https://www.google.com/travel/search")
            page.get_by_role(
                "combobox", name="Search for places, hotels and more"
            ).click()
            page.get_by_role(
                "combobox", name="Search for places, hotels and more"
            ).fill(hotel_name)
            page.get_by_role(
                "combobox", name="Search for places, hotels and more"
            ).press("Enter")
            logger.debug(
                "Waiting for page load for hotel name %s checkin date %s and length of stay %s",
                hotel_name,
                checkin_date,
                length_of_stay,
            )

            is_right_page = (
                page.get_by_label("Reviews", exact=True).first.is_visible()
                and page.get_by_label("About", exact=True).first.is_visible()
                and page.get_by_label("Overview", exact=True).first.is_visible()
            )

            if not is_right_page:
                time.sleep(5)
                is_list_item_shown = page.get_by_label(
                    hotel_name, exact=False
                ).all_inner_texts()
                if "View Prices".lower() in "".join(is_list_item_shown).lower():
                    # Clean s2 by removing non-alphanumeric characters and converting to lowercase
                    regex_hotel_name_str = re.sub(r"\W+", "", hotel_name).lower()
                    prefix = "View prices for "
                    # Generate a regex pattern from cleaned_s2 that allows for any non-alphanumeric characters between its characters
                    pattern = re.escape(prefix) + "".join(
                        f"{char}[^A-Za-z0-9]*" for char in regex_hotel_name_str
                    )
                    butt = page.get_by_label(
                        re.compile(pattern, re.IGNORECASE)
                    ).locator("button")
                    butt.click()
            logger.debug(
                "Overview tab is visible: %s",
                page.get_by_label("Overview", exact=True).first.is_visible(),
            )
            logger.debug(
                "About tab is visible: %s",
                page.get_by_label("About", exact=True).first.is_visible(),
            )
            logger.debug(
                "Reviews tab is visible: %s",
                page.get_by_label("Reviews", exact=True).first.is_visible(),
            )
            logger.debug(
                "Right page shown: %s, List item shown: %s",
                is_right_page,
                is_list_item_shown,
            )

            checkin_picker = page.get_by_role(
                "textbox", name=re.compile("Check-in", re.IGNORECASE)
            )
            checkout_picker = page.get_by_role(
                "textbox", name=re.compile("Check-out", re.IGNORECASE)
            )
            orig_value = checkin_picker.input_value()
            logger.debug("Original checkin date: %s", orig_value)
            dest_checkin_date = pendulum.parse(checkin_date)
            checkin_picker.fill(dest_checkin_date.format("ddd, MMM D"))
            time.sleep(4)
            checkout_picker.fill(
                (dest_checkin_date + pendulum.duration(days=length_of_stay)).format(
                    "ddd, MMM D"
                )
            )
            checkout_picker.press("Enter")
            logger.debug(
                "Right page shown: %s, List item shown: %s",
                is_right_page,
                is_list_item_shown,
            )
            logger.debug(
                "New checkin date: %s and checkout date: %s",
                checkin_picker.input_value(),
                checkout_picker.input_value(),
            )
            time.sleep(2)
            logger.debug(
                "New checkin date: %s and checkout date: %s",
                checkin_picker.input_value(),
                checkout_picker.input_value(),
            )
            while not page.get_by_role(
                "button", name=re.compile("Fewer Options", re.IGNORECASE)
            ).is_visible():
                page.get_by_role(
                    "button",
                    name=re.compile("View more options from \$\d+", re.IGNORECASE),
                ).click()
                time.sleep(6)
            logger.debug("All prices loaded")
            all_prices = page.get_by_role(
                "link", name=re.compile("\w+(\s\w+)* \$\d+ \w+(\s\w+)*")
            ).all()
            logger.debug("Found %s prices for hotel %s", len(all_prices), hotel_name)
            for price in all_prices:
                text = price.all_inner_texts()
                # Rules to clean up the text
                text = re.sub(re.escape("View site"), "", text[0], flags=re.IGNORECASE)
                text = text.replace("\n", ";;")
                price_obj = Price(
                    text=text + ";;desktop",
                    hotel_name=hotel_name,
                    checkin_date=checkin_date,
                    length_of_stay=length_of_stay,
                    scrapped_url=page.url,
                )
                logger.debug("Price object: %s", dict(price_obj))
                prices.append(dict(price_obj))
            sanitized_hotel_name = re.sub(r"[^A-Za-z0-9]", "-", hotel_name)
            screenshot_filepath = Path(
                SCREENSHOTS_DPATH,
                f'desktop_full_page_screenshot__{sanitized_hotel_name}__{dest_checkin_date.format("YYYYMMDD")}__{pendulum.now("UTC").format("YYYYMMDDHHmm")}.png',
            )
            logger.info(f"Saving screenshot to {screenshot_filepath}")
            page.screenshot(path=screenshot_filepath, full_page=True)
            browser.close()
            logger.debug("Closed browser")
            return prices
        except TimeoutError as e:
            logger.error(
                f"Failed to load page for hotel {hotel_name} with checkin date {checkin_date} and length of stay {length_of_stay}: {e}"
            )
            return [
                {
                    "hotel_name": hotel_name,
                    "checkin_date": checkin_date,
                    "length_of_stay": length_of_stay,
                    "scrapped_url": page.url,
                    "text": "Failed to load page",
                }
            ]


async def fetch_google_hotel_prices_mobile(
    hotel_name, checkin_date, length_of_stay
) -> List[dict]:
    logger.debug(
        f"Starting mobile price fetch for hotel: {hotel_name}, check-in: {checkin_date}, stay length: {length_of_stay} days"
    )
    async with semaphore:
        async with async_playwright() as playwright:
            prices = []
            iphone_14_plus = playwright.devices["iPhone 14 Plus"]
            browser = await playwright.chromium.launch(
                headless=True,
            )
            logger.debug("Launching browser for mobile iphone 14 plus")
            context = await browser.new_context(**iphone_14_plus)
            page = await context.new_page()
            try:
                await page.goto("https://www.google.com/")
                await page.get_by_role("textbox", name="Google Search").click()
                await page.get_by_role("textbox", name="Google Search").fill(hotel_name)
                await page.get_by_role(
                    "option", name=re.compile(hotel_name, re.IGNORECASE)
                ).first.click()
                await page.get_by_role("button", name="Check availability").click()
                logger.debug(
                    "Searched on google for hotel %s and clicked on check availability",
                    hotel_name,
                )
                await asyncio.sleep(5)
                cin_elements = (
                    await page.get_by_role("main")
                    .get_by_text(
                        re.compile(
                            "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
                            re.IGNORECASE,
                        )
                    )
                    .first.all_inner_texts()
                )
                original_checkin_date = cin_elements[0] if cin_elements else None
                cout_elements = (
                    await page.get_by_role("main")
                    .get_by_text(
                        re.compile(
                            "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
                            re.IGNORECASE,
                        )
                    )
                    .nth(1)
                    .all_inner_texts()
                )
                original_checkout_date = cout_elements[0] if cout_elements else None
                logger.debug(
                    f"Original checkin date and checkout date: {original_checkin_date} and {original_checkout_date}"
                )
                orig_checkin_date = pendulum.from_format(
                    original_checkin_date, "ddd, MMM D"
                )
                target_date = pendulum.parse(checkin_date)
                delta_days = (target_date - orig_checkin_date).days
                while delta_days > 0:
                    await page.get_by_role(
                        "button", name="Set Check-in one day later"
                    ).click()
                    delta_days -= 1
                    await asyncio.sleep(0.1)
                logger.debug(f"Set new checkin date to {checkin_date}")
                cin_elements = (
                    await page.get_by_role("main")
                    .get_by_text(
                        re.compile(
                            "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
                            re.IGNORECASE,
                        )
                    )
                    .first.all_inner_texts()
                )
                final_checkin_date = cin_elements[0] if cin_elements else None
                cout_elements = (
                    await page.get_by_role("main")
                    .get_by_text(
                        re.compile(
                            "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
                            re.IGNORECASE,
                        )
                    )
                    .nth(1)
                    .all_inner_texts()
                )
                final_checkout_date = cout_elements[0] if cout_elements else None
                fin_checkin_date = pendulum.from_format(
                    final_checkin_date, "ddd, MMM D"
                )
                fin_checkout_date = pendulum.from_format(
                    final_checkout_date, "ddd, MMM D"
                )
                logger.debug(
                    f"Final checkin date and checkout date: {final_checkin_date} and {final_checkout_date}"
                )
                itin_length_of_stay = (fin_checkout_date - fin_checkin_date).days
                assert itin_length_of_stay > 0
                while length_of_stay > itin_length_of_stay:
                    await page.get_by_role(
                        "button", name="Set Check-out one day later"
                    ).click()
                    itin_length_of_stay += 1
                    await asyncio.sleep(0.1)
                await asyncio.sleep(5)

                while not await page.get_by_role(
                    "button", name=re.compile("Fewer Options", re.IGNORECASE)
                ).is_visible():
                    await page.get_by_role(
                        "button",
                        name=re.compile("View more options from \$\d+", re.IGNORECASE),
                    ).first.click()
                    await asyncio.sleep(10)

                sanitized_hotel_name = re.sub(r"[^A-Za-z0-9]", "-", hotel_name)
                screenshot_filepath = Path(
                    SCREENSHOTS_DPATH,
                    f'mobile_full_page_screenshot__{sanitized_hotel_name}__{target_date.format("YYYYMMDD")}__{pendulum.now("UTC").format("YYYYMMDDHHmm")}.png',
                )
                logger.info(f"Saving screenshot to {screenshot_filepath}")
                await page.screenshot(path=screenshot_filepath, full_page=True)

                all_prices = await page.get_by_role(
                    "link", name=re.compile("\w+(\s\w+)* \$\d+ \w+(\s\w+)*")
                ).all()
                for price in all_prices:
                    text = await price.all_inner_texts()
                    # Rules to clean up the text
                    text = re.sub(
                        re.escape("View site"), "", text[0], flags=re.IGNORECASE
                    )
                    text = text.replace("\n", ";;")
                    price_obj = Price(
                        text=text + ";;mobile",
                        hotel_name=hotel_name,
                        checkin_date=checkin_date,
                        length_of_stay=length_of_stay,
                        scrapped_url=page.url,
                    )
                    logger.debug("Price object: %s", dict(price_obj))
                    prices.append(dict(price_obj))
                await context.close()
                logger.debug("Closed context and closing browser")
                await browser.close()
            except Exception as e:
                logger.error(
                    f"Error during mobile price scraping for hotel {hotel_name} with checkin date {checkin_date} and length of stay {length_of_stay}: {e}"
                )
                price_obj = Price(
                    text=f"Failed to load page {e}",
                    hotel_name=hotel_name,
                    checkin_date=checkin_date,
                    length_of_stay=length_of_stay,
                    scrapped_url=page.url,
                )
                logger.debug("Price object: %s", dict(price_obj))
                prices.append(dict(price_obj))

            return prices


def execute_price_scraping_workflow(inputs):
    results = [
        fetch_google_hotel_prices_desktop(
            hotel_name="Candlewood Suites New York City- Times Square, an IHG Hotel",
            checkin_date="2024-04-12",
            length_of_stay=1,
        )
        for i in range(1)
    ]
    return results


if __name__ == "__main__":
    inputs = [
        {
            "hotel_name": "B Ocean Resort",
            "checkin_date": "2024-03-14",
            "length_of_stay": 1,
        },
        {
            "hotel_name": "hilton london kensington",
            "checkin_date": "2024-04-15",
            "length_of_stay": 1,
        },
        {
            "hotel_name": "chelsea hotel, toronto",
            "checkin_date": "2024-04-20",
            "length_of_stay": 1,
        },
    ]
    failed_inputs = [
        {
            "hotel_name": "DoubleTree By Hilton New York Downtown",
            "checkin_date": "2024-02-09",
            "length_of_stay": 1,
        },
        {
            "hotel_name": "DoubleTree By Hilton New York Downtown",
            "checkin_date": "2024-02-09",
            "length_of_stay": 1,
        },
        {
            "hotel_name": "Candlewood Suites New York City-Times Square, An IHG Hotel",
            "checkin_date": "2024-02-09",
            "length_of_stay": 1,
        },
        {
            "hotel_name": "Hilton Toronto",
            "checkin_date": "2024-04-12",
            "length_of_stay": 1,
        },
        {
            "hotel_name": "Hilton Toronto",
            "checkin_date": "2024-04-13",
            "length_of_stay": 1,
        },
        {
            "hotel_name": "Boardwalk Beach Resort By Panhandle Getaways",
            "checkin_date": "2024-03-11",
            "length_of_stay": 1,
        },
        {
            "hotel_name": "Boardwalk Beach Resort By Panhandle Getaways",
            "checkin_date": "2024-03-12",
            "length_of_stay": 1,
        },
        {
            "hotel_name": "Boardwalk Beach Resort By Panhandle Getaways",
            "checkin_date": "2024-03-13",
            "length_of_stay": 1,
        },
    ]
    execute_price_scraping_workflow(inputs)
