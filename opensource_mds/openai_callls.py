import instructor
import base64
import logging
import os
from openai import AsyncOpenAI
import asyncio
from pydantic import Field, BaseModel
from typing import List
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

# Add logger
logging.basicConfig()
logger = logging.getLogger("app")
logger.setLevel("INFO")


class Rate(BaseModel):
    # Hotel Rate. Each rate is a list of 3 elements: [PROVIDER, CURRENCY, RATE]
    provider: str = Field(description="Provider name", example="Booking.com")
    currency: str = Field(description="Currency", example="USD")
    rate: float = Field(description="Rate", example=110.0)

class RateList(BaseModel):
    # List of Hotel Rates. Each rate is a list of 3 elements: [PROVIDER, CURRENCY, RATE]
    hotel_name: str = Field(description="Hotel name", example="Ace Hotel, NYC")
    checkin_date: str = Field(description="Checkin date", example="2024-04-12")
    length_of_stay: int = Field(description="Length of stay", example=1)
    rates: List[Rate] = Field(
        description="List of Hotel Rates. Each rate is a list of 3 elements: [PROVIDER, CURRENCY, RATE]",
        example=[
            Rate(provider="Booking.com", currency="USD", rate=110.0),
            Rate(provider="Expedia", currency="USD", rate=90.0)
        ])
    error: bool = Field(default=False, description="Error flag. True if there was an error, False otherwise.")


client = instructor.patch(AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY")))


async def ask_ai(content) -> RateList:
    """
    Function to ask the AI to parse all the content
    """
    return await client.chat.completions.create(
        model="gpt-4",
        response_model=RateList,
        messages=[
            {
                "role": "system",
                "content": "Extract and resolve a list of rates from the following content (scraped of google hotel search prices). Discard a list item if it is a hotel name or a room type/description. Content:",
            },
            {
                "role": "user",
                "content": content,
            },
        ],
    )  # type: ignore



async def main():
    result = await ask_ai("""
- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Expedia.com
$299
Visit site
Verified guest ratings · Easy rate comparison
Customer support: 24/7 Phone · Email · Facebook · X (Twitter)
Earn rewards towards future trips

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Standard Room, 1 Double Bed, Multiple View (Single)
1 double bed · 1 single bed · Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Standard Room, 1 Double Bed, Multiple View (Small)
1 double bed · Free cancellation until Apr 11
$319
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Ace Hotel New York
 Official Site
Free cancellation until Apr 11
$293
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Booking.com
Free cancellation until Apr 10
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Hotels.com
Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Ace Hotel New York
 Official Site
Free cancellation until Apr 11
$293
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Despegar
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Booking.com
Free cancellation until Apr 10
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Expedia.com
Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Hotels.com
Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Trip.com
Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text CheapHotelsHub.com
4 guests
$323
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Travelocity.com
Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Reserving.com
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Super.com
$323
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Orbitz.com
Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Hotwire.com
Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text eDreams
Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Bluepillow.com
Free cancellation until Apr 10
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Vio.com
Free cancellation until Apr 11
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Clicktrip.com
$299
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text Hotels In America
$323
Visit site

- Hotel Name ace hotel, nyc, Checkin Date 2024-04-12, Length of Stay 1, Price Text CheapTickets.com
Free cancellation until Apr 11
$299
Visit site
           """)
    print(result)
    return result

if __name__ == "__main__":
    asyncio.run(main())


import instructor
import base64
import logging
import os
from openai import AsyncOpenAI, OpenAI
import asyncio
from pydantic import Field, BaseModel
from typing import List
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

# Add logger
logging.basicConfig()
logger = logging.getLogger("app")
logger.setLevel("INFO")


class Rate(BaseModel):
    # Hotel Rate. Each rate is a list of 3 elements: [PROVIDER, CURRENCY, RATE]
    provider: str = Field(description="Provider name", example="Booking.com")
    currency: str = Field(description="Currency", example="USD")
    rate: float = Field(description="Rate", example=110.0)

class RateList(BaseModel):
    # List of Hotel Rates. Each rate is a list of 3 elements: [PROVIDER, CURRENCY, RATE]
    rates: List[Rate] = Field(
        description="List of Hotel Rates. Each rate is a list of 3 elements: [PROVIDER, CURRENCY, RATE]",
        example=[
            Rate(provider="Booking.com", currency="USD", rate=110.0),
            Rate(provider="Expedia", currency="USD", rate=90.0)
        ])
    error: bool = Field(default=False, description="Error flag. True if there was an error, False otherwise.")

def encode_image(image_path) -> str:
    """Encode image to base64 string."""
    with open(image_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    return encoded_string.decode("utf-8")


client_image = instructor.patch(
    AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY")),
    mode=instructor.Mode.MD_JSON
)


async def read_prices(image_file_path: str) -> RateList:
    """Read prices from image."""
    encoded_string = encode_image(image_file_path)
    return await client_image.chat.completions.create(
        model='gpt-4-vision-preview',
        response_model=RateList,
        max_tokens=4000,
        temperature=0.0,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Read prices from screenshot of google hotel search for chelsea hotel toronto. Read all the prices you can find."
                    },
                    {
                        "type": "image_url",
                        "image_url": f"data:image/png;base64,{encoded_string}"
                    }
                ]
            }
        ]
    )

async def main():
    result = await read_prices("full_page_screenshot.png")
    print(result)
    return result



if __name__ == "__main__":
    asyncio.run(main())
