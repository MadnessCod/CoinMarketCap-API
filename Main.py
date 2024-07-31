import time
import requests

from celery import chain
from tasks import metadata_database, write_to_database, latest_database, download, convert_date
from database_creation import Coin
from local_settings import API_KEY

base_url = "https://pro-api.coinmarketcap.com"

headers = {
    "Accepts": "application/json",
    "Accept-Encoding": "gzip, deflate,",
    "X-CMC_PRO_API_KEY": API_KEY,
}

endpoints = {
    1: "/v1/cryptocurrency/map",
    2: "/v2/cryptocurrency/info",
    3: "/v1/cryptocurrency/listings/latest",
    # 4: '/v1/cryptocurrency/listings/historical',
    # 5: '/v2/cryptocurrency/quotes/latest',
    # 6: '/v2/cryptocurrency/quotes/historical',
    # 7: '/v2/cryptocurrency/market-pairs/latest',
    # 8: '/v2/cryptocurrency/ohlcv/latest',
    # 9: '/v2/cryptocurrency/ohlcv/historical',
    # 10: '/v2/cryptocurrency/price-performance-stats/latest',
    # 11: '/v1/cryptocurrency/categories',
    # 12: '/v1/cryptocurrency/category',
    # 13: '/v1/cryptocurrency/airdrops',
    # 14: '/v1/cryptocurrency/airdrop',
    # 15: '/v1/cryptocurrency/trending/latest',
    # 16: '/v1/cryptocurrency/trending/most-visited',
    # 17: '/v1/cryptocurrency/trending/gainers-losers',
}


def debug(*msg, separator=True):
    print(*msg)
    if separator:
        print('_' * 40)


def request(url, parameters=None):
    try:
        response = requests.get(url=url, headers=headers, params=parameters)
    except requests.exceptions.HTTPError as http_error:
        print(f'HTTP Error {http_error}')
    except requests.exceptions.RequestException as error:
        print(f"Requests Error {error}")
    else:
        if response.status_code == requests.codes.ok:
            if response is not None:
                return response


def get():
    map_url = f"{base_url}{endpoints[1]}"
    response = request(map_url)
    if response:
        response = response.json()
        for coin in response['data']:
            chain(
                convert_date.s(coin['first_historical_data']),
                convert_date.s(coin['last_historical_data']),
                write_to_database.s(coin),
            ).apply_async()


def metadata_get():
    coins = Coin.select()
    query = "?id="
    for counter, coin in enumerate(coins):
        query += f"{coin.cap_id},"
        if counter % 100 == 0 and counter != 0:
            metadata_url = f"{base_url}{endpoints[2]}{query[:-1]}"
            response = request(metadata_url)
            time.sleep(1)
            query = "?id="
            if response:
                response = response.json()
                for count, entry in enumerate(response['data'].values()):
                    print(entry['logo'])
                    chain(download.s(entry['logo']), metadata_database.s(entry)).apply_async()


def latest():
    coins = Coin.select()
    for number in range(1, len(coins), 5000):
        parameters = {
            "start": number,
            "limit": 5000,
        }
        latest_url = f'{base_url}{endpoints[2]}'
        response = request(latest_url, parameters=parameters)
        if response:
            response = response.json()
            for coin in response['data']:
                latest_database.delay(coin)
