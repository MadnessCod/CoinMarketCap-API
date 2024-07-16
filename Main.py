import requests
from datetime import datetime

from celery import Celery
from local_settings import API_KEY, BROKER_URL, BACKEND_URL
from database_creation import Map

app = Celery('CoinMarketCapTasks', broker=BROKER_URL, backend=BACKEND_URL)
url = 'https://pro-api.coinmarketcap.com'

headers = {
    'Accepts': 'application/json',
    'Accept-Encoding': 'gzip, deflate,',
    'X-CMC_PRO_API_KEY': API_KEY,
}

endpoints = {
    1: '/v1/cryptocurrency/map',
    2: '/v2/cryptocurrency/info',
    # 3: '/v1/cryptocurrency/listings/latest',
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


@app.task
def convert_date(date_string):
    data_time_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    return data_time_obj.date()


@app.task
def write_to_database(data):
    for coin in data['data']:
        Map.get_or_create(
            cap_id=coin['id'],
            rank=int(coin['rank']),
            name=coin['name'],
            symbol=coin['symbol'],
            slug=coin['slug'],
            is_active=bool(coin['is_active']),
            first_date=convert_date(coin['first_historical_data']),
            last_date=convert_date.delay(coin['last_historical_data']).get(),
        )


class CoinMarketCapApi:
    def __init__(self, endpoint):
        self.headers = headers
        self.endpoint = endpoint

    def get(self):
        try:
            response = requests.get(url=url + self.endpoint, headers=self.headers)
        except requests.exceptions.RequestException as error:
            print(f'Request Error {error}')
        else:
            return write_to_database.delay(response.json())

    def metadata(self):
        maps = Map.select()
        query = ''
        for counter, coin in enumerate(maps):
            query += f'{coin.cap_id},'
            if counter == 100:
                response = requests.get(url=url + self.endpoint[2] + query, headers=self.headers)
                query = ''








first_try = CoinMarketCapApi(endpoint=endpoints[1])
first_try.get()
