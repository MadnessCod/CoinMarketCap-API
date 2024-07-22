import json

import requests
from datetime import datetime

from celery import Celery
from local_settings import API_KEY, BROKER_URL, BACKEND_URL
from database_creation import (Map, Coin, Platform, URLs,
                               ContractAddress, MetaData, Tags,
                               MetadataTag, MetadataUrl, MetadataContractAddress)

app = Celery('CoinMarketCapTasks', broker=BROKER_URL, backend=BACKEND_URL)
base_url = 'https://pro-api.coinmarketcap.com'

headers = {
    'Accepts': 'application/json',
    'Accept-Encoding': 'gzip, deflate,',
    'X-CMC_PRO_API_KEY': API_KEY,
}

endpoints = {
    1: '/v1/cryptocurrency/map',
    2: '/v2/cryptocurrency/info',
    3: '/v1/cryptocurrency/listings/latest',
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
    if date_string:
        data_time_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        return data_time_obj.date()
    return None


@app.task
def write_to_database(data):
    for coin in data['data']:
        coin_instance, _ = Coin.get_or_create(
            cap_id=coin['id'],
            name=coin['name'],
            symbol=coin['symbol'],
            slug=coin['slug'],
        )
        Map.get_or_create(
            rank=int(coin['rank']),
            is_active=bool(coin['is_active']),
            first_date=convert_date.delay(coin['first_historical_data']).get(),
            last_date=convert_date.delay(coin['last_historical_data']).get(),
            coin=coin_instance,
        )


@app.task
def relate(metadata, others, database_name):
    for other in others:
        database_name.get_or_create(
            data=metadata,
            other=other,
        )


@app.task
def metadata_database(data):
    for coin in data['data'].values():
        coin_instance = Coin.get(Coin.cap_id == coin['id'])
        contract_addresses = list()
        for address in coin['contract_address']:
            inside_coin_instance, _ = Coin.get_or_create(
                cap_id=address['platform']['coin']['id'],
                name=address['platform']['coin']['name'],
                symbol=address['platform']['coin']['symbol'],
                slug=address['platform']['coin']['slug'],
            )
            platform_instance, _ = Platform.get_or_create(
                name=address['platform']['name'],
                coin=inside_coin_instance,
            )
            contract_instance, _ = ContractAddress.get_or_create(
                contract_address=address['contract_address'],
                platform=platform_instance,
            )
            contract_addresses.append(contract_instance)

        tag_instances = tuple()
        try:
            for tag, category in zip(coin['tags'], coin['tag-groups']):
                instance, _ = Tags.get_or_create(
                    name=tag,
                    category=category,
                )
                tag_instances += instance
        except TypeError:
            print('there are no tags or groups')

        url_instance = list()
        for name, links in coin['urls'].items():
            for link in links:
                instance, _ = URLs.get_or_create(
                    coin=coin_instance,
                    name=name,
                    url=link,
                )
                url_instance.append(instance)
        metadata_instance, _ = MetaData.get_or_create(
            coin=coin_instance,
            category=coin['category'],
            description=coin['description'],
            logo=coin['logo'],
            subreddit=coin['subreddit'],
            notice=coin['notice'],
            platform=coin['platform'],  # this need fixing
            twitter_username=coin['twitter_username'],
            is_hidden=bool(coin['is_hidden']),
            date_launched=convert_date(coin['date_launched']),
            self_reported_circulating_supply=bool(coin['self_reported_circulating_supply']),
            self_reported_tags=''.join(coin['self_reported_tags']) if coin['self_reported_tags'] else None,
            self_reported_market_cap=coin['self_reported_market_cap'],
            infinity_supply=coin['infinite_supply'],
        )

        relate.delay(metadata_instance, url_instance, MetadataUrl)
        relate.delay(metadata_instance, tag_instances, MetadataTag)
        relate.delay(metadata_instance, contract_addresses, MetadataContractAddress)


@app.task
def download(download_url):
    try:
        response = requests.get(download_url)
    except requests.exceptions.RequestException as e:
        print(f'Requests Error {e}')
    else:
        with open('image.jpg', 'wb') as f:
            f.write(response.content)


class CoinMarketCapApi:
    def __init__(self, endpoint):
        self.url = 'https://pro-api.coinmarketcap.com'
        self.headers = headers
        self.endpoint = endpoint

    def request(self, url, parameters=None):
        try:
            response = requests.get(url=url, headers=self.headers, params=parameters)
        except requests.exceptions.RequestException as error:
            print(f'Requests Error {error}')
        else:
            return response.json()

    def get(self):
        map_url = f'{self.url}{self.endpoint}'
        response = self.request(map_url)
        write_to_database.delay(response.json())

    def metadata_get(self):
        coins = Coin.select()
        query = '?id='
        for counter, coin in enumerate(coins):
            query += f'{coin.cap_id},'
            if counter % 100 == 0 and counter != 0:
                metadata_url = f'{self.url}{self.endpoint}{query[:-1]}'
                response = self.request(metadata_url)
                query = '?id='
                metadata_database.delay(response.json())

    def latest(self):
        coins = Coin.select()
        for number in range(1, len(coins), 5000):
            parameters = {
                'start': number,
                'limit': 5000,
            }
            latest_url = self.url + self.endpoint
            response = self.request(latest_url, parameters)
