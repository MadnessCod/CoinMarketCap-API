from datetime import datetime

import peewee
import requests

from celery import shared_task
from api.database_creation import (
    Coin,
    Platform,
    URLs,
    ContractAddress,
    Tags,
    CoinTag,
    CoinUrl,
    CoinContractAddress,
)


def debug(*msg, separator=True):
    print(*msg)
    if separator:
        print('_' * 40)


@shared_task
def convert_date(date_string):
    if date_string:
        data_time_obj = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
        return data_time_obj.date()
    return None


@shared_task
def write_to_database(dates, coin):
    first_date, last_date = dates
    try:
        coin_instance, _ = Coin.get_or_create(
            cap_id=coin["id"],
            name=coin["name"],
            symbol=coin["symbol"],
            slug=coin["slug"],
            rank=int(coin["rank"]),
            is_active=bool(coin["is_active"]),
            first_date=first_date,
            last_date=last_date,
        )
    except peewee.IntegrityError:
        pass


@shared_task(ignore_result=True)
def metadata_database(image, coin):
    coin_instance = Coin.get(Coin.cap_id == coin["id"])
    contract_addresses = list()
    for address in coin["contract_address"]:
        try:
            inside_coin_instance = Coin.get(
                Coin.cap_id == int(address["platform"]["coin"]["id"])
            )
        except (peewee.DoesNotExist, peewee.IntegrityError):
            inside_coin_instance = Coin.create(
                cap_id=int(address["platform"]["coin"]["id"]),
                name=address["platform"]["coin"]["name"],
                symbol=address["platform"]["coin"]["symbol"],
                slug=address["platform"]["coin"]["slug"],
            )

        platform_instance, _ = Platform.get_or_create(
            name=address["platform"]["name"],
            coin=inside_coin_instance,
        )

        contract_instance, _ = ContractAddress.get_or_create(
            contract_address=address["contract_address"],
            platform=platform_instance,
        )
        contract_addresses.append(contract_instance)

    tag_instances = list()
    try:
        for tag, category in zip(coin["tags"], coin["tag-groups"]):
            instance, _ = Tags.get_or_create(
                name=tag,
                category=category,
            )
            tag_instances.append(instance)
    except TypeError:
        print("there are no tags or groups")

    url_instance = list()
    for name, links in coin["urls"].items():
        for link in links:
            instance, _ = URLs.get_or_create(
                coin=coin_instance,
                name=name,
                url=link,
            )
            url_instance.append(instance)
    try:
        Coin.update(
            category=coin["category"],
            description=coin["description"],
            logo=image,
            subreddit=coin["subreddit"],
            notice=coin["notice"],
            platform=coin["platform"],
            twitter_username=coin["twitter_username"],
            is_hidden=bool(coin["is_hidden"]),
            date_launched=convert_date(coin["date_launched"]),
            self_reported_circulating_supply=bool(
                coin["self_reported_circulating_supply"]
            ),
            self_reported_tags=(
                ", ".join(coin["self_reported_tags"])
                if coin["self_reported_tags"]
                else None
            ),
            self_reported_market_cap=coin["self_reported_market_cap"],
            infinity_supply=bool(coin["infinite_supply"]),
        ).where(Coin.cap_id == coin["id"]).execute()
    except peewee.DataError as e:
        print(f"Error : {e}")
    else:
        for entry in url_instance:
            CoinUrl.get_or_create(
                data=coin_instance,
                other=entry,
            )
        for entry in tag_instances:
            CoinTag.get_or_create(
                data=coin_instance,
                other=entry,
            )
        for entry in contract_addresses:
            CoinContractAddress.get_or_create(data=coin_instance, other=entry)


@shared_task(ignore_result=True)
def latest_database(coin):
    try:
        Coin.get(Coin.cap_id == coin["id"])
    except peewee.DoesNotExist:
        Coin.create(
            cap_id=coin["id"],
            name=coin["name"],
            symbol=coin["symbol"],
            slug=coin["slug"],
        )
    try:
        Coin.update(
            price=float(coin["quote"]["USD"]["price"]),
            volume=coin["quote"]["USD"]["volume_24h"],
            market_cap=coin["quote"]["USD"]["market_cap"],
            dominance=float(coin["quote"]["USD"]["market_cap_dominance"]),
            max_supply=coin["max_supply"],
            circulating_supply=coin["circulating_supply"],
            total_supply=coin["total_supply"],
            market_pairs=int(coin["num_market_pairs"]),
        ).where(Coin.cap_id == coin["id"]).execute()
    except KeyError as e:
        print(f'Key Error : {e}')


@shared_task
def download(download_url):
    try:
        response = requests.get(download_url)
    except requests.exceptions.RequestException as e:
        print(f"Requests Error {e}")
        return None
    else:
        return response.content
