from datetime import datetime

import peewee

from api.database_manager import DatabaseManager
from local_settings import DATABASE

database_manager = DatabaseManager(
    database_name=DATABASE["name"],
    user=DATABASE["user"],
    password=DATABASE["password"],
    host=DATABASE["host"],
    port=DATABASE["port"],
)


class MyBaseModel(peewee.Model):
    create_date = peewee.DateField(default=datetime.now)
    updated_date = peewee.DateTimeField(default=datetime.now)
    class Meta:
        database = database_manager.db


class Coin(MyBaseModel):
    cap_id = peewee.IntegerField(unique=True, verbose_name="Coin id")
    name = peewee.CharField(max_length=100, verbose_name="Coin name")
    symbol = peewee.CharField(max_length=100, verbose_name="Coin symbol", null=True)
    slug = peewee.CharField(max_length=100, verbose_name="Coin slug", null=True)
    rank = peewee.IntegerField(verbose_name="Coin rank", null=True)
    price = peewee.FloatField(verbose_name="Price", null=True)
    volume = peewee.CharField(verbose_name="Volume", null=True)
    market_cap = peewee.CharField(verbose_name="Market Cap", null=True)
    dominance = peewee.FloatField(verbose_name="Dominance", null=True)
    max_supply = peewee.CharField(verbose_name="Max supply", null=True)
    circulating_supply = peewee.CharField(verbose_name="Circulating supply", null=True)
    total_supply = peewee.CharField(verbose_name="Total supply", null=True)
    market_pairs = peewee.IntegerField(verbose_name="Market Pairs", null=True)
    is_active = peewee.BooleanField(default=True, null=True)
    first_date = peewee.DateField(verbose_name="Coin first date", null=True)
    last_date = peewee.DateField(verbose_name="Coin last date", null=True)
    category = peewee.CharField(verbose_name="Category", null=True)
    description = peewee.TextField(verbose_name="Description", null=True)
    logo = peewee.BlobField(verbose_name="Logo", null=True)
    subreddit = peewee.CharField(verbose_name="Subreddit", null=True)
    notice = peewee.TextField(verbose_name="Notice", null=True)
    platform = peewee.TextField(verbose_name="Platform", null=True)
    twitter_username = peewee.CharField(verbose_name="Twitter Username", null=True)
    is_hidden = peewee.BooleanField(default=False, verbose_name="Hidden", null=True)
    date_launched = peewee.DateField(verbose_name="Date Launched", null=True)
    self_reported_circulating_supply = peewee.BooleanField(
        default=False, verbose_name="Self Reported Circulating Supply", null=True
    )
    self_reported_tags = peewee.TextField(verbose_name="Self Reported Tags", null=True)
    self_reported_market_cap = peewee.FloatField(
        verbose_name="Self Reported Market Cap", null=True
    )
    infinity_supply = peewee.BooleanField(
        default=False, verbose_name="Infinity Supply", null=True
    )


class Platform(MyBaseModel):
    name = peewee.CharField()
    coin = peewee.ForeignKeyField(
        Coin,
        on_delete="CASCADE",
        verbose_name="coin",
    )


class ContractAddress(MyBaseModel):
    contract_address = peewee.CharField(verbose_name="Contract Address")
    platform = peewee.ForeignKeyField(
        Platform,
        on_delete="CASCADE",
        verbose_name="platform",
        null=True,
    )


class Tags(MyBaseModel):
    name = peewee.CharField(max_length=100, verbose_name="Name")
    category = peewee.CharField(max_length=100, verbose_name="Category")


class URLs(MyBaseModel):
    coin = peewee.ForeignKeyField(
        Coin,
        on_delete="CASCADE",
        verbose_name="coin",
    )
    name = peewee.CharField(max_length=100, verbose_name="Name")
    url = peewee.CharField(verbose_name="URL", null=True)


class CoinTag(MyBaseModel):
    data = peewee.ForeignKeyField(Coin)
    other = peewee.ForeignKeyField(Tags)


class CoinUrl(MyBaseModel):
    data = peewee.ForeignKeyField(Coin)
    other = peewee.ForeignKeyField(URLs)


class CoinContractAddress(MyBaseModel):
    data = peewee.ForeignKeyField(Coin)
    other = peewee.ForeignKeyField(ContractAddress)


if __name__ == "__main__":
    models = [
        Coin,
        Platform,
        ContractAddress,
        Tags,
        URLs,
        CoinTag,
        CoinUrl,
        CoinContractAddress,
    ]
    try:
        for model in models:
            if not model.table_exists():
                database_manager.create_tables(models=[model])
    except peewee.DatabaseError as error:
        print(f"Database Error : {error}")
    finally:
        database_manager.db.close()
