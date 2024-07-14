import peewee

from database_manager import DatabaseManager
from local_settings import DATABASE

database_manager = DatabaseManager(
    database_name=DATABASE['name'],
    user=DATABASE['user'],
    password=DATABASE['password'],
    host=DATABASE['host'],
    port=DATABASE['port'],
)


class Map(peewee.Model):
    cap_id = peewee.CharField(max_length=10, verbose_name='CoinMarketCap id')
    name = peewee.CharField(max_length=100, verbose_name='Coin name')
    rank = peewee.IntegerField(verbose_name='Coin rank', null=True)
    symbol = peewee.CharField(max_length=100, verbose_name='Coin symbol', null=True)
    slug = peewee.CharField(max_length=100, verbose_name='Coin slug', null=True)
    is_active = peewee.BooleanField(default=True, null=True)
    first_date = peewee.DateField(verbose_name='Coin first date', null=True)
    last_date = peewee.DateField(verbose_name='Coin last date', null=True)

    class Meta:
        database = database_manager.db
        order_by = ['rank']


if __name__ == '__main__':
    try:
        if not Map.table_exists():
            database_manager.create_tables(models=[Map])
    except peewee.DatabaseError as error:
        print(f'Database Error : {error}')
    finally:
        database_manager.db.close()
