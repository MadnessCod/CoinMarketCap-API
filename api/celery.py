from celery import Celery

from local_settings import BROKER_URL, BACKEND_URL

app = Celery("CoinMarketCapTasks", broker=BROKER_URL, backend=BACKEND_URL)

app.autodiscover_tasks(["api"])
app.broker_connection_retry_on_startup = True
