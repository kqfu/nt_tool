from concurrent.futures import ThreadPoolExecutor
import logging
import time

import functions_framework
from google.cloud import datastore

from flight_query import FlightQuery
from process import find_air_bounds
from aa_searcher import Aa_Searcher
from ac_searcher2 import Ac_Searcher2
from dl_searcher import Dl_Searcher
from nt_models import AirBound

FLIGHT_QUERIES_KIND = "flight_queries"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ds_client = datastore.Client()

aas = Aa_Searcher()
acs = Ac_Searcher2()
dls = Dl_Searcher()


def run_one_query(q):
    air_bounds = find_air_bounds(aas, acs, dls, q)
    update_last_run_time(q)
    for air_bound in air_bounds:
        send_notification(air_bound, q)


def add_query_in_datastore(q: FlightQuery):
    entity = datastore.Entity(ds_client.key(FLIGHT_QUERIES_KIND, q.id))
    d = q.to_dict()
    del d["id"]
    entity.update(d)
    ds_client.put(entity)


def update_last_run_time(q: FlightQuery):
    try:
        entity = ds_client.get(ds_client.key(FLIGHT_QUERIES_KIND, q.id))
        if entity['last_run'] == q.last_run:
            entity['last_run'] = int(time.time())
            ds_client.put(entity)
    except Exception as e:
        logger.warning("Failed to update last run time for %s. Error: %s", q.short_string(), e)


def fetch_all_queries_from_datastore(limit, min_run_gap) \
        -> list[FlightQuery]:
    query = ds_client.query(kind=FLIGHT_QUERIES_KIND)
    query.add_filter("last_run", "<", int(time.time()) - min_run_gap)
    entities = list(query.fetch(limit=limit))
    return [FlightQuery(id=entity.key.name, **dict(entity)) for entity in entities]


def send_notification(air_bound: AirBound, q: FlightQuery):
    logger.info("Send notification with %s", q.short_string())


@functions_framework.cloud_event
def run(event):
    limit = event.get("limit", 20)
    min_run_gap = event.get("min_run_gap", 3600)
    max_workers = event.get("max_workers", 2)

    with ThreadPoolExecutor(max_workers) as executor:
        futures = []
        for q in fetch_all_queries_from_datastore(limit, min_run_gap):
            futures.append(executor.submit(run_one_query, q))
        for future in futures:
            future.result()

    return "success"
