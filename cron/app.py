import boto3

from concurrent.futures import ThreadPoolExecutor
from dynamo import fetch_all_queries_from_dynamo
from process import find_air_bounds, send_notification, update_last_run_time
from aa_searcher import Aa_Searcher
from ac_searcher import Ac_Searcher
from dl_searcher import Dl_Searcher

dynamodb = boto3.resource('dynamodb')
flight_queries_table = dynamodb.Table('flight_queries')
ses_client = boto3.client('ses')

aas = Aa_Searcher()
acs = Ac_Searcher()
dls = Dl_Searcher()


def run_one_query(q):
    air_bounds = find_air_bounds(aas, acs, dls, q)
    update_last_run_time(flight_queries_table, q)
    for air_bound in air_bounds:
        send_notification(air_bound, q, ses_client)


def handler(event, context):
    limit = event.get("limit", 100)
    min_run_gap = event.get("min_run_gap", 900)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for q in fetch_all_queries_from_dynamo(flight_queries_table, limit, min_run_gap):
            futures.append(executor.submit(run_one_query, q))
        for future in futures:
            future.result()

    return "success"
