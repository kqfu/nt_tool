import boto3
from concurrent.futures import ThreadPoolExecutor
import logging
import time

from flight_query import FlightQuery
from process import find_air_bounds, update_last_run_time
from aa_searcher import Aa_Searcher
from ac_searcher2 import Ac_Searcher2
from dl_searcher import Dl_Searcher
from nt_models import AirBound

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
flight_queries_table = dynamodb.Table('flight_queries')
ses_client = boto3.client('ses')

aas = Aa_Searcher()
acs = Ac_Searcher2()
dls = Dl_Searcher()


def run_one_query(q):
    air_bounds = find_air_bounds(aas, acs, dls, q)
    update_last_run_time(flight_queries_table, q)
    for air_bound in air_bounds:
        send_notification(air_bound, q)


def add_query_in_dynamo(flight_queries_table, query: FlightQuery):
    flight_queries_table.put_item(Item=query.to_dict())


def fetch_all_queries_from_dynamo(flight_queries_table, limit=None, min_run_gap=None) \
        -> list[FlightQuery]:
    kwargs = {
        'FilterExpression': 'last_run < :last_run',
        'ExpressionAttributeValues': {
            ':last_run': int(time.time()) - min_run_gap
        }
    }

    queries = []
    resp = flight_queries_table.scan(**kwargs)
    for item in resp.get("Items"):
        queries.append(FlightQuery(**item))
        if len(queries) >= limit:
            return queries

    while resp.get("LastEvaluatedKey"):
        resp = flight_queries_table.scan(
            ExclusiveStartKey=resp.get("LastEvaluatedKey"),
            **kwargs
        )
        for item in resp.get("Items"):
            queries.append(FlightQuery(**item))
            if len(queries) >= limit:
                return queries

    return queries


def send_notification(air_bound: AirBound, q: FlightQuery):
    resp = ses_client.list_identities(IdentityType='EmailAddress')
    if not resp.get("Identities"):
        raise Exception("Cannot send notification because no ses verified identity exists")
    source_email = resp["Identities"][0]
    target_emails = q.email if isinstance(q.email, list) else [q.email]

    logger.info("Sending email for %s", air_bound.to_cust_dict())
    ses_client.send_email(
        Source=source_email,
        Destination={
            'ToAddresses': target_emails,
        },
        Message={
            'Subject': {
                'Data': f'Reward Ticket Found for {q.origin}-{q.destination} on {q.date}'
            },
            'Body': {
                'Text': {
                    'Data': "\n".join([str(x) for x in air_bound.to_flatted_list()])
                }
            }
        }
    )


def handler(event, context):
    limit = event.get("limit", 20)
    min_run_gap = event.get("min_run_gap", 3600)
    max_workers = event.get("max_workers", 2)

    with ThreadPoolExecutor(max_workers) as executor:
        futures = []
        for q in fetch_all_queries_from_dynamo(flight_queries_table, limit, min_run_gap):
            futures.append(executor.submit(run_one_query, q))
        for future in futures:
            future.result()

    return "success"
