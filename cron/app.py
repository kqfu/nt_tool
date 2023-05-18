import boto3

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


def handler(event, context):
    # EventBridge puts payload JSON in `detail` field.
    d = event.get('detail', {})
    limit = d.get("limit", 100)
    min_run_gap = d.get("min_run_gap", 3600)

    for q in fetch_all_queries_from_dynamo(flight_queries_table, limit, min_run_gap):
        for air_bound in find_air_bounds(aas, acs, dls, q):
            send_notification(air_bound, q, ses_client)
        update_last_run_time(flight_queries_table, q)

    return "success"
