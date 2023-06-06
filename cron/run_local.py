from datetime import date, timedelta
import random

from aa_searcher import Aa_Searcher
from ac_searcher2 import Ac_Searcher2
from dl_searcher import Dl_Searcher

from process import find_air_bounds
from flight_query import FlightQuery

# This is the email that you want to receive notification.
# You need to verify it in SES first before using it.
EMAILS = ["your_email@gmail.com"]

# Test run Lambda function.
# from app import handler, add_query_in_dynamo
# handler({
#     "limit": 10,
#     "min_run_gap": 3600,
#     "max_workers": 1
# }, None)

# Test run Cloud Functions.
from main import run, add_query_in_datastore
run({
    "limit": 10,
    "min_run_gap": 3600,
    "max_workers": 1
})


# Run search locally. You can also use this block to add routes in DynamoDB.
aas = Aa_Searcher()
acs = Ac_Searcher2()
dls = Dl_Searcher()

dry = False
start_date = date(2023, 10, 1)
end_date = date(2023, 10, 4)
for origin in {"SFO"}:
    for dest in {"LAX"}:
        cur_date = start_date
        while cur_date <= end_date:
            query = FlightQuery(
                id=str(random.randint(0, 100000000)),
                origin=origin,
                destination=dest,
                date=cur_date.isoformat(),
                num_passengers=1,
                cabin_class="ECO",
                max_stops=0,
                max_duration=20,
                max_ac_points=100000,
                max_aa_points=100000,
                max_dl_points=100000,
                exact_airport=False,
                exclude_airports=None,
                depart_window=None,
                email=EMAILS,
                last_run=0,
            )

            # Print results directly
            # print(list(find_air_bounds(aas, acs, dls, query)))

            # Add query to DynamoDB
            # print(f"Adding {origin}-{dest} on {cur_date}")
            # if not dry:
            #     add_query_in_dynamo(query)

            # Add query to Datastore
            print(f"Adding {origin}-{dest} on {cur_date}")
            if not dry:
                add_query_in_datastore(query)

            # Update date for the next run. DO NOT COMMENT THIS LINE.
            cur_date += timedelta(days=1)
