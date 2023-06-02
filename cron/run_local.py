from datetime import date, timedelta
import random

from app import handler, flight_queries_table, aas, acs, dls
from process import find_air_bounds
from dynamo import FlightQuery, add_query_in_dynamo

# This is the email that you want to receive notification.
# You need to verify it in SES first before using it.
EMAILS = ["your_email@gmail.com"]

# Test run Lambda function.
# handler({
#     "limit": 10,
#     "min_run_gap": 3600,
#     "max_workers": 1
# }, None)

# Run search locally. You can also use this block to add routes in DynamoDB.
dry = False
start_date = date(2023, 10, 4)
end_date = date(2023, 10, 4)
for origin in {"HND"}:
    for dest in {"PVG"}:
        cur_date = start_date
        while cur_date <= end_date:
            query = FlightQuery(
                id=str(random.randint(0, 100000000)),
                origin=origin,
                destination=dest,
                date=cur_date.isoformat(),
                num_passengers=1,
                cabin_class="ECO",
                max_stops=1,
                max_duration=20,
                max_ac_points=50000,
                max_aa_points=50000,
                max_dl_points=50000,
                exact_airport=True,
                exclude_airports=None,
                depart_window=[17, 24],
                email=EMAILS,
                last_run=0,
            )

            # Print results directly
            print(list(find_air_bounds(aas, acs, dls, query)))

            # Add query to DynamoDB
            # print(f"Adding {origin}-{dest} on {cur_date}")
            # if not dry:
            #     add_query_in_dynamo(flight_queries_table, query)

            # Update date for the next run. DO NOT COMMENT THIS LINE.
            cur_date += timedelta(days=1)
