from dataclasses import asdict
from datetime import date, timedelta
import random

from app import handler, flight_queries_table, aas, acs, dls
from process import find_air_bounds
from dynamo import FlightQuery

# This is the email that you want to receive notification.
# You need to verify it in SES first before using it.
EMAIL = "your_email@gmail.com"

# Test run Lambda function.
# handler(1, 1)

# Run search locally. You can also use this block to add routes in DynamoDB.
dry = False
start_date = date(2023, 9, 1)
end_date = date(2023, 10, 6)
for origin in {"SFO"}:
    for dest in {"ICN", "NRT", "KIX", "HKG"}:
        cur_date = start_date
        while cur_date <= end_date:
            query = FlightQuery(
                id=str(random.randint(0, 100000000)),
                origin=origin,
                destination=dest,
                date=cur_date.isoformat(),
                num_passengers=1,
                cabin_class="BIZ",
                max_stops=0,
                max_duration=20,
                max_ac_points=150000,
                max_aa_points=150000,
                max_dl_points=150000,
                exact_airport=False,
                email=EMAIL,
                last_run=0,
            )

            # Print results directly
            print(list(find_air_bounds(aas, acs, dls, query)))

            # Add query to DynamoDB
            # print(f"Adding {origin}-{dest} on {cur_date}")
            # if not dry:
            #     flight_queries_table.put_item(asdict(query))

            # Update date for the next run. DO NOT COMMENT THIS LINE.
            cur_date += timedelta(days=1)
