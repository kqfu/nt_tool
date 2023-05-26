from dataclasses import dataclass, asdict
import time
from typing import Optional, Union


@dataclass
class FlightQuery:
    id: str
    origin: str
    destination: str
    date: str  # YYYY-MM-DD
    email: Union[str, list[str]]
    last_run: int  # unix epoch time

    num_passengers: Optional[int] = None
    cabin_class: Optional[str] = None  # one of ECO, PRE, BIZ, FIRST
    max_stops: Optional[int] = None
    max_duration: Optional[int] = None  # in hours
    max_aa_points: Optional[int] = None
    max_ac_points: Optional[int] = None
    max_dl_points: Optional[int] = None
    exact_airport: Optional[bool] = None
    exclude_airports: Optional[list[str]] = None
    depart_window: Optional[list[int]] = None  # 2 integers in hours

    def short_string(self):
        return f"{self.origin}-{self.destination} {self.date}"


def add_query_in_dynamo(flight_queries_table, query: FlightQuery):
    flight_queries_table.put_item(Item=asdict(query))


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
