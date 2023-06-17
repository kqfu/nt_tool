from dataclasses import dataclass, asdict
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

    def to_dict(self):
        return asdict(self)
