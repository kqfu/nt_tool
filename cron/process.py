from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import logging
import time

from aa_searcher import Aa_Searcher
from ac_searcher2 import Ac_Searcher2
from dl_searcher import Dl_Searcher
from flight_query import FlightQuery
from nt_models import CabinClass, AirBound
from nt_parser import convert_aa_response_to_models, convert_ac_response_to_models2, \
    convert_dl_response_to_models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def match_query(air_bound: AirBound, q: FlightQuery):
    if q.max_stops is not None and air_bound.stops > q.max_stops:
        return False
    if q.max_duration and air_bound.excl_duration_in_all_in_seconds > timedelta(
            hours=int(q.max_duration)):
        return False
    if q.exact_airport and not (air_bound.from_to.startswith(q.origin) and
                                air_bound.from_to.endswith(q.destination)):
        return False

    for price in air_bound.price:
        if q.cabin_class and price.cabin_class != CabinClass.from_string(q.cabin_class):
            continue
        if q.num_passengers and price.quota < q.num_passengers:
            continue
        if q.exclude_airports:
            for airport in q.exclude_airports:
                if airport in air_bound.from_to:
                    return False
        if q.depart_window:
            if not (q.depart_window[0] <= air_bound.excl_departure_time.hour <= q.depart_window[1]):
                return False

        if air_bound.engine.upper() == "AA" and price.excl_miles <= q.max_aa_points:
            return True
        if air_bound.engine.upper() == "AC" and price.excl_miles <= q.max_ac_points:
            return True
        if air_bound.engine.upper() == "DL" and price.excl_miles <= q.max_dl_points:
            return True

    # Did not find any price in selected cabin.
    return False


def find_air_bounds(aas: Aa_Searcher, acs: Ac_Searcher2, dls: Dl_Searcher, q: FlightQuery):
    def get_aa_air_bounds():
        response = aas.search_for(q.origin, q.destination, q.date)
        air_bounds = convert_aa_response_to_models(response)
        logger.info("Engine AA returned %d for %s", len(air_bounds), q.short_string())
        return air_bounds

    def get_ac_air_bounds():
        response = acs.search_for(q.origin, q.destination, q.date)
        air_bounds = convert_ac_response_to_models2(response)
        logger.info("Engine AC returned %d for %s", len(air_bounds), q.short_string())
        return air_bounds

    def get_dl_air_bounds():
        response = dls.search_for(q.origin, q.destination, q.date)
        air_bounds = convert_dl_response_to_models(response)
        logger.info("Engine DL returned %d for %s", len(air_bounds), q.short_string())
        return air_bounds

    logger.info('Start searching for %s', q.short_string())

    air_bounds_futures = {}

    with ThreadPoolExecutor() as executor:
        # Search from AA.
        if q.max_aa_points and q.max_aa_points > 0:
            air_bounds_futures['AA'] = executor.submit(get_aa_air_bounds)

        # Search from AC.
        if q.max_ac_points and q.max_ac_points > 0:
            air_bounds_futures['AC'] = executor.submit(get_ac_air_bounds)

        # Search from DL.
        if q.max_dl_points and q.max_dl_points > 0:
            air_bounds_futures['DL'] = executor.submit(get_dl_air_bounds)

    # Process each result and yield if found a match.
    for engine, air_bounds_future in air_bounds_futures.items():
        for air_bound in air_bounds_future.result():
            if match_query(air_bound, q):
                logger.info("Found a match with engine %s:\n%s",
                            engine, "\n".join([str(x) for x in air_bound.to_flatted_list()]))
                yield air_bound
