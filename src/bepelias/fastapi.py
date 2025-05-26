#!/usr/bin/env python
# coding: utf-8

"""
Fastapi part of bePelias geocoder

@author: Vandy Berten (vandy.berten@smals.be)

"""
import os
import sys

import warnings
import re

from urllib.parse import unquote_plus

from typing import Annotated, Union
# from enum import Enum

import logging

from fastapi import FastAPI, Query, Path, Request, Response, status
from fastapi.openapi.utils import get_openapi
from fastapi.responses import RedirectResponse
from typing_extensions import Literal
from pydantic import AfterValidator

from elasticsearch import Elasticsearch  #, NotFoundError
from elasticsearch.exceptions import ElasticsearchWarning




# from bepelias.utils import (log,  vlog,
#                             build_address, to_rest_guidelines,
#                             struct_or_unstruct, advanced_mode,
#                             add_precision, unstructured_mode
#                             )

from bepelias.base import log
from bepelias.base import (geocode, geocode_reverse, geocode_unstructured,
                           get_by_id, search_city, health
                            )

from bepelias.model import (GeocodeOutput, BePeliasError, Health,
                            ReverseGeocodeOutput, SearchCityOutput,
                            GetByIdOutput, BESTID_PATTERN)

from bepelias.pelias import Pelias  #, PeliasException

logging.basicConfig(format='[%(asctime)s]  %(message)s', stream=sys.stdout)

# WARNING : no logs
# INFO : a few logs
# DEBUG : lots of logs

logger = logging.getLogger()

env_log_level = os.getenv('LOG_LEVEL', "HIGH").upper().strip()

log(f"log level: {env_log_level}")


if env_log_level == "LOW":
    logger.setLevel(logging.WARNING)
elif env_log_level == "MEDIUM":
    logger.setLevel(logging.INFO)
elif env_log_level == "HIGH":
    logger.setLevel(logging.DEBUG)
else:
    print(f"Unkown log level '{env_log_level}'. Should be LOW/MEDIUM/HIGH")


log(f"log level: {env_log_level}")


logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

warnings.simplefilter('ignore', ElasticsearchWarning)


env_pelias_host = os.getenv('PELIAS_HOST')
if env_pelias_host:
    logging.debug("get PELIAS_HOST from env: %s", env_pelias_host)
    pelias_host = env_pelias_host
else:
    logging.error("Missing PELIAS_HOST in docker-compose.yml or environment variable")
    sys.exit(1)

env_pelias_elastic = os.getenv('PELIAS_ES_HOST')
if env_pelias_elastic:
    logging.debug("get PELIAS_ES_HOST from env: %s", env_pelias_elastic)
    pelias_es_host = env_pelias_elastic
else:
    logging.error("Missing PELIAS_ES_HOST in docker-compose.yml or environment variable")
    sys.exit(1)

env_pelias_interpol = os.getenv('PELIAS_INTERPOL_HOST')
if env_pelias_interpol:
    logging.debug("get PELIAS_INTERPOL_HOST from env: %s", env_pelias_interpol)
    pelias_interpol_host = env_pelias_interpol
else:
    logging.error("Missing PELIAS_INTERPOL_HOST in docker-compose.yml or environment variable")
    sys.exit(1)


pelias = Pelias(domain_api=pelias_host,
                domain_elastic=pelias_es_host,
                domain_interpol=pelias_interpol_host)


app = FastAPI(version='1.0.0',
              title='bePelias API',
              description="""A service that allows geocoding (postal address cleansing and conversion into geographical coordinates), based on Pelias and BestAddresses.

          Code available on https://github.com/SmalsResearch/bePelias/

          """,
              root_path='/REST/bepelias/v1',
              contact={
                "name": "Vandy BERTEN",
                "url": "https://www.smalsresearch.be",
                "email": "vandy.berten@smals.be"
              },
              )


@app.get("/doc", include_in_schema=False)
async def redirect():
    """ redirect /doc to /docs"""
    response = RedirectResponse(url='/docs')
    return response


##############
#  /geocode  #
##############


@app.get("/geocode", response_model_exclude_none=True, responses={
                status.HTTP_200_OK: {
                    "model": GeocodeOutput,
                    "description": "Model in case of success"
                },
                status.HTTP_500_INTERNAL_SERVER_ERROR: {
                    "model": BePeliasError,
                    "description": "In case an error occurred"
                }
            })
def _geocode(street_name: Annotated[
                            Union[str, None],
                            Query(description="The name of a passage or way through from one location to another (cf. Fedvoc).",
                                  example='Avenue Fonsny',
                                  alias="streetName")] = None,
             house_number: Annotated[
                            Union[str, None],
                            Query(description="An official alphanumeric code assigned to building units, mooring places, stands or parcels (cf. Fedvoc).",
                                  example='20',
                                  alias="houseNumber")] = None,
             post_code: Annotated[
                            Union[str, None],
                            Query(description="The post code (a.k.a postal code, zip code etc.) (cf. Fedvoc).",
                                  example='1060',
                                  alias="postCode")] = None,
             post_name: Annotated[
                            Union[str, None],
                            Query(description="Name with which the geographical area that groups the addresses for postal purposes can be indicated, usually the city (cf. Fedvoc).",
                                  example='Saint-Gilles',
                                  alias="postName")] = None,
             mode: Annotated[
                 Literal["basic", "simple", "advanced"],
                 Query(description="""
How Pelias is used:

- basic: Just call the structured version of Pelias
- simple: Call the structured version of Pelias. If it does not get any result, call the unstructured version
- advanced: Try several variants until it gives a result""")] = "advanced",
             with_pelias_result: Annotated[
                bool,
                Query(description="If True, return Pelias result as such in 'peliasRaw'.",
                      alias="withPeliasResult")
            ] = False,
            request: Request = None,
            response: Response = None):
    """ Single address geocoding"""

    log(f"Geocode ({mode}): {street_name} / {house_number} / {post_code} / {post_name}")

    res = geocode(pelias, street_name, house_number, post_code, post_name, mode, with_pelias_result)

    if "status_code" in res:
        response.status_code = res["status_code"]
    res["self"] = str(request.url)

    return res

###########################
#  /geocode/unstructured  #
###########################


@app.get("/geocode/unstructured", response_model_exclude_none=True, responses={
                status.HTTP_200_OK: {
                    "model": GeocodeOutput,
                    "description": "Model in case of success"
                },
                status.HTTP_500_INTERNAL_SERVER_ERROR: {
                    "model": BePeliasError,
                    "description": "In case an error occurred"
                }
            })
def _geocode_unstructured(address: Annotated[str,
                                             Query(description="The whole address in a single string",
                                                   example='Avenue Fonsny 20, 1060 Saint-Gilles')],
                          mode: Annotated[
                             Literal["basic", "advanced"],
                             Query(description="""
How Pelias is used:

- basic: Just call the structured version of Pelias
- advanced: Try several variants until it gives a result""")] = "advanced",
                          with_pelias_result: Annotated[
                            bool,
                            Query(description="If True, return Pelias result as such in 'peliasRaw'.",
                                  alias="withPeliasResult")
                         ] = False,
                          request: Request = None,
                          response: Response = None):
    """ Single (unstructured) address geocoding
    """

    log(f"Geocode (unstruct - {mode}): {address}")
    res = geocode_unstructured(pelias, address, mode, with_pelias_result)

    if "status_code" in res:
        response.status_code = res["status_code"]
    res["self"] = str(request.url)

    return res

##############
#  /reverse  #
##############


@app.get("/reverse", response_model_exclude_none=True, responses={
                status.HTTP_200_OK: {
                    "model": ReverseGeocodeOutput,
                    "description": "Model in case of success"
                },
                status.HTTP_500_INTERNAL_SERVER_ERROR: {
                    "model": BePeliasError,
                    "description": "In case an error occurred"
                }
            })
def _geocode_reverse(lat: Annotated[float, Query(description="Latitude, in EPSG:4326. Angular distance from some specified circle or plane of reference",
                                                 gt=49.49, lt=51.51,
                                                 example=50.83582)],
                     lon: Annotated[float, Query(description="Longitude, in EPSG:4326. Angular distance measured on a great circle of reference from the intersection " +
                                                             "of the adopted zero meridian with this reference circle to the similar intersection of the meridian passing through the object",
                                                 gt=2.4, lt=6.41,
                                                 example=4.33844)],
                     radius: Annotated[float, Query(description="Distance (in kilometers)",
                                                    gt=0, lt=350,
                                                    example=1)] = 1,
                     size: Annotated[int, Query(description="Maximal number of results (default: 10; maximum: 20)",
                                                gt=0, lt=20,
                                                example=10)] = 10,
                     with_pelias_result: Annotated[
                            bool,
                            Query(description="If True, return Pelias result as such in 'peliasRaw'.",
                                  alias="withPeliasResult")
                         ] = False,
                     request: Request = None,
                     response: Response = None):
    """
    Reverse geocoding

    """

    res = geocode_reverse(pelias, lat, lon, radius, size, with_pelias_result)

    if "status_code" in res:
        response.status_code = res["status_code"]
    res["self"] = str(request.url)

    return res


#################
#  /searchCity  #
#################


@app.get("/searchCity", response_model_exclude_none=True, responses={
                status.HTTP_200_OK: {
                    "model": SearchCityOutput,
                    "description": "Found one or several matches for city/postal code"
                },
                status.HTTP_500_INTERNAL_SERVER_ERROR: {
                    "model": BePeliasError,
                    "description": "In case an error occurred"
                }
            })
def _search_city(
            post_code: Annotated[
                            Union[str, None],
                            Query(description="The post code (a.k.a postal code, zip code etc.) (cf. Fedvoc).",
                                  example='1060',
                                  alias="postCode")] = None,
            city_name: Annotated[
                            Union[str, None],
                            Query(description="Name with which the geographical area that groups the addresses for postal purposes can be indicated, usually the city (cf. Fedvoc).",
                                  example='Saint-Gilles',
                                  alias="cityName")] = None,
            request: Request = None,
            response: Response = None):
    """
Search a city based on a postal code or a name (could be municipality name, part of municipality name or postal name)

    """
    client = Elasticsearch(pelias.elastic_api)
    res = search_city(client, post_code, city_name)

    if "status_code" in res:
        response.status_code = res["status_code"]
    res["self"] = str(request.url)

    return res


##################
#  /id/<bestid>  #
##################

def check_valid_bestid(bestid: str):
    """ Check that then "quoted" bestid is valid"""
    if "%2F" in bestid:
        bestid = unquote_plus(bestid)

    mtch = re.search(BESTID_PATTERN, bestid,  re.IGNORECASE)

    if mtch is None or len(mtch.groups()) != 5:
        raise ValueError(f"Cannot parse best id '{bestid}'")

    return mtch, bestid


@app.get("/id/{bestid:str}", response_model_exclude_none=True, responses={
                status.HTTP_200_OK: {
                    "model": GetByIdOutput,
                    "description": "Found a match for this BeSt Id"
                },
                status.HTTP_500_INTERNAL_SERVER_ERROR: {
                    "model": BePeliasError,
                    "description": "In case an error occurred"
                }
            })
def _get_by_id(
            bestid: Annotated[str,
                              Path(description="BeSt Id for an address, a street or a municipality. Value has to be url encoded (i.e., replace '/' by '%2F', ':' by '%3A')",
                                   example='https%3A%2F%2Fdatabrussels.be%2Fid%2Faddress%2F219307%2F7',
                                   alias="bestid"
                                   ),
                              AfterValidator(check_valid_bestid)],
            request: Request = None,
            response: Response = None):

    """Search for a Best item by its id in Elastic database
    """

    res = get_by_id(pelias, bestid)
    if "status_code" in res:
        response.status_code = res["status_code"]
    res["self"] = str(request.url)

    return res


############
# /health  #
############


@app.get('/health', response_model_exclude_none=True, responses={
                status.HTTP_200_OK: {
                    "model": Health,
                    "description": "Up & (partially) running"
                },
                status.HTTP_503_SERVICE_UNAVAILABLE: {
                    "model": Health,
                    "description": "Not running"
                }})
def _health(response: Response, request: Request = None) -> Health:
    res = health(pelias)
    if "status_code" in res:
        response.status_code = res["status_code"]
    res["self"] = str(request.url)

    return res


# app.openapi_schema["components"]["schemas"]

def custom_openapi():
    """Update openapi.json to be conform to REST Guidelines
    """
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(title=app.title,
                                 version=app.version,
                                 routes=app.routes,
                                 summary=app.summary,
                                 description=app.description,
                                 openapi_version=app.openapi_version,
                                 servers=[{"url": app.root_path}],
                                 contact=app.contact,
                                 )

    # Rename HTTPValidationError into HttpValidationError
    openapi_schema["components"]["schemas"]["HttpValidationError"] = openapi_schema["components"]["schemas"]["HTTPValidationError"]
    del openapi_schema["components"]["schemas"]["HTTPValidationError"]

#     openapi_schema["components"]["schemas"]["HttpValidationError"] = {
#     "type": "object",
#     "properties": {
#         "error": {"type": "string"},
#     },
#     "media_type": "application/problem+json"
# }

    for rte in openapi_schema["paths"]:
        if '422' in openapi_schema["paths"][rte]["get"]["responses"]:
            openapi_schema["paths"][rte]["get"]["responses"]["422"]["content"]["application/json"]["schema"]["$ref"] = "#/components/schemas/HttpValidationError"

    # Remove title properties

    for _, sch in openapi_schema["components"]["schemas"].items():
        for prop in sch["properties"]:
            if "title" in sch["properties"][prop]:
                del sch["properties"][prop]["title"]
        if "title" in sch:
            del sch["title"]

    for path in openapi_schema["paths"]:
        for meth in openapi_schema["paths"][path]:
            if "parameters" in openapi_schema["paths"][path][meth]:
                for param in openapi_schema["paths"][path][meth]["parameters"]:
                    # del openapi_schema["paths"][path][meth]["parameters"][param]["schema"]["title"]
                    del param["schema"]["title"]

            # move application/json in error response to application/problem+json
            for resp in openapi_schema["paths"][path][meth]["responses"]:
                if resp != "200":
                    content = openapi_schema["paths"][path][meth]["responses"][resp]["content"]
                    content["application/problem+json"] = content["application/json"]
                    del content["application/json"]
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
