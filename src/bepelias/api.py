#!/usr/bin/env python
# coding: utf-8

"""
Flask part of bePelias geocoder

@author: Vandy Berten (vandy.berten@smals.be)

"""

# pylint: disable=line-too-long
# pylint: disable=invalid-name

import os
import logging
import sys
import json
import re
import warnings

from urllib.parse import unquote_plus

from flask import Flask, url_for
from flask_restx import Api, Resource


from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.exceptions import ElasticsearchWarning

from bepelias.utils import (log, vlog, get_arg,
                            build_address, to_rest_guidelines,
                            struct_or_unstruct, advanced_mode,
                            add_precision, unstructured_mode)

from bepelias.pelias import Pelias, PeliasException

from bepelias.model import (namespace,
                            geocode_parser, geocode_output_model,
                            unstructured_parser,
                            reverse_parser, reverse_output_model,
                            city_search_parser, search_city_output_model,
                            get_by_id_output_model,
                            health_output_model)


warnings.simplefilter('ignore', ElasticsearchWarning)

logging.basicConfig(format='[%(asctime)s]  %(message)s', stream=sys.stdout)

# WARNING : no logs
# INFO : a few logs
# DEBUG : lots of logs

logger = logging.getLogger()

env_log_level = os.getenv('LOG_LEVEL', "HIGH").upper().strip()
if env_log_level == "LOW":
    logger.setLevel(logging.WARNING)
elif env_log_level == "MEDIUM":
    logger.setLevel(logging.INFO)
elif env_log_level == "HIGH":
    logger.setLevel(logging.DEBUG)
else:
    print(f"Unkown log level '{env_log_level}'. Should be LOW/MEDIUM/HIGH")

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)

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

vlog("test Pelias: ")
try:
    vlog(pelias.geocode("20, Avenue Fonsny, 1060 Bruxelles"))
except PeliasException as exc:
    log("Test failed!!")
    log(exc)


pelias.wait()

log("Waiting for requests...")


app = Flask(__name__)
api = Api(app,
          version='1.0.0',
          title='bePelias API',
          description="""A service that allows geocoding (postal address cleansing and conversion into geographical coordinates), based on Pelias and BestAddresses.

          Code available on https://github.com/SmalsResearch/bePelias/

          """,
          doc='/doc',
          prefix='/REST/bepelias/v1',
          contact='Vandy BERTEN',
          contact_email='vandy.berten@smals.be'
          )

app.config["RESTX_MASK_SWAGGER"] = False

# namespace = api.namespace(
#     '',
#     'Main namespace')

api.add_namespace(namespace)

with_https = os.getenv('HTTPS', "NO").upper().strip()

if with_https == "YES":
    log("with https")
    # It runs behind a reverse proxy

    @property
    def specs_url(self) -> str:
        """
            If it runs behind a reverse proxy
        """
        return url_for(self.endpoint('specs'), _external=True, _scheme='https')

    Api.specs_url = specs_url


##############
#  /geocode  #
##############

# error_400 = namespace.model("Error400", {"message":   fields.String(description="Error message")})

@namespace.route('/geocode')
class Geocode(Resource):
    """ Single address geocoding"""

    @namespace.expect(geocode_parser)
    @namespace.response(500, 'Internal Server error')
    @namespace.response(400, 'Error in arguments')
    @namespace.marshal_with(geocode_output_model,
                            description='Found one or several matches for this address',
                            skip_none=True)
    def get(self):
        """
Geocode (postal address cleansing and conversion into geographical coordinates) a single address.

        """

        # log("geocode")

        mode = get_arg("mode", "advanced")
        if mode not in ["basic", "simple", "advanced"]:
            namespace.abort(400, f"Invalid mode {mode}")

        street_name = get_arg("streetName", None)
        house_number = get_arg("houseNumber", None)
        post_code = get_arg("postCode", None)
        post_name = get_arg("postName", None)

        withPeliasResult = get_arg("withPeliasResult", "False")
        if withPeliasResult.lower() == "false":
            withPeliasResult = False
        elif withPeliasResult.lower() == "true":
            withPeliasResult = True
        else:
            namespace.abort(400, f"Invalid withPeliasResult value ({withPeliasResult}). Should be 'true' or 'false'")

        if street_name:
            street_name = street_name.strip()
        if house_number:
            house_number = house_number.strip()
        if post_code:
            post_code = post_code.strip()
        if post_name:
            post_name = post_name.strip()

        log(f"Geocode ({mode}): {street_name} / {house_number} / {post_code} / {post_name}")

        try:
            if mode in ("basic"):
                pelias_res = pelias.geocode({"address": build_address(street_name, house_number),
                                             "postalcode": post_code,
                                             "locality": post_name})
                add_precision(pelias_res)

                return to_rest_guidelines(pelias_res, withPeliasResult)

            if mode == "simple":
                pelias_res = struct_or_unstruct(street_name, house_number, post_code, post_name, pelias)
                add_precision(pelias_res)

                return to_rest_guidelines(pelias_res, withPeliasResult)

            if mode == "advanced":
                vlog("advanced...")

                pelias_res = advanced_mode(street_name, house_number, post_code, post_name, pelias)
                return to_rest_guidelines(pelias_res, withPeliasResult)

        except PeliasException as exc:
            log("Exception during process: ")
            log(exc)
            return {"error": str(exc)}, 500

        return "Wrong mode!"  # Should neve occur...


###########################
#  /geocode/unstructured  #
###########################


@namespace.route('/geocode/unstructured')
class GeocodeUnstructured(Resource):
    """ Single (unstructured) address geocoding"""

    @namespace.expect(unstructured_parser)
    @namespace.response(500, 'Internal Server error')
    @namespace.response(400, 'Error in arguments')
    @namespace.marshal_with(geocode_output_model,
                            description='Found one or several matches for this address',
                            skip_none=True)
    def get(self):
        """
[BETA] Geocode (postal address cleansing and conversion into geographical coordinates) a single address.

        """
        mode = get_arg("mode", "advanced")
        if mode not in ["basic", "advanced"]:
            namespace.abort(400, f"Invalid mode {mode}")

        address = get_arg("address", None)

        withPeliasResult = get_arg("withPeliasResult", "False")
        if withPeliasResult.lower() == "false":
            withPeliasResult = False
        elif withPeliasResult.lower() == "true":
            withPeliasResult = True
        else:
            namespace.abort(400, f"Invalid withPeliasResult value ({withPeliasResult}). Should be 'true' or 'false'")

        if address:
            address = address.strip()
        else:
            address.abort(400, "Argument 'address' mandatory")

        log(f"Geocode (unstruct - {mode}): {address}")

        try:
            if mode in ("basic"):
                pelias_res = pelias.geocode(address)
                add_precision(pelias_res)

                return to_rest_guidelines(pelias_res, withPeliasResult)

            if mode == "advanced":
                vlog("advanced...")

                pelias_res = unstructured_mode(address, pelias)

                return to_rest_guidelines(pelias_res, withPeliasResult)

        except PeliasException as exc:
            log("Exception during process: ")
            log(exc)
            return {"error": str(exc)}, 500

        return "Wrong mode!"  # Should neve occur...


##############
#  /reverse  #
##############

@namespace.route('/reverse')
class Reverse(Resource):
    """ Reverse geocoding"""

    @namespace.expect(reverse_parser)
    @namespace.response(500, 'Internal Server error')
    @namespace.response(400, 'Error in arguments')
    @namespace.marshal_with(reverse_output_model,
                            description='Found one or several matches within the given radius (in km) of point (lat, lon)',
                            skip_none=True)
    def get(self):
        """
Reverse geocoding

        """

        vlog("reverse")

        lat = get_arg("lat", None)
        lon = get_arg("lon", None)

        radius = get_arg("radius", 1)
        size = get_arg("size", 10)

        withPeliasResult = get_arg("withPeliasResult", "False")
        if withPeliasResult.lower() == "false":
            withPeliasResult = False
        elif withPeliasResult.lower() == "true":
            withPeliasResult = True
        else:
            namespace.abort(400, f"Invalid withPeliasResult value ({withPeliasResult}). Should be 'true' or 'false'")

        if lat:
            try:
                lat = float(lat)
            except ValueError:
                namespace.abort(400, "Argument 'lat' should be a float number")
        else:
            namespace.abort(400, "Argument 'lat' mandatory")

        if lon:
            try:
                lon = float(lon)
            except ValueError:
                namespace.abort(400, "Argument 'lon' should be a float number")
        else:
            namespace.abort(400, "Argument 'lon' mandatory")

        try:
            radius = float(radius)
        except ValueError:
            namespace.abort(400, "Argument 'radius' should be a float number")

        try:
            size = int(size)
        except ValueError:
            namespace.abort(400, "Argument 'size' should be a integer")

        log(f"Reverse geocode: ({lat}, {lon}) / radius: {radius} / size:{size} ")

        try:
            # Note: max size for Pelias = 40. But as most records are duplicated in Pelias (one record in each languages for bilingual regions,
            # we first take twice too many results)
            pelias_res = pelias.reverse(lat=lat,
                                        lon=lon,
                                        radius=radius,
                                        size=size*2)

            res = to_rest_guidelines(pelias_res, withPeliasResult)

            res["items"] = res["items"][0:size]
            res["total"] = len(res["items"])
            return res
        except PeliasException as exc:
            log("Exception during process: ")
            log(exc)
            return {"error": str(exc)}, 500


#################
#  /searchCity  #
#################


@namespace.route('/searchCity')
class SearchCity(Resource):
    """ Search city level results"""

    @namespace.expect(city_search_parser)
    @namespace.response(400, 'Error in arguments')
    @namespace.response(500, 'Internal Server error')
    @namespace.marshal_with(search_city_output_model,
                            description='Found one or several matches for city/postal code',
                            skip_none=True)
    def get(self):
        """
Search a city based on a postal code or a name (could be municipality name, part of municipality name or postal name)

        """
        vlog("search city")

        post_code = get_arg("postCode", None)
        city_name = get_arg("cityName", None)

        log(f"searchCity: {post_code} / {city_name}")
        must = [{"term": {"layer": "locality"}}]
        if post_code:
            must.append({"term": {"address_parts.zip": post_code}})
        if city_name:
            must.append({"query_string": {"query": f"name.default:\"{city_name}\""}})

        try:
            client = Elasticsearch(pelias.elastic_api)
            resp = client.search(index="pelias",
                                 size=100,
                                 body={
                                     "query": {
                                         "bool": {
                                             "must": must
                                             }
                                        }
                                    })
            vlog("resp:")
            vlog(resp)

            resp = resp["hits"]["hits"]

            final_result = []
            for resp_item in resp:
                if "addendum" in resp_item["_source"] and "best" in resp_item["_source"]["addendum"]:
                    it = {"properties": {"addendum": {"best": json.loads(resp_item["_source"]["addendum"]["best"])}}}
                    if "center_point" in resp_item["_source"]:
                        it["geometry"] = {"coordinates": resp_item["_source"]["center_point"]}
                    it["name"] = resp_item["_source"]["name"]

                    final_result.append(it)

            # Remove duplicate results
            final_result = {"features": [i for n, i in enumerate(final_result) if i not in final_result[:n]]}

            return to_rest_guidelines(final_result, False)
        except NotFoundError:
            return {"features": []}
        except ConnectionError as exc:
            log("ES ConnectionError")
            log(exc)

            return {"error": f"Cannot connect to Elastic: {exc}"}, 500


##################
#  /id/<bestid>  #
##################


@namespace.route('/id/<string:bestid>')
@namespace.param('bestid',
                 type=str,
                 default='https%3A%2F%2Fdatabrussels.be%2Fid%2Faddress%2F219307%2F7',
                 help="BeSt Id for an address, a street or a municipality. Value has to be url encoded (i.e., replace '/' by '%2F', ':' by '%3A')")
class GetById(Resource):
    """ Get ressource by best id. This does not replace a call to Bosa BeSt Address API !!
    Only works for addresses, streets and municipalities (not for postalinfos, part of municipalities)
    """
    @namespace.response(400, 'Error in arguments')
    @namespace.response(500, 'Internal Server error')
    @namespace.marshal_with(get_by_id_output_model,
                            description='Found one or several matches for this id',
                            skip_none=True)
    def get(self, bestid):
        """Search for a Best item by its id in Elastic database

        Args:
            bestid (str): best if for an address, a street or a municipality

        Returns:
            list: json list of corresponding best objects
        """
        # raw = get_arg("raw", False)

        if "%2F" in bestid:
            bestid = unquote_plus(bestid)

        log(f"Get by id: {bestid}")

        client = Elasticsearch(pelias.elastic_api)

        mtch = re.search(r"([a-z\.]+.be)/id/([a-z]+)/", bestid,  re.IGNORECASE)

        if mtch is None or len(mtch.groups()) != 2:
            namespace.abort(400, f"Cannot parse best id '{bestid}'")

        vlog(f"mtch[2].lower(): '{mtch[2].lower()}'")
        obj_type = None
        if mtch[2].lower() in ["address", "adres"]:
            obj_type = "address"
        elif mtch[2].lower() in ["streetname", "straatnaam"]:
            obj_type = "street"
        elif mtch[2].lower() in ["municipality", "gemeente", "partofmunicipality"]:
            obj_type = "locality"
        else:
            namespace.abort(400, f"Object type '{mtch[2]}' not supported so far in '{bestid}'")

        try:
            resp = client.search(index="pelias", body={
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"layer": obj_type}},
                            {"prefix": {"source_id": {"value": bestid.lower()}}}
                        ]
                    }
                }
            })

            resp = resp["hits"]["hits"]

            final_result = []
            for resp_item in resp:
                if "addendum" in resp_item["_source"] and "best" in resp_item["_source"]["addendum"]:
                    it = {"properties": {"addendum": {"best": json.loads(resp_item["_source"]["addendum"]["best"])}}}
                    if "center_point" in resp_item["_source"]:
                        it["geometry"] = {"coordinates": resp_item["_source"]["center_point"]}
                    it["name"] = resp_item["_source"]["name"]

                    final_result.append(it)

            final_result = {"features": final_result}

            return to_rest_guidelines(final_result, with_pelias_raw=False)
        except NotFoundError:
            return to_rest_guidelines({"features": []}, with_pelias_raw=False)
        except ConnectionRefusedError as exc:
            log("ES ConnectionRefusedError")
            log(exc)

            return f"Cannot connect to Elastic: {exc}", 500

        except ConnectionError as exc:
            log("ES ConnectionError")
            log(exc)

            return f"Cannot connect to Elastic: {exc}", 500

#############
#  /health  #
#############


@namespace.route('/health', methods=['GET'])
class Health(Resource):
    """ Check service status """
    @namespace.response(500, 'Internal Server error', health_output_model, default="blah")
    @namespace.response(503, 'Service is "DOWN"', health_output_model)
    @namespace.marshal_with(health_output_model,
                            description='ServiceStatus',
                            skip_none=True)
    def get(self):
        """Health status

        Returns
        -------
        - {'status': 'DOWN'}: Pelias server does not answer (or gives an unexpected answer)
        - {'status': 'DEGRADED'}: Either Libpostal or Photon is down (or gives an unexpected answer). Geocoding is still possible as long as it does not requires one of those transformers
        - {'status': 'UP'}: Service works correctly

        """
        # Checking Pelias

        pelias_res = pelias.check()

        if pelias_res is False:
            log("Pelias not up & running")
            log(f"Pelias host: {pelias_host}")

            return {"status": "DOWN",
                    "details": {"errorMessage": "Pelias server does not answer",
                                "details": "Pelias server does not answer"}}, 503
        if pelias_res is not True:
            return {"status": "DOWN",
                    "details": {"errorMessage": "Pelias server answers, but gives an unexpected answer",
                                "details": f"Pelias answer: {pelias_res}"}}, 503

        # Checking Interpolation

        try:
            interp_res = pelias.interpolate(lat=50.83582,
                                            lon=4.33844,
                                            number=20,
                                            street="Avenue Fonsny")
            vlog(interp_res)
            if len(interp_res) > 0 and "geometry" not in interp_res:
                return {
                    "status": "DEGRADED",
                    "details": {
                        "errorMessage": "Interpolation server answers, but gives an unexpected answer",
                        "details": f"Interpolation answer: {interp_res}"
                    }}, 200

        except Exception:
            return {"status": "DEGRADED",
                    "details": {"errorMessage": "Interpolation server does not answer",
                                "details": "Interpolation server does not answer"}}, 200

        return {"status": "UP"}, 200
