""" Class PeliasElastic, managing calls to Elastic embedded in Pelias
"""
import json
from fastapi import status

from elasticsearch import Elasticsearch, NotFoundError

from bepelias.utils import (log, to_rest_guidelines)


class PeliasElastic:
    """ Calls to Elastic embedded in Pelias
    """
    def __init__(self, elastic_api):
        self.es_client = Elasticsearch(elastic_api)

    def get_postcode_list(self, city):
        """ Get a list with all postcode matching with 'city' (as municipality name, postal info or part of municipality)"""
        postcodes = set()

        search_city_resp = self.search_city(None, city)

        for search_city_item in search_city_resp.get("items", []):
            search_city_postalcode = search_city_item.get("postalInfo", {}).get("postalCode", None)
            if search_city_postalcode is not None:
                postcodes.add(search_city_postalcode)
        return postcodes

    def search_city(self, post_code, city_name):
        """
        See fastapi._search_city
        """
        # vlog("search city")

        # log(f"searchCity: {post_code} / {city_name}")

        if post_code is None and city_name is None:
            return {"error": "Either 'postCode' or 'cityName' should be provided",
                    "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY}

        must = [{"term": {"layer": "locality"}}]
        if post_code:
            must.append({"term": {"address_parts.zip": post_code}})
        if city_name:
            must.append({"bool": {
                            "should": [
                                {"query_string": {"query": f"name.default:\"{city_name}\""}},
                                {"query_string": {"query": f"name.fr:\"{city_name}\""}},
                                {"query_string": {"query": f"name.nl:\"{city_name}\""}},
                                {"query_string": {"query": f"name.de:\"{city_name}\""}}
                            ]}}
                        )

        try:

            resp = self.es_client.search(index="pelias",
                                         size=100,
                                         body={
                                             "query": {
                                                 "bool": {
                                                     "must": must
                                                     }
                                             }
                                         })
            # vlog("resp:")
            # vlog(resp)

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

            res = to_rest_guidelines(final_result, False)

            return res
        except NotFoundError:
            return {"features": []}
        except ConnectionError as exc:
            log("ES ConnectionError")
            log(exc)
            return {"error": f"Cannot connect to Elastic: {exc}",
                    "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}

    def get_by_id(self, bestid):

        """ see _get_by_id
        """

        mtch, bestid = bestid  # check_valid_bestid result

        if mtch is None or len(mtch.groups()) != 5:
            return {"error": f"Cannot parse best id '{bestid}'",
                    "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY}

        # vlog(f"mtch[2].lower(): '{mtch[3].lower()}'")
        obj_type = None
        if mtch[3].lower() in ["address", "adres"]:
            obj_type = "address"
        elif mtch[3].lower() in ["streetname", "straatnaam"]:
            obj_type = "street"
        elif mtch[3].lower() in ["municipality", "gemeente", "partofmunicipality"]:
            obj_type = "locality"
        else:
            return {"error": f"Object type '{mtch[3]}' not supported so far in '{bestid}'",
                    "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY}

        try:
            resp = self.es_client.search(index="pelias", body={
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
            return {"error": f"Cannot connect to Elastic: {exc}",
                    "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}

        except ConnectionError as exc:
            log("ES ConnectionError")
            log(exc)
            return {"error": f"Cannot connect to Elastic: {exc}",
                    "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}
