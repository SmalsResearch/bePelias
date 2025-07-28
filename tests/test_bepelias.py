"""
Unitest for bepelias using pytest
"""

import json
from typing import Literal
from urllib.parse import quote_plus

import requests

import pytest

import pandas as pd

WS_HOSTNAME = "172.27.0.64:4001"  # bePelias hostname:port

STREET_FIELD = "streetName"
HOUSENBR_FIELD = "houseNumber"
POSTCODE_FIELD = "postCode"
CITY_FIELD = "postName"

FILENAME = "data.csv"  # A csv file with as header "streetName,houseNumber,postCode,postName"


def call_ws(url, params):
    """
        Call bePelias web service
    """
    try:
        r = requests.get(
            url,
            params=params,
            timeout=30)

    except Exception as e:
        print("Exception !")
        print(params)
        print(e)
        raise e

    try:
        res = json.loads(r.text)
    except ValueError as ve:
        print("Cannot decode result:")
        print(ve)
        print(r.text)
        res = {"error": f"Cannot decode {r.text}"}
    res["status_code"] = r.status_code
    return res


def call_health():
    """
        Call bePelias web service
    """
    return call_ws(f'http://{WS_HOSTNAME}/REST/bepelias/v1/health', {})


def call_geocode(addr_data, mode="advanced"):
    """
        Call bePelias web service
    """
    if isinstance(addr_data, pd.Series):
        addr_data = addr_data.to_dict()

    addr_data["mode"] = mode
    addr_data["withPeliasResult"] = False

    return call_ws(f'http://{WS_HOSTNAME}/REST/bepelias/v1/geocode',
                   addr_data)


def call_unstruct(address, mode="advanced"):
    """Call unstructured bePelias

    Args:
        address (str): address
        mode (str, optional):
    """
    addr_data = {"address": address}

    addr_data["mode"] = mode
    addr_data["withPeliasResult"] = False

    return call_ws(f'http://{WS_HOSTNAME}/REST/bepelias/v1/geocode/unstructured',
                   addr_data)


def call_reverse(lat, lon, radius=1, size=10):
    """Call reverse geocoder
    """
    data = {"lat": lat, "lon": lon, "radius": radius, "size": size}

    return call_ws(f'http://{WS_HOSTNAME}/REST/bepelias/v1/reverse',
                   data)


def call_search_city(postcode=None, cityname=None):
    """call searchCity endpoing

    Args:
        postcode (str, optional): postal code. Defaults to None.
        postname (str, optional): (part of) City name. Defaults to None.
    """
    data = {"postCode": postcode,
            "cityName": cityname,
            "raw": True
            }

    return call_ws(f'http://{WS_HOSTNAME}/REST/bepelias/v1/searchCity',
                   data)


def call_get_by_id(bestid):
    """call searchCity endpoing
    """
    return call_ws(f'http://{WS_HOSTNAME}/REST/bepelias/v1/id/{quote_plus(bestid)}', {})


test_data = {
    "smals": {
        "fixture": {
            "streetName": "Av Fonsny",
            "houseNumber": 20,
            "postCode": "1060",
            "postName": "Saint-Gilles"
            },
        "unstruct_fixture": "Av Fonsny 20, 1060 Saint-Gilles",
        "expectings": [
            (["items", 0, "postalInfo", "postalCode"], "1060"),
            (["items", 0, "postalInfo", "name", "fr"], "Saint-Gilles"),
            (["items", 0, "street", "name", "fr"], "Avenue Fonsny"),
            (["items", 0, "street", "name", "nl"], "Fonsnylaan"),
            (["items", 0, "municipality", "code"], "21013"),
            (["items", 0, "housenumber"], "20"),
            (["total"], 1),
            (["items", 0, "precision"], "address")
        ]
    },
    "charleroi": {
        "fixture": {
            "streetName": "Avenue Jean Dupuis",
            "houseNumber": 1,
            "postCode": "6000",
            "postName": "Charleroi"},
        "expectings": [
            (["items", 0, "postalInfo", "postalCode"], "6000"),
            (["items", 0, "municipality", "code"], "52011"),
            (["total"], 1),
            # (["total"], 1),
            # (["items", 0, "name"], "Charleroi"),
            # (["items", 0, "precision"], "city")
        ]
    },
    "gent": {
        "fixture": {
            "streetName": "Eedverbondkaai",
            "houseNumber": 41,
            "postCode": "9000",
            "postName": "Gent"},
        "unstruct_fixture": "Eedverbondkaai 41, 9000 Gent",
        "expectings": [
            (["items", 0, "postalInfo", "postalCode"], "9000"),
            (["items", 0, "municipality", "code"], "44021"),
            (["items", 0, "street", "name", "nl"], "Eedverbondkaai"),
            (["items", 0, "housenumber"], "41"),
            (["items", 0, "precision"], "address"),

            (["total"], 1),
        ]

    },
    "nores": {
        "fixture": {
            "streetName": "Ave Fabsnyaefaeef",
            "houseNumber": 20,
            "postCode": "1234",
            "city": "azerty"},
        "expectings": [
            (["items"], []),
            (["total"], 0)
            ]
    }
}


def check_expectings(actual, expectings):
    """Check that API results corresponds to expectings

    Args:
        actual (dict): _description_
        expectings (list): _description_
    """
    for keys, value in expectings:
        cur = actual
        for k in keys:
            if isinstance(k, int):
                assert isinstance(cur, list), f"Expecting '{cur}' to be a list"
                assert len(cur) > k, f"Expecting at least {k+1} elements (found {len(cur)}) in {cur}"
            else:
                assert k in cur, f"Expecting '{k}' in '{cur}': {actual}"

            cur = cur[k]
        assert cur == value, f"Expected value for {keys}: '{value}', found '{cur}'. {actual}"


def test_check_health():
    """Check health
    """
    health = call_health()
    assert "status" in health and health["status"] == "UP"


@pytest.mark.parametrize(
        "addr, expectings",
        [
            (it["fixture"], it["expectings"]) for (k, it) in test_data.items()
        ]
)
def test_check_single_addr(addr, expectings):
    """Check result for struct call

    Args:
        addr (dict): _description_
        expectings (list): _description_
    """
    actual = call_geocode(addr)

    check_expectings(actual, expectings)


@pytest.mark.parametrize(
        "function, params, expected_code",
        [
            (call_geocode, {"addr_data": test_data["smals"]["fixture"], "mode": "1"}, 422),
            (call_unstruct, {"address": None}, 422),
            (call_unstruct, {"address": "test", "mode": "1"}, 422),
            (call_reverse, {"lat": 0, "lon": 0}, 422),
            (call_reverse, {"lat": 50.8, "lon": None}, 422),
            (call_search_city, {"postcode": None, "cityname": None}, 422),
            (call_get_by_id, {"bestid": ""}, 404),
            (call_get_by_id, {"bestid": "1234"}, 422)
        ]
)
def test_wrong_calls(function, params, expected_code):
    """ Testing calls with wrong paramters"""
    res = function(**params)
    assert "status_code" in res and res["status_code"] == expected_code


@pytest.mark.parametrize(
        "addr, expectings",
        [
            (it["unstruct_fixture"], it["expectings"]) for (k, it) in test_data.items() if "unstruct_fixture" in it
        ]
)
def test_check_unstruct(addr, expectings):
    """Check that unstructured geocode give the expected result

    Args:
        addr (str): input address
        expectings (list): _description_
    """
    actual = call_unstruct(addr)

    check_expectings(actual, expectings)


@pytest.mark.parametrize(
        "addr, expectings",
        [
            ((1060, None),  [(["items", 0, "municipality", "code"], "21013"),
                             (["total"], 1)]),
            ((None, "Saint-Gilles"),  [(["items", 0, "municipality", "code"], "21013"),
                                       (["total"], 1)]),
            ((1060, "Saint-Gilles"),  [(["items", 0, "municipality", "code"], "21013"),
                                       (["total"], 1)]),
            ((5190, "Spy"),  [(["items", 0, "municipality", "code"], "92140"),
                              (["total"], 1)]),
            (("0612", None),  [(["total"], 0)]),
            ((9000, None),  [(["items", 0, "municipality", "code"], "44021"),
                             (["total"], 1)]),
        ]
)
def test_check_city_search(addr, expectings):
    """Check that unstructured geocode give the expected result

    Args:
        addr (str): input address
        expectings (list): _description_
    """
    actual = call_search_city(addr[0], addr[1])

    check_expectings(actual, expectings)


@pytest.mark.parametrize(
        "addr, unstruct",
        [
            (it["fixture"], it["unstruct_fixture"]) for (k, it) in test_data.items() if "unstruct_fixture" in it
        ]
)
def test_compare_struct_unstruct(addr, unstruct):
    """Check that structured and unstructured version give the same result

    Args:
        addr (dict): structured address
        unstruct (str): unstructured address
    """
    actual_struct = call_geocode(addr)
    actual_unstruct = call_unstruct(unstruct)

    assert "items" in actual_struct and isinstance(actual_struct["items"], list)
    assert "items" in actual_unstruct and isinstance(actual_unstruct["items"], list)

    assert len(actual_struct["items"]) > 0, "At least on item expected"
    assert len(actual_unstruct["items"]) > 0, "At least on item expected"
    assert actual_struct["items"][0] == actual_unstruct["items"][0], "Comparison failed between struct and unstruct!"


@pytest.mark.parametrize(
        "addr",
        [
            it["fixture"] for (k, it) in test_data.items()
        ]
)
def test_get_by_id(addr):
    """Check result for struct call

    Args:
        addr (dict): _description_
        expectings (list): _description_
    """
    res = call_geocode(addr)

    for item in res["items"]:
        if "bestId" in item:
            # print(quote_plus(item["bestId"]))
            res_by_id = call_get_by_id(quote_plus(item["bestId"]))

            assert res_by_id["items"][0]["bestId"] == item["bestId"]
            assert res_by_id["items"][0]["coordinates"] == item["coordinates"]


@pytest.mark.parametrize(
        "filename",
        [
            "tests/data.csv"
        ]
)
def test_batch_call(filename: Literal['tests/data.csv']):
    """Send all addresses from filename to API

    Args:
        filename (str): CVS filename
    """
    addresses = pd.read_csv(filename).iloc[0:10]
    addresses["json"] = addresses.fillna("").apply(call_geocode, mode="advanced", axis=1)
    for json_item in addresses["json"]:
        assert "items" in json_item
        #  assert len(json_item["items"]) > 0, f"Expecting at least one result: {json_item}"
        assert json_item["total"] == len(json_item["items"])
        for item in json_item["items"]:
            assert "precision" in item
