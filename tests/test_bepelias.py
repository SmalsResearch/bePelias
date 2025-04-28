"""
Unitest for bepelias using pytest
"""

import json
from typing import Literal
import requests

import pytest

import pandas as pd

WS_HOSTNAME = "172.27.0.64:4001"  # bePelias hostname:port

STREET_FIELD = "streetName"
HOUSENBR_FIELD = "houseNumber"
POSTCODE_FIELD = "postCode"
CITY_FIELD = "postName"

FILENAME = "data.csv"  # A csv file with as header "streetName,houseNumber,postCode,postName"


def call_ws(addr_data, mode="advanced"):
    """
        Call bePelias web service
    """
    if isinstance(addr_data, pd.Series):
        addr_data = addr_data.to_dict()

    addr_data["mode"] = mode
    addr_data["withPeliasResult"] = False
    try:
        r = requests.get(
            f'http://{WS_HOSTNAME}/REST/bepelias/v1/geocode',
            params=addr_data,
            timeout=30)

    except Exception as e:
        print("Exception !")
        print(addr_data)
        print(e)
        raise e

    if r.status_code == 204:
        print("No result!")
        print(addr_data)
        print(r.text)
        return r.status_code
    if r.status_code == 400:
        print("Argument error")
        print(r.text)
        return r.status_code
    if r.status_code == 200:
        try:
            res = json.loads(r.text)
        except ValueError as ve:
            print("Cannot decode result:")
            print(ve)
            print(r.text)
            return r.text
        return res

    print(f"Unknown return code: {r.status_code} ")
    print(r.text)
    return None


def call_unstruct_ws(address, mode="advanced"):
    """Call unstructured bePelias

    Args:
        address (str): address
        mode (str, optional):

    Raises:
        e: _description_

    Returns:
        _type_: _description_
    """
    addr_data = {"address": address}

    addr_data["mode"] = mode
    addr_data["withPeliasResult"] = False
    try:
        r = requests.get(
            f'http://{WS_HOSTNAME}/REST/bepelias/v1/geocode/unstructured',
            params=addr_data,
            timeout=30)

    except Exception as e:
        print("Exception !")
        print(addr_data)
        print(e)
        raise e

    if r.status_code == 204:
        print("No result!")
        print(addr_data)
        print(r.text)
        return
    if r.status_code == 400:
        print("Argument error")
        print(r.text)
        return r.status_code
    if r.status_code == 200:
        try:
            res = json.loads(r.text)
        except ValueError as ve:
            print("Cannot decode result:")
            print(ve)
            print(r.text)
            return r.text
        return res

    print(f"Unknown return code: {r.status_code} ")
    print(r.text)


def call_ws_search_city(postcode=None, postname=None):
    data = {"postCode": postcode,
            "cityName": postname,
            "raw": True
           }

    try:
        r = requests.get(
            f'http://{WS_HOSTNAME}/REST/bepelias/v1/searchCity',
            params=data,
            timeout=30)

    except Exception as e:
        print("Exception !")
        print(e)
        raise e

    if r.status_code == 204:
        return
    elif r.status_code == 400:
        print("Argument error")
        print(r.text)
        return r.status_code
    elif r.status_code == 200:
        try:
            res = json.loads(r.text)
            # res["time"] = (datetime.now() - t).total_seconds()
        except ValueError as ve:

            print("Cannot decode result:")
            print(ve)
            print(r.text)
            return r.text
        except AttributeError as ae:
            print(ae)
            print(type(r.text))
            print(r.text)
        return res
    else: 
        print(f"Unknown return code: {r.status_code} ")
        print(r.text)


data = {
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
            "postCode": "6010",
            "postName": "Charleroi"},
        "expectings": [
            # (["items", 1, "postalInfo", "postalCode"], "6010"),
            # (["items", 1, "municipality", "code"], "52011"),
            # (["items", 1, "partOfMunicipality", "name", "fr"], "Couillet"),
            # (["total"], 2),
            (["total"], 1),
            (["items", 0, "name"], "Charleroi"),
            (["items", 0, "precision"], "city")
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
            "streetName": "Ave Fonsnyaefaeef",
            "housenumber": 20,
            "postcode": "1060",
            "city": "Saint-Gilles"},
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
                assert len(cur) > k, f"Expecting at least '{k+1}' elements (found {len(cur)}) in {cur}"
            else:
                assert k in cur, f"Expecting '{k}' in '{cur}': {actual}"

            cur = cur[k]
        assert cur == value, f"Expected value for {keys}: '{value}', found '{cur}'. {actual}"


@pytest.mark.parametrize(
        "addr, expectings",
        [
            (it["fixture"], it["expectings"]) for (k, it) in data.items()
        ]
)
def test_check_single_addr(addr: Any, expectings: Any):
    """Check result for struct call

    Args:
        addr (dict): _description_
        expectings (list): _description_
    """
    actual = call_ws(addr)

    check_expectings(actual, expectings)


def test_code_400():
    """Check that API returns a code 400 if parameters are wrong
    """
    code_400 = call_ws(data["smals"]["fixture"], mode="1")
    assert code_400 == 400, f"Expecting code 400, got {code_400}"


@pytest.mark.parametrize(
        "addr, expectings",
        [
            (it["unstruct_fixture"], it["expectings"]) for (k, it) in data.items() if "unstruct_fixture" in it
        ]
)
def test_check_unstruct(addr: Any, expectings: Any):
    """Check that unstructured geocode give the expected result

    Args:
        addr (str): input address
        expectings (list): _description_
    """
    actual = call_unstruct_ws(addr)

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
                              (["total"], 1)])
        ]
)
def test_chech_city_search(addr, expectings):
    """Check that unstructured geocode give the expected result

    Args:
        addr (str): input address
        expectings (list): _description_
    """
    actual = call_ws_search_city(addr[0], addr[1])

    check_expectings(actual, expectings)


@pytest.mark.parametrize(
        "addr, unstruct",
        [
            (it["fixture"], it["unstruct_fixture"]) for (k, it) in data.items() if "unstruct_fixture" in it
        ]
)
def test_compare_struct_unstruct(addr: Any, unstruct: Any):
    """Check that structured and unstructured version give the same result

    Args:
        addr (dict): structured address
        unstruct (str): unstructured address
    """
    actual_struct = call_ws(addr)
    actual_unstruct = call_unstruct_ws(unstruct)

    assert "items" in actual_struct and isinstance(actual_struct["items"], list)
    assert "items" in actual_unstruct and isinstance(actual_unstruct["items"], list)

    assert len(actual_struct["items"]) > 0, "At least on item expected"
    assert len(actual_unstruct["items"]) > 0, "At least on item expected"
    assert actual_struct["items"][0] == actual_unstruct["items"][0], "Comparison failed between struct and unstruct!"



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
    addresses["json"] = addresses.fillna("").apply(call_ws, mode="advanced", axis=1)
    for json_item in addresses["json"]:
        assert "items" in json_item
        #  assert len(json_item["items"]) > 0, f"Expecting at least one result: {json_item}"
        assert json_item["total"] == len(json_item["items"])
        for item in json_item["items"]:
            assert "precision" in item
