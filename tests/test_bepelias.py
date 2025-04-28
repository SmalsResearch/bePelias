"""
Unitest for bepelias using pytest
"""

import json
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
    elif r.status_code == 400:
        print("Argument error")
        print(r.text)
        return r.status_code
    elif r.status_code == 200:
        try:
            res = json.loads(r.text)
        except ValueError as ve:
            print("Cannot decode result:")
            print(ve)
            print(r.text)
            return r.text
        return res
    else:
        print(f"Unknown return code: {r.status_code} ")
        print(r.text)
        return None


def call_unstruct_ws(address, mode="advanced"):
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
    elif r.status_code == 400:
        print("Argument error")
        print(r.text)
        return r.status_code
    elif r.status_code == 200:
        try:
            res = json.loads(r.text)
        except ValueError as ve:
            print("Cannot decode result:")
            print(ve)
            print(r.text)
            return r.text
        return res
    else:
        print(f"Unknown return code: {r.status_code} ")
        print(r.text)


smals_addr = {"streetName": "Av Fonsny",
              "houseNumber": 20,
              "postCode": "1060",
              "postName": "Saint-Gilles"}
smals_unstruct_addr = "Av Fonsny 20, 1060 Saint-Gilles"

smals_expectings = [
        (["items", 0, "postalInfo", "postalCode"], "1060"),
        (["items", 0, "postalInfo", "name", "fr"], "Saint-Gilles"),
        (["items", 0, "street", "name", "fr"], "Avenue Fonsny"),
        (["items", 0, "street", "name", "nl"], "Fonsnylaan"),
        (["items", 0, "municipality", "code"], "21013"),
        (["items", 0, "housenumber"], "20"),
        (["total"], 1),
        (["items", 0, "precision"], "address")
    ]

nores_addr = {"streetName": "Ave Fonsnyaefaeef",
              "housenumber": 20,
              "postcode": "1060",
              "city": "Saint-Gilles"}
nores_expectings = [
    (["items"], []),
    (["total"], 0)
]

char_addr = {"streetName": "Avenue Jean Dupuis",
             "houseNumber": 1,
             "postCode": "6010",
             "postName": "Charleroi"}

char_expectings = [
        (["items", 1, "postalInfo", "postalCode"], "6010"),
        (["items", 1, "municipality", "code"], "52011"),
        (["items", 1, "partOfMunicipality", "name", "fr"], "Couillet"),
        (["total"], 2),
        (["items", 1, "precision"], "city")
    ]


def check_expectings(actual, expectings):
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
            (smals_addr, smals_expectings),
            (nores_addr, nores_expectings),
            (char_addr, char_expectings)
        ]
)
def test_check_single_addr(addr, expectings):
    """Check result for struct call

    Args:
        addr (dict): _description_
        expectings (list): _description_
    """
    actual = call_ws(addr)

    check_expectings(actual, expectings)



def test_code_400():
    code_400 = call_ws(smals_addr, mode="1")
    assert code_400 == 400, f"Expecting code 400, got {code_400}"


@pytest.mark.parametrize(
        "addr, expectings",
        [
            (smals_unstruct_addr, smals_expectings),
        ]
)
def test_chech_unstruct(addr, expectings):
    actual = call_unstruct_ws(addr)

    check_expectings(actual, expectings)

@pytest.mark.parametrize(
        "addr, unstruct",
        [
            (smals_addr, smals_unstruct_addr),
        ]
)
def test_compare_struct_unstruct(addr, unstruct):
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
def test_batch_call(filename):
    addresses = pd.read_csv(filename)#.iloc[0:200]
    addresses["json"] = addresses.fillna("").apply(call_ws, mode="advanced", axis=1)
    for json_item in addresses["json"]:
        assert "items" in json_item
        assert len(json_item["items"]) > 0
        assert json_item["total"] == len(json_item["items"])
        for item in json_item["items"]:
            assert "precision" in item
