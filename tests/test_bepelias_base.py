"""
Unitest for bepelias 'utils' functions using pytest
"""
import sys
import logging

import pytest

sys.path.append("src/")

from bepelias.base import (check_locality, check_streetname,  # pylint: disable=C0413, E0401  # noqa: E402
                           check_best_streetname, is_building,
                           build_address, build_city,
                           interpolate, get_precision,
                           search_for_coordinates, transform,
                           add_precision)
from bepelias.pelias import Pelias  # pylint: disable=C0413, E0401 # noqa: E402
from bepelias.utils import log  # pylint: disable=C0413, E0401 # noqa: E402


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            (["", None], 1),
            ([{'properties': {'locality': 'Auderghem'}}, 'Auderghem'], 1),
            ([{'properties': {'addendum': {'best': {'municipality_name_fr': 'Auderghem'}}}}, 'Auderghem'], 1),
            ([{'properties': {'addendum': {'best': {'municipality_name_nl': 'Ouderghem'}}}}, 'Ouderghem'], 1),
            ([{'properties': {'addendum': {'best': {'postname_nl': 'Ouderghem'}}}}, 'Ouderghem'], 1),
            ([{'properties': {}}, 'Ouderghem'], None)
        ]
)
def test_check_locality(input_value, output_value):
    """ test check_locality """
    assert check_locality(input_value[0], input_value[1]) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            (["", None], 1),
            ([{'properties':
                {'street': "avenue fonsny"}}, "AVENUE FONSNY"], 1),
            ([{'properties':
                {'street': "avenue fonsny",
                 'addendum': {'best': {
                    "streetname_fr": "avenue fonsny",
                    "streetname_nl": "fonsnylaan"
                 }}}}, "CHAUSSEE DE WAVRE"], None),
            ([{'properties':
                {}}, "CHAUSSEE DE WAVRE"], 1),
            ([{'properties':
                {'street': "avenue fonsny"}}, "AVENUE FONSNY (saint gilles)"], 1),
        ]
)
def test_check_streetname(input_value, output_value):
    """ test check_streetname """
    assert check_streetname(input_value[0], input_value[1]) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ([{'features': [{'properties': {'street': "avenue fonsny"}}]}, "AVENUE FONSNY"],
             {'features': [{'properties': {'street': "avenue fonsny"}}]}),
            ([{'features': [{'properties': {'street': "avenue fonsny"}},
                            {'properties': {'street': "chaussée de wavre"}}]}, "AVENUE FONSNY"],
             {'features': [{'properties': {'street': "avenue fonsny"}}]})
        ]
)
def test_check_best_streetname(input_value, output_value):
    """ test check_best_streetname """
    assert check_best_streetname(input_value[0], input_value[1]) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ({'properties': {'match_type': "exact", "housenumber": 1}}, True),
            ({'properties': {'match_type': "fake", 'accuracy': "point", "housenumber": 1}}, True),
            ({'properties': {'match_type': "fake", 'accuracy': 'fake'}}, False)
        ]
)
def test_is_building(input_value, output_value):
    """ test check_best_streetname """
    assert is_building(input_value) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            (["avenue fonsnsy", "20"], "avenue fonsnsy, 20"),
            ([None, "20"], ""),
            (["", "20"], ""),
            (["avenue fonsnsy", ""], "avenue fonsnsy"),
            (["avenue fonsnsy", None], "avenue fonsnsy"),
        ]
)
def test_build_address(input_value, output_value):
    """ test check_best_streetname """
    assert build_address(input_value[0], input_value[1]) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            (["1160", "Auderghem"], "1160 Auderghem"),
            ([None, "Auderghem"], "Auderghem"),
            (["1160", ""], "1160")
        ]
)
def test_build_city(input_value, output_value):
    """ test build_city """
    assert build_city(input_value[0], input_value[1]) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ({'properties': {'street': "avenue fonsny", "postalcode": "1160", "housenumber": "20"}},
             {'geometry': {'coordinates': [2, 2]}}),
            ({'properties': {}}, {}),
            ({'properties': {'street': "avenue fonsny"}}, {}),

        ]
)
def test_interpolate(input_value, output_value):
    """ test interpolate """

    pelias = Pelias("", "", "", "")
    # emulate geocoder
    pelias.geocode = lambda addr: {'features': [{'properties': {'postalcode': addr['postalcode']},
                                                 'geometry': {'coordinates': [1, 1]}}]}
    pelias.interpolate = lambda lat, lon, number, street: [2, 2]
    assert interpolate(input_value, pelias) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ({'properties': {'street': "avenue fonsny", "postalcode": "1160", "housenumber": "20"}},
             {'street_geometry': {'coordinates': [1, 1]}})
        ]
)
def test_interpolate2(input_value, output_value):
    """ test interpolate """

    pelias = Pelias("", "", "", "")
    # emulate geocoder
    pelias.geocode = lambda addr: {'features': [{'properties': {'postalcode': addr['postalcode']},
                                                 'geometry': {'coordinates': [1, 1]}}]}
    pelias.interpolate = lambda lat, lon, number, street: []
    assert interpolate(input_value, pelias) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
              ({'properties': {'layer': "address"}, 'geometry': {'coordinates': [0, 0]}}, "address_00"),
              ({'properties': {'layer': "address"},
                'geometry': {'coordinates': [1, 1]},
                'bepelias': {'interpolated': 'street_center'}}, "address_streetcenter"),
              ({'properties': {'layer': "address"},
                'geometry': {'coordinates': [1, 1]},
                'bepelias': {'interpolated': True}}, "address_interpol"),
              ({'properties': {'layer': "address", 'match_type': 'interpolated', 'id': '.../streetname/...'},
                'geometry': {'coordinates': [1, 1]},
                'bepelias': {}}, "street_interpol"),
              ({'properties': {'layer': "address", 'match_type': 'interpolated', 'id': '...'},
                'geometry': {'coordinates': [1, 1]},
                'bepelias': {}}, "address_interpol2"),
              ({'properties': {'layer': "address", 'match_type': 'exact'},
                'geometry': {'coordinates': [1, 1]},
                'bepelias': {}}, "address"),
              ({'properties': {'layer': "street"},
                'geometry': {'coordinates': [0, 0]}}, "street_00"),
              ({'properties': {'layer': "street"},
                'geometry': {'coordinates': [1, 1]}}, "street"),
              ({'properties': {'layer': "city"},
                'geometry': {'coordinates': [0, 0]}}, "city_00"),
              ({'properties': {'layer': "city"},
                'geometry': {'coordinates': [1, 1]}}, "city"),
              ({'properties': {'layer': "street"},
                'geometry': {'coordinates': [0, 0]}}, "street_00"),
              ({'properties': {'layer': "region"}}, "country"),
              ({'properties': {'layer': "fake"}}, "[todo]"),
              ({}, "[keyerror]"),
        ]
)
def test_get_precision(input_value, output_value):
    """ test get_precision """
    assert get_precision(input_value) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
              ({"features": [{'properties': {'layer': "address"}, 'geometry': {'coordinates': [0, 0]}}]}, "address_00"),

              ({"features": [{'properties': {'layer': "address", 'match_type': 'exact'},
                'geometry': {'coordinates': [1, 1]},
                'bepelias': {}}]}, "address"),
        ]
)
def test_add_precision(input_value, output_value):
    """ test add_precision """

    add_precision(input_value)

    for feat in input_value["features"]:
        assert feat["bepelias"]["precision"] == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
              ({'properties': {
                  'addendum': {
                      "best": {
                          "box_info": [{"coordinates": {"lat": 1, "lon": 1}}]}}},
                "geometry": {"coordinates": [0, 0]}},
               {"geometry": {"coordinates_orig": [0, 0], "coordinates": [1, 1]},
                "bepelias": {"interpolated": "from_boxnumber"}}),
              ({'properties': {
                  'addendum': {
                      "best": {}},
                  'street': 'avenue fonsny',
                  'postalcode': 1060,
                  'housenumber': 20},
                "geometry": {"coordinates": [0, 0]}},
               {"geometry": {"coordinates_orig": [0, 0], "coordinates": [2, 2]},
                "bepelias": {"interpolated": True}})
        ]
)
def test_search_for_coordinates(input_value, output_value):
    """ test search_for_coordinates """

    pelias = Pelias("", "", "", "")
    # emulate geocoder
    pelias.geocode = lambda addr: {'features': [{'properties': {'postalcode': addr['postalcode']},
                                                 'geometry': {'coordinates': [1, 1]}}]}
    pelias.interpolate = lambda lat, lon, number, street: [2, 2]

    search_for_coordinates(input_value, pelias)

    log("after update: ")
    log(input_value)
    for k, val in output_value.items():
        assert input_value[k] == val, f"input_value[{k}] = {input_value[k]} != {val} / {input_value}"


@pytest.mark.parametrize(
        "input_value, output_value",
        [
              ({'properties': {
                  'addendum': {
                      "best": {}},
                  'street': 'avenue fonsny',
                  'postalcode': 1060,
                  'housenumber': 20},
                "geometry": {"coordinates": [0, 0]}},
               {"geometry": {"coordinates_orig": [0, 0], "coordinates": [1, 1]},
                "bepelias": {"interpolated": 'street_center'}})
        ]
)
def test_search_for_coordinates2(input_value, output_value):
    """ test search_for_coordinates (with 'pelias.interpolate' not giving any result)"""

    pelias = Pelias("", "", "", "")
    # emulate geocoder
    pelias.geocode = lambda addr: {'features': [{'properties': {'postalcode': addr['postalcode']},
                                                 'geometry': {'coordinates': [1, 1]}}]}
    pelias.interpolate = lambda lat, lon, number, street: []

    search_for_coordinates(input_value, pelias)

    for k, val in output_value.items():
        assert input_value[k] == val, f"input_value[{k}] = {input_value[k]} != {val} / {input_value}"


smals_addr = {'post_name': 'Saint-Gilles',
              'house_number': "20",
              'street_name': 'Avenue Fonsny'}


@pytest.mark.parametrize(
        "input_value, output_value",
        [
              ([smals_addr, "no_city"],
               smals_addr | {"post_name": ""}),
              ([smals_addr, "no_hn"],
               smals_addr | {"house_number": ""}),
              ([smals_addr, "no_street"],
               smals_addr | {"house_number": "", "street_name": ""}),

              ([smals_addr, "clean_hn"],
               smals_addr),
              ([smals_addr | {'house_number': "20-22"}, "clean_hn"],
               smals_addr),
              ([smals_addr | {'house_number': "20 A"}, "clean_hn"],
               smals_addr),

              ([smals_addr, "clean"],
               smals_addr),
              ([smals_addr | {'street_name': "Avenue Fonsny (St Gilles)"}, "clean"],
               smals_addr),
              ([smals_addr | {'street_name': "Avenue Fonsny, St Gilles"}, "clean"],
               smals_addr),
              ([smals_addr | {'street_name': "Avenue X Fonsny"}, "clean"],
               smals_addr),
              ([smals_addr | {'street_name': "  Avenue Fonsny  "}, "clean"],
               smals_addr),

              ([smals_addr | {'post_name': "Saint-Gilles (Bruxelles)"}, "clean"],
               smals_addr)



        ]
)
def test_transform(input_value, output_value):
    """ test transform"""
    assert transform(input_value[0], input_value[1]) == output_value
