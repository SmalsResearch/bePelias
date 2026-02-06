"""
Unitest for BePelias class using pytest
"""

import sys
import logging

import pytest

sys.path.append("src/")

from bepelias.bepelias import BePelias  # pylint: disable=wrong-import-position, import-error # noqa: E402

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

bepelias = BePelias("", "localhost:9200", "", 3, 0.8)


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

    # emulate geocoder
    bepelias.pelias.geocode = lambda addr: {'features': [{'properties': {'postalcode': addr['postalcode']},
                                            'geometry': {'coordinates': [1, 1]}}]}
    bepelias.pelias.interpolate = lambda lat, lon, number, street: {"geometry": {"coordinates": [2, 2]}}
    assert bepelias._interpolate(input_value) == output_value  # pylint: disable=protected-access


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ({'properties': {'street': "avenue fonsny", "postalcode": "1160", "housenumber": "20"}},
             {'street_geometry': {'coordinates': [1, 1]}})
        ]
)
def test_interpolate2(input_value, output_value):
    """ test interpolate """

    # emulate geocoder
    bepelias.pelias.geocode = lambda addr: {'features': [{'properties': {'postalcode': addr['postalcode']},
                                            'geometry': {'coordinates': [1, 1]}}]}
    bepelias.pelias.interpolate = lambda lat, lon, number, street: []
    assert bepelias._interpolate(input_value) == output_value  # pylint: disable=protected-access


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

    # emulate geocoder
    bepelias.pelias.geocode = lambda addr: {'features': [{'properties': {'postalcode': addr['postalcode']},
                                            'geometry': {'coordinates': [1, 1]}}]}

    bepelias.pelias.interpolate = lambda lat, lon, number, street: {"geometry": {"coordinates": [2, 2]}}

    bepelias._search_for_coordinates(input_value)  # pylint: disable=protected-access

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

    # emulate geocoder
    bepelias.pelias.geocode = lambda addr: {'features': [{'properties': {'postalcode': addr['postalcode']},
                                            'geometry': {'coordinates': [1, 1]}}]}
    bepelias.pelias.interpolate = lambda lat, lon, number, street: []

    bepelias._search_for_coordinates(input_value)  # pylint: disable=protected-access

    for k, val in output_value.items():
        assert input_value[k] == val, f"input_value[{k}] = {input_value[k]} != {val} / {input_value}"
