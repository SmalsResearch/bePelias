"""
Unitest for bepelias 'utils' functions using pytest
"""

import sys
import logging

import pytest
import pandas as pd

sys.path.append("src/")

from bepelias.bepelias import remove_patterns  # pylint: disable=wrong-import-position, import-error # noqa: E402

from bepelias.utils import (to_camel_case, convert_coordinates,  # pylint: disable=wrong-import-position, import-error # noqa: E402
                            to_rest_guidelines, feature_to_df, is_building, build_address, build_city, get_precision, add_precision,
                            transform)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ("input_value", "inputValue"),
            ({"input_value": 5}, {"inputValue": 5}),
            ({"input_value": "this_is_value"}, {"inputValue": "this_is_value"}),
            (["input_value_1", "input_value_2", "input_value_3"], (["inputValue1", "inputValue2", "inputValue3"])),
            (5, 5)
        ]
)
def test_to_camel_case(input_value, output_value):
    """ test to_camel_case """
    assert to_camel_case(input_value) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ([0.55, 1.22], {"lon": 0.55, "lat": 1.22}),
            ({"lon": 0.55, "lat": 1.22}, {"lon": 0.55, "lat": 1.22}),
            (452, 452)
        ]
)
def test_convert_coordinates(input_value, output_value):
    """ test convert_coordinates """
    assert convert_coordinates(input_value) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ("test", "test"),
            ({'features': [{'properties': {'addendum': {'best': {'x': 'y'}}},
                            'geometry': {'coordinates': [0, 1]},
                            'bepelias': {'a': 'b'}}
                           ],
              'bepelias': {'A': 'B'}},
             {'items': [{'coordinates': {'lat': 1, 'lon': 0}, 'x': 'y', 'a': 'b'}],
              'A': 'B',
              'peliasRaw': {'features': [{'geometry': {'coordinates': [0, 1]}, 'properties': {}}]},
              'total': 1}),
            ({'features': [{'properties': {'name': "testname"},
                            'geometry': {'coordinates': [0, 1]}}
                           ]},
             {'items': [{'coordinates': {'lat': 1, 'lon': 0}, 'name': 'testname'}],
              'peliasRaw': {'features': [{'geometry': {'coordinates': [0, 1]}, 'properties': {'name': 'testname'}}]},
              'total': 1})
        ]
)
def test_to_rest_guidelines(input_value, output_value):
    """ test to_rest_guidelines """
    assert to_rest_guidelines(input_value) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ([{
               "bepelias": {"precision": "test_prec"},
               'properties': {
                    'source': 'test_src',
                    'locality': 'test_loc',
                    'addendum': {
                        'best': {
                            "housenumber": "123",
                            "postal_info": {"postal_code": "1000"},
                            "street": {"name": {"fr": "rue de test"}},
                            "municipality": {"name": {"fr": "Saint-Gilles"}}
                        }}}}],
             [{"source": "test_src",
               "precision": "test_prec",
               "housenumber": "123",
               "postal_code": "1000",
               "street": {'fr': "rue de test"},
               "city": {'fr': 'Saint-Gilles'}}]),
            ([{'properties': {'source': 'test_src'}}],
             [{"source": "test_src", "precision": None, "city": None}])
        ]
)
def test_feature_to_df(input_value, output_value):
    """ test get_feature_city_names """
    df = feature_to_df(input_value, to_string=False)
    expected_df = pd.DataFrame(output_value)

    assert df.shape == expected_df.shape
    assert set(df.columns) == set(expected_df.columns)
    expected_df = expected_df[df.columns]
    assert (df.fillna("-") == expected_df.fillna("-")).all().all()

    df_str = feature_to_df(input_value, to_string=True)
    for col in expected_df.columns:
        assert col in df_str
    for _, row in expected_df.iterrows():
        for cell in row:
            if pd.isna(cell):
                assert "None" in df_str
            else:
                assert str(cell) in df_str


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
    assert transform(input_value[0], input_value[1], remove_patterns) == output_value
