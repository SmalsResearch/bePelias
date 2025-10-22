"""
Unitest for bepelias 'utils' functions using pytest
"""
import sys
import pytest

sys.path.append("src/")

from bepelias.utils import (to_camel_case, convert_coordinates,  # pylint: disable=C0413, E0401 # noqa: E402
                            to_rest_guidelines, pelias_check_postcode,
                            get_street_names, remove_street_types,
                            is_partial_substring, apply_sim_functions)


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
            ([{'features': [{'properties': {'postalcode': 1160}},
                            {'properties': {'postalcode': 1161}},
                            {'properties': {'postalcode': 1170}}]}, 1160],
             ({'features': [{'properties': {'postalcode': 1160}},
                            {'properties': {'postalcode': 1161}}]})),
            ([{}, 1160], {'features': []})
        ]
)
def test_pelias_check_postcode(input_value, output_value):
    """ test convert_coordinates """
    assert pelias_check_postcode(input_value[0], input_value[1]) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ({'properties':
                {'street': "av fonsny",
                 'addendum': {'best': {
                    "streetname_fr": "avenue fonsny",
                    "streetname_nl": "fonsnylaan",
                    "streetname_de": "fonsnylaan de"
                 }}}},
             ['AV FONSNY', 'AVENUE FONSNY', 'FONSNYLAAN', 'FONSNYLAAN DE']),
            ({'properties': {'street': "av fonsny"}},
             ['AV FONSNY'])
        ]
)
def test_get_street_names(input_value, output_value):
    """ test get_street_names """
    assert list(get_street_names(input_value)) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ("AVENUE FONSNY", "FONSNY"),
            ("FONSNYLAAN", "FONSNY"),
            ("RUE DE LA LIBERTE", "LIBERTE"),
            ("CHAUSSEE DE WAVRE", "WAVRE")
        ]
)
def test_remove_street_types(input_value, output_value):
    """ test remove_street_types """
    assert remove_street_types(input_value) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            (["Rue Albert", "Rue Marcel Albert"], 1),
            (["Rue Marcel Albert", "Rue Albert"], 1),
            (["Rue Albert", "Rue Albert Marcel"], 1),
            (["Rue Albert", "Rue Marcel"], 0)
        ]
)
def test_is_partial_substring(input_value, output_value):
    """ test is_partial_substring """
    assert is_partial_substring(input_value[0], input_value[1]) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            (["Rue Albert", "Rue albert", 0.9], 0.96),
            (["Rue Albert", "Rue olbert", 1.0], None),
        ]
)
def test_apply_sim_functions(input_value, output_value):
    """ test is_partial_substring """
    assert apply_sim_functions(input_value[0], input_value[1], input_value[2]) == output_value
