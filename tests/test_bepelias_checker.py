"""
Unitest for bepelias ResultChecker class functions using pytest
"""
import sys
import logging
import pytest

sys.path.append("src/")

from bepelias.result_checker import ResultChecker  # pylint: disable=wrong-import-position, import-error # noqa: E402

from bepelias.bepelias import remove_patterns  # pylint: disable=wrong-import-position, import-error # noqa: E402


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

res_checker = ResultChecker(remove_patterns=remove_patterns, postcode_match_length=3, similarity_threshold=0.9)


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
    assert res_checker.check_locality(input_value[0], input_value[1]) == output_value


@pytest.mark.parametrize(
        "input_value, sim_value",
        [
            (["", None], 1),
            ([{'properties':
                {'street': "avenue fonsny"}}, "AVENUE FONSNY"], 1),
            ([{'properties':
                {'street': "avenue fonsny, saint-gilles"}}, "AVENUE FONSNY"], 0.85),
            ([{'properties':
                {'street': "avenue fonsny",
                 'addendum': {'best': {
                    "streetname_fr": "avenue fonsny",
                    "streetname_nl": "fonsnylaan"
                 }}}}, "CHAUSSEE DE WAVRE"], None),
            ([{'properties':
                {}}, "CHAUSSEE DE WAVRE"], 1),
            ([{'properties':
                {'street': "avenue fonsny"}}, "AVENUE FONSNY (saint gilles)"], 0.85),
        ]
)
def test_check_streetname(input_value, sim_value):
    """ test check_streetname """
    val = res_checker.check_streetname(input_value[0], input_value[1])
    assert val is None and sim_value is None or val >= sim_value


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
    assert res_checker.filter_streetname(input_value[0], input_value[1]) == output_value


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
    assert res_checker.check_postcode(input_value[0], input_value[1]) == output_value


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
def test_get_feature_street_names(input_value, output_value):
    """ test get_feature_street_names """
    assert list(res_checker.get_feature_street_names(input_value)) == output_value


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            ({'properties':
                {'addendum': {'best': {
                    "postname_fr": "saint-gilles",
                    "postname_nl": "sint-gillis",
                    "municipality_name_fr": "Saint-Gilles",
                 }}}},
             ['SAINT-GILLES', 'SINT-GILLIS']),
            ({'properties': {'street': "av fonsny"}},
             [])
        ]
)
def test_get_feature_city_names(input_value, output_value):
    """ test get_feature_city_names """
    assert list(res_checker._get_feature_city_names(input_value)) == output_value  # pylint: disable=protected-access


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
    assert res_checker._remove_street_types(input_value) == output_value  # pylint: disable=protected-access


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
    assert res_checker._is_partial_substring(input_value[0], input_value[1]) == output_value  # pylint: disable=protected-access


@pytest.mark.parametrize(
        "input_value, output_value",
        [
            (["Rue Albert", "Rue albert", 0.9], 0.96),
            (["Rue Albert", "Rue olbert", 1.0], None),
        ]
)
def test_apply_sim_functions(input_value, output_value):
    """ test is_partial_substring """
    assert res_checker._apply_sim_functions(input_value[0], input_value[1], input_value[2]) == output_value  # pylint: disable=protected-access
