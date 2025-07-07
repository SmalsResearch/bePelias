"""All functions need by bepelias main module

"""
import logging
import re
import copy


import textdistance


# General functions


def log(arg):
    """
    Message printed if DEBUG_LEVEL is HIGH or MEDIUM

    Parameters
    ----------
    arg : object
        object to print.

    Returns
    -------
    None.
    """
    logging.info(arg)


def vlog(arg):
    """
    Message printed if DEBUG_LEVEL is HIGH

    Parameters
    ----------
    arg : object
        object to print.

    Returns
    -------
    None.
    """
    logging.debug(arg)


def to_camel_case(data):
    """
    Convert a snake_case object to a camelCase.
    If d is a string, convert the string
    If d is a dict, convert all keys, recursively (i.e., values are dict or list), but not simple values
    If d is a list, convert all objects in the list

    Parameters
    ----------
    data: str, dict or list
        Object to camelize

    Returns
    -------
    Object of the same structure as data, but where :
    - dictionary keys have been camelized if data is a dict
    - input string has been camelized if data is a string
    """

    if isinstance(data, str):
        return re.sub(r"(_)([a-z0-9])", lambda m: m.group(2).upper(),  data)
    if isinstance(data, dict):
        return {to_camel_case(key): to_camel_case(item) if isinstance(item, (dict, list)) else item for key, item in data.items()}
    if isinstance(data, list):
        return [to_camel_case(item) for item in data]
    return data


def convert_coordinates(coordinates):
    """ Convert coordinates to format "{'lat':yy, 'lon':xx}"

    Parameters
    ----------
    coordinates: list or dict

    Returns
    -------
    {'lat':yy, 'lon':xx}
    """
    if isinstance(coordinates, list) and len(coordinates) == 2:
        return {"lat": coordinates[1],
                "lon": coordinates[0]}
    if isinstance(coordinates, dict) and "lat" in coordinates and "lon" in coordinates:
        return coordinates

    log("Cannot convert coordinates!!")
    log(coordinates)
    return coordinates


def to_rest_guidelines(pelias_res, with_pelias_raw=True):
    """Convert a pelias result into a REST Guideline compliant object

    Args:
        pelias_res (dict): (be)pelias result

    Returns:
        dict: REST Guideline compliant version of input
    """

    vlog("Converting to to_rest_guidelines")
    if not isinstance(pelias_res, dict):
        return pelias_res
    items = []
    for feat in pelias_res["features"]:
        if "addendum" in feat["properties"] and "best" in feat["properties"]["addendum"]:
            item = feat["properties"]["addendum"]["best"]
            item["coordinates"] = convert_coordinates(feat["geometry"]["coordinates"])
        else:
            item = {"coordinates": convert_coordinates(feat["geometry"]["coordinates"]),
                    "name": feat["properties"]["name"]}
        if "bepelias" in feat:
            item |= feat["bepelias"]
        items.append(item)
    # Remove duplicate results
    items = [i for n, i in enumerate(items) if i not in items[:n]]

    rest_res = {"items": items,
                "total": len(items)}

    if "bepelias" in pelias_res:
        rest_res |= pelias_res["bepelias"]

    rest_res = to_camel_case(rest_res)

    if with_pelias_raw:
        pelias_res_raw = copy.deepcopy(pelias_res)
        for fld in ["bepelias", "score"]:
            if fld in pelias_res_raw:
                del pelias_res_raw[fld]
        for feat in pelias_res_raw["features"]:
            vlog(feat)
            if "addendum" in feat["properties"]:
                del feat["properties"]["addendum"]
            if "bepelias" in feat:
                del feat["bepelias"]
        rest_res["peliasRaw"] = pelias_res_raw

    vlog(rest_res)
    return rest_res

# Check result functions


def pelias_check_postcode(pelias_res, postcode, match_length=3):
    """
    List a Pelias feature list by removing all feature having a postcode which
    does not start by the same 'match_length' digits as 'postcode'. If no postal code is
    provide in a feature, keep it

    Parameters
    ----------
    pelias_res : list
        List of Pelias features.
    postcode : str in int
        Postal code

    Returns
    -------
    list
        Same as 'pelias_res', but excluding mismatching results.
    """

    if "features" not in pelias_res:  # Should not occur!
        log("Missing features in pelias_res:")
        log(pelias_res)
        pelias_res["features"] = []

    nb_res = len(pelias_res["features"])
    filtered_feat = list(filter(lambda feat: "postalcode" not in feat["properties"] or str(feat["properties"]["postalcode"])[0:match_length] == str(postcode)[0:match_length],
                                pelias_res["features"]))

    pelias_res["features"] = filtered_feat

    vlog(f"Check postcode : {nb_res} --> {len(filtered_feat)}")
    return pelias_res


def get_street_names(feature):
    """
    From a Pelias feature, extract all possible street name

    Parameters
    ----------
    feature : dict
        Pelias feature.

    Yields
    ------
    str
        street name.
    """

    if "street" in feature["properties"]:
        yield feature["properties"]["street"].upper()
    if "addendum" not in feature["properties"] or "best" not in feature["properties"]["addendum"]:
        return

    best = feature["properties"]["addendum"]["best"]
    for n in ["streetname_fr", "streetname_nl", "streetname_de"]:
        if n in best:
            yield best[n].upper()


def remove_street_types(street_name):
    """
    From a street name, remove most 'classical' street types, in French and Dutch
    (Rue, Avenue, Straat...). Allow to improve string comparison reliability

    Parameters
    ----------
    street_name : str
        A street name.

    Returns
    -------
    str
        Cleansed version of input street_name.
    """

    to_remove = ["^RUE ", "^AVENUE ", "^CHAUSSEE ", "^ALLEE ", "^BOULEVARD ", "^PLACE ",
                 "STRAAT$", "STEENWEG$", "LAAN$"]

    for s in to_remove:
        street_name = re.sub(s, "", street_name)

    to_remove = ["^DE LA ", "^DE ", "^DU ", "^DES "]

    for s in to_remove:
        street_name = re.sub(s, "", street_name)

    return street_name.strip()


def is_partial_substring(s1, s2):
    """
    Check that s1 (assuming s1 is shorter than s2) is a subsequence of s2, i.e.,
    s1 can be obtained by removing some characters to s2.
    Example:"Rue Albert" vs "Rue Marcel Albert", or vs "Rue Albert Marcel". "Rue M. Albert" vs "Rue Marcel Albert"

    Parameters
    ----------
    s1 : str

    s2 : str

    Returns
    -------
    int
        1 if the shortest can be obtained by removing some characters from the longest
        0 otherwise
    """

    s1 = re.sub("[. ]", "", s1)
    s2 = re.sub("[. ]", "", s2)

    if len(s1) > len(s2):
        s1, s2 = s2, s1

    while len(s1) > 0 and len(s2) > 0:
        if s1[0] == s2[0]:
            s1 = s1[1:]
            s2 = s2[1:]
        else:
            s2 = s2[1:]

    return int(len(s1) == 0)  # and len(s2)==0


def apply_sim_functions(str1, str2, threshold):
    """
    Apply a sequence of similarity functions on (str1, str2) until one give a value
    above "threshold", and return this value. If none of them are above the threshold,
    return None

    Following string similarities are tested: Jaro-Winkler, Sorensen-Dice,
        Levenshtiein similarity

    Parameters
    ----------
    str1 : str
        Any string
    str2: str
        Any string.
    threshold : float
        String similarity we want to reach.

    Returns
    -------
    sim : float or None
        First string similarity between str1 and str2 bellow threshold. If None
        of them if bellow, return None.
    """

    sim_functions = [textdistance.jaro_winkler,
                     textdistance.sorensen_dice,
                     lambda s1, s2: 1 - textdistance.levenshtein(s1, s2)/max(len(s1), len(s2)),
                     is_partial_substring
                     ]
    for sim_fct in sim_functions:
        sim = sim_fct(str1, str2)
        if sim >= threshold:
            return sim
    return None
