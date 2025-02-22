"""All functions need by bepelias main module

"""
import logging
import re
import copy

from flask import request

import textdistance
from unidecode import unidecode
import pandas as pd

transformer_sequence = [
    [],
    ["clean"],
    ["clean", "no_city"],
    ["no_city"],
    ["clean_hn"],
    ["no_city", "clean_hn"],
    ["clean", "no_city", "clean_hn"],
    ["no_hn"],
    ["no_city", "no_hn"],
    ["no_street"],
]

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


def get_arg(argname, def_val):
    """
    Get argument from request form. Sometimes get it from request.form (from payload),
    sometimes from request.args.get (from url args)

    Parameters
    ----------
    argname : str
        Argument name.
    def_val : str
        Default value.

    Returns
    -------
    str
        Argument value from request.
    """

    if argname in request.form:  # in payload
        return request.form[argname]
    return request.args.get(argname, def_val)  # in args


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

    log("Converting to to_rest_guidelines")
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

    rest_res = {"self": request.url,
                "items": items,
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
            log(feat)
            if "addendum" in feat["properties"]:
                del feat["properties"]["addendum"]
            if "bepelias" in feat:
                del feat["bepelias"]
        rest_res["peliasRaw"] = pelias_res_raw

    log(rest_res)
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


def check_locality(feature, locality_name, threshold=0.8):
    """
    Check that a feature contains a locality name close enough to "locality_name"
    (with a similarity at least equal to threshold)

    Parameters
    ----------
    feature : dict
        A Pelias feature.
    locality_name : str
        Input locality name.
    threshold : float, optional
        DESCRIPTION. The default is 0.8.

    Returns
    -------
    float or None
        1 if feature does not contain any street name or locality_name is null.
        a value between threshold and 1 if a street name matches
        None if no street name matches
    """

    if pd.isnull(locality_name):
        return 1

    prop = feature["properties"]

    if "locality" in prop:
        sim = apply_sim_functions(unidecode(locality_name).lower(),
                                  prop["locality"].lower(),
                                  threshold)
        if sim and sim >= threshold:
            vlog(f"locality ('{locality_name}' vs '{prop['locality']}'): {sim}")
            return sim

    if "addendum" in prop and "best" in prop["addendum"]:
        for c in ["postname", "municipality_name", "part_of_municipality_name"]:
            for lang in ["fr", "nl", "de"]:
                if f"{c}_{lang}" in prop["addendum"]["best"]:

                    cty = unidecode(prop["addendum"]["best"][f"{c}_{lang}"].lower())
                    sim = apply_sim_functions(unidecode(locality_name).lower(), cty, threshold)
                    vlog(f"{c}_{lang} ('{locality_name}' vs '{cty}'): {sim}")
                    if sim and sim >= threshold:
                        return sim

    return None


def check_streetname(feature, street_name, threshold=0.8):
    """
    Check that a feature contains a street name close enough to "street_name"
    (with a similarity at least equal to threshold)

    Parameters
    ----------
    feature : dict
        A Pelias feature.
    street_name : str
        Input street name.
    threshold : float, optional
        DESCRIPTION. The default is 0.8.

    Returns
    -------
    float or None
        1 if feature does not contain any street name or street_name is null.
        a value between threshold and 1 if a street name matches
        None if no street name matches
    """

    if pd.isnull(street_name):
        return 1

    street_name = remove_street_types(unidecode(street_name.upper()))

    for pat, rep in remove_patterns:
        street_name = re.sub(pat, rep, street_name) if not pd.isnull(street_name) else None

    feat_street_names = []

    vlog(f"checking '{street_name}'")
    for feat_street_name in get_street_names(feature):

        feat_street_name = remove_street_types(unidecode(feat_street_name))
        if feat_street_name in feat_street_names:
            continue

        sim = apply_sim_functions(feat_street_name, street_name, threshold)
        vlog(f"'{street_name}' vs '{feat_street_name}': {sim}")
        if sim:
            return sim

        feat_street_names.append(feat_street_name)

    if len(feat_street_names) == 0:  # No street name found --> ok
        return 1

    # Cleansing
    for pat, rep in remove_patterns:
        street_name = re.sub(pat, rep, street_name)
        for i, _ in enumerate(feat_street_names):
            feat_street_names[i] = re.sub(pat, rep, feat_street_names[i])

    for feat_street_name in get_street_names(feature):
        sim = apply_sim_functions(feat_street_name, street_name, threshold)
        if sim:
            return sim

    # Adding city name

    for c in ["postname_fr", "postname_nl", "postname_de",
              "municipality_name_fr", "municipality_name_nl", "municipality_name_de"]:

        if "addendum" in feature["properties"] and "best" in feature["properties"]["addendum"] and c in feature["properties"]["addendum"]["best"]:
            cty = unidecode(feature["properties"]["addendum"]["best"][c].upper())

            for feat_street_name in get_street_names(feature):
                sim = apply_sim_functions(f"{cty}, {feat_street_name}", street_name, threshold)
                if sim:
                    return sim
    return None


def check_best_streetname(pelias_res, street_name, threshold=0.8):
    """
    Filter a Pelias feature list to keep only with a street name similar to "street_name"

    Parameters
    ----------
    pelias_res : dict
        Pelias result.
    street_name : str
        Input street name.
    threshold : float, optional
        Similarity threshold. The default is 0.8.

    Returns
    -------
    dict
        A Pelias result with only features matching street_name.
    """

    nb_res = len(pelias_res["features"])

    filtered_feat = list(filter(lambda feat: check_streetname(feat, street_name, threshold) is not None,
                                pelias_res["features"]))

    pelias_res["features"] = filtered_feat

    vlog(f"Check street : {nb_res} --> {len(filtered_feat)}")
    return pelias_res

# Main logig functions


def is_building(feature):
    """
    Check that a Pelias feature corresponds to the position of a building

    Parameters
    ----------
    feature : dict
        A pelias feature.

    Returns
    -------
    bool
        True if the feature corresponds to a building.
    """

    return (feature["properties"]["match_type"] in ("exact", "interpolated") or feature["properties"]["accuracy"] == "point") and "housenumber" in feature["properties"]


def interpolate(feature, pelias):
    """
    Try to interpolate the building position (typically because coordinates are missing)

    Parameters
    ----------
    feature : str
        A Pelias feature.

    Returns
    -------
    interp_res : dict
        Object containing interpolated geometry.
    """

    # get street center
    if 'street' not in feature['properties']:
        log("No street property in feature: ")
        log(feature['properties'])
        return {}
    if 'postalcode' not in feature['properties']:
        log("No postalcode property in feature: ")
        log(feature['properties'])
        return {}

    addr = {"address": f"{feature['properties']['street']}",
            "postalcode": feature['properties']['postalcode'],
            "locality": ""}
    street_res = pelias.geocode(addr)
    log(f"Interpolate: street center: {street_res}")

    # Keep only results maching input postalcode

    street_res["features"] = list(filter(lambda f: f["properties"]["postalcode"] == feature['properties']['postalcode'] if "postalcode" in f["properties"] else False,
                                         street_res["features"]))

    if len(street_res["features"]) == 0:
        return {}

    street_center_coords = street_res["features"][0]["geometry"]["coordinates"]
    log(f"street_center_coords: {street_center_coords}")

    interp_res = pelias.interpolate(lat=street_center_coords[1],
                                    lon=street_center_coords[0],
                                    number=feature['properties']['housenumber'],
                                    street=feature['properties']['street'])

    if len(interp_res) == 0:
        interp_res = {"street_geometry": {"coordinates": street_center_coords}}

    log(interp_res)
    return interp_res


def build_address(street_name, house_number):
    """
    Build a string in the style "street_name, house_number", taking into account
    that both arguments could be empty:
        - if street_name is null or empty : returns ""
        - else if house_number is null or empty: return street_name
        - otherwise, return "street_name, house_number"

    Parameters
    ----------
    street_name : str
        Street name.
    house_number : str
        House number.

    Returns
    -------
    str
        "street_name, house_number", unless one of them is empty
    """
    if pd.isnull(street_name) or len(street_name.strip()) == 0:
        return ""

    if pd.isnull(house_number) or len(house_number.strip()) == 0:
        return street_name

    return f"{street_name}, {house_number}"


def build_city(post_code, post_name):
    """Build a string containing a post code and a city name, both of them being possibly empty

    Args:
        post_code (str): postcode, or "", or None
        post_name (str): city name, or "", or None

    Returns:
        str: something like "1000 Bruxelles", "or "Bruxelles"
    """

    if pd.isnull(post_code) or len(post_code) == 0:
        return post_name or ""

    if pd.isnull(post_name) or len(post_name) == 0:
        return post_code or ""

    return f"{post_code} {post_name}"


def search_for_coordinates(feat, pelias):
    """
    If a feature has (0,0) as coordinates, try to find better location:
    - If address contains boxes and the first box has non null coordinates, use them
    - Otherwise, try the interpolation engine
    """

    log("Coordinates==0,0, check if any box number contains coordinates...")

    try:
        boxes = feat["properties"]["addendum"]["best"]["box_info"]
    except KeyError:
        boxes = []

    if len(boxes) > 0 and boxes[0]["coordinates"]["lat"] != 0:
        vlog("Found coordinates in first box number")
        feat["geometry"]["coordinates_orig"] = [0, 0]
        feat["geometry"]["coordinates"] = boxes[0]["coordinates"]["lon"], boxes[0]["coordinates"]["lat"]
        feat["bepelias"] = {"interpolated": "from_boxnumber"}
    else:
        log("Coordinates==0,0, try to interpolate...")
        interp = interpolate(feat, pelias)
        if "geometry" in interp:
            feat["geometry"]["coordinates_orig"] = [0, 0]
            feat["geometry"]["coordinates"] = interp["geometry"]["coordinates"]
            feat["bepelias"] = {"interpolated": True}
        elif "street_geometry" in interp:
            feat["geometry"]["coordinates_orig"] = [0, 0]
            feat["geometry"]["coordinates"] = interp["street_geometry"]["coordinates"]
            feat["bepelias"] = {"interpolated": "street_center"}


def struct_or_unstruct(street_name, house_number, post_code, post_name, pelias, check_postcode=True):
    """
    Try structed version of Pelias. If it did not succeed, try the unstructured version, and keep the best result.

    Parameters
    ----------
    street_name : str
        Street name.
    house_number : str
        House number.
    post_code : str
        Postal code.
    post_name : str
        City name.

    Returns
    -------
    dict
        Pelias result.
    """

    vlog(f"struct_or_unstruct('{street_name}', '{house_number}', '{post_code}', '{post_name}', {check_postcode})")
    # Try structured
    addr = {"address": build_address(street_name, house_number),
            "locality": post_name}
    if post_code is not None:
        addr["postalcode"] = post_code

    vlog(f"Call struct: {addr}")

    layers = None
    # If street name is empty, prevent to receive a "street" of "address" result by setting layers to "locality"
    if street_name is None or len(street_name) == 0:
        layers = "locality"
    # If there is no digit in street+housenumber, only keep street and locality layers
    elif re.search("[0-9]", addr["address"]) is None:
        layers = "street,locality"
    pelias_struct = pelias.geocode(addr, layers=layers)

    pelias_struct["bepelias"] = {"call_type": "struct",
                                 "in_addr": addr,
                                 "pelias_call_count": 1}

    if post_code is not None:
        if check_postcode:
            pelias_struct = pelias_check_postcode(pelias_struct, post_code)
    else:
        vlog("No postcode in input")

    if len(pelias_struct["features"]) > 0:
        for feat in pelias_struct["features"]:
            vlog(feat["properties"]["name"] if "name" in feat["properties"] else feat["properties"]["label"] if "label" in feat["properties"] else "--")
            if is_building(feat):
                if feat["geometry"]["coordinates"] == [0, 0]:
                    search_for_coordinates(feat, pelias)

                vlog("Found a building in res1")
                vlog(feat)
                vlog("pelias_struct")
                vlog(pelias_struct)
                vlog("-------")

                return pelias_struct

    # Try unstructured
    addr = build_address(street_name, house_number) + ", " + build_city(post_code, post_name)
    addr = re.sub("^,", "", addr.strip()).strip()
    addr = re.sub(",$", "", addr).strip()
    vlog(f"Call unstruct: '{addr}'")
    if addr and len(addr.strip()) > 0 and not re.match("^[0-9]+$", addr):
        pelias_unstruct = pelias.geocode(addr, layers=layers)
        cnt = 2
    else:
        vlog("Unstructured: empty inputs or only numbers, skip call")
        cnt = 1
        pelias_unstruct = {"features": []}
    pelias_unstruct["bepelias"] = {"call_type": "unstruct",
                                   "in_addr": addr,
                                   "pelias_call_count": cnt}
    pelias_struct["bepelias"]["pelias_call_count"] = cnt

    if post_code is not None:
        if check_postcode:
            pelias_unstruct = pelias_check_postcode(pelias_unstruct, post_code)
    else:
        vlog("No postcode in input")

    pelias_unstruct = check_best_streetname(pelias_unstruct, street_name)

    if len(pelias_unstruct["features"]) > 0:

        for feat in pelias_unstruct["features"]:
            vlog(feat["properties"]["name"] if "name" in feat["properties"] else feat["properties"]["label"] if "label" in feat["properties"] else "--")
            if is_building(feat):
                if feat["geometry"]["coordinates"] == [0, 0]:
                    search_for_coordinates(feat, pelias)
                return pelias_unstruct

    # No result has a building precision -> get the best one, according the first feature

    # If confidence of struct is better that confidence of unstruct OR struct contains 'street' --> choose struct
    if len(pelias_struct["features"]) > 0:
        if (pelias_unstruct["features"]) and len(pelias_unstruct["features"]) > 0 \
           and pelias_struct["features"][0]["properties"]["confidence"] > pelias_unstruct["features"][0]["properties"]["confidence"] \
           or "street" in pelias_struct["features"][0]["properties"]:
            return pelias_struct

    # Otherwise, if 'street' in unstruct --> choose unstruct
    if len(pelias_unstruct["features"]) > 0 and "street" in pelias_unstruct["features"][0]["properties"]:
        return pelias_unstruct

    # Otherwise, if there are struct result --> choose struct
    if len(pelias_struct["features"]) > 0:
        return pelias_struct

    # Otherwhise, choose unstruct
    return pelias_unstruct


remove_patterns = [(r"\(.+\)$",      ""),
                   ("[, ]*(SN|ZN)$", ""),
                   ("' ", "'"),
                   (" [a-zA-Z][. ]", " "),
                   ("[.]", " "),
                   (",[a-zA-Z .'-]*$", " ")
                   ]


def transform(addr_data, transformer):
    """
    Transform an address applying a transformer.

    Parameters
    ----------
    addr_data : dict
        dict with fields "post_name", "house_number", "street_name"
    transformer : str
        Transformer name. Could be:
            - no_city: Remove city name
            - no_hn: Remove house number
            - clean_hn: Clean house number, by keeping only the first sequence of digits
            - clean: Clean street and city names, by applying the substitutions
              described in 'remove_patterns'

    Returns
    -------
    addr_data : dict
    """

    addr_data = addr_data.copy()

    if transformer == "no_city":
        addr_data["post_name"] = ""

    elif transformer == "no_hn":
        addr_data["house_number"] = ""

    elif transformer == "no_street":
        addr_data["street_name"] = ""
        addr_data["house_number"] = ""

    elif transformer == "clean_hn":
        if "house_number" in addr_data and not pd.isnull(addr_data["house_number"]):
            if "-" in addr_data["house_number"]:
                addr_data["house_number"] = addr_data["house_number"].split("-")[0].strip()  # useful? "match" bellow will do the same

            hn = re.match("^[0-9]+", addr_data["house_number"])
            if hn:
                addr_data["house_number"] = hn[0]
    elif transformer == "clean":
        for pat, rep in remove_patterns:
            addr_data["street_name"] = re.sub(pat, rep, addr_data["street_name"]) if not pd.isnull(addr_data["street_name"]) else None
            addr_data["post_name"] = re.sub(pat, rep, addr_data["post_name"]) if not pd.isnull(addr_data["post_name"]) else None

    return addr_data


def get_precision(feature):
    """Get the precision of a pelias result feature

    Args:
        pelias_res (dict): pelias result

    Returns:
        str: a value amongst address, address_00, street_center, address_streetcenter, address_interpol,
                street_interpol, street_00, street,
                city_00, city, country
    """

    log("get_precision")
    try:
        feat_prop = feature["properties"]
        if feat_prop["layer"] == "address":
            if feature["geometry"]["coordinates"] == [0, 0]:
                return "address_00"
            if 'interpolated' in feature['bepelias'] and feature['bepelias']['interpolated'] == 'street_center':
                return "address_streetcenter"
            if 'interpolated' in feature['bepelias'] and feature['bepelias']['interpolated'] is True:
                return "address_interpol"
            if feat_prop["match_type"] == "interpolated":
                if "/streetname/" in feat_prop["id"].lower() or "/straatnaam/" in feat_prop["id"].lower():
                    return "street_interpol"
                return "address_interpol2"  # Should not occur?
            if feat_prop["match_type"] == "exact" or feat_prop["accuracy"] == "point":
                return "address"

        if feat_prop["layer"] == "street":
            if feature["geometry"]["coordinates"] == [0, 0]:
                return "street_00"
            return "street"

        if feat_prop["layer"] in ("city", "locality", "postalcode", "localadmin", "neighbourhood"):
            if feature["geometry"]["coordinates"] == [0, 0]:
                return "city_00"
            return "city"

        if feat_prop["layer"] in ("region", "macroregion", "county"):
            return "country"

    except KeyError as e:
        log("KeyError in get_precision")
        log(feature)
        log(e)
        return "[keyerror]"

    return "[todo]"


def add_precision(pelias_res):
    """Add 'precision' to each features item
    Args:
        pelias_res (dict): pelias result

    Returns:
        None
    """

    # log("add precision")
    for feat in pelias_res["features"]:
        if "bepelias" not in feat:
            feat["bepelias"] = {}
        feat["bepelias"]["precision"] = get_precision(feat)


def advanced_mode(street_name, house_number, post_code, post_name, pelias):
    """The full logic of bePelias

    Args:
        street_name (str): Street name
        house_number (str): House number
        post_code (str): Postal code
        post_name (str): Post (city/locality/...) name
        pelias (Pelias): Pelias object

    Returns:
        dict: json result
    """

    addr_data = {"street_name": street_name,
                 "house_number": house_number,
                 "post_name": post_name,
                 "post_code": post_code}
    all_res = []

    call_cnt = 0
    for check_postcode in [True, False]:
        previous_attempts = []
        for transf in transformer_sequence:
            transf_addr_data = addr_data.copy()
            for t in transf:
                transf_addr_data = transform(transf_addr_data, t)

            log(f"transformed address: ({ ';'.join(transf)})")
            if transf_addr_data in previous_attempts:
                vlog("Transformed address already tried, skip Pelias call")
            elif len(list(filter(lambda v: v and len(v) > 0, transf_addr_data.values()))) == 0:
                vlog("No value to send, skip Pelias call")
            else:
                previous_attempts.append(transf_addr_data)

                pelias_res = struct_or_unstruct(transf_addr_data["street_name"],
                                                transf_addr_data["house_number"],
                                                transf_addr_data["post_code"],
                                                transf_addr_data["post_name"],
                                                pelias,
                                                check_postcode=check_postcode)
                pelias_res["bepelias"]["transformers"] = ";".join(transf) + ("(no postcode check)" if not check_postcode else "")
                call_cnt += pelias_res["bepelias"]["pelias_call_count"]

                if len(pelias_res["features"]) > 0 and is_building(pelias_res["features"][0]):
                    pelias_res["bepelias"]["pelias_call_count"] = call_cnt
                    add_precision(pelias_res)
                    return pelias_res
                all_res.append(pelias_res)
        if sum(len(r["features"]) for r in all_res) > 0:
            # If some result were found (even street-level), we stop here and select the best one.
            # Otherwise, we start again, accepting any postcode in the result
            log("Some result found with check_postcode=True")
            break

    log("No building result, keep the best match")
    # Get a score for each result
    fields = ["housenumber", "street", "locality", "postalcode", "best"]
    scores = []
    for res in all_res:
        score = {}
        res["score"] = 0
        if len(res["features"]) > 0:
            prop = res["features"][0]["properties"]
            if "postalcode" in prop and prop["postalcode"] == post_code:
                score["postalcode"] = 1.5

            locality_sim = check_locality(res["features"][0], post_name, threshold=0.8)
            if locality_sim:
                score["locality"] = 1.0+locality_sim

            if "street" in prop:
                score["street"] = 1.0
                street_sim = check_streetname(res["features"][0], street_name, threshold=0.8)
                if street_sim:
                    score["street"] += street_sim

            if "housenumber" in prop:
                score["housescore"] = 0.5
                if prop["housenumber"] == house_number:
                    score["housescore"] += 1.0
                else:
                    n1 = re.match("[0-9]+", prop["housenumber"])
                    n2 = re.match("[0-9]+", house_number)
                    if n1 and n2 and n1[0] == n2[0]:  # len(n1)>0 and n1==n2:
                        score["housescore"] += 0.8
            if res["features"][0]["geometry"]["coordinates"] != [0, 0]:
                score["coordinates"] = 1.5

            # log('res["features"]["addendum"]:')
            # log(res["features"])
            if "addendum" in res["features"][0]["properties"] and "best" in res["features"][0]["properties"]["addendum"]:
                score["best"] = 1.0

            res["score"] = sum(score.values())

            score_line = {f: prop[f] if f in prop else '[NA]' for f in fields}
            score_line["coordinates"] = str(res["features"][0]["geometry"]["coordinates"])
            for f in fields + ["coordinates"]:
                if f in score:
                    score_line[f] += f" ({score[f]:.3})"

            score_line["score"] = res["score"]
            scores.append(score_line)

    with pd.option_context("display.max_columns", None, 'display.width', None):
        log("\n"+str(pd.DataFrame(scores)))

    all_res = sorted(all_res, key=lambda x: -x["score"])
    if len(all_res) > 0:
        final_res = all_res[0]
        if len(final_res["features"]) == 0:
            return {"features": [], "bepelias": {"pelias_call_count": call_cnt}}

        final_res["bepelias"]["pelias_call_count"] = call_cnt

        add_precision(final_res)

        return final_res

    return {"features": [], "bepelias": {"pelias_call_count": call_cnt}}


def call_unstruct(address, pelias):
    """
    Call the unstructured version of Pelias with "address" as input
    If Pelias was able to parse the address (i.e., split it into component),
    we try to check that the results is not too far away from the input.

    Args:
        address (str): full address in a single string
        pelias (Pelias): Pelias object

    Returns:
        dict: json result
    """

    layers = None
    # If there is no digit in street+housenumber, only keep street and locality layers
    if re.search("[0-9]", address) is None:
        layers = "street,locality"

    pelias_unstruct = pelias.geocode(address, layers=layers)

    pelias_unstruct["bepelias"] = {"call_type": "unstruct",
                                   "in_addr": address,
                                   "pelias_call_count": 1}

    parsed = pelias_unstruct["geocoding"]["query"]["parsed_text"]

    log(f"parsed: {parsed}")

    if "postalcode" in parsed:
        pelias_unstruct = pelias_check_postcode(pelias_unstruct, parsed["postalcode"])

    else:
        vlog("No postcode in input")

    if "street" in parsed:
        pelias_unstruct = check_best_streetname(pelias_unstruct, parsed["street"])

    add_precision(pelias_unstruct)

    return pelias_unstruct


def unstructured_mode(address, pelias):
    """The full logic of bePelias when input in unstructured

    Args:
        address (str): address (unstructured) to geocode
        pelias (Pelias): Pelias object

    Returns:
        dict: json result
    """

    pelias_unstruct = call_unstruct(address, pelias)

    if len(pelias_unstruct["features"]) > 0 and is_building(pelias_unstruct["features"][0]):
        return pelias_unstruct

    log("No (address) result with simple unstructured call, try a simple clean")
    address_clean = address

    remove_patterns_unstruct = [(r"\(.+?\)",  "")]

    for pat, rep in remove_patterns_unstruct:
        address_clean = re.sub(pat, rep, address_clean)

    if address_clean != address:
        log(f"cleansed address: '{address_clean}'")
        log(f"initial  address: '{address}'")
        pelias_unstruct = call_unstruct(address_clean, pelias)
        pelias_unstruct["bepelias"]["pelias_call_count"] = 2

        if len(pelias_unstruct["features"]) > 0 and is_building(pelias_unstruct["features"][0]):
            return pelias_unstruct
    else:
        log("Cleansing has no impact, skip...")

    log("No (address) result with simple unstructured call, try advanced structured mode")
    # No result with a simple call, try advanced mode
    parsed = pelias_unstruct["geocoding"]["query"]["parsed_text"]
    if "street" in parsed and "postalcode" in parsed:
        pelias_res = advanced_mode(street_name=parsed["street"],
                                   house_number=parsed["housenumber"] if "housenumber" in parsed else "",
                                   post_code=parsed["postalcode"],
                                   post_name=parsed["city"] if "city" in parsed else "",
                                   pelias=pelias)
        pelias_res["bepelias"]["pelias_call_count"] += 2
        return pelias_res
    else:
        log("Cannot parse address, skip...")

    # Advanced mode not applicable, return (empty) initial result
    return pelias_unstruct
