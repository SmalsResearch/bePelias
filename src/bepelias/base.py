"""
Base code for bePelias
"""
import json
import re

from unidecode import unidecode

import pandas as pd
from fastapi import status
from elasticsearch import Elasticsearch, NotFoundError


from bepelias.pelias import PeliasException

from bepelias.utils import (apply_sim_functions, log, vlog, remove_street_types, get_feature_street_names, get_feature_city_names,
                            pelias_check_postcode, to_rest_guidelines, feature_to_df, final_res_to_df)


default_transformer_sequence = [
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

unstruct_transformer_sequence = [  # Transformer sequence used in unstructured_mode
    ["no_city"],
    ["clean", "no_city"],
    ["clean_hn", "no_city"],
    ["clean", "clean_hn", "no_city"],
    [],
    ["clean"],
    ["clean_hn"],
    ["no_hn"],
    ["no_city", "no_hn"],
    ["no_street"],
]

remove_patterns = [(r"\(.+\)$",      ""),
                   ("[, ]*(SN|ZN)$", ""),
                   ("' ", "'"),
                   (" [a-zA-Z][. ]", " "),
                   ("[.]", " "),
                   (",[a-zA-Z .'-]*$", " "),
                   ("[ ]+$", ""),
                   ("^[ ]+", "")
                   ]


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
            # vlog(f"locality ('{locality_name}' vs '{prop['locality']}'): {sim}")
            return sim

    if "addendum" in prop and "best" in prop["addendum"]:
        for c in ["postname", "municipality_name", "part_of_municipality_name"]:
            for lang in ["fr", "nl", "de"]:
                if f"{c}_{lang}" in prop["addendum"]["best"]:

                    cty = unidecode(prop["addendum"]["best"][f"{c}_{lang}"].lower())
                    sim = apply_sim_functions(unidecode(locality_name).lower(), cty, threshold)
                    # vlog(f"{c}_{lang} ('{locality_name}' vs '{cty}'): {sim}")
                    if sim and sim >= threshold:
                        return sim

    return None


def get_streetname_variants(street_name):
    """
    Get variants of a street name by applying the remove_patterns:
    - If street name contains commas, split it and process each part separately
    - For each part (if at least 5 characters), first yield the part without any modification (except removing street types)
    - Then, apply each pattern in remove_patterns sequentially, yielding the result

    Examples:
    - "Rue de la Loi, SN" --> yields "RUE DE LA LOI, SN" then "RUE DE LA LOI"
    - "Rue de la Loi, Bruxelles" --> yields "RUE DE LA LOI, BRUXELLES", "RUE DE LA LOI"

    Parameters
    ----------
    street_name : str
        Input street name.

    Returns
    -------
    list of str
        List of street name variants.
    """

    street_name = unidecode(street_name.upper())

    previous_res = set()

    street_name_parts = [street_name]

    if "," in street_name:
        for snp in street_name.split(","):
            snp = snp.strip()
            if len(snp) > 5:
                street_name_parts.append(snp)

    for street_name_part in street_name_parts:
        street_name_part = remove_street_types(street_name_part)

        # Yield original part (without street types)
        if street_name_part not in previous_res:
            previous_res.add(street_name_part)
            yield street_name_part

        # Clean and Yield cleansed version
        for pat, rep in remove_patterns:
            street_name_part = re.sub(pat, rep, street_name_part) if not pd.isnull(street_name_part) else None

        if street_name_part not in previous_res:
            previous_res.add(street_name_part)
            yield street_name_part


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
    # vlog(f"checking '{street_name}'")

    if pd.isnull(street_name):
        return 1
    sim = -1

    for with_city in [False, True]:
        for in_street_name in get_streetname_variants(street_name):
            for feat_street_name in get_feature_street_names(feature):
                feat_street_name = remove_street_types(unidecode(feat_street_name))

                if with_city:
                    feat_street_names = [feat_street_name]
                else:  # prepend street name with city names
                    feat_street_names = [f"{cty}, {feat_street_name}" for cty in get_feature_city_names(feature)]

                for feat_street_name in feat_street_names:
                    sim = apply_sim_functions(feat_street_name, in_street_name, threshold)
                    vlog(f"'{in_street_name}' vs '{feat_street_name}': {sim}")
                    if sim:
                        return sim

    if sim == -1:  # No street name found --> ok
        return 1

    return None  # No match found


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

    vlog(f"    Check street : {nb_res} --> {len(filtered_feat)}")
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
        vlog("No street property in feature: ")
        vlog(feature['properties'])
        return {}
    if 'postalcode' not in feature['properties']:
        vlog("No postalcode property in feature: ")
        log(feature['properties'])
        return {}

    addr = {"address": f"{feature['properties']['street']}",
            "postalcode": feature['properties']['postalcode'],
            "locality": ""}
    street_res = pelias.geocode(addr)
    # vlog(f"Interpolate: street center: {street_res}")

    # Keep only results maching input postalcode

    street_res["features"] = list(filter(lambda f: f["properties"]["postalcode"] == feature['properties']['postalcode'] if "postalcode" in f["properties"] else False,
                                         street_res["features"]))

    if len(street_res["features"]) == 0:
        return {}

    street_center_coords = street_res["features"][0]["geometry"]["coordinates"]
    vlog(f"    street_center_coords: {street_center_coords}")

    interp_res = pelias.interpolate(lat=street_center_coords[1],
                                    lon=street_center_coords[0],
                                    number=feature['properties']['housenumber'],
                                    street=feature['properties']['street'])

    if len(interp_res) == 0:
        return {"street_geometry": {"coordinates": street_center_coords}}

    vlog(f"interp_res:{interp_res}")
    return {"geometry": {"coordinates": interp_res["geometry"]["coordinates"]}}


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

    vlog("    Coordinates==0,0, check if any box number contains coordinates...")

    try:
        boxes = feat["properties"]["addendum"]["best"]["box_info"]
    except KeyError:
        boxes = []

    if len(boxes) > 0 and boxes[0]["coordinates"]["lat"] != 0:
        vlog("    Found coordinates in first box number")
        feat["geometry"]["coordinates_orig"] = [0, 0]
        feat["geometry"]["coordinates"] = [boxes[0]["coordinates"]["lon"], boxes[0]["coordinates"]["lat"]]
        feat["bepelias"] = {"interpolated": "from_boxnumber"}
    else:
        vlog("    Coordinates==0,0, try to interpolate...")
        interp = interpolate(feat, pelias)
        vlog(f"    Interpolate result: {interp}")
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

    # vlog(f"struct_or_unstruct('{street_name}', '{house_number}', '{post_code}', '{post_name}', {check_postcode})")
    # Try structured
    addr = {"address": build_address(street_name, house_number),
            "locality": post_name}
    if post_code is not None:
        addr["postalcode"] = post_code
    vlog("")
    vlog(f"  Call struct: {addr}")

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
        vlog("    No postcode in input")

    if len(pelias_struct["features"]) > 0:
        vlog("    Structured results:")
        vlog(feature_to_df(pelias_struct["features"]))

        for feat in pelias_struct["features"]:
            # vlog(feat["properties"]["name"] if "name" in feat["properties"] else feat["properties"]["label"] if "label" in feat["properties"] else "--")
            if is_building(feat):
                if feat["geometry"]["coordinates"] == [0, 0]:
                    search_for_coordinates(feat, pelias)

                # vlog("Found a building in res1")
                # vlog(feat)
                # vlog("pelias_struct")
                # vlog(pelias_struct)
                # vlog("-------")

                return pelias_struct

    # Try unstructured
    addr = build_address(street_name, house_number) + ", " + build_city(post_code, post_name)
    addr = re.sub("^,", "", addr.strip()).strip()
    addr = re.sub(",$", "", addr).strip()

    vlog("")
    vlog(f"  Call unstruct: '{addr}'")
    if addr and len(addr.strip()) > 0 and not re.match("^[0-9]+$", addr):
        pelias_unstruct = pelias.geocode(addr, layers=layers)
        cnt = 2
    else:
        vlog("    Unstructured: empty inputs or only numbers, skip call")
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
        vlog("    No postcode in input")

    pelias_unstruct = check_best_streetname(pelias_unstruct, street_name)

    if len(pelias_unstruct["features"]) > 0:
        vlog("    Unstructured results:")
        vlog(feature_to_df(pelias_unstruct["features"]))

        for feat in pelias_unstruct["features"]:
            # vlog(feat["properties"]["name"] if "name" in feat["properties"] else feat["properties"]["label"] if "label" in feat["properties"] else "--")
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

    # vlog("get_precision")
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


def advanced_mode(street_name, house_number, post_code, post_name, pelias, transformer_sequence=None):
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
    if transformer_sequence is None:
        transformer_sequence = default_transformer_sequence
    all_res = []

    call_cnt = 0
    for check_postcode in [True, False]:
        previous_attempts = []
        for transf in transformer_sequence:
            transf_addr_data = addr_data.copy()
            for t in transf:
                transf_addr_data = transform(transf_addr_data, t)

            vlog("")
            vlog(f"Transformed address ({';'.join(transf)}): {transf_addr_data}")
            if transf_addr_data in previous_attempts:
                vlog("    Already tried, skip Pelias call")
            elif len(list(filter(lambda v: v and len(v) > 0, transf_addr_data.values()))) == 0:
                vlog("    No value to send, skip Pelias call")
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

                if len(pelias_res["features"]) > 0:
                    if is_building(pelias_res["features"][0]):
                        pelias_res["bepelias"]["pelias_call_count"] = call_cnt
                        add_precision(pelias_res)
                        return pelias_res

                    # If:
                    # - 'no_city' in transformer
                    # - first result is a BeSt result
                    # - street name matches input street name
                    # - postcode matches input postcode
                    # - input house number contains only digits
                    # --> keep this result
                    # Conclusion after testing: has only an impact on +/- 1-2% of the addresses, slightly reducing the number of calls to Pelias.
                    # It increase the complexity of the code, so it is disabled for the moment.
                    if (transf_addr_data.get("post_name") or "") == "":
                        feat0 = pelias_res["features"][0]
                        if any(sn == street_name.upper() for sn in get_feature_street_names(feat0)):
                            if post_code is not None and "postalcode" in feat0["properties"] and feat0["properties"]["postalcode"] == post_code:
                                if re.match("^[0-9]+$", house_number or ""):

                                    add_precision(pelias_res)

                                    if feat0["bepelias"]["precision"] == "street":
                                        vlog("Found a BeSt result matching street name and postcode, with numeric house number")
                                        pelias_res["bepelias"]["pelias_call_count"] = call_cnt

                                        return pelias_res

                all_res.append(pelias_res)
        if sum(len(r["features"]) for r in all_res) > 0:
            # If some result were found (even street-level), we stop here and select the best one.
            # Otherwise, we start again, accepting any postcode in the result
            vlog("Some result found with check_postcode=True")
            break

    vlog("No building result, keep the best match")
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

            score_line = {f: prop.get(f, '[NA]') for f in fields}
            score_line["coordinates"] = str(res["features"][0]["geometry"]["coordinates"])
            for f in fields + ["coordinates"]:
                if f in score:
                    score_line[f] += f" ({score[f]:.3})"

            score_line["score"] = res["score"]
            scores.append(score_line)

    with pd.option_context("display.max_columns", None, 'display.width', None):
        vlog("\n"+str(pd.DataFrame(scores)))

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

    vlog(f"    Parsed by Pelias: {parsed}")

    if "postalcode" in parsed:
        pelias_unstruct = pelias_check_postcode(pelias_unstruct, parsed["postalcode"])

    else:
        vlog("    No postcode in input")

    if "street" in parsed:
        pelias_unstruct = check_best_streetname(pelias_unstruct, parsed["street"])

    add_precision(pelias_unstruct)

    return pelias_unstruct


def get_postcode_list(city, pelias):
    """ Get a list with all postcode matching with 'city' (as municipality name, postal info or part of municipality)"""
    postcodes = set()

    es_client = Elasticsearch(pelias.elastic_api)
    search_city_resp = search_city(es_client, None, city)

    for search_city_item in search_city_resp.get("items", []):
        search_city_postalcode = search_city_item.get("postalInfo", {}).get("postalCode", None)
        if search_city_postalcode is not None:
            postcodes.add(search_city_postalcode)
    return postcodes


def unstructured_mode(address, pelias):
    """The full logic of bePelias when input in unstructured

    - We first try unstructured mode, with the raw input
    - If no building level addess found, we try unstructured mode with some simple cleansing
      (removing parenthesis content, ...), if this cleansing has any effect
    - If still no building found, we check if Pelias was able to parse the (cleansed) address :
        - If yes and postcode was found, we try the structured logic (advenced_mode) with this postcode
        - Otherwise, if a city name was found, we get all postcodes matching with this city name (get_postcode_list),
          and we try advanced_mode with each of these postcodes
        - Otherwise, we give up

    If at the end no building level address was found, we keep the best candidate amongst all attempts, i.e. with the lower score,
    according to the following rules:
    - If no feature was found, score = 20
    - Otherwise, score depending on precision:
        - address: 0
        - address_interpol: 1
        - address_streetcenter: 2
        - street_interpol: 3
        - street: 4
        - city: 5
        - other: 10
    - In the structured part, if selected postcode matches with postcode in result, -0.5 bonus


    Args:
        address (str): address (unstructured) to geocode
        pelias (Pelias): Pelias object

    Returns:
        dict: json result
    """

    remove_patterns_unstruct = [(r"\(.+?\)",  "")]
    precision_order = {"address": 0,
                       "address_interpol": 1,
                       "address_streetcenter": 2,
                       "street_interpol": 3,
                       "street": 4,
                       "address_00": 5,
                       "street_00": 6,
                       "city": 7}

    all_res = []
    previous_attempts = []
    call_cnt = 0
    attempt_id = 1

    # Unstructured attempts with simple transformations
    for transf in ["", "clean"]:
        if transf == "clean":
            for pat, rep in remove_patterns_unstruct:
                address = re.sub(pat, rep, address)
        if address not in previous_attempts:
            vlog("")
            vlog(f"Attempt {attempt_id}: unstructured address='{address}' (transformer='{transf}')")
            attempt_id += 1
            pelias_res = call_unstruct(address, pelias)
            call_cnt += 1
            pelias_res["bepelias"]["pelias_call_count"] = call_cnt
            pelias_res["bepelias"]["transformers"] = transf
            vlog("    Results:")
            vlog(feature_to_df(pelias_res["features"]))
            if len(pelias_res["features"]) > 0 and is_building(pelias_res["features"][0]):
                if pelias_res["features"][0]["geometry"]["coordinates"] == [0, 0]:
                    search_for_coordinates(pelias_res["features"][0], pelias)
                    add_precision(pelias_res)

                return pelias_res
            pelias_res["bepelias"]["in"] = address  # only used for logging

            if len(pelias_res["features"]) > 0:
                pelias_res["bepelias"]["score"] = precision_order.get(pelias_res["features"][0]["bepelias"]["precision"], 10)
            else:
                pelias_res["bepelias"]["score"] = 20

            all_res.append(pelias_res)

        previous_attempts.append(address)

    vlog("")
    vlog("Unstructured failed, try to parse...")

    # Structured attempts (if parsing was successful)
    # Simple transformation weren't enough, we try to parse and use advanced structured mode
    parsed = pelias_res["geocoding"]["query"]["parsed_text"]

    if "postalcode" in parsed:
        postalcode_candidates = [parsed["postalcode"]]
    elif "city" in parsed:
        postalcode_candidates = sorted(get_postcode_list(parsed["city"], pelias))
    else:
        postalcode_candidates = []

    vlog(f"Postcode candidates: {postalcode_candidates}")
    if len(postalcode_candidates) > 4:
        # Optimisation: With some city names (Antwerp, Charleroi, ...) there are too many postcodes --> we search for street (without housenumer/postcode),
        # and start search for postcodes being in the street search
        pelias_street = pelias.geocode({"address": parsed.get("street", "")})
        street_postcodes = set(feat["properties"].get("postalcode") for feat in pelias_street["features"])
        vlog(f"Postcodes in street search: {street_postcodes}")

        intersection = set(postalcode_candidates) & street_postcodes
        if len(intersection) > 0:
            postalcode_candidates = list(intersection)
            vlog(f"Filtered postcode candidates: {postalcode_candidates}")
        else:
            vlog("No intersection between street postcodes and city postcodes, keep original list")

    for cp in postalcode_candidates:
        vlog("")
        vlog(f"Attempt {attempt_id}: structured address='{parsed.get('street', '')} / {parsed.get('housenumber', '')} / {cp} / {parsed.get('city', '')}'")
        attempt_id += 1

        pelias_res = advanced_mode(street_name=parsed.get("street", ""),
                                   house_number=parsed.get("housenumber", ""),
                                   post_code=cp,
                                   post_name=parsed.get("city", ""),
                                   pelias=pelias,
                                   transformer_sequence=unstruct_transformer_sequence)
        call_cnt += pelias_res["bepelias"]["pelias_call_count"]
        pelias_res["bepelias"]["pelias_call_count"] = call_cnt
        pelias_res["bepelias"]["transformers"] = f"parsed(postcode={cp});"+pelias_res["bepelias"]["transformers"]

        vlog("    Results:")
        vlog(feature_to_df(pelias_res["features"]))

        if len(pelias_res["features"]) > 0 and is_building(pelias_res["features"][0]):
            return pelias_res

        pelias_res["bepelias"]["in"] = parsed | {"postalcode": cp}  # only used for logging
        if len(pelias_res["features"]) > 0:
            pelias_res["bepelias"]["score"] = precision_order.get(pelias_res["features"][0]["bepelias"]["precision"], 10)
            # vlog(f'{pelias_res["features"][0]["properties"]["postalcode"]} =? {cp}')
            if pelias_res["features"][0]["properties"].get("postalcode") == cp:
                pelias_res["bepelias"]["score"] -= 0.5  # small bonus if postcode matches
        else:
            pelias_res["bepelias"]["score"] = 20

        all_res.append(pelias_res)

    # No result with building level --> keep the best candidate
    vlog("")
    vlog("Unstructured mode: no building result, keep the best match")
    vlog(pd.DataFrame([{"in": pr["bepelias"]["in"],
                        "precision": pr["features"][0]["bepelias"]["precision"] if len(pr["features"]) > 0 else "-",
                        "score": pr["bepelias"]["score"]} for pr in all_res]))

    best_pelias_res = min(all_res, key=lambda pr: pr["bepelias"]["score"])
    best_pelias_res["bepelias"]["pelias_call_count"] = call_cnt
    del best_pelias_res["bepelias"]["score"]
    del best_pelias_res["bepelias"]["in"]

    return best_pelias_res

#################
# API Endpoints #
#################


def geocode(pelias, street_name, house_number, post_code, post_name, mode, with_pelias_result):
    """ cf api._geocode """

    if street_name:
        street_name = street_name.strip()
    if house_number:
        house_number = house_number.strip()
    if post_code:
        post_code = post_code.strip()
    if post_name:
        post_name = post_name.strip()

    try:
        if mode in ("basic"):
            pelias_res = pelias.geocode({"address": build_address(street_name, house_number),
                                         "postalcode": post_code,
                                         "locality": post_name})
            add_precision(pelias_res)

            return to_rest_guidelines(pelias_res, with_pelias_result)

        elif mode == "simple":
            pelias_res = struct_or_unstruct(street_name, house_number, post_code, post_name, pelias)
            add_precision(pelias_res)

            return to_rest_guidelines(pelias_res, with_pelias_result)

        else:  # --> mode == "advanced":
            # log("advanced...")

            pelias_res = advanced_mode(street_name, house_number, post_code, post_name, pelias)

            # vlog("result (before rest_guidelines):")
            # vlog(pelias_res)
            # vlog("------")

            res = to_rest_guidelines(pelias_res, with_pelias_result)

            vlog("\nFinal result:")
            vlog(final_res_to_df(res))

            return res

    except PeliasException as exc:
        log("Exception during process: ")
        log(exc)
        return {"error": str(exc),
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}


def geocode_unstructured(pelias, address, mode, with_pelias_result):
    """ see _geocode_unstructured
    """

    try:
        if mode in ("basic"):
            pelias_res = pelias.geocode(address)
            add_precision(pelias_res)

        else:  # --> mode == "advanced":
            pelias_res = unstructured_mode(address, pelias)

        # vlog(pelias_res)
        res = to_rest_guidelines(pelias_res, with_pelias_result)

        vlog("\nFinal result:")
        vlog(final_res_to_df(res))

        return res

    except PeliasException as exc:
        log("Exception during process: ")
        log(exc)
        return {"error": str(exc),
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}


def geocode_reverse(pelias, lat, lon, radius, size, with_pelias_result):
    """
    see _geocode_reverse
    """

    # vlog("reverse")

    # log(f"Reverse geocode: ({lat}, {lon}) / radius: {radius} / size:{size} ")

    try:
        # Note: max size for Pelias = 40. But as most records are duplicated in Pelias (one record in each languages for bilingual regions,
        # we first take twice too many results)
        pelias_res = pelias.reverse(lat=lat,
                                    lon=lon,
                                    radius=radius,
                                    size=size*2)

        res = to_rest_guidelines(pelias_res, with_pelias_result)
        res["items"] = res["items"][0:size]
        res["total"] = len(res["items"])
        return res
    except PeliasException as exc:
        log("Exception during process: ")
        log(exc)
        return {"error": str(exc),
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}


def search_city(es_client, post_code, city_name):
    """
    see _search_city
    """
    # vlog("search city")

    # log(f"searchCity: {post_code} / {city_name}")

    if post_code is None and city_name is None:
        return {"error": "Either 'postCode' or 'cityName' should be provided",
                "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY}

    must = [{"term": {"layer": "locality"}}]
    if post_code:
        must.append({"term": {"address_parts.zip": post_code}})
    if city_name:
        must.append({"bool": {
                        "should": [
                            {"query_string": {"query": f"name.default:\"{city_name}\""}},
                            {"query_string": {"query": f"name.fr:\"{city_name}\""}},
                            {"query_string": {"query": f"name.nl:\"{city_name}\""}},
                            {"query_string": {"query": f"name.de:\"{city_name}\""}}
                        ]}}
                    )

    try:

        resp = es_client.search(index="pelias",
                                size=100,
                                body={
                                    "query": {
                                        "bool": {
                                            "must": must
                                            }
                                    }
                                })
        # vlog("resp:")
        # vlog(resp)

        resp = resp["hits"]["hits"]

        final_result = []
        for resp_item in resp:
            if "addendum" in resp_item["_source"] and "best" in resp_item["_source"]["addendum"]:
                it = {"properties": {"addendum": {"best": json.loads(resp_item["_source"]["addendum"]["best"])}}}
                if "center_point" in resp_item["_source"]:
                    it["geometry"] = {"coordinates": resp_item["_source"]["center_point"]}
                it["name"] = resp_item["_source"]["name"]

                final_result.append(it)

        # Remove duplicate results
        final_result = {"features": [i for n, i in enumerate(final_result) if i not in final_result[:n]]}

        res = to_rest_guidelines(final_result, False)

        return res
    except NotFoundError:
        return {"features": []}
    except ConnectionError as exc:
        log("ES ConnectionError")
        log(exc)
        return {"error": f"Cannot connect to Elastic: {exc}",
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}


def get_by_id(pelias, bestid):

    """ see _get_by_id
    """
    # raw = get_arg("raw", False)

    # log(f"Get by id: {bestid}")

    client = Elasticsearch(pelias.elastic_api)

    mtch, bestid = bestid  # check_valid_bestid result

    if mtch is None or len(mtch.groups()) != 5:
        return {"error": f"Cannot parse best id '{bestid}'",
                "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY}

    # vlog(f"mtch[2].lower(): '{mtch[3].lower()}'")
    obj_type = None
    if mtch[3].lower() in ["address", "adres"]:
        obj_type = "address"
    elif mtch[3].lower() in ["streetname", "straatnaam"]:
        obj_type = "street"
    elif mtch[3].lower() in ["municipality", "gemeente", "partofmunicipality"]:
        obj_type = "locality"
    else:
        return {"error": f"Object type '{mtch[3]}' not supported so far in '{bestid}'",
                "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY}

    try:
        resp = client.search(index="pelias", body={
            "query": {
                "bool": {
                    "must": [
                        {"term": {"layer": obj_type}},
                        {"prefix": {"source_id": {"value": bestid.lower()}}}
                    ]
                }
            }
        })

        resp = resp["hits"]["hits"]

        final_result = []
        for resp_item in resp:
            if "addendum" in resp_item["_source"] and "best" in resp_item["_source"]["addendum"]:
                it = {"properties": {"addendum": {"best": json.loads(resp_item["_source"]["addendum"]["best"])}}}
                if "center_point" in resp_item["_source"]:
                    it["geometry"] = {"coordinates": resp_item["_source"]["center_point"]}
                it["name"] = resp_item["_source"]["name"]

                final_result.append(it)

        final_result = {"features": final_result}

        return to_rest_guidelines(final_result, with_pelias_raw=False)

    except NotFoundError:
        return to_rest_guidelines({"features": []}, with_pelias_raw=False)

    except ConnectionRefusedError as exc:
        log("ES ConnectionRefusedError")
        log(exc)
        return {"error": f"Cannot connect to Elastic: {exc}",
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}

    except ConnectionError as exc:
        log("ES ConnectionError")
        log(exc)
        return {"error": f"Cannot connect to Elastic: {exc}",
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}


def health(pelias):
    """Health status
    """
    # Checking Pelias

    pelias_res = pelias.check()

    if pelias_res is False:
        log("Pelias not up & running")
        # log(f"Pelias host: {pelias_host}")

        return {"status": "DOWN",
                "details": {"errorMessage": "Pelias server does not answer",
                            "details": "Pelias server does not answer"},
                "status_code": status.HTTP_503_SERVICE_UNAVAILABLE}
    if pelias_res is not True:
        return {"status": "DOWN",
                "details": {"errorMessage": "Pelias server answers, but gives an unexpected answer",
                            "details": f"Pelias answer: {pelias_res}"},
                "status_code": status.HTTP_503_SERVICE_UNAVAILABLE}

    # Checking Interpolation

    try:
        interp_res = pelias.interpolate(lat=50.83582,
                                        lon=4.33844,
                                        number=20,
                                        street="Avenue Fonsny")
        # vlog(interp_res)
        if len(interp_res) > 0 and "geometry" not in interp_res:
            return {
                "status": "DEGRADED",
                "details": {
                    "errorMessage": "Interpolation server answers, but gives an unexpected answer",
                    "details": f"Interpolation answer: {interp_res}"
                }}

    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"status": "DEGRADED",
                "details": {"errorMessage": "Interpolation server does not answer",
                            "details": f"Interpolation server does not answer: {exc}"}}

    return {"status": "UP"}
