"""All functions need by bepelias main module

"""
import logging
import re
import copy
import pprint
import pandas as pd

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

    for ln in str(arg).split("\n"):
        logging.info(ln)


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
    for ln in str(arg).split("\n"):
        logging.debug(ln)


# Data conversion

def to_camel_case(data):
    """
    Convert a snake_case object to a camelCase.
    If data is a string, convert the string
    If data is a dict, convert all keys, recursively (i.e., values are dict or list), but not simple values
    If data is a list, convert all objects in the list

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

    # vlog("Converting to to_rest_guidelines")
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

    # vlog(rest_res)
    return rest_res


def feature_to_df(features, to_string=True, margin=4):
    """ Convert a list of Pelias features into a Pandas DataFrame
        Convertible to a string for pretty printing if to_string is True, with a left margin of 'margin' spaces
     """
    rows = []
    for feature in features:
        row = {"source": feature["properties"]["source"],
               "precision": feature["bepelias"]["precision"] if "bepelias" in feature and "precision" in feature["bepelias"] else None
               }
        row["city"] = feature["properties"].get("locality") or feature["properties"].get("name")

        if "addendum" in feature["properties"]:
            if "best" in feature["properties"]["addendum"]:
                best = feature["properties"]["addendum"]["best"]
                if "housenumber" in best:
                    row["housenumber"] = best["housenumber"]
                if "postal_info" in best and "postal_code" in best["postal_info"]:
                    row["postal_code"] = best["postal_info"]["postal_code"]
                if "street" in best and "name" in best["street"]:
                    row["street"] = best["street"]["name"]
                if "municipality" in best and "name" in best["municipality"]:
                    row["city"] = best["municipality"]["name"]

        rows.append(row)

    df = pd.DataFrame(rows)

    if to_string:
        margin_str = " " * margin
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.expand_frame_repr', False):
            return margin_str + str(df).replace("\n", "\n"+margin_str)
    return df


def final_res_to_df(final_res, to_string=True, margin=0):
    """ Convert a bepelias final result into a Pandas DataFrame
        Convertible to a string for pretty printing if to_string is True, with a left margin of 'margin' spaces
     """

    rows = []
    for item in final_res["items"]:
        row = {"precision": item.get("precision"),
               "housenumber": item.get("housenumber"),
               "street": item.get("street").get("name") if item.get("street") else None,
               "postalcode": item.get("postalInfo").get("postalCode") if item.get("postalInfo") else None,
               "city": item.get("municipality").get("name") if item.get("municipality") else None
               }
        rows.append(row)

    df = pd.DataFrame(rows)

    if to_string:
        margin_str = " " * margin
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.expand_frame_repr', False):
            return margin_str + str(df).replace("\n", "\n"+margin_str) + "\n"+pprint.pformat({k: v for (k, v) in final_res.items() if k != "items"})
    return df

# Main logic functions


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


def transform(addr_data, transformer, remove_patterns):
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
