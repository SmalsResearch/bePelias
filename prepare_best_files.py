#!/usr/bin/env python
# coding: utf-8

"""
Convert BestAddress files (from https://opendata.bosa.be/) into a file readable
by Pelias (csv module)

@author: Vandy Berten (vandy.berten@smals.be)

"""
import os
import sys
import urllib.request
import logging

import getopt
import glob

# from dask.threaded import get

import pandas as pd
import numpy as np

import geopandas as gpd
import shapely

logging.basicConfig(format='[%(asctime)s]  %(message)s', stream=sys.stdout)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# General functions


name_mapping = {
    "bru": "Brussels",
    "vlg": "Flanders",
    "wal": "Wallonia"
}


SPLIT_RECORDS = True


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


def download(url, filename):
    """

    Parameters
    ----------
    url: str
       url to fetch
    filename: str
       local file to save

    Returns
    -------

    None
    """
    log(f"Downloading {url} in to {filename}")
    with urllib.request.urlopen(url) as response:
        with open(filename, "wb") as file:
            file.write(response.read())


def get_language_prefered_order(region):
    """
    Get a list of language by preference following the region

    Args:
        region (str): "bru", "wal", or "vlg"

    Returns:
        tuple: a tuple with three strings ("fr", "nl", "de") ordered according to the region
    """

    return ("fr", "nl", "de") if region == "bru" \
        else ("nl", "fr", "de") if region == "vlg" \
        else ("fr", "de", "nl")


def build_addendum(data_dict, no_quotes):
    """
    Build the addendum_json_best column

    Parameters
    ----------
    data_dict : dict
        List of fields.
    no_quotes : list of st

    Returns
    -------
    res : pd.Series
        column "addendum_json_best".
    """
    res = ""

    for key, data in data_dict.items():
        if isinstance(data, pd.Series):
            if key in no_quotes:
                col = data.astype(str).fillna("")+', '
            else:
                col = '"' + data.astype(str).fillna("").str.replace('"', "'")+'", '

            res += np.where(data.isnull(),
                            "",
                            f'"{key}": ' + col)
        else:
            recursive_addendum = build_addendum(data, no_quotes)
            res += np.where(recursive_addendum.str.len() <= 2,
                            "",
                            f'"{key}": ' + recursive_addendum+', ')

    return '{'+pd.Series(res).str[0:-2]+'}'  # remove the last ", "


def build_locality(data, lang):
    """
    Create a column containing a "locality name" in the language "lang".
    If a municipality_name is available for the given language, start with it
    If a postname is available and is different from municipality, append it between parenthesis
    If a part_of_municipality is available and is different from municipality, append it between parenthesis

    Note : as postname is only used in BRU and VLG and part_of_municipality is only avaolable in WAL, we will never have two parenthesized values
    Parameters
    ----------
    data : pd.DataFrame

    lang : str
        "fr", "nl" or "de.

    Returns
    -------
    locality_lang : pd.Series
        Strings with municipality (postname/part_of_municipality if available and <> municipality) in 'lang'

    """
    locality_lang = data[f"municipality_name_{lang}"].copy()

    # Add postal name, if exists and <> municipality name
    locality_lang += np.where(
            (data[f"municipality_name_{lang}"] == data[f"postname_{lang}"]) |
            data[f"postname_{lang}"].isnull(),
            "",
            " (" + data[f"postname_{lang}"].fillna("")+")")

    locality_lang += np.where(  # Add part of municipality name, if exists and <> municipality name
            (data[f"municipality_name_{lang}"] == data[f"part_of_municipality_name_{lang}"]) |
            data[f"part_of_municipality_name_{lang}"].isnull(),
            "",
            " (" + data[f"part_of_municipality_name_{lang}"].fillna("")+")")
    return locality_lang


def get_base_data_xml(region):
    """
    Download BestAddress file for 'region', and convert it to the appropriate
    pandas DataFrame. This dataframe will be used by most other functions

    Parameters
    ----------
    region : str
        "bru", "wal" or "vlg.

    Returns
    -------
    data : pd.DataFrame
        All addresses for the given region.

    """
    log(f"[base-{region}] Building data for {region}")

    best_fn = f"{DATA_DIR_IN}/{name_mapping[region]}_addresses.csv"

    dtypes = {"box": str,
              "city_fr": str,
              "city_nl": str,
              "city_de": str,
              "postal_fr":   str,
              "postal_nl":   str,
              "street_de": str,
              "street_nl": str,
              "street_fr": str
              }

    log(f"[base-{region}] - Reading")
    data = pd.read_csv(best_fn, dtype=dtypes)

    data = data.rename(columns={
        "id": "address_id",
        "street_nl": "streetname_nl",
        "street_fr":  "streetname_fr",
        "street_de":  "streetname_de",

        "number": "house_number",
        "box":     "box_number",

        "city_id": "municipality_id",
        "city_nl": "municipality_name_nl",
        "city_fr": "municipality_name_fr",
        "city_de": "municipality_name_de",

        "citypart_id": "part_of_municipality_id",
        "citypart_nl": "part_of_municipality_name_nl",
        "citypart_fr": "part_of_municipality_name_fr",
        "citypart_de": "part_of_municipality_name_de",

        "postal_id": "postcode",
        "postal_nl": "postname_nl",
        "postal_fr": "postname_fr",
        "postal_de": "postname_de",

        "gpsy": "lat",
        "gpsx": "lon"
    })

    data["lon"] = data["lon"].where(data["lambertx"] != 0, pd.NA)
    data["lat"] = data["lat"].where(data["lamberty"] != 0, pd.NA)

    log(f"[base-{region}] - Combining boxes ...")

    # log(data.iloc[0])

    # Combine all addresses at the same number in one record with "box_info" field
    with_box = data[data.box_number.notnull()]

    box_info = with_box.fillna({"lat": 0, "lon": 0}).groupby(["house_number",
                                                              "municipality_id", "municipality_name_de", "municipality_name_fr", "municipality_name_nl",
                                                              "postcode", "postname_fr", "postname_nl", "postname_de",
                                                              "street_id", "streetname_de", "streetname_fr", "streetname_nl"],
                                                             dropna=False)
    box_info = box_info[["lat", "lon", "box_number", "address_id", "status"]].apply(lambda x: x.to_json(orient='records')).rename("box_info").reset_index()

    log("box_info:")
    log(box_info)
    base_address = data.sort_values("box_number", na_position="first")
    base_address = base_address.drop_duplicates(subset=["municipality_id", "street_id",
                                                        "postcode", "house_number"])
    base_address = base_address.drop("box_number", axis=1)

    cnt_before_mg = data.shape[0]
    del data

    data = base_address.merge(box_info, how="outer")

    del base_address, box_info

    log(f"[base-{region}] -   --> from {cnt_before_mg} to {data.shape[0]} records")

    if "postname_de" not in data:
        data["postname_de"] = pd.NA

    if SPLIT_RECORDS:
        log(f"[base-{region}] -   Splitting records")
        log(f"[base-{region}]        in:  {data.shape[0]} ")
        data_all = []
        for lang in ["fr", "nl", "de"]:
            for locality_field in ["municipality_name", "postname", "part_of_municipality_name"]:
                data_item = data[data[f"{locality_field}_{lang}"].notnull() & data[f"streetname_{lang}"].notnull()].copy()
                if locality_field != "municipality_name":
                    data_item = data_item[data_item[f"{locality_field}_{lang}"].astype(str).str.upper() != data_item[f"municipality_name_{lang}"].astype(str).str.upper()]

                if data_item.shape[0] > 0:
                    data_item["locality"] = data_item[f"{locality_field}_{lang}"]

                    data_item["streetname"] = data_item[f"streetname_{lang}"]
                    data_item["name"] = data_item["house_number"].fillna("")+", " + data_item["streetname"].fillna("") + ", "
                    data_item["name"] += data_item["postcode"].fillna("").astype(str) + " " + data_item["locality"].fillna("")

                    data_item["name"] = data_item["name"].where(data_item["streetname"].notnull(), pd.NA)
                    data_all.append(data_item)
        del data
        data = pd.concat(data_all).reset_index()
        del data_all

        #  add a stable suffix to best id to avoid duplicates
        epoch = data.groupby("address_id").cumcount()+1
        data["id"] = data.address_id + "_" + epoch.astype(str)

        log(f"[base-{region}]        out: {data.shape[0]} ")

    else:
        log(f"[base-{region}] -   Adding language data")
        for lang in ["fr", "nl", "de"]:

            data[f"locality_{lang}"] = build_locality(data, lang)

            data[f"name_{lang}"] = data["house_number"].fillna("") + ", " + data[f"streetname_{lang}"].fillna("") + ", "
            data[f"name_{lang}"] += data["postcode"].fillna("").astype(str)+" " + data[f"locality_{lang}"].fillna("")

            data[f"name_{lang}"] = data[f"name_{lang}"].where(data[f"streetname_{lang}"].notnull(),
                                                              pd.NA)

        (lg1, lg2, lg3) = get_language_prefered_order(region)

        for f in ["name", "streetname", "locality"]:
            data_cols = data[[f"{f}_{lg1}", f"{f}_{lg2}", f"{f}_{lg3}"]]
            data[f] = data_cols.apply(lambda lst: [x for x in lst if not pd.isnull(x)], axis=1).apply(lambda lst: " / ".join(lst) if len(lst) > 0 else pd.NA)

        data["id"] = data.address_id

    data["country"] = "Belgium"
    data["region_code"] = f"BE-{region.upper()}"


#     if split_records:
#         log(f"[base-{region}] - remove language columns")
#         log(data.columns)

#         data = data.drop(columns=[ col for col in data if col[-3:] in ["_fr", "_nl", "_de"]])

#         log(data.columns)

    log(f"[base-{region}] -   Rename")
    data = data.rename(columns={"region_code":   "source",
                                "house_number":  "housenumber",
                                "postcode":      "postalcode"
                                })

    log("no coordinates: ")
    log(data[data.lat.isnull()])

    log(f"[base-{region}] Done!")
    return data


def get_empty_data_xml(region):
    """
    Download BestAddress empty streets file for 'region', and convert it to the appropriate
    pandas DataFrame. This dataframe will be used by create_street_data

    Parameters
    ----------
    region : str
        "bru", "wal" or "vlg.

    Returns
    -------
    empty_street_all : pd.DataFrame
        All empty streets for the given region.

    """
    log(f"[empty_street-{region}] - Downloading")

    best_fn = f"{DATA_DIR_IN}/{name_mapping[region]}_empty_street.csv"

    empty_streets = pd.read_csv(best_fn)

    log(f"[empty_street-{region}] - Building data")

    # Uniformizing column names to match with main CSV files
    for lang in ["fr", "nl", "de"]:
        empty_streets = empty_streets.rename(columns={f"street_{lang}":   f"streetname_{lang}",
                                                      f"city_{lang}":     f"municipality_name_{lang}",
                                                      f"postal_{lang}":   f"postname_{lang}",
                                                      f"citypart_{lang}": f"part_of_municipality_name_{lang}"
                                                      })

    empty_streets["street_id"] = empty_streets["street_prefix"]+"/"+empty_streets["street_no"].astype(str)+"/"+empty_streets["street_version"].astype(str)
    empty_streets["municipality_id"] = empty_streets["city_prefix"]+"/"+empty_streets["city_no"].astype(str)+"/"+empty_streets["city_version"].astype(str)
    empty_streets = empty_streets.rename(columns={"postal_id": "postalcode"})

    if SPLIT_RECORDS:
        data_all = []
        for lang in ["fr", "nl", "de"]:
            for locality_field in ["municipality_name", "postname", "part_of_municipality_name"]:
                data_item = empty_streets[empty_streets[f"{locality_field}_{lang}"].notnull()].copy()
                if locality_field != "municipality_name":
                    data_item = data_item[data_item[f"{locality_field}_{lang}"] != data_item[f"municipality_name_{lang}"]]

                if data_item.shape[0] > 0:
                    data_item["locality"] = data_item[f"{locality_field}_{lang}"]
                    data_item["streetname"] = data_item[f"streetname_{lang}"]

                    data_all.append(data_item)
        empty_streets = pd.concat(data_all).reset_index()
        # empty_streets["id"] = data.address_id +"_"+data.index.astype(str)

    else:
        for lang in ["fr", "nl", "de"]:

            empty_streets[f"locality_{lang}"] = build_locality(empty_streets, lang)

    empty_streets["source"] = f"BE-{region.upper()}-emptystreets"
    empty_streets["country"] = "Belgium"
    empty_streets["lat"] = 0
    empty_streets["lon"] = 0

    empty_streets = empty_streets[[f for f in ["locality_fr",   "locality_nl", "locality_de", "locality",
                                               "streetname_fr", "streetname_nl", "streetname_de", "streetname",
                                               "municipality_name_fr", "municipality_name_nl", "municipality_name_de",
                                               "part_of_municipality_name_fr", "part_of_municipality_name_nl", "part_of_municipality_name_de",
                                               "postalcode", "source", "country", "lat", "lon", "street_id",
                                               "municipality_id"] if f in empty_streets]]
    log(f"[empty_street-{region}] - data: ")
    log(empty_streets)

    return empty_streets


def create_address_data(data, region):
    """
    Get the result of "get_base_data", and create CSV with all addresses
    for the given region

    Parameters
    ----------
    data : pd.DataFrame
        output of get_base_data.
    region : str
        "bru", "wal" or "vlg".

    Returns
    -------
    addresses_all: pd.DataFrame
        Content of all addresses CSV



    """
    log(f"[addr-{region}] - Building address data")

    # addresses_all = data.copy()

    addresses_all = data[[f for f in ["id", "lat", "lon", "housenumber",
                                      "postalcode", "source", "layer",
                                      "locality", "streetname",  # "streetname_fr", "streetname_nl","streetname_de",
                                      "name", "name_fr", "name_nl", "name_de",
                                      "country"] if f in data]].fillna({"lat": 0, "lon": 0}).assign(layer="address").rename(columns={"streetname": "street"})
    # log(data[data.lat.isnull()])

    log(f"[addr-{region}] -   Adding addendum")

    addresses_all["addendum_json_best"] = build_addendum({
        "best_id": data.address_id,
        "street": {
            "name": {"fr": data.streetname_fr, "nl": data.streetname_nl, "de": data.streetname_de},
            "id": data.street_id
        },
        "municipality": {
            "name": {"fr": data.municipality_name_fr, "nl": data.municipality_name_nl, "de": data.municipality_name_de},
            "code": data.municipality_id.str.extract(r"/([0-9]{5})/")[0],
            "id":  data.municipality_id
        },
        "part_of_municipality": {
            "name": {"fr": data.part_of_municipality_name_fr, "nl": data.part_of_municipality_name_nl, "de": data.part_of_municipality_name_de},
            "id": data.part_of_municipality_id
        },
        "postal_info": {
            "name": {"fr": data.postname_fr, "nl": data.postname_nl, "de": data.postname_de},
            "postal_code": data.postalcode
        },
        "housenumber": data.housenumber,
        "status": data.status,
        "box_info": data.box_info
        }, ['box_info'])

    fname = f"{DATA_DIR_OUT}/bestaddresses_be{region}.csv"
    log(f"[addr-{region}] -->{fname}")
    # addresses_all = addresses_all.rename(columns={"streetname": "street"})

    addresses_all.to_csv(fname, index=False)

    log(f"[addr-{region}] Done!")

    return addresses_all


def middle_points(pt1, pt2):
    """
    Compute a (shapely) point in the middle of two (shapely) points pt1, pt2.
    If one of the is empty, take the other one.

    Parameters
    ----------
    pt1 : shapely.geometry.Point
        A point (or None).
    pt2 : shapely.geometry.Point
        A point (or None).

    Returns
    -------
    shapely.geometry.Point
        A point in the middle of pt1 and pt2.

    """
    if pt1 is None:
        return pt2
    if pt2 is None:
        return pt1

    return shapely.geometry.Point((pt1.x+pt2.x)/2, (pt1.y+pt2.y)/2)


def create_street_data(data, empty_street, region):
    """
    Using the output of get_base_data and get_empty_data, build a CSV file with
    all street data for the given region.

    Parameters
    ----------
    data : pd.DataFrame
        Ouput of get_base_data.
    empty_street : pd.DataFrame
        output of get_empty_data.
    region : str
        "bru", "wal" or "vlg".

    Returns
    -------
    None.

    """

    def get_street_center(data, parity):
        # log("get_street_center")
        # log(data)
        data_parity = data[data.housenumber_num.mod(2) == parity]
        data_parity = data_parity.sort_values(["municipality_id", "street_id",
                                              "housenumber_num", "housenumber"])

        streets_geo = data_parity.groupby(["municipality_id", "street_id"]).geometry.apply(lambda bloc: shapely.geometry.LineString(bloc)
                                                                                           if bloc.shape[0] > 1
                                                                                           else bloc.iloc[0])

        streets_geo_multi = streets_geo[streets_geo.geom_type == "LineString"].geometry.apply(shapely.line_interpolate_point,
                                                                                              distance=0.5, normalized=True)
        streets_geo_point = streets_geo[streets_geo.geom_type == "Point"].geometry

        return pd.concat([streets_geo_multi, streets_geo_point])

    def get_streets_centers_duo(data):
        geo_data = data[data.lat.notnull()]

        geo_data = geo_data.assign(housenumber_num=geo_data.housenumber.str.extract("^([0-9]*)").astype(int, errors="ignore"))

        # If some number where not converted to int (did not start by digits) --> ignore them
        if geo_data.housenumber_num.dtype != int:
            geo_data = geo_data[geo_data.housenumber_num.str.isdigit()]
            geo_data["housenumber_num"] = geo_data["housenumber_num"].astype(int)

        geo_data["geometry"] = gpd.points_from_xy(geo_data["lon"], geo_data["lat"])
        geo_data = gpd.GeoDataFrame(geo_data)

        street_centers = [get_street_center(geo_data, 0),
                          get_street_center(geo_data, 1)]

        streets_centers_duo = pd.merge(street_centers[0].rename("even"),
                                       street_centers[1].rename("odd"),
                                       left_index=True, right_index=True, how="outer")

        streets_centers_duo["center"] = streets_centers_duo.apply(lambda row: middle_points(row.even,
                                                                                            row.odd),
                                                                  axis=1)
        streets_centers_duo["lat"] = streets_centers_duo.center.geometry.y
        streets_centers_duo["lon"] = streets_centers_duo.center.geometry.x

        return streets_centers_duo

    # compute center of linestrings for both odd and even sides,
    # then take the middle of those points

    streets_centers_duo = get_streets_centers_duo(data)

    log(f"[street-{region}] - Building streets data")
    fields = [f for f in ["municipality_id", "municipality_name_fr", "municipality_name_nl", "municipality_name_de",
                          "part_of_municipality_id", "part_of_municipality_name_fr", "part_of_municipality_name_nl", "part_of_municipality_name_de",
                          "postname_fr",   "postname_nl", "postname_de",
                          "streetname", "streetname_fr", "streetname_nl", "streetname_de", "street_id",
                          "locality", "locality_fr", "locality_nl", "locality_de",
                          "postalcode", "source", "country"] if f in data]
    all_streets = data[fields].drop_duplicates().merge(streets_centers_duo[["lat", "lon"]],
                                                       left_on=["municipality_id", "street_id"],
                                                       right_index=True,
                                                       how="left").fillna({"lat": 0, "lon": 0})

    del streets_centers_duo

    log(f"[street-{region}] - Combining data and empty streets")

    all_streets = pd.concat([all_streets, empty_street])

    all_streets["id"] = all_streets.street_id

    if SPLIT_RECORDS:
        all_streets["name"] = all_streets["streetname"] + ", " + all_streets["postalcode"].astype(str) + " " + all_streets["locality"]

        # add a stable suffix to best id to avoid duplicates
        epoch = all_streets.groupby("street_id").cumcount()+1
        all_streets["id"] = all_streets.street_id + "_" + epoch.astype(str)

    else:
        for lang in ["fr", "nl", "de"]:
            all_streets[f"name_{lang}"] = all_streets[f"streetname_{lang}"] + ", " + all_streets["postalcode"].astype(str) + " " + all_streets[f"locality_{lang}"]

        (lg1, lg2, lg3) = get_language_prefered_order(region)

        for f in ["name"]:  # , "street", "locality":
            data_cols = all_streets[[f"{f}_{lg1}", f"{f}_{lg2}", f"{f}_{lg3}"]]
            all_streets[f] = data_cols.apply(lambda lst: [x for x in lst if not pd.isnull(x)], axis=1).apply(lambda lst: " / ".join(lst) if len(lst) > 0 else pd.NA)

    all_streets["addendum_json_best"] = build_addendum({
        # "best_id": all_streets.address_id,
        "street": {
            "name": {"fr": all_streets.streetname_fr, "nl": all_streets.streetname_nl, "de": all_streets.streetname_de},
            "id": all_streets.street_id
        },
        "municipality": {
            "name": {"fr": all_streets.municipality_name_fr, "nl": all_streets.municipality_name_nl, "de": all_streets.municipality_name_de},
            "code": all_streets.municipality_id.str.extract(r"/([0-9]{5})/")[0],
            "id":  all_streets.municipality_id
        },
        "part_of_municipality": {
            "name": {"fr": all_streets.part_of_municipality_name_fr, "nl": all_streets.part_of_municipality_name_nl, "de": all_streets.part_of_municipality_name_de},
            "id": all_streets.part_of_municipality_id
        },
        "postal_info": {
            "name": {"fr": all_streets.postname_fr, "nl": all_streets.postname_nl, "de": all_streets.postname_de},
            "postal_code": all_streets.postalcode
        }
        }, [])

    all_streets = all_streets.rename(columns={"streetname": "street"})
    all_streets = all_streets[[f for f in ["id",  "locality", "street", "postalcode", "source",
                                           "country", "lat", "lon",
                                           "name_fr", "name_nl", "name_de", "name", "addendum_json_best"] if f in all_streets]]

    all_streets = all_streets.fillna({"lat": 0, "lon": 0})

    log(all_streets)

    all_streets["layer"] = "street"

    fname = f"{DATA_DIR_OUT}/bestaddresses_streets_be{region}.csv"
    log(f"[street-{region}] -->{fname}")
    all_streets.to_csv(fname, index=False)

    log(f"[street-{region}] Done!")


def create_locality_data(data, region):
    """
    Given the output of get_base_data, create a CSV file with data for all municipalities

    Parameters
    ----------
    data : pd.DataFrame
        output of get_base_data.
    region : str
        "bru", "wal" or "vlg".

    Returns
    -------
    None.

    """
    log(f"[loc-{region}] - Building localities data")

    data_localities_all = data.groupby([f for f in ["municipality_id", "municipality_name_fr", "municipality_name_nl", "municipality_name_de",
                                                    "part_of_municipality_id", "part_of_municipality_name_fr", "part_of_municipality_name_nl", "part_of_municipality_name_de",
                                                    "postname_fr", "postname_nl", "postname_de",
                                                    "locality", "locality_fr", "locality_nl", "locality_de",
                                                    "postalcode", "source", "country"] if f in data],
                                       dropna=False)[["lat", "lon"]].mean().reset_index()

    # data_localities_all = []

    data_localities_all["layer"] = "locality"

    data_localities_all["addendum_json_best"] = build_addendum({
        "municipality": {
            "name": {"fr": data_localities_all.municipality_name_fr, "nl": data_localities_all.municipality_name_nl, "de": data_localities_all.municipality_name_de},
            "code": data_localities_all.municipality_id.str.extract(r"/([0-9]{5})/")[0],
            "id":  data_localities_all.municipality_id
        },
        "part_of_municipality": {
            "name": {"fr": data_localities_all.part_of_municipality_name_fr, "nl": data_localities_all.part_of_municipality_name_nl, "de": data_localities_all.part_of_municipality_name_de},
            "id":   data_localities_all.part_of_municipality_id
        },
        "postal_info": {
            "name": {"fr": data_localities_all.postname_fr, "nl": data_localities_all.postname_nl, "de": data_localities_all.postname_de},
            "postal_code": data_localities_all.postalcode
        }
        }, [])

    # add a stable suffix to best id to avoid duplicates
    epoch = data_localities_all.groupby("municipality_id").cumcount()+1
    data_localities_all["id"] = data_localities_all.municipality_id + "_" + epoch.astype(str)

    #  data_localities_all["id"] = data_localities_all.municipality_id+"_"+data_localities_all.index.astype(str)

    if SPLIT_RECORDS:
        data_localities_all["name"] = data_localities_all["postalcode"].astype(str) + " " + data_localities_all["locality"]
    else:
        (lg1, lg2, lg3) = get_language_prefered_order(region)

        for lang in ["fr", "nl", "de"]:

            data_localities_all[f"name_{lang}"] = data_localities_all["postalcode"].astype(str) + " " + data_localities_all[f"locality_{lang}"]

        for f in ["name"]:
            data_cols = data_localities_all[[f"{f}_{lg1}", f"{f}_{lg2}", f"{f}_{lg3}"]]
            data_localities_all[f] = data_cols.apply(lambda lst: [x for x in lst if not pd.isnull(x)], axis=1).apply(lambda lst: " / ".join(lst) if len(lst) > 0 else pd.NA)

    data_localities_all = data_localities_all[[f for f in ["locality", "postalcode", "source",
                                                           "country", "lat", "lon", "id",
                                                           "layer", "name", "name_fr", "name_nl", "name_de", "addendum_json_best"] if f in data_localities_all]]

    data_localities_all = data_localities_all.fillna({"lat": 0, "lon": 0})

    log(data_localities_all)
    fname = f"{DATA_DIR_OUT}/bestaddresses_localities_be{region}.csv"

    log(f"[loc-{region}] -->{fname}")

    data_localities_all.to_csv(fname, index=False)
    log(f"[loc-{region}] Done!")


def create_interpolation_data(addresses, region):
    """
    Given create_address_data output, prepare a file for the interpolation engine

    Parameters
    ----------
    addresses : TYPE
        DESCRIPTION.
    region : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """

    log(f"[interpol-{region}] Prepare interpolation data")

    log(f"[interpol-{region}] init: {addresses.shape[0]}")

    addresses = addresses[addresses.lat > 0.0]

    log(f"[interpol-{region}] remove 0,0: {addresses.shape[0]}")

    # addresses = addresses[addresses.addendum_json_best.str.contains('"status": "current"')]

    log(f"[interpol-{region}] only current: {addresses.shape[0]}")

    addresses.columns = addresses.columns.str.upper()

    addresses = addresses.rename(columns={
        "HOUSENUMBER": "NUMBER",
    })

    addresses["NUMBER"] = addresses["NUMBER"].str.extract("^([0-9]*)").astype(int, errors="ignore")

    addresses = addresses[addresses["NUMBER"] != ""]

    log(f"[interpol-{region}] remove non digits: {addresses.shape[0]}")

    # log(addresses)
    # log(addresses.columns)
    # test no language split
    if not SPLIT_RECORDS:
        addresses = pd.concat([
            addresses[addresses.STREETNAME_FR.notnull()][["ID", "STREETNAME_FR", "NUMBER",
                                                          "POSTALCODE", "LAT", "LON"]].rename(columns={"STREETNAME_FR": "STREET"}),
            addresses[addresses.STREETNAME_NL.notnull()][["ID", "STREETNAME_NL", "NUMBER",
                                                          "POSTALCODE", "LAT", "LON"]].rename(columns={"STREETNAME_NL": "STREET"}),
            addresses[addresses.STREETNAME_DE.notnull()][["ID", "STREETNAME_DE", "NUMBER",
                                                          "POSTALCODE", "LAT", "LON"]].rename(columns={"STREETNAME_DE": "STREET"})

        ])
    else:
        addresses = addresses[["ID", "STREETNAME", "NUMBER",
                               "POSTALCODE", "LAT", "LON"]].rename(columns={"STREETNAME": "STREET"})
    ##
    # log(addresses)
    # log(addresses.columns)

    addresses = addresses[["ID", "STREET", "NUMBER",
                           "POSTALCODE", "LAT", "LON"]]
    addresses = addresses.drop_duplicates(subset=["STREET", "NUMBER", "POSTALCODE"])

    fname = f"{DATA_DIR_OUT}/bestaddresses_interpolation_be{region}.csv"

    log(f"[loc-{region}] -->{fname}")
    addresses.to_csv(fname, index=False)

    log(f"[interpol-{region}] Done!")


def clean_up(region=None):
    """
    Delete all csv files in the "in" directory

    Returns
    -------
    None.

    """

    if region and region in name_mapping:
        file_pattern = f"{DATA_DIR_IN}/{name_mapping[region]}*.csv*"
    else:
        file_pattern = f"{DATA_DIR_IN}/*.csv*"
    for file in glob.glob(file_pattern):
        log(f"[clean] Cleaning file {file})")

        os.remove(file)


DATA_DIR_IN = "/data/in/"
DATA_DIR_OUT = "/data/"

regions = ["bru", "wal", "vlg"]
try:
    opts, args = getopt.getopt(sys.argv[1:], "hfo:i:r:", ["output=", "region="])
except getopt.GetoptError:
    print('prepare_best_files.py -o <outputdir> -r <region>')
    sys.exit(2)

for opt, argm in opts:
    if opt in ("-o"):
        DATA_DIR_OUT = argm
        log(f"Data dir out: {DATA_DIR_OUT}")
    if opt in ("-i"):
        DATA_DIR_INT = argm
        log(f"Data dir in: {DATA_DIR_IN}")

    if opt in ("-r"):
        regions = [argm]
    if opt in ("-f"):  # within notebook
        DATA_DIR_IN = "./data/in/"
        DATA_DIR_OUT = "./data/"


os.makedirs(f"{DATA_DIR_OUT}", exist_ok=True)
os.makedirs(f"{DATA_DIR_IN}", exist_ok=True)

# Sequential run
for reg in regions:
    base = get_base_data_xml(reg)
    empty = get_empty_data_xml(reg)

    addr = create_address_data(base, reg)
    create_street_data(base, empty, reg)
    create_locality_data(base, reg)
    create_interpolation_data(base, reg)

    clean_up(reg)

# dsk = {}

# for reg in regions:

#     dsk[f'load-{reg}']    =    (get_base_data_xml  if source=="xml" else get_base_data_csv,  reg)
#     dsk[f'empty_street-{reg}']=(get_empty_data_xml if source=="xml" else get_empty_data_csv, reg)
#     dsk[f'addr-{reg}']    =    (create_address_data,  f'load-{reg}', reg, source)
#     dsk[f'streets-{reg}'] =    (create_street_data,   f'load-{reg}', f'empty_street-{reg}', reg, source)
#     dsk[f'localities-{reg}'] = (create_locality_data, f'load-{reg}', reg, source)

#     dsk[f'interpol-{reg}'] = (create_interpolation_data, f'addr-{reg}', reg)

# get(dsk, f"localities-{regions[0]}") # 'result' could be any task, we don't use it
