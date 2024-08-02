#!/usr/bin/env python
# coding: utf-8

# In[1]:


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

import zipfile

from dask.threaded import get

import pandas as pd
import numpy as np

import geopandas as gpd
import shapely

logging.basicConfig(format='[%(asctime)s]  %(message)s', stream=sys.stdout)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

## General functions


name_mapping= {
    "bru": "Brussels",
    "vlg": "Flanders",
    "wal": "Wallonia"
}

    
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



def build_addendum(fields, dfr):
    """
    Build the addendum_json_best column

    Parameters
    ----------
    fields : list
        List of fields.
    dfr : pd.DataFrame


    Returns
    -------
    res : pd.Series
        column "addendum_json_best".

    """
    res=""
    for fld in fields:
        for lang in ["fr", "nl", "de"]:
            fld_name = f"{fld}_{lang}"
            if fld_name in dfr:
                fld_val = dfr[f"{fld}_{lang}"]
                res += np.where(
                    fld_val.isnull(),
                    "",
                    f'"{fld}_{lang}": "'+fld_val.fillna("").str.replace('"', "'")+'", ')
    return res



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

    data = data.rename(columns= {
        "id" : "address_id",
        # "street_id",
        "street_nl" : "streetname_nl",
        "street_fr":  "streetname_fr",
        "street_de":  "streetname_de",
        
        "number" : "house_number",
        "box":     "box_number",
        
        "city_id" : "municipality_id",
        "city_nl" : "municipality_name_nl",
        "city_fr" : "municipality_name_fr",
        "city_de" : "municipality_name_de",
        
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
    
    log(f"[base-{region}] - Combining boxes ...")

    # log(data.iloc[0])
    
    # Combine all addresses at the same number in one record with "box_info" field
    with_box=data[data.box_number.notnull()]
    
    
    
    box_info = with_box.groupby(["house_number",
                                 "municipality_id", "municipality_name_de",
                                 "municipality_name_fr", "municipality_name_nl",
                                 "postcode", "postname_fr", "postname_nl","postname_de",
                                 #"part_of_municipality_id", "part_of_municipality_name_nl", "part_of_municipality_name_fr", "part_of_municipality_name_de",
                                 "street_id", "streetname_de", "streetname_fr", "streetname_nl"],dropna=False )[["lat", "lon",
                                                                "box_number",
                                                                "address_id",
                                                                "status"]]\
                        .apply(lambda x: x.to_json(orient='records'))\
                        .rename("box_info").reset_index()

    base_address = data.sort_values("box_number",na_position="first" )
    base_address = base_address.drop_duplicates(subset=["municipality_id", "street_id",
                                                        "postcode", "house_number"])
    base_address = base_address.drop("box_number", axis=1)

    cnt_before_mg = data.shape[0]
    del data

    data = base_address.merge(box_info, how="outer")

    del base_address, box_info

    log(f"[base-{region}] -   --> from {cnt_before_mg} to {data.shape[0]} records")


    if "postname_de" not in data:
        data["postname_de"]=pd.NA

    data["lon"] = data["lon"].where(data["lambertx"]!=0, pd.NA)
    data["lat"] = data["lat"].where(data["lamberty"]!=0, pd.NA)

    log(f"[base-{region}] -   Adding language data")
    for lang in ["fr", "nl", "de"]:
        data[f"name_{lang}"]=data["house_number"].fillna("")+", "+ data[f"streetname_{lang}"].fillna("")+", "+ data["postcode"].fillna("").astype(str)+" "+                           data[f"municipality_name_{lang}"].fillna("")

        data[f"name_{lang}"] = data[f"name_{lang}"].where(data[f"streetname_{lang}"].notnull(),
                                                          pd.NA)

    data["layer"]="address"
    data["country"]="Belgium"
    data["region_code"]=f"BE-{region.upper()}"

    log(f"[base-{region}] -   Adding addendum")

    addendum_json_best='{"best_id": "'+data["address_id"].astype(str)+'", '

    addendum_json_best += build_addendum(["name", "streetname",
                                          "municipality_name", 
                                          "part_of_municipality_name", 
                                          "postname"],
                                           data)
    
    # log("Will create addendum based on : ")
    # log(data)
    # log(data.columns)
    # log('part_of_municipality_id' in data)
    # log(data["part_of_municipality_id"].notnull().any())
    
    addendum_json_best += '"NIS": '+data.municipality_id.str.extract(r"/([0-9]{5})/")[0] +', ' +  \
        '"municipality_id": "'+data.municipality_id.astype(str) +'", ' +\
       ('"part_of_municipality_id": "'+data.part_of_municipality_id.astype(str) +'", ' if 'part_of_municipality_id' in data and data["part_of_municipality_id"].notnull().any() else "" )+\
        '"street_id": "'+ data.street_id.astype(str)+'", ' +\
        '"status": "'+ data.status + '"'

    addendum_json_best += np.where(data.box_info.isnull(),
                                     "",
                                     ',  "box_info": '+data.box_info)
    addendum_json_best += '}'

    data["addendum_json_best"] = addendum_json_best


    log(f"[base-{region}] -   Rename")
    data = data.rename(columns={"address_id":    "id",
                                "region_code":   "source",
                                "house_number":  "housenumber",
                               "postcode":      "postalcode" 
                               })
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

    log(f"[empty_street-{region}] - Building per language data")
    empty_street_all = []

    # Uniformizing column names to match with main CSV files
    for lang in ["fr", "nl", "de"]:
        empty_streets = empty_streets.rename(columns = {f"street_{lang}": f"streetname_{lang}",
                                                        f"city_{lang}": f"municipality_name_{lang}",
                                                        f"postal_{lang}": f"postname_{lang}"
                                                       })
        
    
    empty_streets["street_id"] = empty_streets["street_prefix"]+"/"+empty_streets["street_no"].astype(str)+"/"+empty_streets["street_version"].astype(str)
    empty_streets["municipality_id"] = empty_streets["city_prefix"]+"/"+empty_streets["city_no"].astype(str)+"/"+empty_streets["city_version"].astype(str)
    empty_streets = empty_streets.rename(columns = {"postal_id": "postalcode"})


    for lang in ["fr", "nl", "de"]:

        empty_streets_lg = empty_streets[empty_streets[f"streetname_{lang}"].notnull()].copy()

        if empty_streets_lg.shape[0] == 0:
            continue

        empty_streets_lg["locality"] = empty_streets_lg[f"municipality_name_{lang}"]
        empty_streets_lg["street"] =   empty_streets_lg[f"streetname_{lang}"]

        empty_streets_lg["source"] =   f"BE-{region.upper()}-emptystreets"
        empty_streets_lg["country"] =  "Belgium"
        empty_streets_lg["lat"] =      0
        empty_streets_lg["lon"]=       0
        empty_streets_lg["id"] =       empty_streets_lg.street_id
        empty_streets_lg["layer"] = "street"

        empty_streets_lg["name"] =   empty_streets_lg["street"]+", " + empty_streets_lg["postalcode"].astype(str)+" " + empty_streets_lg["locality"]

        empty_streets_lg["addendum_json_best"]='{' + build_addendum(["streetname", "municipality_name", "postname", "part_of_municipality_name"],
                           empty_streets_lg) +\
            '"NIS": '+empty_streets_lg.municipality_id.str.extract(r"/([0-9]{5})/")[0] +', ' +\
            '"municipality_id": "'+empty_streets_lg.municipality_id.astype(str) +'", ' +\
            '"street_id": "'+empty_streets_lg.street_id.astype(str) + '"}'




        empty_street_all.append(empty_streets_lg)



    empty_street_all = pd.concat(empty_street_all)
    empty_street_all = empty_street_all[["locality", "street","postalcode","source",
                                         "country","lat","lon","id","layer",
                                         "name", "addendum_json_best"]]
    log(f"[empty_street-{region}] - data: ")
    log(empty_street_all)

    return empty_street_all

def get_base_data_csv(region):
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

    best_fn = f"{DATA_DIR_IN}/openaddress-be{region}.zip"

    url = f"https://opendata.bosa.be/download/best/openaddress-be{region}.zip"
    log(f"[base-{region}] - Downloading {url}")

    download(url, best_fn)


    dtypes = {"box_number": str,
         "municipality_name_de": str,
         "municipality_name_nl": str,
         "municipality_name_fr": str,
         "postname_nl":   str,
         "postname_fr":   str,
         "streetname_de": str,
         "streetname_nl": str,
         "streetname_fr": str
         }


    log(f"[base-{region}] - Reading")
    data = pd.read_csv(best_fn, dtype=dtypes)

    log(f"[base-{region}] - Combining boxes ...")

    # Combine all addresses at the same number in one record with "box_info" field
    with_box=data[data.box_number.notnull()]
    box_info = with_box.rename(columns={"EPSG:4326_lat": "lat",
                                        "EPSG:4326_lon": "lon"})\
                       .groupby(["house_number",
                                 "municipality_id", "municipality_name_de",
                                 "municipality_name_fr", "municipality_name_nl",
                                 "postcode", "postname_fr", "postname_nl",
                                 "street_id", "streetname_de", "streetname_fr", "streetname_nl",
                                 "region_code"],dropna=False )[["lat", "lon",
                                                                "box_number",
                                                                "address_id",
                                                                "status"]]\
                        .apply(lambda x: x.to_json(orient='records'))\
                        .rename("box_info").reset_index()

    base_address = data.sort_values("box_number",na_position="first" )
    base_address = base_address.drop_duplicates(subset=["municipality_id", "street_id",
                                                        "postcode", "house_number"])
    base_address = base_address.drop("box_number", axis=1)

    cnt_before_mg = data.shape[0]
    del data

    data = base_address.merge(box_info, how="outer")

    del base_address, box_info

    log(f"[base-{region}] -   --> from {cnt_before_mg} to {data.shape[0]} records")


    if "postname_de" not in data:
        data["postname_de"]=pd.NA

    data["EPSG:4326_lat"] = data["EPSG:4326_lat"].where(data["EPSG:31370_y"]!=0, pd.NA)
    data["EPSG:4326_lon"] = data["EPSG:4326_lon"].where(data["EPSG:31370_x"]!=0, pd.NA)

    log(f"[base-{region}] -   Adding language data")
    for lang in ["fr", "nl", "de"]:
        data[f"name_{lang}"]=data["house_number"].fillna("")+", "+\
                           data[f"streetname_{lang}"].fillna("")+", "+\
                           data["postcode"].fillna("").astype(str)+" "+\
                           data[f"municipality_name_{lang}"].fillna("")

        data[f"name_{lang}"] = data[f"name_{lang}"].where(data[f"streetname_{lang}"].notnull(),
                                                          pd.NA)

    data["layer"]="address"
    data["country"]="Belgium"

    log(f"[base-{region}] -   Adding addendum")

    addendum_json_best='{"best_id": '+data["address_id"].astype(str)+', '

    addendum_json_best += build_addendum(["name", "streetname",
                                          "municipality_name", "postname", "part_of_municipality_name"],
                                           data)

    addendum_json_best += '"NIS": '+data.municipality_id.astype(str) +', ' +\
                            '"street_id": '+ data.street_id.astype(str)+', ' +\
                            '"status": "'+ data.status + '"'

    addendum_json_best += np.where(data.box_info.isnull(),
                                     "",
                                     ',  "box_info": '+data.box_info)
    addendum_json_best += '}'

    data["addendum_json_best"] = addendum_json_best


    log(f"[base-{region}] -   Rename")
    data = data.rename(columns={"EPSG:4326_lat": "lat",
                                "EPSG:4326_lon": "lon",
                                "address_id":    "id",
                                "region_code":   "source",
                                "house_number":  "housenumber",
                                "postcode":      "postalcode" })
    log(f"[base-{region}] Done!")
    return data


def get_empty_data_csv(region):
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
    url = "https://opendata.bosa.be/download/best/postalstreets-empty-latest.zip"
    best_fn = f"{DATA_DIR_IN}/postalstreets-empty-latest.zip"

    name_mapping= {
        "bru": "Brussels",
        "vlg": "Flanders",
        "wal": "Wallonia"
    }

    if not os.path.isfile(best_fn):
        download(url, best_fn)

        
    # open zipped dataset
    with zipfile.ZipFile(best_fn) as zipf:
   # open the csv file in the dataset
        with zipf.open(f"{name_mapping[region]}_empty_street.csv") as csvf:

            empty_streets = pd.read_csv(csvf)

    log(f"[empty_street-{region}] - Building per language data")
    empty_street_all = []

    # Uniformizing column names to match with main CSV files
    for lang in ["fr", "nl", "de"]:
        empty_streets = empty_streets.rename(columns = {f"street_{lang}": f"streetname_{lang}",
                                                        f"city_{lang}": f"municipality_name_{lang}",
                                                        f"postal_{lang}": f"postname_{lang}"
                                                       })
    empty_streets = empty_streets.rename(columns = {"postal_id": "postalcode",
                                                   "city_no": "municipality_id",
                                                   "street_no": "street_id"})


    for lang in ["fr", "nl", "de"]:

        empty_streets_lg = empty_streets[empty_streets[f"streetname_{lang}"].notnull()].copy()

        if empty_streets_lg.shape[0] == 0:
            continue

        empty_streets_lg["locality"] = empty_streets_lg[f"municipality_name_{lang}"]
        empty_streets_lg["street"] =   empty_streets_lg[f"streetname_{lang}"]

        empty_streets_lg["source"] =   f"BE-{region.upper()}-emptystreets"
        empty_streets_lg["country"] =  "Belgium"
        empty_streets_lg["lat"] =      0
        empty_streets_lg["lon"]=       0
        empty_streets_lg["id"] =       f"be{region}_{lang}_street_"+ \
                                            empty_streets_lg.street_id.astype(str)
        empty_streets_lg["layer"] = "street"

        empty_streets_lg["name"] =   empty_streets_lg["street"]+", "\
                                   + empty_streets_lg["postalcode"].astype(str)+" "\
                                   + empty_streets_lg["locality"]

        empty_streets_lg["addendum_json_best"]='{' +\
            build_addendum(["streetname", "municipality_name", "postname", "part_of_municipality_name"],
                           empty_streets_lg) +\
            '"NIS": '      +empty_streets_lg.municipality_id.astype(str) + ', ' +\
            '"street_id": '+empty_streets_lg.street_id.astype(str) + '}'




        empty_street_all.append(empty_streets_lg)



    empty_street_all = pd.concat(empty_street_all)
    empty_street_all = empty_street_all[["locality", "street","postalcode","source",
                                         "country","lat","lon","id","layer",
                                         "name", "addendum_json_best"]]
    log(f"[empty_street-{region}] - data: ")
    log(empty_street_all)

    return empty_street_all


def create_address_data(data, region, source):
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
    log(f"[addr-{region}] - Building per language data")

    addresses_all = []
    for lang in ["fr", "nl", "de"]:
#         print(lg)
        addresses_lg = data[data[f"name_{lang}"].notnull()].copy()
        if source=="csv":
            addresses_lg["id"] = f"be{region}_{lang}_"+addresses_lg["id"].astype(str)
        else:
            addresses_lg["id"] = addresses_lg["id"].astype(str)+f"_{lang}"

        addresses_lg["name"]=    addresses_lg[f"name_{lang}"]
        addresses_lg["street"]=  addresses_lg[f"streetname_{lang}"]
        addresses_lg["locality"]=addresses_lg[f"municipality_name_{lang}"]
        addresses_all.append(addresses_lg)
        #display(data_lg)
    addresses_all = pd.concat(addresses_all)[["id", "lat", "lon", "housenumber","locality",
                                    "street", "postalcode", "source", "name",
                                    "name_fr", "name_nl","name_de",
                                    "layer", "country", "addendum_json_best"]]


    addresses_all = addresses_all.drop_duplicates(addresses_all.drop("id", axis=1).columns)


    addresses_all = addresses_all.fillna({"lat" :0, "lon":0})

    log(addresses_all)


    fname = f"{DATA_DIR_OUT}/bestaddresses_be{region}.csv"
    log(f"[addr-{region}] -->{fname}")
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




def create_street_data(data, empty_street, region, source):
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
        data_parity = data[data.housenumber_num.mod(2)==parity]
        data_parity =data_parity.sort_values(["municipality_id", "street_id",
                                              "housenumber_num", "housenumber"])

        streets_geo =  data_parity.groupby(["municipality_id", "street_id" ]).geometry.apply(lambda bloc: shapely.geometry.LineString(bloc)
                                                      if bloc.shape[0]>1
                                                      else bloc.iloc[0])

        streets_geo_multi = streets_geo[streets_geo.geom_type == "LineString"].geometry.apply(shapely.line_interpolate_point,
                                           distance=0.5,
                                           normalized=True)
        streets_geo_point = streets_geo[streets_geo.geom_type == "Point"].geometry

        return  pd.concat([streets_geo_multi, streets_geo_point])

    log(f"[street-{region}] - Building streets data")

    # old version : simple mean of coordinates
#     all_streets = data.groupby([f for f in ["municipality_id",
#                     "municipality_name_fr", "municipality_name_nl", "municipality_name_de",
#                     "postname_fr", "postname_nl", "postname_de",
#                     "streetname_fr", "streetname_nl", "streetname_de","street_id",
#                     "postalcode", "source", "country"] if f in data],
#                                dropna=False)[["lat", "lon"]].mean().reset_index()

    # new version : compute center of linestrings for both odd and even sides,
    # then take the middle of those points
    geo_data = data[data.lat.notnull()]

    geo_data = geo_data.assign(housenumber_num =  geo_data.housenumber.str.extract("^([0-9]*)").astype(int, errors="ignore"))

    # If some number where not converted to int (did not start by digits) --> ignore them
    if geo_data.housenumber_num.dtype !=int:
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

    fields = [f for f in ["municipality_id",
                      "municipality_name_fr", "municipality_name_nl", "municipality_name_de",
                      "postname_fr",   "postname_nl", "postname_de",
                      "streetname_fr", "streetname_nl", "streetname_de","street_id",
                      "postalcode",    "source", "country"] if f in data]
    all_streets = data[fields].drop_duplicates()                              .merge(streets_centers_duo[["lat", "lon"]],
                                     left_on=["municipality_id", "street_id"],
                                     right_index=True)

    data_street_all = []
    for lang in ["fr", "nl", "de"]:
#         print(lg)

        data_street_lg = all_streets[all_streets[f"streetname_{lang}"].notnull()].copy()

        if data_street_lg.shape[0]==0:
            continue

        # To be replaced by BestID
        if source=="csv":
            data_street_lg["id"] = f"be{region}_{lang}_street_"+data_street_lg.street_id.astype(str)
        else:
            data_street_lg["id"] = data_street_lg.street_id+f"_{lang}" #f"be{region}_{lang}_street_"+data_street_lg.street_id.astype(str)

        data_street_lg["layer"]="street"
        data_street_lg["name"] = data_street_lg[f"streetname_{lang}"]+", "+ data_street_lg["postalcode"].astype(str)+" "+ data_street_lg[f"municipality_name_{lang}"]


        data_street_lg["locality"] = data_street_lg[f"municipality_name_{lang}"]
        data_street_lg["street"] =   data_street_lg[f"streetname_{lang}"]


        data_street_lg["addendum_json_best"]='{' +            build_addendum(["streetname", "municipality_name", "postname", "part_of_municipality_name"],
                           data_street_lg) +\
            ('"NIS": '      +data_street_lg.municipality_id.str.extract(r"/([0-9]{5})/")[0] + ', ' if source=="xml" else "") +\
            '"municipality_id": "'+data_street_lg.municipality_id.astype(str) +'", ' +\
            '"street_id": "'+data_street_lg.street_id.astype(str) + '"}'


        data_street_lg = data_street_lg[["locality", "street","postalcode","source",
                                         "country","lat","lon","id","layer",
                                         "name", "addendum_json_best"]]
        data_street_all.append(data_street_lg)


    data_street_all = pd.concat(data_street_all)
    data_street_all = data_street_all.fillna({"lat" :0, "lon":0})


    log(data_street_all)

    log(f"[street-{region}] - Combining data and empty streets")

    data_street_all = pd.concat([data_street_all, empty_street])


    fname = f"{DATA_DIR_OUT}/bestaddresses_streets_be{region}.csv"
    log(f"[street-{region}] -->{fname}")
    data_street_all.to_csv(fname, index=False)

    log(f"[street-{region}] Done!")

    


def create_locality_data(data, region, source):
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

    all_localities = data.groupby([f for f in ["municipality_id",
                        "municipality_name_fr", "municipality_name_nl", "municipality_name_de",
                        "postname_fr", "postname_nl", "postname_de",
                        "postalcode", "source", "country"] if f in data],
                                  dropna=False)[["lat", "lon"]].mean().reset_index()


    data_localities_all = []

    for lang in ["fr", "nl", "de"]:

        data_localities_lg= all_localities[all_localities[f"municipality_name_{lang}"].notnull()].copy()

        if data_localities_lg.shape[0]==0:
            continue


        if source=="csv":
            data_localities_lg["id"] = f"be{region}_{lang}_locality_"+\
                data_localities_lg.municipality_id.astype(str)+"_"+\
                data_localities_lg["postalcode"].astype(str)+"_"+\
                data_localities_lg.index.astype(str)
        else:
            data_localities_lg["id"] = data_localities_lg.municipality_id+f"_{lang}"


        data_localities_lg["layer"]="city"
        data_localities_lg["name"] = data_localities_lg["postalcode"].astype(str)+" "+            data_localities_lg[f"municipality_name_{lang}"] +            np.where(
                (data_localities_lg[f"municipality_name_{lang}"]==data_localities_lg[f"postname_{lang}"]) |
                      data_localities_lg[f"postname_{lang}"].isnull() ,
                "",
                " ("+ data_localities_lg[f"postname_{lang}"].fillna("")+")")


        data_localities_lg["locality"] = data_localities_lg[f"municipality_name_{lang}"]

        data_localities_lg["addendum_json_best"]='{'+build_addendum(["municipality_name",
                                                                     "postname"],
                                                                     data_localities_lg)+\
            ('"NIS": '+data_localities_lg.municipality_id.astype(str).str.extract(r"/([0-9]{5})/")[0] +", " if source == "xml" else "")+\
            '"municipality_id": "'+data_localities_lg.municipality_id.astype(str) +'"'+\
        '}'


        data_localities_lg = data_localities_lg[["locality", "postalcode","source",
                                                 "country","lat","lon","id",
                                                 "layer","name", "addendum_json_best"]]
        data_localities_all.append(data_localities_lg)

    data_localities_all = pd.concat(data_localities_all)
    data_localities_all = data_localities_all.fillna({"lat" :0, "lon":0})

    log(data_localities_all)
    fname = f"{DATA_DIR_OUT}/bestaddresses_localities_be{region}.csv"

    log(f"[loc-{region}] -->{fname}")

    data_localities_all.to_csv(fname, index=False)
    log(f"[loc-{region}] Done!")


# In[24]:


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

    addresses = addresses[addresses.addendum_json_best.str.contains('"status": "current"')]

    log(f"[interpol-{region}] only current: {addresses.shape[0]}")

    addresses.columns = addresses.columns.str.upper()

    addresses = addresses.rename(columns={
        "HOUSENUMBER": "NUMBER",
    })

    addresses["NUMBER"] = addresses["NUMBER"].str.extract("^([0-9]*)").astype(int, errors="ignore")

    addresses = addresses[addresses["NUMBER"] != ""]

    log(f"[interpol-{region}] remove non digits: {addresses.shape[0]}")

    addresses = addresses[["ID", "STREET", "NUMBER",
                           "POSTALCODE", "LAT", "LON"]].\
                drop_duplicates(subset=["STREET", "NUMBER", "POSTALCODE"])

    fname = f"{DATA_DIR_OUT}/bestaddresses_interpolation_be{region}.csv"

    log(f"[loc-{region}] -->{fname}")
    addresses.to_csv(fname, index=False)

    log(f"[interpol-{region}] Done!")


# In[25]:


def clean_up():
    """
    Delete all csv files in the "in" directory

    Returns
    -------
    None.

    """
    for file in glob.glob(f"{DATA_DIR_IN}/*.csv*"):
        log(f"[clean] Cleaning file {file})")

        os.remove(file)



DATA_DIR_IN = "/data/in/"
DATA_DIR_OUT = "/data/"

regions = ["vlg", "wal", "bru"]
try:
    opts, args = getopt.getopt(sys.argv[1:],"hfo:i:r:", ["output=", "region=", "source="])
except getopt.GetoptError:
    print ('prepare_best_files.py -o <outputdir> -r <region>')
    sys.exit(2)

source="csv"

for opt, arg in opts:
    if opt in ("-o"):
        DATA_DIR_OUT = arg
        log(f"Data dir out: {DATA_DIR_OUT}")
    if opt in ("-i"):
        DATA_DIR_INT = arg
        log(f"Data dir in: {DATA_DIR_IN}")

    if opt in ("-r"):
        regions = [arg]
    if opt in ("-f"): # within notebook
        DATA_DIR_IN = "./data/in/"
        DATA_DIR_OUT = "./data/"
    

    if opt in ("--source"):
        source=arg

os.makedirs(f"{DATA_DIR_OUT}", exist_ok=True)
os.makedirs(f"{DATA_DIR_IN}", exist_ok=True)

# Sequential run
for region in regions:
    if source=="xml":
        data = get_base_data_xml(region)
        empty = get_empty_data_xml(region)
    else:
        data = get_base_data_csv(region)
        empty = get_empty_data_csv(region)
    
    addr = create_address_data(data, region, source)
    create_street_data(data, empty, region, source)
    create_locality_data(data, region, source)
    create_interpolation_data(addr, region)



# dsk = {}

# for reg in regions:

#     dsk[f'load-{reg}']    =    (get_base_data_xml  if source=="xml" else get_base_data_csv,  reg)
#     dsk[f'empty_street-{reg}']=(get_empty_data_xml if source=="xml" else get_empty_data_csv, reg)
#     dsk[f'addr-{reg}']    =    (create_address_data,  f'load-{reg}', reg, source)
#     dsk[f'streets-{reg}'] =    (create_street_data,   f'load-{reg}', f'empty_street-{reg}', reg, source)
#     dsk[f'localities-{reg}'] = (create_locality_data, f'load-{reg}', reg, source)

#     dsk[f'interpol-{reg}'] = (create_interpolation_data, f'addr-{reg}', reg)

# get(dsk, f"localities-{regions[0]}") # 'result' could be any task, we don't use it


clean_up()






