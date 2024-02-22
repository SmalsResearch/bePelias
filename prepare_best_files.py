"""

Convert BestAddress files (from https://opendata.bosa.be/) into a file readable
by Pelias (csv module)

@author: Vandy Berten (vandy.berten@smals.be)

"""
import os
import sys
import urllib.request
import logging

import zipfile

import pandas as pd
import numpy as np

import sys, getopt, glob

logging.basicConfig(format='[%(asctime)s]  %(message)s', stream=sys.stdout)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

## General functions

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

            fld_val = dfr[f"{fld}_{lang}"]
            res += np.where(
                fld_val.isnull(),
                "",
                f'"{fld}_{lang}": "'+fld_val.fillna("").str.replace('"', "'")+'", ')
    return res
# In[103]:


DATA_DIR = "/data/"

regions = ["vlg", "wal", "bru"]
try:
    opts, args = getopt.getopt(sys.argv[1:],"ho:r:", ["output=", "region="])
except getopt.GetoptError:
    print ('prepare_best_files.py -o <outputdir> -r <region>')
    sys.exit(2)

for opt, arg in opts:
    if opt in ("-o"):
        DATA_DIR = arg
        log(f"Data dir: {DATA_DIR}")
    if opt in ("-r"):
        regions = [arg]
    
    
# os.mkdirs(data)
os.makedirs(f"{DATA_DIR}", exist_ok=True)
os.makedirs(f"{DATA_DIR}/in", exist_ok=True)

def get_base_data(region):
    log(f"[base-{region}] Building data for {region}")

    

    best_fn = f"{DATA_DIR}/in/openaddress-be{region}.zip"
    
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
                                 "municipality_id", "municipality_name_de", "municipality_name_fr", "municipality_name_nl", 
                                 "postcode", "postname_fr", "postname_nl", 
                                 "street_id", "streetname_de", "streetname_fr", "streetname_nl", 
                                 "region_code"],dropna=False )[["lat", "lon", 
                                                                "box_number",
                                                                "address_id",
                                                                "status"]]\
                        .apply(lambda x: x.to_json(orient='records')).rename("box_info").reset_index()

    base_address = data.sort_values("box_number",na_position="first" ).drop_duplicates(subset=["municipality_id", "street_id", "postcode", "house_number"]).drop("box_number", axis=1)
    
    data_mg = base_address.merge(box_info, how="outer")

    log(f"[base-{region}] -   --> from {data.shape[0]} to {data_mg.shape[0]} records")
    data=data_mg
    
    
    
    if "postname_de" not in data:
        data["postname_de"]=pd.NA

    data["EPSG:4326_lat"] = data["EPSG:4326_lat"].where(data["EPSG:31370_y"]!=0, pd.NA)
    data["EPSG:4326_lon"] = data["EPSG:4326_lon"].where(data["EPSG:31370_x"]!=0, pd.NA)

    for lg in ["fr", "nl", "de"]:
        data[f"name_{lg}"]=data["house_number"].fillna("")+", "+\
                           data[f"streetname_{lg}"].fillna("")+", "+\
                           data["postcode"].fillna("").astype(str)+" "+\
                           data[f"municipality_name_{lg}"].fillna("")

        data[f"name_{lg}"] = data[f"name_{lg}"].where(data[f"streetname_{lg}"].notnull(), pd.NA)

    data["layer"]="address"
    data["country"]="Belgium"
    data["addendum_json_best"]='{"best_id": '+data["address_id"].astype(str)+', '

    data["addendum_json_best"] += build_addendum(["name", "streetname",
                                                  "municipality_name", "postname"],
                                                 data)


    data["addendum_json_best"] += '"NIS": '+data.municipality_id.astype(str) +', ' +\
                                  '"street_id": '+ data.street_id.astype(str)+', ' +\
                                  '"status": "'+ data.status + '"'
    

    data["addendum_json_best"] += np.where(data.box_info.isnull(),
                                           "",
                                           ',  "box_info": '+data.box_info)
    
    data["addendum_json_best"] += '}'
    
    # with pd.option_context("display.max_colwidth", None):
    #     log(data[data.box_info.notnull()]["addendum_json_best"])
    # + add part of municipality, postalname

    data = data.rename(columns={"EPSG:4326_lat": "lat",
                                "EPSG:4326_lon": "lon",
                                "address_id":    "id",
                                "region_code":   "source",
                                "house_number":  "housenumber",
                                "postcode":      "postalcode" })
    log(f"[base-{region}] Done!")
    return data

    
def get_empty_data(region):
    log(f"[empty_street-{region}] - Downloading")
    url = "https://opendata.bosa.be/download/best/postalstreets-empty-latest.zip"
    best_fn = f"{DATA_DIR}/in/postalstreets-empty-latest.zip"
        
    name_mapping= {
        "bru": "Brussels",
        "vlg": "Flanders",
        "wal": "Wallonia"
    }
    
    if not os.path.isfile(best_fn):
        download(url, best_fn)
        
    # open zipped dataset
    with zipfile.ZipFile(best_fn) as z:
   # open the csv file in the dataset
       with z.open(f"{name_mapping[region]}_empty_street.csv") as f:     
       
        empty_streets = pd.read_csv(f)
        
    log(f"[empty_street-{region}] - Building per language data")
    empty_street_all = []
    
    # Uniformizing column names to match with main CSV files
    for lg in ["fr", "nl", "de"]:
        empty_streets = empty_streets.rename(columns = {f"street_{lg}": f"streetname_{lg}",
                                                        f"city_{lg}": f"municipality_name_{lg}",
                                                        f"postal_{lg}": f"postname_{lg}"
                                                       })
    empty_streets = empty_streets.rename(columns = {"postal_id": "postalcode",
                                                   "city_no": "municipality_id",
                                                   "street_no": "street_id"})
    
        
    for lg in ["fr", "nl", "de"]:

        empty_streets_lg = empty_streets[empty_streets[f"streetname_{lg}"].notnull()].copy()

        if empty_streets_lg.shape[0] == 0:
            continue

        empty_streets_lg["locality"] =   empty_streets_lg[f"municipality_name_{lg}"]
        empty_streets_lg["street"] =     empty_streets_lg[f"streetname_{lg}"]
        
        empty_streets_lg["source"] =     f"BE-{region.upper()}-emptystreets"
        empty_streets_lg["country"] =    "Belgium"
        empty_streets_lg["lat"] =        0
        empty_streets_lg["lon"]=         0
        empty_streets_lg["id"] =         f"be{region}_{lg}_street_"+empty_streets_lg.street_id.astype(str) 
        empty_streets_lg["layer"] = "street"

        empty_streets_lg["name"] = empty_streets_lg["street"]+", "+empty_streets_lg["postalcode"].astype(str)+" "+empty_streets_lg["locality"]
        
        empty_streets_lg["addendum_json_best"]='{' +\
            build_addendum(["streetname", "municipality_name", "postname"],
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
    
    
def create_address_data(data, region):
    log(f"[addr-{region}] - Building per language data")

    data_all = []
    for lg in ["fr", "nl", "de"]:
#         print(lg)
        data_lg = data[data["name_"+lg].notnull()].copy()
        data_lg["id"] = f"be{region}_{lg}_"+data_lg["id"].astype(str)
        data_lg["name"]=data_lg["name_"+lg]
        data_lg["street"]=data_lg["streetname_"+lg]
        data_lg["locality"]=data_lg["municipality_name_"+lg]
        data_all.append(data_lg)
        #display(data_lg)
    data_all = pd.concat(data_all)[["id", "lat", "lon", "housenumber","locality",
                                    "street", "postalcode", "source", "name",
                                    "name_fr", "name_nl","name_de",
                                    "layer", "country", "addendum_json_best"]]


    data_all = data_all.drop_duplicates(data_all.drop("id", axis=1).columns)


    data_all = data_all.fillna({"lat" :0, "lon":0})

    log(data_all)

    
    fname = f"{DATA_DIR}/bestaddresses_be{region}.csv"
    log(f"[addr-{region}] -->{fname}")
    data_all.to_csv(fname, index=False)
    
    log(f"[addr-{region}] Done!")
    
    
def create_street_data(data, empty_street, region):
    
    log(f"[street-{region}] - Building streets data")

    all_streets = data.groupby([f for f in ["municipality_id",
                    "municipality_name_fr", "municipality_name_nl", "municipality_name_de",
                    "postname_fr", "postname_nl", "postname_de",
                    "streetname_fr", "streetname_nl", "streetname_de","street_id",
                    "postalcode", "source", "country"] if f in data], 
                               dropna=False)[["lat", "lon"]].mean().reset_index()



    data_street_all = []
    for lg in ["fr", "nl", "de"]:
#         print(lg)

        data_street_lg = all_streets[all_streets["streetname_"+lg].notnull()].copy()

        if data_street_lg.shape[0]==0:
            continue

        # To be replaced by BestID
        data_street_lg["id"] = f"be{region}_{lg}_street_"+data_street_lg.street_id.astype(str) #data_street_lg.index.astype(str)

        data_street_lg["layer"]="street"
        data_street_lg["name"] = data_street_lg["streetname_"+lg]+", "+\
                data_street_lg["postalcode"].astype(str)+" "+\
                data_street_lg["municipality_name_"+lg]


        data_street_lg["locality"] = data_street_lg["municipality_name_"+lg]
        data_street_lg["street"] =   data_street_lg["streetname_"+lg]


        data_street_lg["addendum_json_best"]='{' +\
            build_addendum(["streetname", "municipality_name", "postname"],
                           data_street_lg) +\
            '"NIS": '      +data_street_lg.municipality_id.astype(str) + ', ' +\
            '"street_id": '+data_street_lg.street_id.astype(str) + '}'


        data_street_lg = data_street_lg[["locality", "street","postalcode","source",
                                         "country","lat","lon","id","layer",
                                         "name", "addendum_json_best"]]
        data_street_all.append(data_street_lg)


    data_street_all = pd.concat(data_street_all)
    data_street_all = data_street_all.fillna({"lat" :0, "lon":0})


    log(data_street_all)
    
    log(f"[street-{region}] - Combining data and empty streets")
    
    data_street_all = pd.concat([data_street_all, empty_street])
    
    
    fname = f"{DATA_DIR}/bestaddresses_streets_be{region}.csv"
    log(f"[street-{region}] -->{fname}")
    data_street_all.to_csv(fname, index=False)
    
    log(f"[street-{region}] Done!")
    
def create_locality_data(data, region):
    log(f"[loc-{region}] - Building localities data")

    all_localities = data.groupby([f for f in ["municipality_id",
                        "municipality_name_fr", "municipality_name_nl", "municipality_name_de",
                        "postname_fr", "postname_nl", "postname_de",
                        "postalcode", "source", "country"] if f in data], 
                                  dropna=False)[["lat", "lon"]].mean().reset_index()


    data_localities_all = []

    for lg in ["fr", "nl", "de"]:

        data_localities_lg= all_localities[all_localities["municipality_name_"+lg].notnull()].copy()

        if data_localities_lg.shape[0]==0:
            continue


        data_localities_lg["id"] = f"be{region}_{lg}_locality_"+\
            data_localities_lg.municipality_id.astype(str)+"_"+\
            data_localities_lg["postalcode"].astype(str)+"_"+\
            data_localities_lg.index.astype(str) # To be replaced by BestID

        data_localities_lg["layer"]="city"
        data_localities_lg["name"] = data_localities_lg["postalcode"].astype(str)+" "+\
            data_localities_lg["municipality_name_"+lg] +\
            np.where(
                (data_localities_lg["municipality_name_"+lg]==data_localities_lg["postname_"+lg]) |
                      data_localities_lg["postname_"+lg].isnull() ,
                "",
                " ("+ data_localities_lg["postname_"+lg].fillna("")+")")


        data_localities_lg["locality"] = data_localities_lg["municipality_name_"+lg]

        data_localities_lg["addendum_json_best"]='{'+build_addendum(["municipality_name",
                                                                     "postname"],
                                                                     data_localities_lg)+\
            '"NIS": '+data_localities_lg.municipality_id.astype(str) + '}'


        data_localities_lg = data_localities_lg[["locality", "postalcode","source",
                                                 "country","lat","lon","id",
                                                 "layer","name", "addendum_json_best"]]
        data_localities_all.append(data_localities_lg)

    data_localities_all = pd.concat(data_localities_all)
    data_localities_all = data_localities_all.fillna({"lat" :0, "lon":0})

    log(data_localities_all)
    fname = f"{DATA_DIR}/bestaddresses_localities_be{region}.csv"
    
    log(f"[loc-{region}] -->{fname}")
    
    data_localities_all.to_csv(fname, index=False)
    log(f"[loc-{region}] Done!")
    

def clean_up():
    
    
    for f in glob.glob(f"{DATA_DIR}/in/*.zip"):
        log(f"[clean-{region}] Cleaning file {f})")
        
        os.remove(f)
        
        
        
# # Sequential run
# for region in ["bru", "vlg", "wal"]:
#     data = get_base_data(region)
#     empty = get_empty_data(region)
#     create_address_data(data, region)
#     create_street_data(data, empty, region)
#     create_locality_data(data, region)
    

from dask.threaded import get
dsk = {}

for region in regions:
    
    
    dsk[f'load-{region}']    =    (get_base_data,                          region)
    dsk[f'empty_street-{region}']=(get_empty_data,                         region)
    dsk[f'addr-{region}']    =    (create_address_data,  f'load-{region}', region)    
    dsk[f'streets-{region}'] =    (create_street_data,   f'load-{region}', f'empty_street-{region}', region)
    dsk[f'localities-{region}'] = (create_locality_data, f'load-{region}', region)  
    

get(dsk, f"localities-{regions[0]}") # 'result' could be any task, we don't use it


clean_up()