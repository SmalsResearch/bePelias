"""

Convert BestAddress files (from https://opendata.bosa.be/) into a file readable
by Pelias (csv module)

@author: Vandy Berten (vandy.berten@smals.be)

"""
import os
import urllib.request

import pandas as pd
import numpy as np

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

# os.mkdirs(data)
os.makedirs(f"{DATA_DIR}", exist_ok=True)
os.makedirs(f"{DATA_DIR}/in", exist_ok=True)



for region in ["bru", "vlg", "wal"]:

    print(f"Building data for {region}")

    

    best_fn = f"{DATA_DIR}/in/openaddress-be{region}.zip"
    
    url = f"https://opendata.bosa.be/download/best/openaddress-be{region}.zip"
    print(f"- Downloading {url}")
    
    download(url, best_fn)


    dtypes = {"box_number": str,
         "municipality_name_de": str,
         "municipality_name_nl": str,
         "postname_nl": str,
         "postname_fr": str,
         "streetname_de": str,
         "streetname_nl": str,
         "streetname_fr": str
         }


    print("- Reading")
    data = pd.read_csv(best_fn, dtype=dtypes)


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


    box_nbr= data.box_number.astype(str).str.replace('"', "'").str.replace('\\', "/", regex=False)
    data["addendum_json_best"] += np.where(data.box_number.isnull(),
                                           "",
                                           '"box_number": "'+box_nbr+'", ')+  \
                            '"NIS": '+data.municipality_id.astype(str) + '}'

    # + add part of municipality, postalname

    data = data.rename(columns={"EPSG:4326_lat": "lat",
                                "EPSG:4326_lon": "lon",
                                "address_id":    "id",
                                "region_code":   "source",
                                "house_number":  "housenumber",
                                "postcode":      "postalcode" })

    print("- Building per language data")

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

    print(data_all)

    fname = f"{DATA_DIR}/bestaddresses_be{region}.csv"
    data_all.to_csv(fname, index=False)

    print(f" -->{fname}")

    print("- Building streets data")


    all_streets = data.groupby([f for f in ["municipality_id",
                    "municipality_name_fr", "municipality_name_nl", "municipality_name_de",
                    "postname_fr", "postname_nl", "postname_de",
                    "streetname_fr", "streetname_nl", "streetname_de",
                    "postalcode", "source", "country"] if f in data], 
                               dropna=False)[["lat", "lon"]].mean().reset_index()



    data_street_all = []
    for lg in ["fr", "nl", "de"]:
#         print(lg)

        data_street_lg = all_streets[all_streets["streetname_"+lg].notnull()].copy()

        if data_street_lg.shape[0]==0:
            continue

        # To be replaced by BestID
        data_street_lg["id"] = f"be{region}_{lg}_street_"+data_street_lg.index.astype(str)

        data_street_lg["layer"]="street"
        data_street_lg["name"] = data_street_lg["streetname_"+lg]+", "+\
                data_street_lg["postalcode"].astype(str)+" "+\
                data_street_lg["municipality_name_"+lg]


        data_street_lg["locality"] = data_street_lg["municipality_name_"+lg]
        data_street_lg["street"] =   data_street_lg["streetname_"+lg]


        data_street_lg["addendum_json_best"]='{' +\
            build_addendum(["streetname", "municipality_name", "postname"],
                           data_street_lg) +\
            '"NIS": '+data_street_lg.municipality_id.astype(str) +  '}'


        data_street_lg = data_street_lg[["locality", "street","postalcode","source",
                                         "country","lat","lon","id","layer",
                                         "name", "addendum_json_best"]]
        data_street_all.append(data_street_lg)


    data_street_all = pd.concat(data_street_all)
    data_street_all = data_street_all.fillna({"lat" :0, "lon":0})


    print(data_street_all)

    fname = f"{DATA_DIR}/bestaddresses_streets_be{region}.csv"
    data_street_all.to_csv(fname, index=False)
    print(f" -->{fname}")

    print("- Building localities data")

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

        data_localities_lg["layer"]="locality"
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

    print(data_localities_all)

    fname = f"{DATA_DIR}/bestaddresses_localities_be{region}.csv"
    data_localities_all.to_csv(fname, index=False)
    print(f" -->{fname}")
    print()
    print()
    