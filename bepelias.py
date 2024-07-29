#!/usr/bin/env python
# coding: utf-8

"""
Flask part of bePelias geocoder

@author: Vandy Berten (vandy.berten@smals.be)

"""

# pylint: disable=line-too-long
# pylint: disable=invalid-name


import os
import sys
import urllib

import time
import logging
import json
import re

from urllib.parse import unquote_plus


import textdistance
from unidecode import unidecode

from elasticsearch import Elasticsearch, NotFoundError

from flask import Flask,  request, url_for
from flask_restx import Api, Resource, reqparse

import pandas as pd

logging.basicConfig(format='[%(asctime)s]  %(message)s', stream=sys.stdout)


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

    if argname in request.form: # in payload
        return request.form[argname]

    return request.args.get(argname, def_val) # in args


## Pelias functions/classes
class PeliasException(Exception):
    """
    Exceptions related to Pelias
    """

class Pelias:
    """
    Class calling Pelias REST API
    """
    def __init__(
            self,
            domain,
            scheme="http",
    ):

        self.geocode_path = '/v1/search'
        self.geocode_struct_path = '/v1/search/structured'

        self.interpolate_path = '/search/geojson'

        self.verbose=False
        self.scheme = scheme
        self.domain = domain.strip('/')

        self.geocode_api = (
            f'{self.scheme}://{self.domain}{self.geocode_path}'
        )

        self.geocode_struct_api = (
            f'{self.scheme}://{self.domain}{self.geocode_struct_path}'
        )

        self.interpolate_api = (
            f'{self.scheme}://{self.domain.replace("4000", "4300")}{self.interpolate_path}'
        )
        
        self.elastic_api = (
            f'{self.scheme}://{self.domain.replace("4000", "9200")}'
        )


    def call_service(self, url, nb_attempts = 6):
        """
        Call URL. If something went wrong, wait a short delay, and try again,
        up to nb_attempts times

        Parameters
        ----------
        url : TYPE
            DESCRIPTION.
        nb_attempts : TYPE, optional
            DESCRIPTION. The default is 6.

        Raises
        ------
        PeliasException
            If a valid answer is not received after nb_attempts .

        Returns
        -------
        dict
            Pelias result.
        """
        delay=1
        while nb_attempts>0:
            try:
                with urllib.request.urlopen(url) as response:
                    res = response.read()
                    res = json.loads(res)
                    return res
            except urllib.error.HTTPError as exc:
                if exc.code==400 and self.interpolate_api in url: # bad request, typically bad house number format
                    log(f"Error 400 ({url}): {exc}")
                    return {}

                if nb_attempts==1:
                    log(f"Cannot get Pelias results after several attempts({url}): {exc}")
                    raise PeliasException (f"Cannot get Pelias results after several attempts ({url}): {exc}") from exc
                nb_attempts-=1
                log(f"Cannot get Pelias results ({url}): {exc}. Try again in {delay} seconds...")
                time.sleep(delay)
                delay += 0.5

            except Exception as exc:
                log(f"Cannot get Pelias results ({url}): {exc}")
                raise exc

    def geocode(self, query):
        """
        Call Pelias geocoder

        Parameters
        ----------
        query : dict or str
            if dict, should contain "address", "locality" and "postalcode" fields
            if str, should contain an address

        Raises
        ------
        PeliasException
            If anything went wrong while calling Pelias.

        Returns
        -------
        res : str
            Pelias result.
        """
        if isinstance(query, dict):
            struct=True
            params={
                'address':    query['address'],
                'locality':   query['locality']
            }
            if 'postalcode' in query:
                params["postalcode"] = query['postalcode']

        else:
            struct=False
            params = {'text': query}

        url = self.geocode_struct_api if struct else self.geocode_api


        params = urllib.parse.urlencode(params)


        url = f"{url}?{params}"
        vlog(f"Call to Pelias: {url}")

        return self.call_service(url)


    def interpolate(self, lat, lon, number, street):
        """
        Call Pelias interpolate service

        Parameters
        ----------
        lat: float
            Approximate latitude
        lon: float
            Approximate longiture
        number: str
            House number to interpolate
        street: str
            Street name where the number should be interpolate

        Raises
        ------
        PeliasException
            If anything went wrong while calling Pelias.

        Returns
        -------
        res : str
            Pelias result.
        """

        url = self.interpolate_api

        params = urllib.parse.urlencode({"lat": lat, "lon": lon, "number": number, "street": street})

        url = f"{url}?{params}"
        vlog(f"Call to interpolate: {url}")

        return self.call_service(url)

def check_pelias():
    """
    Check that Pelias server is up&running

    Returns
    -------
    Object
        True: Everything is fine
        False: Server does not answer
        list of dict: answer from Nominatim if it does not contain the expected values
    """

    try:
        pelias_res = pelias.geocode(city_test_from)
        if city_test_from.lower() == pelias_res["geocoding"]["query"]["text"].lower():
            return True # Everything is fine
        return pelias_res  # Server answers, but gives an unexpected result
    except PeliasException as exc:
        vlog("Exception occured: ")
        vlog(exc)
        return False    # Server does not answer

def wait_for_pelias():
    """
    Wait for Pelias to be up & running. Give up after 10 attempts, with a delay
    starting at 2 seconds, being increased by 0.5 second each round.

    Returns
    -------
    None.
    """

    delay=2
    for i in range(10):
        pel = check_pelias()
        if pel is True:
            log("Pelias working properly")
            break
        log("Pelias not up & running")
        log(f"Try again in {delay} seconds")
        if pel is not False:
            log("Answer:")
            log(pel)

            log(f"Pelias host: {pelias_host}")

            #raise e
        time.sleep(delay)
        delay+=0.5
    if i == 9:
        log("Pelias not up & running !")
        log(f"Pelias: {pelias_host}")

## Check result functions



def pelias_check_postcode(pelias_res, postcode):
    """
    List a Pelias feature list by removing all feature having a postcode which
    does not start by the same two digits as 'postcode'. If no postal code is
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
    if not "features" in pelias_res: # Should not occur!
        log("Missing features in pelias_res:")
        log(pelias_res)
        pelias_res["features"] = []

    nb_res = len(pelias_res["features"])
    filtered_feat = list(filter(lambda feat: not "postalcode"  in feat["properties"] or str(feat["properties"]["postalcode"])[0:2] == str(postcode)[0:2], pelias_res["features"]))

    pelias_res["features"] = filtered_feat

    vlog(f"Check postcode : {nb_res} --> {len(filtered_feat)}")
    return pelias_res# return None #res_list[0]


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
    if "addendum" not in feature["properties"] or "best" not in feature["properties"]["addendum"] :
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

    to_remove=["^RUE ", "^AVENUE ", "^CHAUSSEE ", "^ALLEE ", "^BOULEVARD ", "^PLACE ",
               "STRAAT$", "STEENWEG$", "LAAN$"]

    for s in to_remove:
        street_name = re.sub(s, "", street_name)

    return street_name.strip()


def is_partial_substring(s1, s2):
    """
    Check that s1 (assuming s1 is shorter than s2) is a subsequence of s2, i.e.,
    s1 can be obtained by removing some characters to s2.
    Example:"Rue Albert" vs "Rue Marcel Albert", or vs "Rue Albert Marcel"

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

    s1 = re.sub(" ", "", s1)
    s2 = re.sub(" ", "", s2)

    if len(s1)>len(s2):
        s1, s2=s2, s1

    while len(s1)>0 and len(s2)>0:
        if s1[0]==s2[0]:
            s1 = s1[1:]
            s2 = s2[1:]
        else:
            s2 = s2[1:]

    return int(len(s1)==0) #and len(s2)==0

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

    feat_street_names= []

    for feat_street_name in get_street_names(feature):

        feat_street_name = remove_street_types(unidecode(feat_street_name))
        if feat_street_name in feat_street_names:
            continue

        sim = apply_sim_functions(feat_street_name, street_name, threshold)
        if sim:
            return sim

        feat_street_names.append(feat_street_name)

    if len(feat_street_names) ==0: # No street name found --> ok
        return 1

    # Cleansing
    for pat, rep in remove_patterns:
        street_name = re.sub(pat, rep, street_name)
        for i in range(len(feat_street_names)):
            feat_street_names[i] = re.sub(pat, rep, feat_street_names[i])


    for feat_street_name in get_street_names(feature):
        sim = apply_sim_functions(feat_street_name, street_name, threshold)
        if sim:
            return sim

    # Adding city name

    for c in ["postname_fr", "postname_nl", "postname_de", "municipality_name_fr", "municipality_name_nl", "municipality_name_de"]:

        if "addendum" in feature["properties"] and "best" in feature["properties"]["addendum"] and c in feature["properties"]["addendum"]["best"] :
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

    filtered_feat= list(filter(lambda feat: check_streetname(feat, street_name,threshold) is not None, pelias_res["features"]))

    pelias_res["features"] = filtered_feat

    vlog(f"Check street : {nb_res} --> {len(filtered_feat)}")
    return pelias_res# return None #res_list[0]


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
    return (feature["properties"]["match_type"] in ("exact", "interpolated") or feature["properties"]["accuracy"]=="point" ) and "housenumber" in feature["properties"]


def interpolate(feature):
    """
    Try to interpolate the building position (typically become coordinates are missing)

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
    addr = {"address": f"{feature['properties']['street']}",
            "postalcode": feature['properties']['postalcode'],
            "locality": ""}
    street_res = pelias.geocode(addr)
    log(f"Interpolate: street center: {street_res}")


    if len(street_res["features"]) == 0:
        return {}

    street_center_coords = street_res["features"][0]["geometry"]["coordinates"]
    log(f"street_center_coords: {street_center_coords}")

    interp_res=pelias.interpolate(lat=street_center_coords[1],
                                lon=street_center_coords[0],
                                number=feature['properties']['housenumber'],
                                street=feature['properties']['street'])

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
    if pd.isnull(street_name) or len(street_name)==0:
        return ""

    if pd.isnull(house_number) or len(house_number)==0:
        return street_name

    return f"{street_name}, {house_number}"


def build_city(post_code, post_name):
    if pd.isnull(post_code) or len(post_code)==0:
        return post_name or ""
    
    if pd.isnull(post_name) or len(post_name)==0:
        return post_code or ""
    
    return f"{post_code} {post_name}"
    


def struct_or_unstruct(street_name, house_number, post_code, post_name):
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
    vlog(f"struct_or_unstruct('{street_name}', '{house_number}', '{post_code}', '{post_name}')")
    # Try structured
    addr= {"address": build_address(street_name, house_number),
           "locality": post_name}
    if post_code is not None:
        addr["postalcode"] = post_code

    vlog(f"Call struct: {addr}")
    pelias_struct= pelias.geocode(addr)
    pelias_struct["bepelias"] = {"call_type": "struct",
                                 "in_addr": addr, 
                                 "call_cnt":1}

    if post_code is not None:
        pelias_struct = pelias_check_postcode(pelias_struct, post_code)
    else:
        vlog("No postcode in input")


    if len(pelias_struct["features"]) > 0 :

        #log(pelias_struct)
        for feat in pelias_struct["features"]:
            vlog(feat["properties"]["name"] if "name" in feat["properties"] else feat["properties"]["label"] if "label" in feat["properties"] else "--")
            if is_building(feat):

                if feat["geometry"]["coordinates"] == [0,0]:
                    log("Coordinates==0,0, try to interpolate...")
                    interp = interpolate(feat)
                    if "geometry" in interp:
                        feat["geometry"]["coordinates_orig"] = [0,0]
                        feat["geometry"]["coordinates"] = interp["geometry"]["coordinates"]
                        pelias_struct["bepelias"]["interpolated"] = True
                vlog("Found a building in res1")
                vlog(feat)
                vlog("pelias_struct")
                vlog(pelias_struct)
                vlog("-------")

                return pelias_struct

    # Try unstructured
    addr = build_address(street_name, house_number) + ", "  + build_city(post_code, post_name)
    vlog(f"Call unstruct: '{addr}'")
    if addr and len(addr.strip())>0:
        pelias_unstruct= pelias.geocode(addr)
        cnt=2
    else: 
        vlog("Unstructured: empty inputs, skip call")
        cnt=1
        pelias_unstruct = { "features": []}
    pelias_unstruct["bepelias"] = {"call_type": "unstruct",
                                   "in_addr": addr,
                                   "call_cnt":cnt}
    pelias_struct["bepelias"]["call_cnt"]=cnt
    
    if post_code is not None:

        pelias_unstruct = pelias_check_postcode(pelias_unstruct, post_code)
    else:
        vlog("No postcode in input")


    pelias_unstruct = check_best_streetname(pelias_unstruct, street_name)


    if len(pelias_unstruct["features"]) > 0 :


        for feat in pelias_unstruct["features"]:
            vlog(feat["properties"]["name"] if "name" in feat["properties"] else feat["properties"]["label"] if "label" in feat["properties"] else "--")
            if is_building(feat):
                if feat["geometry"]["coordinates"] == [0,0]:
                    vlog("Coordinates==0,0, try to interpolate...")
                    interp = interpolate(feat)
                    if "geometry" in interp:
                        feat["geometry"]["coordinates_orig"] = [0,0]
                        feat["geometry"]["coordinates"] = interp["geometry"]["coordinates"]
                        pelias_unstruct["bepelias"]["interpolated"] = True
                return pelias_unstruct

    # No result has a building precision -> get the best one, according the first feature

    # If confidence of struct is better that confidence of unstruct OR struct contains 'street' --> choose struct
    if len(pelias_struct["features"]) >0:
        if (pelias_unstruct["features"]) and len(pelias_unstruct["features"]) >0 \
            and pelias_struct["features"][0]["properties"]["confidence"] >  pelias_unstruct["features"][0]["properties"]["confidence"] \
            or "street" in pelias_struct["features"][0]["properties"]:

            return pelias_struct

    # Otherwise, if 'street' in unstruct --> choose unstruct
    if len(pelias_unstruct["features"]) >0 and "street" in pelias_unstruct["features"][0]["properties"]:

        return pelias_unstruct

    # Otherwise, if there are struct result --> choose struct
    if len(pelias_struct["features"]) >0:
        return pelias_struct

    # Otherwhise, choose unstruct
    return pelias_unstruct


remove_patterns = [(r"\(.+\)$",      ""),
               ("[, ]*(SN|ZN)$", ""),
               ("' ", "'"),
               (" [a-zA-Z][. ]", " "),
               ("[.]", " "),
               (",[a-zA-Z .]*$", " ")
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

    if transformer=="no_city":
        addr_data["post_name"] = ""

    elif transformer=="no_hn":
        addr_data["house_number"] = ""

    elif transformer=="clean_hn":
        if "house_number" in addr_data and not pd.isnull( addr_data["house_number"]):
            if "-" in addr_data["house_number"]:
                addr_data["house_number"] = addr_data["house_number"].split("-")[0].strip() # useful? "match" bellow will do the same

            hn = re.match("^[0-9]+", addr_data["house_number"])
            if hn:
                addr_data["house_number"] = hn[0]
    elif transformer=="clean":
        for pat, rep in remove_patterns:
            addr_data["street_name"] = re.sub(pat, rep, addr_data["street_name"]) if not pd.isnull(addr_data["street_name"]) else None
            addr_data["post_name"] =   re.sub(pat, rep, addr_data["post_name"])   if not pd.isnull(addr_data["post_name"]) else None

    return addr_data

# WARNING : no logs
# INFO : a few logs
# DEBUG : lots of logs

logger = logging.getLogger()

env_log_level = os.getenv('LOG_LEVEL', "HIGH").upper().strip()
if env_log_level == "LOW":
    logger.setLevel(logging.WARNING)
elif env_log_level == "MEDIUM":
    logger.setLevel(logging.INFO)
elif env_log_level == "HIGH":
    logger.setLevel(logging.DEBUG)
else :
    print(f"Unkown log level '{env_log_level}'. Should be LOW/MEDIUM/HIGH")

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

env_pelias_host = os.getenv('PELIAS_HOST')
if env_pelias_host:
    logging.info("get PELIAS_HOST from env: %s", env_pelias_host)
    pelias_host = env_pelias_host
else :
    pelias_host = "10.0.2.15:4000"
    logging.info("Use default osm host: %s", pelias_host)



with_timing = os.getenv('TIMING', "NO").upper().strip()
if with_timing == "NO":
    with_timing_info = False
elif with_timing == "YES":
    with_timing_info = True
else:
    print(f"Unkown TIMING '{with_timing}'. Should be YES/NO")
    with_timing_info = False
log(f"TIMING: {with_timing_info} ({with_timing})")


pelias = Pelias(domain=pelias_host)

log("test Pelias: ")
log(pelias.geocode("20, Avenue Fonsny, 1060 Bruxelles"))

city_test_from="Bruxelles"




wait_for_pelias()

log("Waiting for requests...")




app = Flask(__name__)
api = Api(app,
          version='1.0.0',
          title='bePelias API',
          description="""A service that allows geocoding (postal address cleansing and conversion into geographical coordinates), based on Pelias and BestAddresses.

          """,
          doc='/doc',
          prefix='/REST/bepelias/v1',
          contact='Vandy BERTEN',
          contact_email='vandy.berten@smals.be',
)

namespace = api.namespace(
    '',
    'Main namespace')

with_https = os.getenv('HTTPS', "NO").upper().strip()

if with_https=="YES":
    # It runs behind a reverse proxy
    @property
    def specs_url(self):
        """
            If it runs behind a reverse proxy
        """
        return url_for(self.endpoint('specs'), _external=True, _scheme='https')

    Api.specs_url = specs_url

single_parser = reqparse.RequestParser()

single_parser.add_argument('mode',
                          type=str,
                          choices=('basic', 'simple', 'advanced'),
                          default='advanced',
                          help="""
How Pelias is used:

- basic: Just call the structured version of Pelias
- simple: Call the structured version of Pelias. If it does not get any result, call the unstructured version
- advanced: try several variants until it gives a result""")

single_parser.add_argument('streetName',
                          type=str,
                          default='Avenue Fonsny',
                          help="The name of a passage or way through from one location to another (cf. Fedvoc). Example: 'Avenue Fonsny'",
                          # example= "Avenue Fonsny"
                          )

single_parser.add_argument('houseNumber',
                          type=str,
                          default='20',
                          help="An official alphanumeric code assigned to building units, mooring places, stands or parcels (cf. Fedvoc). Example: '20'",
                          )

single_parser.add_argument('postCode',
                          type=str,
                          default='1060',
                          help="The post code (a.k.a postal code, zip code etc.) (cf. Fedvoc). Example: '1060'",
                          # example= "Avenue Fonsny"
                          )

single_parser.add_argument('postName',
                          type=str,
                          default='Saint-Gilles',
                          help="Name with which the geographical area that groups the addresses for postal purposes can be indicated, usually the city (cf. Fedvoc). Example: 'Bruxelles'",
                          )


id_parser = reqparse.RequestParser()
id_parser.add_argument('bestid',
                          type=str,
                          default='https%3A%2F%2Fdatabrussels.be%2Fid%2Faddress%2F219307%2F4',
                          help="BeSt Id for an address, a street or a municipality. Value has to be url encoded (i.e., replace '/' by '%2F', ':' by '%3A')",
                          location='query'
                          )



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
]


@namespace.route('/geocode')
class Geocode(Resource):
    """ Single address geocoding"""

    @namespace.expect(single_parser)

    @namespace.response(400, 'Error in arguments')
    @namespace.response(500, 'Internal Server error')
    @namespace.response(204, 'No address found')


    def get(self):
        """
Geocode (postal address cleansing and conversion into geographical coordinates) a single address.

        """

        log("geocode")

        mode = get_arg("mode", "advanced")
        if not mode in ["basic", "simple", "advanced", "pelias_struct", "pelias_struct_noloc", "pelias_unstruct"]:
            namespace.abort(400, f"Invalid mode {mode}")


        street_name = get_arg("streetName", None)
        house_number = get_arg("houseNumber", None)
        post_code= get_arg("postCode", None)
        post_name = get_arg("postName", None)

        log(f"Request: {street_name} / {house_number} / {post_code} / {post_name} ")

        log(f"Mode: {mode}")
        if mode in ("basic", "pelias_struct"):
            pelias_res= pelias.geocode({"address": build_address(street_name, house_number),
                                        "postalcode": post_code,
                                        "locality": post_name})

            return pelias_res

        if mode == "pelias_struct_noloc":
            pelias_res= pelias.geocode({"address": build_address(street_name, house_number),
                                        "postalcode": post_code})

            return pelias_res

        if mode == "pelias_unstruct":
            addr = build_address(street_name, house_number) + ", "  + build_city(post_code, post_name)
            pelias_res= pelias.geocode(addr)

            return pelias_res


        if mode == "simple":
            return struct_or_unstruct(street_name, house_number, post_code, post_name)

        if mode == "advanced":
            vlog("advanced...")
            addr_data = {"street_name": street_name,
                        "house_number": house_number,
                        "post_name": post_name,
                        "post_code": post_code}
            all_res=[]
            
            previous_attempts = []
            call_cnt=0
            for transf in transformer_sequence:
                transf_addr_data = addr_data.copy()
                for t in transf:
                    transf_addr_data = transform(transf_addr_data, t)
                    
                    
                
                
                log(f"transformed address: ({ ';'.join(transf)})")
                #if addr_data == transf_addr_data and len(transf)>0:
                if transf_addr_data in previous_attempts:
                    vlog("Transformed address already tried, skip Pelias call")
                    
                elif len(list(filter(lambda v: v and len(v)>0, transf_addr_data.values())))==0:
                    vlog("No value to send, skip Pelias call")
                else:

                    previous_attempts.append(transf_addr_data)
                    
                    pelias_res =  struct_or_unstruct(transf_addr_data["street_name"],
                                                     transf_addr_data["house_number"],
                                                     transf_addr_data["post_code"],
                                                     transf_addr_data["post_name"])
                    pelias_res["bepelias"]["transformers"] = ";".join(transf)
                    call_cnt+= pelias_res["bepelias"]["call_cnt"]
                    
                    #log(f'call count: {pelias_res["bepelias"]} --> {call_cnt}')
                    
                    

                    if len(pelias_res["features"])>0 and is_building(pelias_res["features"][0]):
                        pelias_res["bepelias"]["call_cnt"]=call_cnt
                        return pelias_res
                    all_res.append(pelias_res)

            log("No building result, keep the best match")

            # Get a score for each result
            fields = ["housenumber", "street", "locality", "postalcode"]
            log(" ".join([f"{f:20}" for f in fields]) + "     score")
            for res in all_res:
                res["score"]=0
                if len(res["features"]) >0:
                    prop = res["features"][0]["properties"]
                    #log(prop)
                    if  "postalcode" in prop and prop["postalcode"] == post_code:
                        res["score"] += 3
                    if  "locality" in prop and prop["locality"].lower() == (post_name or "").lower():
                        res["score"] += 2

                    if  "street" in prop :
                        res["score"] += 1
                        street_sim = check_streetname(res["features"][0], street_name, threshold=0.8)
                        vlog(f"Sim '{res['features'][0]['properties']['name']}' vs '{street_name}': {street_sim}")
                        if street_sim:
                            res["score"] += street_sim

                    if  "housenumber" in prop :
                        res["score"] += 0.5
                        if prop["housenumber"] == house_number:
                            res["score"] += 1
                        else:
                            n1 = re.match("[0-9]+", prop["housenumber"])
                            n2 = re.match("[0-9]+", house_number)
                            if n1 and n2 and n1[0] == n2[0] : #len(n1)>0 and n1==n2:
                                res["score"] += 0.8
                    if res["features"][0]["geometry"]["coordinates"] != [0,0]:
                        res["score"] += 1.5


                    log(" ".join([f"{prop[f] if f in prop else '[NA]':20}" for f in fields]) + str(res["features"][0]["geometry"]["coordinates"])+ f"  -> {res['score']}")


            all_res = sorted(all_res, key= lambda x: -x["score"])
            
            if len(all_res)>0:
                final_res= all_res[0]

                final_res["bepelias"]["call_cnt"]=call_cnt
                return final_res
            return None
                

        return "Wrong mode!" # Should neve occur...

    
@namespace.route('/id/<string:bestid>')
# @namespace.route('/id/<string:bestid>/<string:a1>/<string:a2>/<string:a3>/<string:a4>/')
# @namespace.route('/id/<string:bestid>/<string:a1>/<string:a2>/<string:a3>/<string:a4>/<string:a5>/')
@namespace.param('bestid',
                          type=str,
                          default='https%3A%2F%2Fdatabrussels.be%2Fid%2Faddress%2F219307%2F4',
                          help="BeSt Id for an address, a street or a municipality. Value has to be url encoded (i.e., replace '/' by '%2F', ':' by '%3A')")
class GetById(Resource):
    """ Get ressource by best id. This does not replace a call to Bosa BeSt Address API !!
    Only works for addresses, streets and municipalities (not for postalinfos, part of municipalities)
    """

    # @namespace.expect(id_parser)
    
    @namespace.response(400, 'Error in arguments')
    @namespace.response(500, 'Internal Server error')
    @namespace.response(204, 'No address found')


    # def get(self, bestid, a1=None, a2=None, a3=None, a4=None, a5=None):
    def get(self, bestid):
        """

        """

        if "%2F" in bestid:
            bestid = unquote_plus(bestid)
        # log(f"{(bestid, a1, a2, a3, a4, a5)}")
        # if a1 and "http" in bestid:
        #      bestid += "/"
        # for a in [a1, a2, a3, a4, a5]:
        #     if a :
        #         bestid += "/"+a
            
            
            
        log(f"get by id: {bestid}")
        
        client = Elasticsearch(pelias.elastic_api)
        
        mtch = re.search("([a-z\.]+.be)/id/([a-z]+)/", bestid,  re.IGNORECASE)
        
        if mtch is None or len(mtch.groups()) != 2:
             namespace.abort(400, f"Cannot parse best id '{bestid}'")

        reg_map = {
            "geodata.wallonie.be": "be-wal",
            "databrussels.be" :    "be-bru",
            "data.vlaanderen.be":  "be-vlg"
        }
        if mtch[1] not in reg_map:
            namespace.abort(400, f"Cannot find a valid domain in {bestid} ({mtch[1]})")
        reg = reg_map[mtch[1]]
        
        log(f"mtch[2].lower(): '{mtch[2].lower()}'")
        if mtch[2].lower() in ["address", "adres"]:
            obj_type = "address"
        elif mtch[2].lower() in ["streetname", "straatname"]:
            obj_type = "street"
        elif mtch[2].lower() in ["municipality", "gemeente"]:
            obj_type = "city"
        else :
            namespace.abort(400, f"Object type '{mtch[2]}' not supported so far in '{bestid}'")
                
        
        lg_sequence = ["fr", "nl", "de"] if reg == "be-bru" else ["fr", "de", "nl"] if reg == "be-wal" else ["nl", "fr", "de"]
        for lg in lg_sequence:
            try: 
                resp = client.get(index="pelias", id=f"{reg}:{obj_type}:{bestid}_{lg}")
                
                resp["_source"]["addendum"]["best"] =json.loads(resp["_source"]["addendum"]["best"])
                return resp
            except NotFoundError: 
                pass
                # log("Not found !")
        return "Object not found", 204

