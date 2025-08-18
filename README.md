# Context

See more info in webinar "Geocoding" (in French, 28/09/2023): https://www.smalsresearch.be/webinar-geocoding-follow-up/

The current version of Pelias for BeSt Address (https://github.com/pelias/docker/tree/master/projects/belgium ; authentic source for addresses in Belgium) has two main problems:
- Data (trought openaddresses.io) are not updated anymore since mid-2021, as: 
    - Current format for BeSt Address csv file is not recognized by openaddresses, since Feb 2021 (column "EPSG:4326_lat" and similar contain mixed case)
    - OpenAddresses changed its dataflow from "https://results.openaddresses.io/" to "https://batch.openaddresses.io/", and since mid 2021, 
      "results" is no longer updated. But Pelias still uses this dataflow
- Geocoder is not very robust, but some simple changes allow matching to work.

The current projet aims at:
- Building a new dataflow, generating CSV compatible with Pelias, based on BestAddress CSV files
- Adding a "wrapper" above Pelias, trying different versions of an address as long as the address is not recognized.

# Disclaimer

This project is realized by Vandy Berten (Smals Research, https://www.smalsresearch.be/) for a PoC in collaboration with NGI (https://www.ngi.be), CNNC (https://centredecrise.be/fr) and Bosa (https://opendata.bosa.be/). This has not been (so far) approved by any of those partners. We do not offer any support for this project.


# Build

This project is composed of two parts: 
- Pelias, based on custom files. It is built upon an adaptation of https://github.com/pelias/docker/tree/master/projects/belgium, but based on CSV files we prepare. 
   This component is composed of +/- 6 docker containers (named pelias_xxxx)
- bePelias: REST API improving robustness of Pelias ("wrapper") + file preparator

Steps (short version): 

```
./scripts/build.sh                 # Build pelias & bepelias docker images (~30 minutes)
./scripts/feed.sh                  # Prepare files from Bosa and load them. Should also be run to update data (~1h30)
./scripts/run.sh                   # Run Pelias and bePelias API (with default parameters)
```

## More detailled steps

### Build

- `./scripts/build.sh api` : Build bePelias docker images (bepelias/api and bepelias/dataprep : ~5 min)
- `./scripts/build.sh pelias` : Build pelias docker images (~25 min)
- `./scripts/build.sh cleanup` : Shut down everything, remove all docker images and all data


### Feed

- `./scripts/feed.sh <operation> <region>`, where:
    - `<operation>` in:
        - `prepare_csv`: Load data from Bosa website and prepare them to be Pelias ready. Save them in data/ folder
        - `update`: Move CSV files from "data" folder into appropriate Pelias folder and load them
        - `clean`: delete CSV files (useless after load) 
        - `all` (default): `prepare_csv + update + clean`
    - `<region>` in:
        - `bru`: Brussels
        - `wal`: Wallonia
        - `vlg`: Flanders
        - `all` (default): Belgium
- Examples:
    - `./scripts/feed.sh prepare_csv`: Prepare CSV (for all regions)
    - `./scripts/feed.sh update`: Load files (prepared par `prepare_csv`)
    - `./scripts/feed.sh all bru`: Prepare and update only Brussels data 
    - `./scripts/feed.sh prepare_csv bru`: Prepare only Brussels data 
    - `./scripts/feed.sh update bru`: Update only Brussels data

### Run

- Several parameters can be changed in docker-compose.yml, in "services>api>environment"
   - `PELIAS_HOST=172.27.0.64:4000`: IP+port of Pelias server
   - `LOG_LEVEL=LOW`: level of logs (`HIGH`, `MEDIUM` or `LOW`)
   - `NB_WORKERS=8`: number of (gunicorn) workers
- `./scripts/run.sh <action> <target>`, where:
    - `<action>` in:
        - `up` (default): start all containers (Pelias and bePelias API)
        - `down`: stop all containers
    - `<target>` in:
        - `all` (default): start or stop both Pelias and bePelias API
        - `api`: start or stop only bePelias API
        - `pelias`: start or stop only Pelias server

In order to overide options without updating docker-compose.yml: `docker-compose run --rm -d -e LOG_LEVEL=HIGH api`.


### Two machines build

It is possible to split tasks onto two servers: 
- A "back" server: build images, prepare CSV. This server needs an Internet connection
- A "front" server (serving APIs): run images, load CSV. An Internet connection is not required.


- On both back and front servers: git clone or copy the whole bePelias repo
- On the back server: 
   - `./scripts/build.sh`
   - `./scripts/feed.sh prepare_csv` (or, for a shortest test; `./scripts/feed.sh prepare_csv bru`)
   - Save "pelias/*" and "bepelias/api" docker images:
      - `docker save bepelias/api | gzip > docker-images-bepelias-api.tar.gz`
      - `docker save  $(docker-compose  -f pelias/projects/belgium_bepelias/docker-compose.yml config | awk '{if ($1 == "image:") print $2;}' ORS=" ") | gzip > docker-images-pelias.tar.gz`
   - Archive "data/bestaddress*.csv" files:  `tar czf data.tar.gz data`
   - Archive "pelias" folder: `tar czf pelias.tar.gz pelias`
- Copy files (`docker-images-bepelias-api.tar.gz, docker-images-pelias.tar.gz, data.tar.gz,  bepelias.tar.gz`) into "bepelias" folder on front server
- On the front server: 
   - Load "pelias/*" and "bepelias/api" images: 
       - `gunzip -c docker-images-bepelias-api.tar.gz | docker load`
       - `gunzip -c docker-images-pelias.tar.gz | docker load `
   - Unarchive data & pelias files:
       - `tar xzf data.tar.gz`
       - `tar xzf pelias.tar.gz`
   - `./scripts/run.sh pelias`
   - `./scripts/feed.sh update` (or  `./scripts/feed.sh update bru`)
   - `./scripts/run.sh api` 


To update: 
- On the back server: `./scripts/feed.sh prepare_csv`
- Archive "data/bestaddress*.csv" files: `tar czf data.tar.gz data`
- Copy `data.tar.gz` into "bepelias" folder on front server, and unarchive (`tar xzf data.tar.gz`)
- `./scripts/feed.sh update`

## Data from OpenAddress CSV

The above procedure builds Pelias data from BOSA XML (https://opendata.bosa.be/download/best/best-full-latest.zip), 
using a conversion tools on https://github.com/Fedict/best-tools.git. This is a quite heavy process, but at the end, bePelias will be able to provide BeSt Id (not included into the CSV availble on Bosa website). 

# Usage

- Swagger GUI on http://[IP]:4001/doc 
- Example of URL : http://[IP]:4001/REST/bepelias/v1/geocode?streetName=Avenue%20Fonsny&houseNumber=20&postCode=1060&postName=Saint-Gilles

Port can be changed in docker-compose.yml updating the first value of  "sevices>api>ports"

# Requirements

Disk usage: 
- Pelias containers: around 4 GB
- Pelias data: around 6 GB
- bePelias container: 850 MB
- About 8 GB of CSV files are created for importation. They are removed after build.


bePelias itself requires not much RAM (around 100 Mb). But it requires Pelias to run, which needs at least 8 GB RAM (https://github.com/pelias/docker?tab=readme-ov-file#system-requirements)

This has been tested on an Ubuntu machine with Docker, 24 GB of RAM, 8 cores, using Docker version 20.10.21.

# Wrapper logic


## Checks
A pelias result is a list of "features". Before detailing our logic, let first define two checks:

is_building(feature) is True if (and only if):
- "match_type" (in "properties") is "exact" or "interpolated", or "accuracy" is "point"
- AND "housenumber" exists in "properties"

check_postcode(features, postcode): keep only a feature from features if: 
- "postalcode" does not exists in "properties"
- OR the first three characters of "postalcode" are equal to the first three characters in postcode

## Struct or Unstruct

Let now consider the build bloc "struct_or_unstruct(street_name, house_number, post_code, post_name)". 

This will first call the structured version of Pelias. We receive then a "features list". 
We first apply "check_postcode" to remove any result with a "wrong" postal code. 
Then, if any feature in the remaining features list is "is_building", we return the (filtered) features list.

Otherwise, we call the unstructured version of Pelias with the text "street_name, house_number, post_code post_name". 
We apply the same logic as for the structured version: we filter the features list, and if a feature "is_building", 
we return the (filtered) features list. Furthermore, we observed that the reliability of results for unstructured version is rather low. 
We often get completly wrong result. To reduce them, we check, using various string distance metrics, that the street in input is not too far away from the street in result.

If at this point we did not return anything, it means that neither the structured version nor the unstructured provides a valid building result. 
At this point, we will select the best (filtered) features list amongst the structured or unstructured version.

- If the "confidence" of the structured version is (strictly) higher than the confidence of the unstructured version, we return the (filtered) structured features list.
- Otherwise, if the first result of the structured version contains a "street", we return it.
- Otherwise, if the first result of the unstructured version contains a "street", we return it.
- Otherwise, if the structured version gave at least one result, we return it.
- Otherwise, we return the unstructured version.

NOTE: Should we improve this flow?

## Transformers

We consider a set of "transformers" aiming at modifying the input (structured address) in order to allow a result from Pelias.

- no_city: remove city name (post_name in API, locality in Pelias)
- no_hn: remove house_number 
- clean_hn: only keep the first sequence of digits. "10-12" becomes "10", "10A" becomes "10"
- clean: The following replacement will be performed on street_name and post_name: 


| Regex   | Replacement  | Comment |
|---------|--------------|---------|
|"\(.+\)$"        |  ""  | Remove anything between parenthesis at the end |
|"[, ]*(SN\|ZN)$" | ""   | Delete 'SN' (sans numéro) ou 'ZN' (zonder nummer) at the end |
|"' "             | "'"  | Delete space after simple quote |
|" [a-zA-Z][. ]"  | " "  | Remove single letter words |
|"[.]"            | " "  | Remove dots |



## Main logic

The main idea is to send an address to Pelias (using struct_or_unstruct). If it gives a building level result, we return it. 
Otherwise, we try a sequence of transformers, until a building level result is found.

### Transformer sequence

We apply the following sequence of transformers if the original address does not give a building level result:
- clean
- clean, no_city
- no_city
- clean_hn
- no_city, clean_hn
- clean, no_city, clean_hn
- no_hn
- no_city, no_hn

If no result was found so far, we start again the above sequence, but without checking postcode.

### Best result selection

If no transformer sequence sent to struct_or_unstruct gives a building level result, we will choose the best candidate amongst all those struct_or_unstruct results.
For all results (a list of features), we will compute a score, and return the result with the highest score. 
Here is how this score is computed:
- We start with a 0 score
- We then consider the first feature of all features list
- If postalcode is equal to input postal code, we add 3 to score
- If locality is equal to input post name (case insensitive), we add 2
- If feature contains a street, we add 1. We then add a string similarity score between input and output street, between 0 and 1
- If feature contains a house number, we add 0.5
- If this housenumber is equal to input housenumber, we add 1
- Otherwise, if the first sequence of digit of both feature and input housenumber are equal, we add 0.8
- If feature geometry is not equal to [0,0], we add 2

Review this scoring??

# Notes
## Street center

When a house number is not found in BeSt Address data, we may return a street level result, with a BeST street id, a street name, and several higher level data.

The coordinates appearing in the result is computed the following way. 
For each street in a given municipality, we split records into two according to the parity of the housenumber (considering only the numerical part).

For both parities, we draw a line going through all numbers in ascending order, and take the middle of this line. 
The center of the street is the point between the odd and the even centers. 
If one of them is empty (because a street does not have any odd or even numbers), we just take the other one.

Note that there is no guarantie that this point lays on the street. There are rare cases where center is slightly offside, 
mainly with roads with high sinuosily and one side with addresses only on a small parts. 
But this gives much better results as if we'd consider simply the average of all points on a street (which is off the road for mostly all curvated streets).

## Interpolation

There are two situations where coordinates of an address are computed by interpolation (i.e., if number 10 is not know, we assume it is located between 8 and 12):
- Either the number is not provided in BeSt Address data. In this case, Pelias will use its interpolation engine, based on BeSt Address as well as OpenStreetMap data. It this case:
   - Field 'properties'>'match_type' is 'interpolated'
   - Id provided in the result is a street best id, not an address best id
- Or the number is provided in BeSt Address data, but with coordinates (0,0) (only in Wallonia). It his case, bePelias will call the interpolation engine:
   - Field 'bepelias'>'interpolated' is True
   - Id provided in the result is the Street Best Id
   - In 'geometry', we provide 'coordinates_orig', with the original coordinates, and 'coordinates' with the interpolated coordinates

## Result metadata

Beside results coming straight from Pelias, bePelias adds some metadata field:
- call_type: 'struct' or 'unstruct': did we call structured of unstructured Pelias
- in_addr: what was the address sent to Pelias
- transformers: which sequence of transformers were applied to the input address to give the above "in_addr"
- interpolated: did we compute coordinates by interpolation (only when BeSt Address records has a (0,0) location, see above)
- peliasCallCount: how many calls to Pelias were required to get this result

## Box numbers

When an address contains several boxes, BeSt Address provides a BeSt id for the 'main address' (i.e., the housenumber ignoring the box number) as well as a BeSt id for each box number. As Pelias does not handle box numbers, bePelias does not take box number as input, but will provide all box numbers in the result: 
- the "main" address is described in the "top" result (with and id and coordinates);
- all boxes are described in "addendum" part, in 'box_info' list.

In some rare situations, there are no coordinates at the main address level, but there are some at the box level. In such cases, we use those coordinates in the result (with 'bepelias'>'interpolated' = 'from_boxnumber' in the result)

## Precision

Each record contains a "precision" field, giving information about the first features. We could observe the following values (see also above section "Interpolation"):

- address*: we match the input address to a BeSt Address, with a BeSt id...
    - address: ...  and coordinates
    - address_interpol:... but this one does not contain any coordinate. However, by interpolation based on neighbor numbers, we were able to approximate the location
    - address_streetcenter: ... but were not able to approximate the location. We were however able to provide the geometric center of all the known addresses within this street (see "Street center" section)
    - address_00: ... but were unable to provide any coordinate. Coordinates in result are then (0,0) 
- street*: we match the input address to a BeSt street, with a BeSt street id...
    - street_interpol: ... and, by interpolation, were able to approximate the location
    - street: ... and provided the coordinates of all the known addresses within this street  (see "Street center" section)
    - street_00: ... but were unable to provide any coordinate. Coordinates in result are then (0,0) 
- city: Only city/postcode were matched. Most of the time, we do not get any BeSt result, by only OpenStreetMap or Whosonfirst values 
- country: Only element larger that a city are matched (region, province...)

# Architecture

To be developed...

API architecture
```mermaid
flowchart TB
    subgraph network:belgium_bepelias_default 
        subgraph pelias
            api1[api:4000]
            interp[interpolation:4300]
            es[elasticsearch:9200]
        end
        subgraph bepelias
            api2[api:4001]
            api2-- /v1/search
            /v1/search/structured-->api1
            api2-- /search/geojson-->interp
            api2-- /search -->es
        end
    end
    client --/geocode
     /geocode/unstructured
     /health
     /id/{bestid}
     /reverse
     /searchCity
      --> api2
```

Feed dataflow

1. prepare_csv : dataprep : get files from BOSA, create files in /data/bestaddresses_\*be\*.csv
2. update : 
    - copy files to pelias dir
    - run "pelias import csv"
    - run "prepare_interpolation.sh" into "pelias/interpolation"

```mermaid

flowchart TB
    subgraph pelias
        import[csv-importer]
        interpolation
    shared2[(./pelias/projects/belgium_bepelias)]
    end
    subgraph bepelias
        dataprep
        shared1[(./data/)]
    end
    bosa{{opendata.bosa.be}}
    
    
    bosa-.[1 download].-> dataprep
    dataprep -.[2 ].-> shared1
    shared1 -.[3 copy].-> shared2
    shared2 -.[4 pelias import csv].-> import
    shared2 -.[5 prepare_interpolation].-> interpolation

```

# Logical Data model

Appart from "/health" endpoint, the result of all calls is basically a list of "items", representing geographical objects, which could be either an address, a street of a "city" (with a broad definition) :
- With /geocode and /geocode/unstructured, items could be an address, a street or a city depending of the most precise object the engine has been able to find with input;
- With /reverse, items will always be addresses;
- With /id/{bestid}, the list will contain at most one address (if an address BeSt id is provided), a (list of) street(s) (if a street BeSt id is provided) or a (list of) city (if a municipality BeSt id is provided).

## Context

Belgium is administratively divided into 565 "municipalities" (communes/gemeenten). 261 in WAL, 19 in BRU and 285 in VLG.
Each of those municipalities is divided into one or several postal codes, and each of those postal codes is divided into "localities". 

But this is the theory and each region has implemented this concept in a different fashion in BeSt Address structure:
- In BRU:   
    - 18 communes contain only one postal code and one locality, but municipality of "Brussels" has 4 postal codes, each corresponding to one locality (one of them being also named Brussels). Then, no postal code contains more than one locality, and locality names will be stored in the "PostalInfo" objects,
    - The boundaries of the four postal codes of "Bruxelles-ville" (1000, 1130, 1020, 1120) is wider than the "municipality of Brussels". A small part of most surrounding postal codes (1040, 1050, 1030, 1170) surrounding 1000 also belong to "municipality of Brussels". So it is not possible, for several Brussels postal codes, to know the corresponding municipality : it depends of the exact address;
- In VLG, there are no administrative concept bellow postal codes. In a postal info, the name will contain the concatenation of all "locality names";
- In WAL, localities are denoted "Part of municipality" (postal info do not have names, which are contained by "part of municipality").


Note that in the following, a "city" is the combination of a municipality, a postal code, and, if applicable (in WAL), a part of municipality. It this then the lowest level of administrative entity.


## Flat Data model

### Address precision

In the case of an address, a item will gather some attributes specific to an address (a BeSt id, a housenumber, a status, as well a two coordinates), but also references to other level objects:
- The street this address belongs to (BeSt id + names);
- The municipality ("commune/gemeente") (BeSt id, names and 'NIS' code);
- The postal infos (postal code, and, in BRU and VLG, a postal name);
- In WAL, a "part of municipality" (BeSt id + names);
- If an address contains several boxes, a list of boxes (with BeSt Id, coordinates, box number and status).

```mermaid 
classDiagram
class Item {
    bestId: String #123;id#125;
    coordinates.lat/lon: Float,Float
    housenumber: String
    status: Enum#40;currrent, retired, proposed#41;
    precison: String #40;address**#41;
}
class BoxInfo {
    addressId: String #123;id#125;
    coordinates.lat/lon: Float,Float
    boxNumber: String
    status: Enum#40;currrent, retired, proposed#41;
}
class Street{
    id: String #123;id#125;
    name.fr/nl/de: String
}
class Municipality {
    id: String #123;id#125;
    code: Integer #123;id#125;
    name.fr/nl/de: String
}
class PostalInfo {
    postalCode: Integer #123;id#125;
    name.fr/nl/de: String
}
class PartOfMunicipality{
    id: String #123;id#125;
    name.fr/nl/de: String
}
Item "1" -- "0..*" BoxInfo
Item "0..*" -- "1" Street
Item "1..*" -- "1" Municipality
Item "1..*" -- "0..1" PartOfMunicipality
Item "1..*" -- "1" PostalInfo
```

Notes about cardinalities: 
- An address could have 0, 1 or several boxes. But a box BeSt Id is always linked to a single (main) address;
- A street could be "empty", without any address;
- An address is linked to (exactely) one "Part of Municipality" in Wallonia, but not in other regions.


For the sake of readability, we drop datatypes in the following charts.

### Street precision


In the case of a street item, we'll find the same elements as for address, behalve housenumber, status, and box info

```mermaid 
classDiagram
class Item {
    coordinates.lat/lon
    precison: street**
}
class Street{
    id
    name.fr/nl/de
}
class Municipality {
    id
    code
    name.fr/nl/de
}
class PostalInfo {
    postalCode
    name.fr/nl/de
}
class PartOfMunicipality{
    id
    name.fr/nl/de
}
Item "1..*" -- "1" Street
Item "1..*" -- "1" Municipality
Item "1..*" -- "0..1" PartOfMunicipality
Item "1..*" -- "1" PostalInfo
```

### City precision

In case of a city precision item, we'll have the same scheme as for street items, behalve "Street" objects.

If a result has been found in BeSt Address, we will have a municipality, a postal info, and, in WAL, a part of municipality.

```mermaid 
classDiagram
class Item {
    coordinates.lat/lon
    precison: city
}
class Municipality {
    id
    code
    name.fr/nl/de
}
class PostalInfo {
    postalCode
    name.fr/nl/de
}
class PartOfMunicipality{
    id
    name.fr/nl/de
}

Item "1..*" -- "1" Municipality
Item "1..*" -- "0..1" PartOfMunicipality
Item "1..*" -- "1" PostalInfo
```

But if no result was found in BeSt Address, we still could return result from WhosonFirst, and have in this case only "name" value in Item.

```mermaid 
classDiagram
class Item {
    coordinates.lat/lon
    precison: city
    name
}
```


## Street to municipality model 

Beside the fact that items will "point" to one street, municipality, postal info, and, optionaly, one part of municipality, it is important to understand the relationships between those items. For instance, and address will never reference a Flemish municipality and a Walloon postal code.

The first diagram is generic, because it combines the characteristics of all regions. It is present for completeness, but it's easier to consider each region appart.

Two general facts: 
- There are no empty municipality, postal code or part of municipality, meaning that each of them contains at least one street and one address;
- There are no cross-municipality streets. If a street is too long, it will have a different BeSt id in the two municipalities, while keeping the same name. However, a street can cross several postal codes within the same municipality, keeping its id.



```mermaid 
classDiagram
class Street{
    id
    name.fr/nl/de
}
class Municipality {
    id
    code
    name.fr/nl/de
}
class PostalInfo {
    postalCode
    name.fr/nl/de
}
class PartOfMunicipality{
    id
    name.fr/nl/de
}

Street "1..*" --  "1" Municipality
Street "1..*" --  "1..*" PostalInfo
Street "1..*" --  "1..*" PartOfMunicipality

PostalInfo "1..*" --  "1..*" Municipality
PartOfMunicipality "1..*" --  "1" Municipality
PartOfMunicipality "1..*" --  "1" PostalInfo
```

### BRU

In BRU, municipality boundaries are slighly different from postal code boundaries. For instance, most addresses of "1040" are in "Etterbeek", but a few of them are in "Brussels". A postal code is then not always a subset of a municipality. This implies a "1..\*" -- "1..\*" relationships between PostalInfo and Municipality.


```mermaid 
classDiagram
class Street{
    id
    name.fr/nl/de
}
class Municipality {
    id
    code
    name.fr/nl/de
}
class PostalInfo {
    postalCode
    name.fr/nl/de
}

Street "1..*" --  "1" Municipality
Street "1..*" --  "1..*" PostalInfo

PostalInfo "1..*" --  "1..*" Municipality
```

### WAL

In Wallonia, a postal code is always a subset of a municipality, and a part of municipality a subset of a postal code. 
A street can cross several part of municipalities or postal codes, but not municipality.


```mermaid 
classDiagram
class Street{
    id
    name.fr/nl/de
}
class Municipality {
    id
    code
    name.fr/nl/de
}
class PostalInfo {
    postalCode
}
class PartOfMunicipality{
    id
    name.fr/nl/de
}


Street "1..*" --  "1..*" PartOfMunicipality
PartOfMunicipality "1..*" --  "1" PostalInfo
PostalInfo "1..*" --  "1" Municipality

Street "1..*" --  "1" Municipality
```

### VLG


In Flanders, a postal code is always a subset of a municipality. 
A street can cross several postal codes, but not municipality.

```mermaid 
classDiagram
class Street{
    id
    name.fr/nl/de
}
class Municipality {
    id
    code
    name.fr/nl/de
}
class PostalInfo {
    postalCode
    name.fr/nl/de
}

Street "1..*" --  "1..*" PostalInfo
PostalInfo "1..*" --  "1" Municipality

Street "1..*" --  "1" Municipality
```

# Todo

- autocomplete in place of unstructured
- clean variable names in prepare_best_file
- model: split "name" in name street, name municipality... ?
- delete "retired"? low priority ?
- get by id for postalname, part of mun
- postalcode --> code (in postalInfo ?)
- /geocode --> /addresses? /searchCity --> /cities?  /addresses/structured + /addresses/unstructured + /addresses/reverse
- id: "bestid" for addresses, "addressId" for boxes, "id" for other objects. All to "bestid" ? Or "addressId", "streetId", "boxId"...? 


- implementing "size" parameter for search call
- unstructured:
    - interp if coords = 0,0
    - improve "transformers" in output to be more explicit 
- upgrate to pandas 2
- https://bepelias.smalsrech.be/REST/bepelias/v1/geocode?streetName=Route+du+Condroz&houseNumber=235&postCode=4550&postName=Nandrin&mode=advanced&withPeliasResult=True : 2x le premier résultat, une fois avec interpolation, l'autre non. Suppression des doublons avant interpolation ?
- gérer les cas "Marche, Place aux Foires", "2", 
