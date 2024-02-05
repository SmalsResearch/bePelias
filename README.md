# Context

See more info in webinar "Geocoding" (in French, 28/09/2023): https://www.smalsresearch.be/webinar-geocoding-follow-up/

The current version of Pelias for BestAddress (https://github.com/pelias/docker/tree/master/projects/belgium) has two main problems:
- Data (trought openaddresses.io) are not updated anymore since mid-2021, as: 
    - Current format for BestAddress csv file is not recognized by openaddresses, since Feb 2021 (column "EPSG:4326_lat" and similar contain mixed case)
    - OpenAddresses changed its dataflow from "https://results.openaddresses.io/" to "https://batch.openaddresses.io/", and since mid 2021, "results" is no longer updated. But Pelias still uses this dataflow
- Geocoder is not very robust, but some simple changes allow matching to work.

The current projet aims at:
- Building a new dataflow, generating CSV compatible with Pelias, based on BestAddress CSV files
- Adding a "wrapper" above Pelias, trying different versions of an address as long as the address is not recognized.



# Disclaimer

This project is realized by Vandy Berten (Smals Research) for a POC in collaboration with NGI (www.ngi.be) and CNNC (https://centredecrise.be/fr). This has no (so far) been approved by NGI. We do not offer any support for this project.


# Build

This project is composed of two parts: 
- Pelias, based on custom files. It is built upon an adaptation of https://github.com/pelias/docker/tree/master/projects/belgium, but based on CSV files we prepare. This component is composed of +/- 6 docker containers (named pelias_xxxx)
- bePelias: REST API improving robustness of Pelias ("wrapper") + file preparator

Note: in order to avoid to make 3 components, we put in the same docker image the REST API (used once the service is online) and the file preparator (used to build/update the service). It might be cleaner to split them appart.

Steps: 

```
./build.sh build_api                    # Build bePelias container
./build.sh prepare_csv                  # Prepare files for Pelias, within bePelias container
./build.sh build_pelias                 # Build & run Pelias
./build.sh run_api  xx.xx.xx.xx:4000    # Start bePelias API, giving the Pelias IP/port 
```

To update data: 
`./build.sh update`

# Usage

- Swagger GUI on http://[IP]:4001/doc 
- Example of URL : http://[IP]:4001/REST/bepelias/v1/geocode?streetName=Avenue%20de%20Cortenbergh&houseNumber=115&postCode=1000&postName=Bruxelles

Port can be changed in: 
- Dockerfile > EXPOSE 4001
- run.sh > PORT=4001

# Requirements

Disk usage: 
- Pelias containers: around 4 GB
- bePelias container: 850 MB
- About 8 GB of CSV files are created for importation. They could be removed after build.


This has been tested on an Ubuntu machine with Docker, 24 GB of RAM, 8 cores, using Docker version 20.10.21.


# Wrapper logic


## Checks
A pelias result is a list of "features". Before detailing our logic, let first define two checks:

is_building(feature) is True if (and only if):
- "match_type" (in "properties") is "exact" or "interpolated", or "accuracy" is  "point"
- AND "housenumber" exists in "properties"

check_postcode(features, postcode): keep only a feature from features if: 
- "postalcode" does not exists in "properties"
- OR the first two characters of "postalcode" are equal to the first two characters in postcode

## Struct or Unstruct

Let now consider the build bloc "struct_or_unstruct(street_name, house_number, post_code, post_name)". 

This will first call the structured version of Pelias. We receive then a "features list". We first apply "check_postcode" to remove any result with a "wrong" postal code. Then, if any feature in the remaining features list is "is_building", we return the (filtered) features list.

Otherwise, we call the unstructured version of Pelias with the text "street_name, house_number, post_code post_name". We apply the same logic as for the structured version: we filter the features list, and if a feature "is_building", we return the (filtered) features list. Furthermore, we observed that the reliability of results for unstructured version is rather low. We often get completly wrong result. To reduce them, we check, using various string distance metrics, that the street in input is not too far away from the street in result.

If at this point we did not return anything, it means that neither the structured version nor the unstructured provides a valid building result. At this point, we will select the best (filtered) features list amongst the structured or unstructured version.

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

The main idea is to send an address to Pelias (using struct_or_unstruct). If it gives a building level result, we return it. Otherwise, we try a sequence of transformers, until a building level result is found.

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

# Todo

- Rue sans nom/empty streets. Dispo sur https://opendata.bosa.be/download/best/postalstreets-empty-latest.zip, mais aucune coordonnée
- Street id pour les rues
- Full best id pour les adresses -> nécessite de builder les data à partir des XML? objectId présent, mais pas versionId
- When Pelias loads data: 
    - `debug: [wof-admin-lookup] no country lon=0, lat=0` --> missing coords in Best data
    - `debug: [wof-admin-lookup] no country lon=6.406553, lat=50.332375` --> addresses close to boundary. Are they included?
- Comment inclure les box ? Une adresse par box ? Lister toutes les box pour une même "adresse"
- Si coordonnées = 0,0 -> remplacer la autre chose ? Au niveau du "wrapper" ?
- Quid si aucun code postal n'est donné ? 
- Housenumber de type "30_10" (pose problème à l'interpolation) --> uniquement VLG (+/- 17.700)
- Utiliser post_name au lieu de municipality_name
- Utiliser fichiers "localities" à la place de WOF pour les "city"
- Version non structurée. Utiliser libpostal?
- autocomplete vs search?