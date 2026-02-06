"""
Base code for bePelias
"""

import re

import pandas as pd
from fastapi import status

from bepelias.pelias import PeliasException
from bepelias.pelias_elastic import PeliasElastic

from bepelias.utils import (log, vlog, to_rest_guidelines, feature_to_df, final_res_to_df,
                            build_address, build_city, is_building, add_precision, transform)

from bepelias.result_checker import ResultChecker

from bepelias.pelias import Pelias

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


class BePelias:
    """
    Class implementing main bePelias logic
    """
    def __init__(
            self,
            domain_api,
            domain_elastic,
            domain_interpol,
            postcode_match_length,
            similarity_threshold
    ):
        self.pelias = Pelias(domain_api, domain_elastic, domain_interpol)
        self.postcode_match_length = postcode_match_length

        self.pelias_elastic = PeliasElastic(self.pelias.elastic_api)

        self.res_checker = ResultChecker(remove_patterns=remove_patterns, postcode_match_length=postcode_match_length, similarity_threshold=similarity_threshold)

    def _search_for_coordinates(self, feat):
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
            interp = self._interpolate(feat)
            vlog(f"    Interpolate result: {interp}")
            if "geometry" in interp:
                feat["geometry"]["coordinates_orig"] = [0, 0]
                feat["geometry"]["coordinates"] = interp["geometry"]["coordinates"]
                feat["bepelias"] = {"interpolated": True}
            elif "street_geometry" in interp:
                feat["geometry"]["coordinates_orig"] = [0, 0]
                feat["geometry"]["coordinates"] = interp["street_geometry"]["coordinates"]
                feat["bepelias"] = {"interpolated": "street_center"}

    def _interpolate(self, feature):
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
        street_res = self.pelias.geocode(addr)
        # vlog(f"Interpolate: street center: {street_res}")

        # Keep only results maching input postalcode

        street_res["features"] = list(filter(lambda f: f["properties"]["postalcode"] == feature['properties']['postalcode'] if "postalcode" in f["properties"] else False,
                                             street_res["features"]))

        if len(street_res["features"]) == 0:
            return {}

        street_center_coords = street_res["features"][0]["geometry"]["coordinates"]
        vlog(f"    street_center_coords: {street_center_coords}")

        interp_res = self.pelias.interpolate(lat=street_center_coords[1],
                                             lon=street_center_coords[0],
                                             number=feature['properties']['housenumber'],
                                             street=feature['properties']['street'])

        if len(interp_res) == 0:
            return {"street_geometry": {"coordinates": street_center_coords}}

        vlog(f"interp_res:{interp_res}")
        return {"geometry": {"coordinates": interp_res["geometry"]["coordinates"]}}

    def _struct_or_unstruct(self, street_name, house_number, post_code, post_name, check_postcode=True):
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
        pelias_struct = self.pelias.geocode(addr, layers=layers)

        pelias_struct["bepelias"] = {"call_type": "struct",
                                     "in_addr": addr,
                                     "pelias_call_count": 1}

        if post_code is not None:
            if check_postcode:
                pelias_struct = self.res_checker.check_postcode(pelias_struct, post_code)
        else:
            vlog("    No postcode in input")

        if len(pelias_struct["features"]) > 0:
            vlog("    Structured results:")
            vlog(feature_to_df(pelias_struct["features"]))

            for feat in pelias_struct["features"]:
                # vlog(feat["properties"]["name"] if "name" in feat["properties"] else feat["properties"]["label"] if "label" in feat["properties"] else "--")
                if is_building(feat):
                    if feat["geometry"]["coordinates"] == [0, 0]:
                        self._search_for_coordinates(feat)

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
            pelias_unstruct = self.pelias.geocode(addr, layers=layers)
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
                pelias_unstruct = self.res_checker.check_postcode(pelias_unstruct, post_code)
        else:
            vlog("    No postcode in input")

        pelias_unstruct = self.res_checker.filter_streetname(pelias_unstruct, street_name)

        if len(pelias_unstruct["features"]) > 0:
            vlog("    Unstructured results:")
            vlog(feature_to_df(pelias_unstruct["features"]))

            for feat in pelias_unstruct["features"]:
                # vlog(feat["properties"]["name"] if "name" in feat["properties"] else feat["properties"]["label"] if "label" in feat["properties"] else "--")
                if is_building(feat):
                    if feat["geometry"]["coordinates"] == [0, 0]:
                        self._search_for_coordinates(feat)
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

    def _advanced_mode(self, street_name, house_number, post_code, post_name, transformer_sequence=None):
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
                    transf_addr_data = transform(transf_addr_data, t, remove_patterns)

                vlog("")
                vlog(f"Transformed address ({';'.join(transf)}): {transf_addr_data}")
                if transf_addr_data in previous_attempts:
                    vlog("    Already tried, skip Pelias call")
                elif len(list(filter(lambda v: v and len(v) > 0, transf_addr_data.values()))) == 0:
                    vlog("    No value to send, skip Pelias call")
                else:
                    previous_attempts.append(transf_addr_data)

                    pelias_res = self._struct_or_unstruct(transf_addr_data["street_name"],
                                                          transf_addr_data["house_number"],
                                                          transf_addr_data["post_code"],
                                                          transf_addr_data["post_name"],
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
                            if any(sn == street_name.upper() for sn in self.res_checker.get_feature_street_names(feat0)):
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

                locality_sim = self.res_checker.check_locality(res["features"][0], post_name)
                if locality_sim:
                    score["locality"] = 1.0+locality_sim

                if "street" in prop:
                    score["street"] = 1.0
                    street_sim = self.res_checker.check_streetname(res["features"][0], street_name)
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

    def _call_unstruct(self, address):
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

        pelias_unstruct = self.pelias.geocode(address, layers=layers)

        pelias_unstruct["bepelias"] = {"call_type": "unstruct",
                                       "in_addr": address,
                                       "pelias_call_count": 1}

        parsed = pelias_unstruct["geocoding"]["query"]["parsed_text"]

        vlog(f"    Parsed by Pelias: {parsed}")

        if "postalcode" in parsed:
            pelias_unstruct = self.res_checker.check_postcode(pelias_unstruct, parsed["postalcode"])

        else:
            vlog("    No postcode in input")

        if "street" in parsed:
            pelias_unstruct = self.res_checker.filter_streetname(pelias_unstruct, parsed["street"])

        add_precision(pelias_unstruct)

        return pelias_unstruct

    def _unstructured_mode(self, address):
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
                pelias_res = self._call_unstruct(address)
                call_cnt += 1
                pelias_res["bepelias"]["pelias_call_count"] = call_cnt
                pelias_res["bepelias"]["transformers"] = transf
                vlog("    Results:")
                vlog(feature_to_df(pelias_res["features"]))
                if len(pelias_res["features"]) > 0 and is_building(pelias_res["features"][0]):
                    if pelias_res["features"][0]["geometry"]["coordinates"] == [0, 0]:
                        self._search_for_coordinates(pelias_res["features"][0])
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
            postalcode_candidates = sorted(self.pelias_elastic.get_postcode_list(parsed["city"]))
        else:
            postalcode_candidates = []

        vlog(f"Postcode candidates: {postalcode_candidates}")
        if len(postalcode_candidates) > 4:
            # Optimisation: With some city names (Antwerp, Charleroi, ...) there are too many postcodes --> we search for street (without housenumer/postcode),
            # and start search for postcodes being in the street search
            pelias_street = self.pelias.geocode({"address": parsed.get("street", "")})
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

            pelias_res = self._advanced_mode(street_name=parsed.get("street", ""),
                                             house_number=parsed.get("housenumber", ""),
                                             post_code=cp,
                                             post_name=parsed.get("city", ""),
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

    def geocode(self, street_name, house_number, post_code, post_name, mode, with_pelias_result):
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
                pelias_res = self.pelias.geocode({"address": build_address(street_name, house_number),
                                                  "postalcode": post_code,
                                                  "locality": post_name})
                add_precision(pelias_res)

                return to_rest_guidelines(pelias_res, with_pelias_result)

            elif mode == "simple":
                pelias_res = self._struct_or_unstruct(street_name, house_number, post_code, post_name)
                add_precision(pelias_res)

                return to_rest_guidelines(pelias_res, with_pelias_result)

            else:  # --> mode == "advanced":
                # log("advanced...")

                pelias_res = self._advanced_mode(street_name, house_number, post_code, post_name)

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

    def geocode_unstructured(self, address, mode, with_pelias_result):
        """ see _geocode_unstructured
        """

        try:
            if mode in ("basic"):
                pelias_res = self.pelias.geocode(address)
                add_precision(pelias_res)

            else:  # --> mode == "advanced":
                pelias_res = self._unstructured_mode(address)

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

    def geocode_reverse(self, lat, lon, radius, size, with_pelias_result):
        """
        see _geocode_reverse
        """

        # vlog("reverse")

        # log(f"Reverse geocode: ({lat}, {lon}) / radius: {radius} / size:{size} ")

        try:
            # Note: max size for Pelias = 40. But as most records are duplicated in Pelias (one record in each languages for bilingual regions,
            # we first take twice too many results)
            pelias_res = self.pelias.reverse(lat=lat,
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

    def health(self):
        """Health status
        """
        # Checking Pelias

        pelias_res = self.pelias.check()

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
            interp_res = self.pelias.interpolate(lat=50.83582,
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

    def search_city(self, post_code, city_name):
        """
        See fastapi._search_city
        """

        return self.pelias_elastic.search_city(post_code, city_name)

    def get_by_id(self, bestid):
        """
        See fastapi._get_by_id
        """

        return self.pelias_elastic.get_by_id(bestid)
