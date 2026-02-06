""" Functions to check Pelias results

"""


import re

from unidecode import unidecode
import pandas as pd
import textdistance


from bepelias.utils import log, vlog


class ResultChecker:
    """
    Class allowing to check Pelias results
    """
    def __init__(
            self,
            postcode_match_length,
            remove_patterns,
            similarity_threshold
    ):
        self.postcode_match_length = postcode_match_length
        self.remove_patterns = remove_patterns
        self.similarity_threshold = similarity_threshold

    # --------------------
    # Public methods
    # --------------------

    def filter_postcode(self, pelias_res, postcode):
        """
        Filter a Pelias feature list by removing all feature having a postcode which
        does not start by the same 'match_length' digits as 'postcode'. If no postal code is
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

        if "features" not in pelias_res:  # Should not occur!
            log("Missing features in pelias_res:")
            log(pelias_res)
            pelias_res["features"] = []

        nb_res = len(pelias_res["features"])
        filtered_feat = list(filter(lambda feat: "postalcode" not in feat["properties"] or
                                                 str(feat["properties"]["postalcode"])[0:self.postcode_match_length] == str(postcode)[0:self.postcode_match_length],
                                    pelias_res["features"]))

        pelias_res["features"] = filtered_feat

        vlog(f"    Check postcode ({self.postcode_match_length}) : {nb_res} --> {len(filtered_feat)}")
        return pelias_res

    def filter_streetname(self, pelias_res, street_name):
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

        filtered_feat = list(filter(lambda feat: self.check_streetname(feat, street_name) is not None,
                                    pelias_res["features"]))

        pelias_res["features"] = filtered_feat

        vlog(f"    Check street : {nb_res} --> {len(filtered_feat)}")
        return pelias_res

    def check_locality(self, feature, locality_name):
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
            sim = self._apply_sim_functions(unidecode(locality_name).lower(), prop["locality"].lower())
            if sim and sim >= self.similarity_threshold:
                # vlog(f"locality ('{locality_name}' vs '{prop['locality']}'): {sim}")
                return sim

        if "addendum" in prop and "best" in prop["addendum"]:
            for c in ["postname", "municipality_name", "part_of_municipality_name"]:
                for lang in ["fr", "nl", "de"]:
                    if f"{c}_{lang}" in prop["addendum"]["best"]:

                        cty = unidecode(prop["addendum"]["best"][f"{c}_{lang}"].lower())
                        sim = self._apply_sim_functions(unidecode(locality_name).lower(), cty)
                        # vlog(f"{c}_{lang} ('{locality_name}' vs '{cty}'): {sim}")
                        if sim and sim >= self.similarity_threshold:
                            return sim

        return None

    def get_feature_street_names(self, feature):
        """
        From a Pelias feature, extract all possible street name (skipping duplicates)

        Parameters
        ----------
        feature : dict
            Pelias feature.

        Yields
        ------
        str
            street name.
        """
        previous_res = set()

        if "street" in feature["properties"]:
            res = feature["properties"]["street"].upper()
            if res not in previous_res:
                previous_res.add(res)
                yield res

        if "addendum" not in feature["properties"] or "best" not in feature["properties"]["addendum"]:
            return

        best = feature["properties"]["addendum"]["best"]
        for n in ["streetname_fr", "streetname_nl", "streetname_de"]:
            if n in best:
                res = best[n].upper()
                if res not in previous_res:
                    previous_res.add(res)
                    yield res

    def check_streetname(self, feature, street_name):
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
            for in_street_name in self._get_streetname_variants(street_name):
                for feat_street_name in self.get_feature_street_names(feature):
                    feat_street_name = self._remove_street_types(unidecode(feat_street_name))

                    if with_city:
                        feat_street_names = [feat_street_name]
                    else:  # prepend street name with city names
                        feat_street_names = [f"{cty}, {feat_street_name}" for cty in self._get_feature_city_names(feature)]

                    for feat_street_name in feat_street_names:
                        sim = self._apply_sim_functions(feat_street_name, in_street_name)
                        vlog(f"'{in_street_name}' vs '{feat_street_name}': {sim}")
                        if sim:
                            return sim

        if sim == -1:  # No street name found --> ok
            return 1

        return None  # No match found
    # --------------------
    # Protected methods
    # --------------------

    def _get_streetname_variants(self, street_name):
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
            street_name_part = self._remove_street_types(street_name_part)

            # Yield original part (without street types)
            if street_name_part not in previous_res:
                previous_res.add(street_name_part)
                yield street_name_part

            # Clean and Yield cleansed version
            for pat, rep in self.remove_patterns:
                street_name_part = re.sub(pat, rep, street_name_part) if not pd.isnull(street_name_part) else None

            if street_name_part not in previous_res:
                previous_res.add(street_name_part)
                yield street_name_part

    def _get_feature_city_names(self, feature):
        """
        From a Pelias feature, extract all possible city name (skipping duplicates)

        Parameters
        ----------
        feature : dict
            Pelias feature.

        Yields
        ------
        str
            city name.
        """

        previous_res = set()
        for c in ["postname_fr", "postname_nl", "postname_de",
                  "municipality_name_fr", "municipality_name_nl", "municipality_name_de"]:

            if "addendum" in feature["properties"] and "best" in feature["properties"]["addendum"] and c in feature["properties"]["addendum"]["best"]:
                cty = unidecode(feature["properties"]["addendum"]["best"][c].upper())

                if cty not in previous_res:
                    previous_res.add(cty)
                    yield cty

    def _remove_street_types(self, street_name):
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

        to_remove = ["^RUE ", "^AVENUE ", "^CHAUSSEE ", "^ALLEE ", "^BOULEVARD ", "^PLACE ", "^CHEMIN ",
                     "STRAAT$", "STEENWEG$", "LAAN$"]

        for s in to_remove:
            street_name = re.sub(s, "", street_name)

        to_remove = ["^DE LA ", "^DE ", "^DU ", "^DES "]

        for s in to_remove:
            street_name = re.sub(s, "", street_name)

        return street_name.strip()

    def _is_partial_substring(self, s1, s2):
        """
        Check that s1 (assuming s1 is shorter than s2) is a subsequence of s2, i.e.,
        s1 can be obtained by removing some characters to s2.
        Example:"Rue Albert" vs "Rue Marcel Albert", or vs "Rue Albert Marcel". "Rue M. Albert" vs "Rue Marcel Albert"

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

        s1 = re.sub("[. ]", "", s1)
        s2 = re.sub("[. ]", "", s2)

        if len(s1) > len(s2):
            s1, s2 = s2, s1

        while len(s1) > 0 and len(s2) > 0:
            if s1[0] == s2[0]:
                s1 = s1[1:]
                s2 = s2[1:]
            else:
                s2 = s2[1:]

        return int(len(s1) == 0)  # and len(s2)==0

    def _apply_sim_functions(self, str1, str2, threshold=None):
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

        if not threshold:
            threshold = self.similarity_threshold

        sim_functions = [textdistance.jaro_winkler,
                         textdistance.sorensen_dice,
                         lambda s1, s2: 1 - textdistance.levenshtein(s1, s2)/max(len(s1), len(s2)),
                         self._is_partial_substring
                         ]
        for sim_fct in sim_functions:
            sim = sim_fct(str1, str2)
            if sim >= threshold:
                return sim
        return None
