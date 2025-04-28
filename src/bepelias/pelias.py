"""All functionalities to call Pelias API

Raises:
    PeliasException: raised when some unexcepted event occurs when calling Pelias

"""

import urllib
import time
import json

from bepelias.utils import (log, vlog)


# Pelias functions/classes
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
            domain_api,
            domain_elastic,
            domain_interpol,
            scheme="http",
    ):

        self.geocode_path = '/v1/search'
        self.geocode_struct_path = '/v1/search/structured'
        self.reverse_path = '/v1/reverse'
        self.interpolate_path = '/search/geojson'

        self.verbose = False
        self.scheme = scheme
        self.domain_api = domain_api.strip('/')
        self.domain_elastic = domain_elastic.strip('/')
        self.domain_interpol = domain_interpol.strip('/')

        self.geocode_api = (
            f'{self.scheme}://{self.domain_api}{self.geocode_path}'
        )

        self.reverse_api = (
            f'{self.scheme}://{self.domain_api}{self.reverse_path}'
        )

        self.geocode_struct_api = (
            f'{self.scheme}://{self.domain_api}{self.geocode_struct_path}'
        )

        self.interpolate_api = (
            f'{self.scheme}://{self.domain_interpol}{self.interpolate_path}'
        )

        self.elastic_api = (
            f'{self.scheme}://{self.domain_elastic}'
        )

    def call_service(self, url, nb_attempts=6):
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
        delay = 1
        while nb_attempts > 0:
            try:
                with urllib.request.urlopen(url) as response:
                    res = response.read()
                    res = json.loads(res)
                    return res
            except urllib.error.HTTPError as exc:
                if exc.code == 400 and self.interpolate_api in url:  # bad request, typically bad house number format
                    log(f"Error 400 ({url}): {exc}")
                    return {}

                if nb_attempts == 1:
                    log(f"Cannot get Pelias results after several attempts({url}): {exc}")
                    raise PeliasException(f"Cannot get Pelias results after several attempts ({url}): {exc}") from exc
                nb_attempts -= 1
                log(f"Cannot get Pelias results ({url}): {exc}. Try again in {delay} seconds...")
                time.sleep(delay)
                delay += 0.5
            except ConnectionRefusedError as exc:
                raise PeliasException(f"Cannot connect to Pelias, service probably down ({url}): {exc}") from exc
            except urllib.error.URLError as exc:
                raise PeliasException(f"Cannot connect to Pelias, service probably down ({url}): {exc}") from exc
            except Exception as exc:
                log(f"Cannot get Pelias results ({url}): {exc}")
                raise exc

    def geocode(self, query, layers=None):
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
            struct = True
            params = {
                'address':    query['address'],
                'locality':   query['locality']
            }
            if 'postalcode' in query:
                params["postalcode"] = query['postalcode']

        else:
            struct = False
            params = {'text': query}

        if layers:
            params["layers"] = layers

        url = self.geocode_struct_api if struct else self.geocode_api

        params = urllib.parse.urlencode(params)

        url = f"{url}?{params}"
        vlog(f"Call to Pelias: {url}")

        return self.call_service(url)

    def reverse(self, lat, lon, radius, size):
        """
        Call Pelias reverse geocoder

        Parameters
        ----------
            - lat: latitude (float)
            - lon: longitude (float)
            - radius: distance in kilometers from (lat, lon) to search for results
            - size: maximal number of results

        Raises
        ------
        PeliasException
            If anything went wrong while calling Pelias.

        Returns
        -------
        res : str
            Pelias result.
        """

        params = {
            "point.lat": lat,
            "point.lon": lon,
            "boundary.circle.radius": radius,
            "size": size,
            "layers": "address"
        }

        url = self.reverse_api

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

    def check(self, city_test_from="Bruxelles"):
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
            pelias_res = self.geocode(city_test_from)
            if city_test_from.lower() == pelias_res["geocoding"]["query"]["text"].lower():
                return True  # Everything is fine
            return pelias_res  # Server answers, but gives an unexpected result
        except PeliasException as exc:
            vlog("Exception occured: ")
            vlog(exc)
            return False    # Server does not answer

    def wait(self, city_test_from="Bruxelles"):
        """
        Wait for Pelias to be up & running. Give up after 10 attempts, with a delay
        starting at 2 seconds, being increased by 0.5 second each round.

        Returns
        -------
        None.
        """

        delay = 2
        for i in range(10):
            pel = self.check(city_test_from)
            if pel is True:
                log("Pelias working properly")
                break
            vlog("Pelias not up & running")
            vlog(f"Try again in {delay} seconds")
            if pel is not False:
                vlog("Answer:")
                vlog(pel)

                vlog(f"Pelias host: {self.geocode_api }")

                # raise e
            time.sleep(delay)
            delay += 0.5
        if i == 9:
            vlog("Pelias not up & running !")
            vlog(f"Pelias: {self.geocode_api }")
