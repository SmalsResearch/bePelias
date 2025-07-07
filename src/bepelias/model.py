"""
FastAPI models for bepelias responses
"""

from typing import Annotated, Union

from pydantic import BaseModel, Field

from typing_extensions import Literal

BESTID_PATTERN = r'^(https://)?([a-z.]+)/id/([a-zA-Z]+)/(\d{1,8})/(\d{1,3}|[0-9\-T\:\+]+)$'


class BePeliasError(BaseModel):
    """ Response in cas an error occurred"""
    error: str


class HealthDetails(BaseModel):
    """ Details for Health """
    errorMessage: str
    details: str


class Health(BaseModel):
    """
    - {'status': 'DOWN'}: Pelias server does not answer (or gives an unexpected answer)
    - {'status': 'DEGRADED'}: Interpolation engine is down. Geocoding is still possible but might be not optimal
    - {'status': 'UP'}: Service works correctly
    """
    status: Literal["UP", "DOWN", "DEGRADED"]
    details: Union[HealthDetails, None] = None


class Name(BaseModel):
    """ Name in fr/nl/de """
    fr: Annotated[Union[str, None],
                  Field(description="Entity (street, municipality...) name in French.",
                        example="Avenue Fonsny")] = None
    nl: Annotated[Union[str, None],
                  Field(description="Entity (street, municipality...) name in Dutch.",
                        example='Fonsnylaan')] = None
    de: Annotated[Union[str, None],
                  Field(description="Entity (street, municipality...) name in German.",
                        example='Fonsnystraße')] = None


class Street(BaseModel):
    """ Street infos """
    name: Annotated[Union[Name, None],
                    Field(description="Street name in fr/nl/de (when applicable",
                          example={'fr': 'Avenue Fonsny', 'nl': 'Fonsnylaan'})] = None
    id: Annotated[Union[str, None],
                  Field(description="Street BeSt id",
                        example='https://databrussels.be/id/streetname/4921/2',
                        pattern=BESTID_PATTERN)] = None


class Municipality(BaseModel):
    """ Municipality infos """
    name: Annotated[Name,
                    Field(description="Municipality name in fr/nl/de (when applicable)",
                          example={"fr": "Saint-Gilles", "nl": "Sint-Gillis"})]
    code: Annotated[str,
                    Field(description="Municipality code or code NIS",
                          example="21013",
                          pattern=r'^\d{5}$'
                          )]
    id: Annotated[str,
                  Field(description="Municipality BeSt id",
                        example="https://databrussels.be/id/municipality/21013/14",
                        pattern=BESTID_PATTERN
                        )]


class PartOfMunicipality(BaseModel):
    """ Part of Municipality infos (in Wallonia) """
    name: Annotated[Name,
                    Field(description="Part of Municipality name in fr/nl/de (when applicable)",
                          example={"fr": "Limelette"})]
    id: Annotated[str,
                  Field(description="PartOfMunicipality BeSt id",
                        example="geodata.wallonie.be/id/PartOfMunicipality/1415/1",
                        pattern=BESTID_PATTERN
                        )]


class PostalInfo(BaseModel):
    """ Postal infos """
    name: Annotated[Union[Name, None],
                    Field(description="Postal name in fr/nl/de (when applicable ; only in Brussels and Flanders)",
                          example={"fr": "Saint-Gilles", "nl": "Sint-Gillis"})] = None
    postalCode: Annotated[str,
                          Field(description="Postal code (a.k.a post code, zip code etc.) of a location in Belgium",
                                example="1060",
                                pattern=r'^\d{4}$'
                                )]


class Coordinates(BaseModel):
    """ Geographic coordinates (in EPSG:4326)"""
    lat:  Annotated[float,
                    Field(description="Latitude, in EPSG:4326. Angular distance from some specified circle or plane of reference",
                          example=50.8358677)]
    lon: Annotated[float,
                   Field(description="Longitude, in EPSG:4326. Angular distance measured on a great circle of reference from the intersection " +
                                     "of the adopted zero meridian with this reference circle to the similar intersection of the meridian passing through the object",
                         example=4.33850)]


class BoxInfo(BaseModel):
    """ Box infos """
    coordinates: Coordinates
    boxNumber: Annotated[str,
                         Field(description="Box number",
                               example="b001, A, ..."
                               )]
    addressId: Annotated[str,
                         Field(description="Address BeSt id",
                               example="https://databrussels.be/id/address/219307/7",
                               pattern=BESTID_PATTERN
                               )]
    status:  Annotated[Literal["current", "retired", "proposed"],
                       Field(description="BeSt Address status",
                             example="current"
                             )]


class Item(BaseModel):
    """ item """
    bestId: Annotated[Union[str, None],
                      Field(description="Address BeSt id (could be street or municipality?)",
                            example="https://databrussels.be/id/address/219307/7",
                            pattern=BESTID_PATTERN
                            )] = None
    street: Union[Street, None] = None
    municipality: Union[Municipality, None] = None
    partOfMunicipality: Union[PartOfMunicipality, None] = None
    postalInfo: Union[PostalInfo, None] = None
    housenumber: Annotated[Union[str, None],
                           Field(description="House number",
                                 example="20, 20A, 20-22, ..."
                                 )] = None
    coordinates: Coordinates

    status: Annotated[Union[Literal["current", "retired", "proposed"], None],
                      Field(description="BeSt Address status",
                            example="current"
                            )] = None
    precision: Annotated[Union[str, None],
                         Field(description="Level of precision. See README.md#precision",
                               example="address"
                               )] = None

    boxInfo: Union[list[BoxInfo], None] = None

    name: Annotated[Union[str,  None],
                    Field(description="If we can't find any result from BeSt Address but get some approximate results from other sources",
                          example="Bruxelles"
                          )] = None


class GeocodeOutput(BaseModel):
    """ geocode output model"""
    self: Annotated[str, Field(description="Absolute URI (http or https) to the the resource's own location.",
                               example="http://<hostname>/REST/bepelias/v1/geocode?mode=advanced&streetName=Avenue%20Fonsny&houseNumber=20&postCode=1060&postName=Saint-Gilles")]
    items: list[Item]
    total:  Annotated[int,
                      Field(description="Number of results",
                            example=10)]
    peliasRaw: dict = None
    callType: Union[Literal["struct", "unstruct"], None] = None
    inAddr: Union[dict, str, None] = None
    peliasCallCount: int
    transformers: Union[str, None] = None


class ReverseGeocodeOutput(BaseModel):
    """ reverse geocode output model"""
    self: Annotated[str, Field(description="Absolute URI (http or https) to the the resource's own location.",
                               example="http://<hostname>/REST/bepelias/v1/reverse?lat=yy&lon=xx&radius=0.1&size=5")]
    items: list[Item]
    total:  Annotated[int,
                      Field(description="Number of results",
                            example=10)]
    peliasRaw: dict = None


class SearchCityOutput(BaseModel):
    """ reverse geocode output model"""
    self: Annotated[str, Field(description="Absolute URI (http or https) to the the resource's own location.",
                               example="http://<hostname>REST/bepelias/v1/searchCity?postCode=1060&postName=Saint-Gilles")]
    items: list[Item]
    total:  Annotated[int,
                      Field(description="Number of results",
                            example=10)]


class GetByIdOutput(BaseModel):
    """ get by id output model"""
    self: Annotated[str, Field(description="Absolute URI (http or https) to the the resource's own location.",
                               example="http://<hostname>/REST/bepelias/v1/id/https:%2F%2Fdatabrussels.be%2Fid%2Faddress%2F219307%2F7")]
    items: list[Item]
    total:  Annotated[int,
                      Field(description="Number of results",
                            example=10)]
