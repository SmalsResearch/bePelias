"""All input parser and output models

"""

from flask_restx import reqparse, fields, Namespace

###################
#  Input parsers  #
###################

# For /geocode

geocode_parser = reqparse.RequestParser()

geocode_parser.add_argument('mode',
                            type=str,
                            choices=('basic', 'simple', 'advanced'),
                            default='advanced',
                            help="""
How Pelias is used:

- basic: Just call the structured version of Pelias
- simple: Call the structured version of Pelias. If it does not get any result, call the unstructured version
- advanced: Try several variants until it gives a result""")

geocode_parser.add_argument('streetName',
                            type=str,
                            default='Avenue Fonsny',
                            help="The name of a passage or way through from one location to another (cf. Fedvoc). Example: 'Avenue Fonsny'",
                            )

geocode_parser.add_argument('houseNumber',
                            type=str,
                            default='20',
                            help="An official alphanumeric code assigned to building units, mooring places, stands or parcels (cf. Fedvoc). Example: '20'",
                            )

geocode_parser.add_argument('postCode',
                            type=str,
                            default='1060',
                            help="The post code (a.k.a postal code, zip code etc.) (cf. Fedvoc). Example: '1060'",
                            )

geocode_parser.add_argument('postName',
                            type=str,
                            default='Saint-Gilles',
                            help="Name with which the geographical area that groups the addresses for postal purposes can be indicated, usually the city (cf. Fedvoc). Example: 'Bruxelles'",
                            )


geocode_parser.add_argument('withPeliasResult',
                            type=bool,
                            default=False,
                            help="If True, return Pelias result as such in 'peliasRaw'.",
                            )

# For /geocode/unstructured

unstructured_parser = reqparse.RequestParser()

unstructured_parser.add_argument('mode',
                                 type=str,
                                 choices=('basic', 'advanced'),
                                 default='advanced',
                                 help="""
How Pelias is used:

- basic: Just call the unstructured version of Pelias
- advanced: Try several variants until it gives a result""")

unstructured_parser.add_argument('address',
                                 type=str,
                                 default='Avenue Fonsny 20, 1060 Saint-Gilles',
                                 help="The whole address in a single string",
                                 )


unstructured_parser.add_argument('withPeliasResult',
                                 type=bool,
                                 default=False,
                                 help="If True, return Pelias result as such in 'peliasRaw'.",
                                 )

# For /searchCity

city_search_parser = reqparse.RequestParser()
city_search_parser.add_argument('postCode',
                                type=str,
                                default='1060',
                                help="The post code (a.k.a postal code, zip code etc.) (cf. Fedvoc). Example: '1060'",
                                )

city_search_parser.add_argument('cityName',
                                type=str,
                                default='Saint-Gilles',
                                help="Any name usually used to denote an administrative area. Could be a municipality, a locality (or part of municipality). Example: 'Bruxelles', 'Laeken'...",
                                )

# For /id/<bestid>

id_parser = reqparse.RequestParser()
id_parser.add_argument('bestid',
                       type=str,
                       default='https%3A%2F%2Fdatabrussels.be%2Fid%2Faddress%2F219307%2F7',
                       help="BeSt Id for an address, a street or a municipality. Value has to be url encoded (i.e., replace '/' by '%2F', ':' by '%3A')",
                       location='query'
                       )

# For /reverse

reverse_parser = reqparse.RequestParser()
reverse_parser.add_argument('lat',
                            type=float,
                            help="Latitude, in EPSG:4326. Angular distance from some specified circle or plane of reference",
                            default=50.83582,
                            )
reverse_parser.add_argument('lon',
                            type=float,
                            help="Longitude, in EPSG:4326. Angular distance measured on a great circle of reference from the intersection " +
                                 "of the adopted zero meridian with this reference circle to the similar intersection of the meridian passing through the object",
                            default=4.33844
                            )
reverse_parser.add_argument('radius',
                            type=float,
                            help="Distance (in kilometers)",
                            default=1,
                            )
reverse_parser.add_argument('size',
                            type=int,
                            help="Maximal number of results (default: 10; maximum: 20)",
                            default=10
                            )

reverse_parser.add_argument('withPeliasResult',
                            type=bool,
                            default=False,
                            help="If True, return Pelias result as such in 'peliasRaw'.",
                            )


##################
#  Ouput models  #
##################

namespace = Namespace('', 'Main namespace')

name_model = namespace.model("ItemNameModel", {
    "fr":  fields.String(example="Avenue Fonsny",
                         description="Entity (street, municipality...) name in French",
                         skip_none=True),
    "nl":  fields.String(example="Fonsnylaan",
                         description="Entity (street, municipality...) name in Nederlands",
                         skip_none=True),
    "de":  fields.String(example="Fonsnystraße",
                         description="Entity (street, municipality...) name in German",
                         skip_none=True)})

street_model = namespace.model("StreetNameModel", {
    "name": fields.Nested(name_model,
                          description="Street name in fr/nl/de (when applicable)",
                          skip_none=True),
    "id": fields.String(example="https://databrussels.be/id/streetname/4921/2",
                        description="Street BeSt id",
                        skip_none=True)
    }, skip_none=True)

municipality_model = namespace.model("MunicipalityModel", {
    "name": fields.Nested(name_model,
                          description="Municipality name in fr/nl/de (when applicable)",
                          skip_none=True),
    "code": fields.String(example="21013",
                          description="Municipality code or code NIS",
                          pattern=r'^\d{5}$'),
    "id": fields.String(example="https://databrussels.be/id/municipality/21013/14",
                        description="Municipality BeSt id", skip_none=True)
    }, skip_none=True)

part_of_municipality_model = namespace.model("PartOfMunicipalityModel", {
    "name": fields.Nested(name_model,
                          description="Part of Municipality name in fr/nl/de (when applicable)",
                          skip_none=True),
    "id": fields.String(example="geodata.wallonie.be/id/PartOfMunicipality/1415/1",
                        description="Part of Municipality BeSt id (only in Wallonia)", skip_none=True)
    }, skip_none=True)

postalinfo_model = namespace.model("PostalInfoModel", {
    "name": fields.Nested(name_model,
                          description="PostalInfo in fr/nl/de (when applicable ; only in Brussels and Flanders)",
                          skip_none=True),
    "postalCode": fields.String(example="1060",
                                description="Postal code (a.k.a post code, zip code etc.) of a location in Belgium",
                                pattern=r'^\d{4}$',
                                skip_none=True)
    }, skip_none=True)

coordinates_model = namespace.model("CoordinatesModel", {
    "lat": fields.Float(description="Latitude, in EPSG:4326. Angular distance from some specified circle or plane of reference",
                        example=50.8358677,
                        skip_none=True
                        ),
    "lon": fields.Float(description="Longitude, in EPSG:4326. Angular distance measured on a great circle of reference from the intersection " +
                                    "of the adopted zero meridian with this reference circle to the similar intersection of the meridian passing through the object",
                        example=4.3385087,
                        skip_none=True)
    }, skip_none=True)

boxinfo_model = namespace.model("BoxinfoModel", {
    "coordinates": fields.Nested(coordinates_model, description="Geographic coordinates (in EPSG:4326)", skip_none=True),
    "boxNumber": fields.String(example="b001, A, ...",
                               description="Box number"),
    "addressId": fields.String(example="https://databrussels.be/id/address/219307/7",
                               description="Address BeSt id"),
    "status": fields.String(example="current/retired/proposed",
                            description="BeSt Address status"),
    }, skip_none=True)

item_model = namespace.model("ItemModel", {
    "bestId": fields.String(example="https://databrussels.be/id/address/219307/7",
                            description="Address BeSt id (could be street or municipality?)"),
    "street": fields.Nested(street_model, description="Street info", skip_none=True),
    "municipality": fields.Nested(municipality_model, description="Municipality info", skip_none=True),
    "partOfMunicipality": fields.Nested(part_of_municipality_model, description="Part of Municipality info (only in Wallonia)", skip_none=True),
    "postalInfo": fields.Nested(postalinfo_model, description="Postal info", skip_none=True),
    "housenumber": fields.String(example="20, 20A, 20-22, ...",
                                 description="House number",
                                 skip_none=True),
    "status": fields.String(example="current/retired/proposed",
                            description="BeSt Address status",
                            skip_none=True),
    "precision": fields.String(example="address",
                               description="Level of precision. See README.md#precision"),
    "coordinates": fields.Nested(coordinates_model, description="Geographic coordinates (in EPSG:4326)", skip_none=True),
    "boxInfo":  fields.List(fields.Nested(boxinfo_model), skip_none=True),
    "name": fields.String(example="Bruxelles",
                          description="If we can't find any result from BeSt Address but get some approximate results from other sources",
                          skip_none=True),
    }, skip_none=True)


city_item_model = namespace.model("CityItemModel", {
    "municipality": fields.Nested(municipality_model, description="Municipality info"),
    "partOfMunicipality": fields.Nested(part_of_municipality_model, description="Part of Municipality info", skip_none=True),
    "postalInfo": fields.Nested(postalinfo_model, description="Postal info", skip_none=True),
    "coordinates": fields.Nested(coordinates_model, description="Geographic coordinates (in EPSG:4326)"),
    "error": fields.String(description="Error message", skip_none=True),
    }, skip_none=True)


geocode_output_model = namespace.model("GeocodeOutput", {
    "self":   fields.String(description="Absolute URI (http or https) to the the resource's own location.",
                            example="http://<hostname>/REST/bepelias/v1/geocode?mode=advanced&streetName=Avenue%20Fonsny&houseNumber=20&postCode=1060&postName=Saint-Gilles",
                            ),
    "items":  fields.List(fields.Nested(item_model, skip_none=True), skip_none=True),
    "total":  fields.Integer(description="Total number of items",
                             example=1),
    "peliasRaw": fields.Raw(default=None,
                            description="Result provided by underlying Pelias. Only with 'witPeliasResult:true",
                            skip_none=True),
    "callType": fields.String(example="struct or unstruct", skip_none=True),
    "inAddr": fields.Raw(example={
                            "address": "Avenue Fonsny, 20",
                            "locality": "",
                            "postalcode": "1060"
                         },
                         description="Address sent to Pelias. Could be a dict (if callType='struct') or a string (if callType='unstruct')"),
    "peliasCallCount": fields.Integer(example=1,
                                      description="How many calls to Pelias were required to get this result"),
    "transformers": fields.String(example="clean;no_city",
                                  description="Which transformation methods were used before sending the address to Pelias"),
    "error": fields.String(description="Error message",
                           skip_none=True),
    }, skip_none=True)

reverse_output_model = namespace.model("ReverseOutput", {
    "self":   fields.String(description="Absolute URI (http or https) to the the resource's own location.",
                            example="http://<hostname>/REST/bepelias/v1/reverse?lat=yy&lon=xx&radius=0.1&size=5",
                            ),
    "items":  fields.List(fields.Nested(item_model, skip_none=True), skip_none=True),
    "total":  fields.Integer(description="Total number of items",
                             example=1),
    "peliasRaw": fields.Raw(default=None,
                            description="Result provided by underlying Pelias. Only with 'witPeliasResult:true",
                            skip_none=True),
    "error": fields.String(description="Error message",
                           skip_none=True),
    }, skip_none=True)


search_city_output_model = namespace.model("SearchCityOutput", {
    "self":   fields.String(description="Absolute URI (http or https) to the the resource's own location.",
                            example="http://<hostname>REST/bepelias/v1/searchCity?postCode=1060&postName=Saint-Gilles"),
    "items":  fields.List(fields.Nested(city_item_model, skip_none=True), skip_none=True),
    "total":  fields.Integer(description="Total number of items",
                             example=1),
    }, skip_none=True)

get_by_id_output_model = namespace.model("GetByIdOutput", {
    "self":   fields.String(description="Absolute URI (http or https) to the the resource's own location.",
                            example="http://<hostname>/REST/bepelias/v1/id/https:%2F%2Fdatabrussels.be%2Fid%2Faddress%2F219307%2F7"),
    "items":  fields.List(fields.Nested(item_model, skip_none=True), skip_none=True),
    "total":  fields.Integer(description="Total number of items",
                             example=1),

    }, skip_none=True)

health_output_model = namespace.model("HealthOutput", {
    "status":   fields.String(description="Service status",
                              example="UP, DOWN, or DEGRADED"),
    "details":  fields.String(description="More details about status",
                              example="", skip_none=True),
    }, skip_none=True)
