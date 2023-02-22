"""Test intake-axds."""
from unittest import mock

import cf_pandas
import intake
import pytest

from test_utils import FakeResponseParams

from intake_axds.axds_cat import AXDSCatalog


# call for sea_water_temperature
class FakeResponse(object):
    def __init__(self):
        pass

    def json(self):
        res = {
            "results": [
                {
                    "uuid": "test_platform_parquet",
                    "label": "test_label",
                    "description": "Test description.",
                    "type": "platform2",
                    "start_date_time": "2019-03-15T02:58:51.000Z",
                    "end_date_time": "2019-04-08T07:54:56.000Z",
                    "source": {
                        "meta": {
                            "attributes": {
                                "institution": "example institution",
                                "geospatial_bounds": "POLYGON ((-156.25421 20.29439, -160.6308 21.64507, -161.15813 21.90021, -163.60744 23.30368, -163.83879 23.67031, -163.92656 23.83893, -162.37264 55.991, -148.04915 22.40486, -156.25421 20.29439))",
                            },
                            "variables": {
                                "lon": {
                                    "attributes": {
                                        "standard_name": "longitude",
                                        "units": "degrees",
                                        "units_id": "2",  # this is made up
                                        "long_name": "Longitude",
                                        "parameter_id": "2",  # made up
                                    },
                                },
                                "temp": {
                                    "attributes": {
                                        "standard_name": "sea_water_temperature",
                                        "units": "degreesC",
                                        "units_id": "5",  # this is made up
                                        "long_name": "Sea Water Temperature",
                                        "parameter_id": "5",  # made up
                                    },
                                },
                                "salt": {
                                    "attributes": {
                                        "standard_name": "sea_water_practical_salinity",
                                        "units": "1",
                                        "units_id": "6",  # this is made up
                                        "long_name": "Sea Water Practical Salinity",
                                        "parameter_id": "6",  # made up
                                    },
                                },
                            },
                        },
                        # "variables": {"lon": "lon", "time": "time"},
                        "files": {
                            "data.csv.gz": {"url": "fake.csv.gz"},
                            "data.viz.parquet": {"url": "fake.parquet"},
                        },
                    },
                    "data": {
                        "location": {"coordinates": [-123.711083, 38.914556, 0.0]},
                        "id": 106793,
                        "figures": [
                            {
                                "label": "label",
                                "parameterGroupId": "parameterGroupId",
                                "plots": [
                                    {
                                        "subPlots": [
                                            {
                                                "datasetVariableId": "datasetVariableId",
                                                "parameterId": "parameterId",
                                                "label": "label",
                                                "deviceId": "deviceId",
                                            }
                                        ]
                                    }
                                ],
                            }
                        ],
                        "datumConversions": [],
                        "version": 2,
                    },
                },
                {
                    "uuid": "test_platform_csv",
                    "label": "test_label",
                    "description": "Test description.",
                    "type": "platform2",
                    "start_date_time": "2019-03-15T02:58:51.000Z",
                    "end_date_time": "2019-04-08T07:54:56.000Z",
                    "source": {
                        "meta": {
                            "attributes": {
                                "institution": "example institution",
                                "geospatial_bounds": "POLYGON ((-156.25421 -20.29439, -160.6308 -21.64507, -161.15813 -21.90021, -163.60744 -23.30368, -163.83879 -23.67031, -163.92656 -23.83893, -162.37264 -55.991, -148.04915 -22.40486, -156.25421 -20.29439))",
                            },
                            "variables": {
                                "lon": {
                                    "attributes": {
                                        "standard_name": "longitude",
                                        "units": "degrees",
                                        "units_id": "2",  # this is made up
                                        "long_name": "Longitude",
                                        "parameter_id": "2",  # made up
                                    },
                                },
                                "temp": {
                                    "attributes": {
                                        "standard_name": "sea_water_temperature",
                                        "units": "degreesC",
                                        "units_id": "5",  # this is made up
                                        "long_name": "Sea Water Temperature",
                                        "parameter_id": "5",  # made up
                                    },
                                },
                            },
                            # "variables": {"lon": "lon", "time": "time"},
                        },
                        "files": {
                            "data.csv.gz": {"url": "fake.csv.gz"},
                        },
                    },
                    "data": {
                        "location": {"coordinates": [-123.711083, 38.914556, 0.0]},
                        "id": 106793,
                        "figures": [
                            {
                                "label": "label",
                                "parameterGroupId": "parameterGroupId",
                                "plots": [
                                    {
                                        "subPlots": [
                                            {
                                                "datasetVariableId": "datasetVariableId",
                                                "parameterId": "parameterId",
                                                "label": "label",
                                                "deviceId": "deviceId",
                                            }
                                        ]
                                    }
                                ],
                            }
                        ],
                        "datumConversions": [],
                        "version": 2,
                    },
                },
            ]
        }
        return res


# call for sea_water_practical_salinity
class FakeResponseSalt(object):
    def __init__(self):
        pass

    def json(self):
        res = {
            "results": [
                {
                    "uuid": "test_platform_parquet",
                    "label": "test_label",
                    "description": "Test description.",
                    "type": "platform2",
                    "start_date_time": "2019-03-15T02:58:51.000Z",
                    "end_date_time": "2019-04-08T07:54:56.000Z",
                    "source": {
                        "meta": {
                            "attributes": {
                                "institution": "example institution",
                                "geospatial_bounds": "POLYGON ((-156.25421 20.29439, -160.6308 21.64507, -161.15813 21.90021, -163.60744 23.30368, -163.83879 23.67031, -163.92656 23.83893, -162.37264 55.991, -148.04915 22.40486, -156.25421 20.29439))",
                            },
                            "variables": {
                                "lon": {
                                    "attributes": {
                                        "standard_name": "longitude",
                                        "units": "degrees",
                                        "units_id": "2",  # this is made up
                                        "long_name": "Longitude",
                                        "parameter_id": "2",  # made up
                                    },
                                },
                                "temp": {
                                    "attributes": {
                                        "standard_name": "sea_water_temperature",
                                        "units": "degreesC",
                                        "units_id": "5",  # this is made up
                                        "long_name": "Sea Water Temperature",
                                        "parameter_id": "5",  # made up
                                    },
                                },
                                "salt": {
                                    "attributes": {
                                        "standard_name": "sea_water_practical_salinity",
                                        "units": "1",
                                        "units_id": "6",  # this is made up
                                        "long_name": "Sea Water Practical Salinity",
                                        "parameter_id": "6",  # made up
                                    },
                                },
                            },
                        },
                        # "variables": {"lon": "lon", "time": "time"},
                        "files": {
                            "data.csv.gz": {"url": "fake.csv.gz"},
                            "data.viz.parquet": {"url": "fake.parquet"},
                        },
                    },
                },
            ]
        }
        return res


# class FakeResponseMeta(object):
#     def __init__(self):
#         pass

#     def json(self):
#         res = [
#             {
#                 "data": {
#                     "resources": {
#                         "files": {
#                             "data.csv.gz": {"url": "fake.csv.gz"},
#                             "deployment.nc": {"url": "fake.nc"},
#                         },
#                     }
#                 }
#             }
#         ]

#         return res


def test_intake_opener():
    assert "open_axds_cat" in intake.openers


@mock.patch("requests.get")
def test_platform_dataframe(mock_requests):
    """Test basic catalog API: platform as dataframe."""

    mock_requests.side_effect = [FakeResponse()]
    cat = AXDSCatalog(datatype="platform2")
    assert list(cat) == ["test_platform_parquet", "test_platform_csv"]
    assert cat["test_platform_parquet"].describe()["args"]["urlpath"] == "fake.parquet"
    assert cat["test_platform_csv"].describe()["args"]["urlpath"] == "fake.csv.gz"


@mock.patch("requests.get")
def test_platform_search(mock_requests):
    """Test catalog with space/time search."""

    mock_requests.side_effect = [FakeResponse()]

    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }

    cat = AXDSCatalog(datatype="platform2", kwargs_search=kw)
    assert list(cat) == ["test_platform_parquet", "test_platform_csv"]
    assert cat["test_platform_parquet"].describe()["args"]["urlpath"] == "fake.parquet"


@mock.patch("requests.get")
def test_platform_search_for(mock_requests):
    """Test catalog with space/time search and search_for."""

    mock_requests.side_effect = [FakeResponse(), FakeResponse()]

    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
        "search_for": "whale",
    }

    cat = AXDSCatalog(datatype="platform2", kwargs_search=kw)
    assert "&query=whale" in cat.get_search_urls()[0]

    # use direct keywords to get the same catalog
    cat2 = AXDSCatalog(
        datatype="platform2",
        start_time=kw["min_time"],
        end_time=kw["max_time"],
        bbox=(kw["min_lon"], kw["min_lat"], kw["max_lon"], kw["max_lat"]),
        search_for=kw["search_for"],
    )
    assert cat.metadata["kwargs_search"] == cat2.metadata["kwargs_search"]


@mock.patch("requests.get")
def test_platform_search_variable_vocab(mock_requests):
    """Test catalog with variable search."""

    # need two fake responses, one for each search_url
    mock_requests.side_effect = [
        FakeResponseParams(),
        FakeResponse(),
        FakeResponse(),
    ]

    criteria = {
        "wind": {
            "standard_name": "wind_gust_to_direction$",
        },
        "humid": {"standard_name": "relative_humidity"},
    }
    cf_pandas.set_options(custom_criteria=criteria)

    cat = AXDSCatalog(datatype="platform2", keys_to_match=["wind", "humid"])
    assert list(cat) == ["test_platform_parquet", "test_platform_csv"]
    assert cat["test_platform_parquet"].describe()["args"]["urlpath"] == "fake.parquet"
    assert sorted(cat.pglabels) == ["Humidity: Relative Humidity", "Winds: Gusts"]
    assert any(
        ["&tag=Parameter+Group:Winds: Gusts" in url for url in cat.get_search_urls()]
    )
    assert any(
        [
            "&tag=Parameter+Group:Humidity: Relative Humidity" in url
            for url in cat.get_search_urls()
        ]
    )


@mock.patch("requests.get")
def test_platform_search_variable_std_name(mock_requests):
    """Test catalog with variable search."""

    # need two fake responses, one for each search_url
    mock_requests.side_effect = [
        FakeResponseParams(),
        FakeResponse(),
        FakeResponse(),
    ]

    cat = AXDSCatalog(
        datatype="platform2",
        standard_names=["relative_humidity", "wind_gust_to_direction"],
    )
    assert list(cat) == ["test_platform_parquet", "test_platform_csv"]
    assert cat["test_platform_parquet"].describe()["args"]["urlpath"] == "fake.parquet"
    assert sorted(cat.pglabels) == ["Humidity: Relative Humidity", "Winds: Gusts"]
    assert any(
        ["&tag=Parameter+Group:Winds: Gusts" in url for url in cat.get_search_urls()]
    )
    assert any(
        [
            "&tag=Parameter+Group:Humidity: Relative Humidity" in url
            for url in cat.get_search_urls()
        ]
    )


@mock.patch("requests.get")
def test_platform_search_variable_std_name_query_type_union(mock_requests):
    """Test catalog with variable search and query_types."""

    # need two fake responses, one for each search_url
    mock_requests.side_effect = [
        FakeResponseParams(),
        FakeResponse(),
        FakeResponseSalt(),
    ]

    # return both
    cat = AXDSCatalog(
        datatype="platform2",
        standard_names=["sea_water_temperature", "sea_water_practical_salinity"],
        query_type="union",
    )
    assert list(cat) == ["test_platform_parquet", "test_platform_csv"]
    search_urls = [
        "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10&verbose=true&type=platform2&tag=Parameter+Group:Salinity",
        "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10&verbose=true&type=platform2&tag=Parameter+Group:Temperature: Water Temperature",
    ]
    assert cat.get_search_urls() == search_urls


@mock.patch("requests.get")
def test_platform_search_variable_std_name_query_type_intersection(mock_requests):
    """Test catalog with variable search and query_types."""

    # need two fake responses, one for each search_url
    mock_requests.side_effect = [
        FakeResponseParams(),
        FakeResponse(),
        FakeResponseSalt(),
    ]

    # return either
    cat = AXDSCatalog(
        datatype="platform2",
        standard_names=["sea_water_temperature", "sea_water_practical_salinity"],
        query_type="intersection",
    )
    assert list(cat) == ["test_platform_parquet"]
    search_urls = [
        "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10&verbose=true&type=platform2&tag=Parameter+Group:Salinity",
        "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10&verbose=true&type=platform2&tag=Parameter+Group:Temperature: Water Temperature",
    ]
    assert cat.get_search_urls() == search_urls


def test_platform_query_type_intersection_constrained():
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="platform2", query_type="intersection_constrained")


def test_invalid_kwarg_search():

    # missing min_lat
    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="platform2", kwargs_search=kw)

    # missing min_time
    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "max_time": "2021-4-2",
    }
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="platform2", kwargs_search=kw)

    # min_lon less than -180
    kw = {
        "min_lon": -185,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }

    with pytest.raises(ValueError):
        AXDSCatalog(datatype="platform2", kwargs_search=kw)


def test_invalid_datatype():
    with pytest.raises(KeyError):
        AXDSCatalog(datatype="invalid")


@mock.patch("requests.get")
def test_verbose(mock_requests, capfd):
    mock_requests.side_effect = [FakeResponse()]

    AXDSCatalog(datatype="platform2", verbose=True)

    out, err = capfd.readouterr()
    assert len(out) > 0


@mock.patch("requests.get")
def test_no_results(mock_requests):
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="sensor_station")


@mock.patch("requests.get")
def test_not_a_standard_name(mock_requests):
    mock_requests.side_effect = [FakeResponseParams()]
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="sensor_station", standard_names="not_a_standard_name")


@mock.patch("requests.get")
def test_sensor_search_variable_std_name_query_type_intersection_constrained(
    mock_requests,
):
    """Test catalog with variable search and query_types.

    Not checking actual data return.
    """

    # need two fake responses, one for each search_url
    mock_requests.side_effect = [
        FakeResponseParams(),
        FakeResponse(),
        FakeResponseSalt(),
    ]

    # return either
    cat = AXDSCatalog(
        datatype="sensor_station",
        standard_names=["sea_water_temperature", "sea_water_practical_salinity"],
        query_type="intersection_constrained",
    )
    assert list(cat) == ["test_platform_parquet"]
    search_urls = [
        "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10&verbose=true&type=sensor_station&tag=Parameter+Group:Salinity",
        "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10&verbose=true&type=sensor_station&tag=Parameter+Group:Temperature: Water Temperature",
    ]
    assert cat.get_search_urls() == search_urls
