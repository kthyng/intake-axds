"""Test intake-axds."""
from unittest import mock

import cf_pandas
import pytest

from test_utils import FakeResponseParams

from intake_axds.axds_cat import AXDSCatalog


class FakeResponse(object):
    def __init__(self):
        pass

    def json(self):
        res = {
            "results": [
                {
                    "uuid": "test_uuid",
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
                            "variables": {"lon": "lon", "time": "time"},
                        },
                        "files": {
                            "data.csv.gz": {"url": "fake.csv.gz"},
                            "data.viz.parquet": {"url": "fake.parquet"},
                            "deployment.nc": {"url": "fake.nc"},
                        },
                    },
                }
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


@mock.patch("requests.get")
def test_axds_catalog_platform_dataframe(mock_requests):
    """Test basic catalog API: platform as dataframe."""

    mock_requests.side_effect = [FakeResponse()]
    cat = AXDSCatalog(
        datatype="platform2", outtype="dataframe", dataframe_filetype="csv"
    )
    assert list(cat) == ["test_uuid"]
    assert cat["test_uuid"].describe()["args"]["urlpath"] == "fake.csv.gz"

    mock_requests.side_effect = [FakeResponse()]
    cat = AXDSCatalog(
        datatype="platform2", outtype="dataframe", dataframe_filetype="parquet"
    )
    assert list(cat) == ["test_uuid"]
    assert cat["test_uuid"].describe()["args"]["urlpath"] == "fake.parquet"

    with pytest.raises(ValueError):
        mock_requests.side_effect = [FakeResponse()]
        cat = AXDSCatalog(
            datatype="platform2", outtype="dataframe", dataframe_filetype="invalid"
        )


@mock.patch("requests.get")
def test_axds_catalog_platform_xarray(mock_requests):
    """Test basic catalog API: platform as xarray"""

    mock_requests.side_effect = [FakeResponse()]
    cat = AXDSCatalog(datatype="platform2", outtype="xarray")
    assert list(cat) == ["test_uuid"]
    assert cat["test_uuid"].describe()["args"]["urlpath"] == "fake.nc"
    # assert cat["test_uuid"].urlpath == "fake.nc"


@mock.patch("requests.get")
def test_axds_catalog_platform_search(mock_requests):
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

    cat = AXDSCatalog(datatype="platform2", outtype="dataframe", kwargs_search=kw)
    assert list(cat) == ["test_uuid"]
    assert cat["test_uuid"].describe()["args"]["urlpath"] == "fake.csv.gz"
    # assert cat["test_uuid"].urlpath == "fake.parquet"


@mock.patch("requests.get")
def test_axds_catalog_platform_search_for(mock_requests):
    """Test catalog with space/time search and search_for."""

    mock_requests.side_effect = [FakeResponse()]

    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
        "search_for": "whale",
    }

    cat = AXDSCatalog(datatype="platform2", outtype="dataframe", kwargs_search=kw)
    assert "&query=whale" in cat.search_url


@mock.patch("requests.get")
def test_axds_catalog_platform_search_variable(mock_requests):
    """Test catalog with variable search."""

    mock_requests.side_effect = [
        FakeResponseParams(),
        FakeResponse(),
    ]

    criteria = {
        "wind": {
            "standard_name": "wind_gust_to_direction$",
        },
    }
    cf_pandas.set_options(custom_criteria=criteria)

    cat = AXDSCatalog(datatype="platform2", outtype="dataframe", keys_to_match="wind")
    assert list(cat) == ["test_uuid"]
    # assert cat["test_uuid"].urlpath == "fake.parquet"
    assert cat["test_uuid"].describe()["args"]["urlpath"] == "fake.csv.gz"
    assert cat.pglabel == "Winds: Gusts"
    assert "Parameter+Group" in cat.search_url


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
        AXDSCatalog(datatype="platform2", outtype="dataframe", kwargs_search=kw)

    # missing min_time
    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "max_time": "2021-4-2",
    }
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="platform2", outtype="dataframe", kwargs_search=kw)

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
        AXDSCatalog(datatype="platform2", outtype="dataframe", kwargs_search=kw)


def test_module_with_dataframe():
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="module", outtype="dataframe")


@mock.patch("requests.get")
def test_verbose(mock_requests, capfd):
    mock_requests.side_effect = [FakeResponse()]

    AXDSCatalog(datatype="platform2", outtype="dataframe", verbose=True)

    out, err = capfd.readouterr()
    assert len(out) > 0


@mock.patch("requests.get")
def test_no_results(mock_requests):
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="platform2", outtype="dataframe")
