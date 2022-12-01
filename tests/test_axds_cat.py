"""Test intake-axds."""
from unittest import mock

import cf_pandas
import pytest

from test_utils import FakeResponseParams

from intake_axds.axds_cat import AXDSCatalog


class FakeResponseSearch(object):
    def __init__(self):
        pass

    def json(self):
        res = {"results": [{"uuid": "test_uuid"}]}
        return res


class FakeResponseMeta(object):
    def __init__(self):
        pass

    def json(self):
        res = [
            {
                "data": {
                    "resources": {
                        "files": {
                            "data.csv.gz": {"url": "fake.csv.gz"},
                            "deployment.nc": {"url": "fake.nc"},
                        },
                    }
                }
            }
        ]

        return res


@mock.patch("requests.get")
def test_axds_catalog_platform_dataframe(mock_requests):
    """Test basic catalog API: platform as dataframe."""

    mock_requests.side_effect = [FakeResponseSearch(), FakeResponseMeta()]

    cat = AXDSCatalog(datatype="platform2", outtype="dataframe")
    assert list(cat) == ["test_uuid"]
    assert cat["test_uuid"].urlpath == "fake.csv.gz"


@mock.patch("requests.get")
def test_axds_catalog_platform_xarray(mock_requests):
    """Test basic catalog API: platform as xarray"""

    mock_requests.side_effect = [FakeResponseSearch(), FakeResponseMeta()]

    cat = AXDSCatalog(datatype="platform2", outtype="xarray")
    assert list(cat) == ["test_uuid"]
    assert cat["test_uuid"].urlpath == "fake.nc"


@mock.patch("requests.get")
def test_axds_catalog_platform_search(mock_requests):
    """Test catalog with space/time search."""

    mock_requests.side_effect = [FakeResponseSearch(), FakeResponseMeta()]

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
    assert cat["test_uuid"].urlpath == "fake.csv.gz"


@mock.patch("requests.get")
def test_axds_catalog_platform_search_variable(mock_requests):
    """Test catalog with variable search."""

    mock_requests.side_effect = [
        FakeResponseParams(),
        FakeResponseSearch(),
        FakeResponseMeta(),
    ]

    criteria = {
        "wind": {
            "standard_name": "wind_gust_to_direction$",
        },
    }
    cf_pandas.set_options(custom_criteria=criteria)

    cat = AXDSCatalog(datatype="platform2", outtype="dataframe", keys_to_match="wind")
    assert list(cat) == ["test_uuid"]
    assert cat["test_uuid"].urlpath == "fake.csv.gz"
    assert cat.pglabel == "Winds: Gusts"
    assert "Parameter+Group" in cat.search_url


def test_invalid_kwarg_search():
    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "max_lat": 66,
        "min_time": "2021-4-1",
        "max_time": "2021-4-2",
    }

    with pytest.raises(ValueError):
        AXDSCatalog(datatype="platform2", outtype="dataframe", kwargs_search=kw)

    kw = {
        "min_lon": -180,
        "max_lon": -156,
        "min_lat": 50,
        "max_lat": 66,
        "max_time": "2021-4-2",
    }

    with pytest.raises(ValueError):
        AXDSCatalog(datatype="platform2", outtype="dataframe", kwargs_search=kw)


def test_module_with_dataframe():
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="module", outtype="dataframe")


@mock.patch("requests.get")
def test_verbose(mock_requests, capfd):
    mock_requests.side_effect = [FakeResponseSearch(), FakeResponseMeta()]

    AXDSCatalog(datatype="platform2", outtype="dataframe", verbose=True)

    out, err = capfd.readouterr()
    assert len(out) > 0


@mock.patch("requests.get")
def test_no_results(mock_requests):
    with pytest.raises(ValueError):
        AXDSCatalog(datatype="platform2", outtype="dataframe")
