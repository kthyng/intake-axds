#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for generic and utility functions."""
from unittest import mock

from intake_axds import utils


def test_get_project_version():
    version = utils._get_version()
    assert version is not None


class FakeResponseParams(object):
    def __init__(self):
        pass

    def json(self):
        params = {
            "parameters": [
                {
                    "id": 4,
                    "label": "Relative Humidity",
                    "idParameterGroup": 22,
                    "idParameterType": 101,
                    "parameterName": "relative_humidity",
                },
                {
                    "id": 226,
                    "label": "Wind Gust To Direction",
                    "idParameterGroup": 186,
                    "idParameterType": 130,
                    "parameterName": "wind_gust_to_direction",
                },
                {
                    "id": 41,
                    "label": "Water Temperature",
                    "urn": "http://mmisw.org/ont/cf/parameter/sea_water_temperature",
                    "sanityMin": 20,
                    "sanityMax": 135,
                    "idSanityUnit": 9,
                    "idParameterGroup": 7,
                    "idParameterType": 91,
                    "parameterName": "sea_water_temperature",
                },
                {
                    "id": 50,
                    "label": "Salinity",
                    "urn": "http://mmisw.org/ont/cf/parameter/sea_water_practical_salinity",
                    "sanityMin": 0,
                    "sanityMax": 50,
                    "stageConfigJson": {"palette": "haline"},
                    "idSanityUnit": 4,
                    "idParameterGroup": 14,
                    "idParameterType": 94,
                    "parameterName": "sea_water_practical_salinity",
                },
            ],
            "parameterGroups": [
                {
                    "id": 186,
                    "label": "Winds: Gusts",
                    "included": True,
                    "legacyId": "Wind Gust",
                    "shortLabel": "Wind Gust",
                },
                {
                    "id": 22,
                    "label": "Humidity: Relative Humidity",
                    "included": True,
                    "legacyId": "RELATIVE_HUMIDITY",
                    "shortLabel": "Humidity",
                },
                {
                    "id": 7,
                    "label": "Temperature: Water Temperature",
                    "included": True,
                    "legacyId": "WATER_TEMPERATURE",
                    "shortLabel": "Water Temp",
                },
                {
                    "id": 14,
                    "label": "Salinity",
                    "included": True,
                    "legacyId": "SALINITY",
                    "shortLabel": None,
                },
            ],
        }

        return params


@mock.patch("requests.get")
def test_parameters(mock_requests):
    """Basic tests of return_parameter_options."""

    mock_requests.return_value = FakeResponseParams()

    output = utils.response_from_url(utils.contexturl)
    assert isinstance(output, dict)

    assert {"parameters", "parameterGroups"} >= output.keys()


@mock.patch("requests.get")
def test_parameters_and_key(mock_requests):
    """match a key"""

    mock_requests.return_value = FakeResponseParams()

    criteria = {
        "wind": {
            "standard_name": "wind_gust_to_direction$",
        },
    }
    out = utils.match_key_to_parameter("wind", criteria)
    match_to_key, pgids = out
    assert match_to_key == ("Winds: Gusts",)
    assert pgids == (186,)


@mock.patch("requests.get")
def test_parameters_and_std_names(mock_requests):
    """match std_names"""

    mock_requests.return_value = FakeResponseParams()
    out = utils.match_std_names_to_parameter(
        ["wind_gust_to_direction", "relative_humidity"]
    )
    match_to_name, pgids = out

    assert sorted(match_to_name) == ["Humidity: Relative Humidity", "Winds: Gusts"]
    assert sorted(pgids) == [22, 186]


def test_make_search_docs_url():
    uuid = "test_id"
    assert (
        f"https://search.axds.co/v2/docs?verbose=false&id={uuid}"
        == utils.make_search_docs_url(uuid=uuid)
    )
    internal_id = "test_internal_id"
    assert (
        f"https://search.axds.co/v2/docs?verbose=false&id=sensor_station:{internal_id}"
        == utils.make_search_docs_url(internal_id=internal_id)
    )


def test_make_metadata_url():
    filter = "filter"
    assert (
        f"{utils.baseurl}/metadata/filter/custom?filter={filter}"
        == utils.make_metadata_url(filter)
    )


def test_make_filter():
    internal_id = "internal_id"
    assert "parameterGroups" in utils.make_filter(
        internal_id=internal_id, parameterGroupId=10
    )
    assert "parameterGroups" not in utils.make_filter(
        internal_id=internal_id, parameterGroupId=None
    )


def test_load_metadata():
    datatype = "sensor_station"
    results = {
        "uuid": "uuid",
        "label": "label",
        "description": "desc",
        "type": "type",
        "start_date_time": "2000-1-1",
        "end_date_time": "2000-1-2",
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
    }
    metadata = utils.load_metadata(datatype, results)
    test_results = {'uuid': 'uuid', 'title': 'label', 'summary': 'desc', 'minTime': '2000-1-1', 'maxTime': '2000-1-2', 'minLongitude': -123.711083, 'minLatitude': 38.914556, 'maxLongitude': -123.711083, 'maxLatitude': 38.914556, 'internal_id': 106793, 'variables_details': [{'label': 'label', 'parameterGroupId': 'parameterGroupId', 'plots': [{'subPlots': [{'datasetVariableId': 'datasetVariableId', 'parameterId': 'parameterId', 'label': 'label', 'deviceId': 'deviceId'}]}]}], 'variables': ['datasetVariableId'], 'datumConversions': [], 'metadata_url': 'https://sensors.axds.co/api/metadata/filter/custom?filter=%7B%22stations%22:%5B%22106793%22%5D%7D', 'version': 2, 'foreignNames': []}
    assert metadata == test_results

    datatype = "platform2"
    results = {
        "uuid": "uuid",
        "label": "label",
        "description": "desc",
        "type": "type",
        "start_date_time": "2000-1-1",
        "end_date_time": "2000-1-2",
        "source": {
            "meta": {
                "attributes": {
                    "institution": "institution",
                    "geospatial_bounds": "POLYGON ((0 -80, 0 90, 359.9200439453125 90, 359.9200439453125 -80, 0 -80))",
                },
                "variables": {
                    "variable_name": {
                        "attributes": {
                            "standard_name": "standard_name",
                            "units": "units",
                            "unit_id": "unit_id",
                            "long_name": "long_name",
                            "parameter_id": "parameter_id",
                        }
                    },
                },
            }
        },
    }
    metadata = utils.load_metadata(datatype, results)
    test_results = {'uuid': 'uuid', 'title': 'label', 'summary': 'desc', 'minTime': '2000-1-1', 'maxTime': '2000-1-2', 'institution': ['institution'], 'geospatial_bounds': 'POLYGON ((0 -80, 0 90, 359.9200439453125 90, 359.9200439453125 -80, 0 -80))', 'minLongitude': 0.0, 'minLatitude': -80.0, 'maxLongitude': 359.9200439453125, 'maxLatitude': 90.0, 'variables_details': [{'variable_name': {'attributes': {'standard_name': 'standard_name', 'units': 'units', 'unit_id': 'unit_id', 'long_name': 'long_name', 'parameter_id': 'parameter_id'}}}], 'variables': ['standard_name']}
    assert metadata == test_results
