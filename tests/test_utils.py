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
            ],
        }

        return params


@mock.patch("requests.get")
def test_parameters(mock_requests):
    """Basic tests of return_parameter_options."""

    mock_requests.return_value = FakeResponseParams()

    output = utils.return_parameter_options()
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
    match_to_key = utils.match_key_to_parameter("wind", criteria)
    assert match_to_key == ["Winds: Gusts"]


@mock.patch("requests.get")
def test_parameters_and_std_names(mock_requests):
    """match std_names"""

    mock_requests.return_value = FakeResponseParams()
    match_to_name = utils.match_std_names_to_parameter(
        ["wind_gust_to_direction", "relative_humidity"]
    )
    assert sorted(match_to_name) == ["Humidity: Relative Humidity", "Winds: Gusts"]
