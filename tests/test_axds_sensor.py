from unittest import mock

import intake
import pytest

from intake_axds.axds import AXDSSensorSource


class FakeResponseSensorAPI123456(object):
    def __init__(self):
        pass

    def json(self):
        res = {
            "data": {
                "stations": [
                    {
                        "uuid": "test_sensor",
                        "version": 2,
                    },
                ]
            }
        }
        return res


class FakeResponseSensorAPI111111(object):
    def __init__(self):
        pass

    def json(self):
        res = {
            "data": {
                "stations": [
                    {
                        "uuid": "test_sensor_1",
                        "version": 1,
                    },
                ]
            }
        }
        return res


class FakeResponseSearchDocsV2(object):
    def __init__(self):
        pass

    def json(self):
        res = [
            {
                "id": "123456",
                "uuid": "test_platform_parquet",
                "label": "test_label",
                "description": "Test description.",
                "type": "platform2",
                "start_date_time": "2019-03-15T02:58:51.000Z",
                "end_date_time": "2019-04-08T07:54:56.000Z",
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
        ]
        return res


class FakeResponseSearchDocsV1(object):
    def __init__(self):
        pass

    def json(self):
        res = [
            {
                "id": "123456",
                "uuid": "test_platform_parquet",
                "label": "test_label",
                "description": "Test description.",
                "type": "platform2",
                "start_date_time": "2019-03-15T02:58:51.000Z",
                "end_date_time": "2019-04-08T07:54:56.000Z",
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
                    "version": 1,
                },
            }
        ]
        return res


def test_intake_opener():
    assert "open_axds_sensor" in intake.openers


def test_binned():
    source = AXDSSensorSource(
        internal_id=123456, uuid="test", bin_interval="monthly"
    )
    assert source.binned


@mock.patch("requests.get")
def test_ids(mock_requests):
    mock_requests.side_effect = [
        # FakeResponseSensorAPI123456(),
        FakeResponseSearchDocsV2(),
        FakeResponseSearchDocsV2(),
    ]
    source = AXDSSensorSource(internal_id=123456)
    assert source.uuid == "test_platform_parquet"

    source = AXDSSensorSource(uuid="test_platform_parquet")
    assert source.internal_id == 123456

    with pytest.raises(ValueError):
        source = AXDSSensorSource()


def test_times():
    # doesn't need response because both internal_id and dataset_id are faked upon init
    source = AXDSSensorSource(
        internal_id=123456, uuid="fake", start_time="2000-1-1"
    )
    assert source.end_time is None
    source = AXDSSensorSource(
        internal_id=123456, uuid="fake", end_time="2000-1-1"
    )
    assert source.start_time is None


# not using this approach now
# @mock.patch("requests.get")
# def test_filters(mock_requests):

#     mock_requests.side_effect = [
#         FakeResponseSensorAPI123456(),
#         FakeResponseSensorAPI111111(),
#     ]
#     # V2
#     source = AXDSSensorSource(internal_id=123456)
#     assert source.get_filters() == ["%7B%22stations%22%3A%5B%22123456%22%5D%7D"]

#     # V1
#     source = AXDSSensorSource(internal_id=1111111, only_pgids=[7])
#     assert source.get_filters() == [
#         "%7B%22stations%22%3A%5B%221111111%22%5D%2C%22parameterGroups%22%3A%5B7%5D%7D"
#     ]


@mock.patch("requests.get")
def test_data_urls_V2(mock_requests):

    mock_requests.side_effect = [
        # FakeResponseSensorAPI123456(),
        FakeResponseSearchDocsV2(),
        FakeResponseSearchDocsV2(),
    ]
    source = AXDSSensorSource(internal_id=123456)
    assert source.get_filters() == ["%7B%22stations%22%3A%5B%22123456%22%5D%7D"]
    urls = [
        "https://sensors.axds.co/api/observations/filter/custom?filter=%7B%22stations%22%3A%5B%22123456%22%5D%7D&start=2019-03-15T02:58:51Z&end=2019-04-08T07:54:56Z"
    ]
    assert source.data_urls == urls


@mock.patch("requests.get")
def test_data_urls_V1(mock_requests):

    mock_requests.side_effect = [
        # FakeResponseSensorAPI111111(),
        FakeResponseSearchDocsV1(),
        FakeResponseSearchDocsV1(),
    ]
    source = AXDSSensorSource(internal_id=123456, only_pgids=[7])
    assert source.get_filters() == [
        "%7B%22stations%22%3A%5B%22123456%22%5D%2C%22parameterGroups%22%3A%5B7%5D%7D"
    ]
    urls = [
        "https://sensors.axds.co/api/observations/filter/custom?filter=%7B%22stations%22%3A%5B%22123456%22%5D%2C%22parameterGroups%22%3A%5B7%5D%7D&start=2019-03-15T02:58:51Z&end=2019-04-08T07:54:56Z"
    ]
    assert source.data_urls == urls
