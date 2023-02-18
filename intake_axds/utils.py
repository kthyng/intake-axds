"""Utils to run."""

from importlib.metadata import PackageNotFoundError, version
from operator import itemgetter
from typing import Optional, Union

import cf_pandas as cfp
import pandas as pd
import requests

from shapely import wkt


search_headers = {"Accept": "application/json"}
baseurl = "https://sensors.axds.co/api"


def _get_version() -> str:
    """Fixes circular import issues."""
    try:
        __version__ = version("ocean-model-skill-assessor")
    except PackageNotFoundError:
        # package is not installed
        __version__ = "unknown"

    return __version__


def return_parameter_options() -> dict:
    """Find parameters for Axiom assets.

    Returns
    -------
    List
        Contains the parameter information for Axiom assets.

    Examples
    --------
    >>> return_parameter_options()
    [{'id': 4,
    'label': 'Relative Humidity',
    'urn': 'http://mmisw.org/ont/cf/parameter/relative_humidity',
    'ratio': False,
    'sanityMin': 0.0,
    'sanityMax': 110.0,
    'parameterGroupDefault': True,
    'configJson': None,
    'stageConfigJson': None,
    'idSanityUnit': 1,
    'idParameterGroup': 22,
    'idParameterType': 101,
    'parameterName': 'relative_humidity'},
    ...
    """

    resp = requests.get("http://oikos.axds.co/rest/context")
    # resp.raise_for_status()
    output = resp.json()
    # params = data["parameters"]

    return output


def available_names() -> list:
    """Return available parameterNames for variables.

    Returns
    -------
    list
        parametersNames, which are a superset of standard_names.
    """

    resp = return_parameter_options()
    params = resp["parameters"]

    # find parameterName options for AXDS. These are a superset of standard_names
    names = [i["parameterName"] for i in params]

    return names


def match_key_to_parameter(
    keys_to_match: list,
    criteria: Optional[dict] = None,
) -> list:
    """Find Parameter Group values that match keys_to_match.

    Parameters
    ----------
    keys_to_match : list
        The custom_criteria key to narrow the search, which will be matched to the category results
        using the custom_criteria that must be set up ahead of time with `cf-pandas`.
    criteria : dict, optional
        Criteria to use to map from variable to attributes describing the variable. If user has
        defined custom_criteria, this will be used by default.

    Returns
    -------
    list
        Parameter Group values that match key, according to the custom criteria.
    """

    resp = return_parameter_options()
    params = resp["parameters"]

    # find parameterName options for AXDS. These are a superset of standard_names
    names = [i["parameterName"] for i in params]
    group_params = resp["parameterGroups"]

    # select parameterName that matches selected key
    vars = cfp.match_criteria_key(names, keys_to_match, criteria)

    # find parametergroupid that matches var
    pgids = [
        i["idParameterGroup"]
        for var in vars
        for i in params
        if i["parameterName"] == var
    ]

    # find parametergroup label to match id
    pglabels = [i["label"] for pgid in pgids for i in group_params if i["id"] == pgid]

    return list(set(pglabels))


def match_std_names_to_parameter(standard_names: list) -> list:
    """Find Parameter Group values that match standard_names.

    Parameters
    ----------
    standard_names : list
        standard_names values to narrow the search.

    Returns
    -------
    list
        Parameter Group values that match standard_names.
    """

    resp = return_parameter_options()
    params = resp["parameters"]

    names = [i["parameterName"] for i in params]

    if not all([std_name in names for std_name in standard_names]):
        raise ValueError(
            """Input standard_names are not all matches with system parameterNames.
                          Check available values with `intake_axds.available_names()`."""
        )

    group_params = resp["parameterGroups"]

    # find parametergroupid that matches std_name
    pgids = [
        i["idParameterGroup"]
        for std_name in standard_names
        for i in params
        if i["parameterName"] == std_name
    ]

    # find parametergroup label to match id
    pglabels = [i["label"] for pgid in pgids for i in group_params if i["id"] == pgid]

    return list(set(pglabels))


def load_metadata(datatype: str, results: dict) -> dict:  #: Dict[str, str]
    """Load metadata for catalog entry.

    Parameters
    ----------
    results : dict
        Returned results from call to server for a single dataset.

    Returns
    -------
    dict
        Metadata to store with catalog entry.
    """

    # matching names in intake-erddap
    keys = ["datasetID", "title", "summary", "type", "minTime", "maxTime"]
    # names of keys in Axiom system.
    items = [
        "uuid",
        "label",
        "description",
        "type",
        "start_date_time",
        "end_date_time",
    ]
    values = itemgetter(*items)(results)
    metadata = dict(zip(keys, values))

    if datatype == "platform2":
        metadata["institution"] = (
            results["source"]["meta"]["attributes"]["institution"]
            if "institution" in results["source"]["meta"]["attributes"]
            else None
        )
        metadata["geospatial_bounds"] = results["source"]["meta"]["attributes"][
            "geospatial_bounds"
        ]

        p1 = wkt.loads(metadata["geospatial_bounds"])
        keys = ["minLongitude", "minLatitude", "maxLongitude", "maxLatitude"]
        metadata.update(dict(zip(keys, p1.bounds)))

        metadata["variables"] = list(results["source"]["meta"]["variables"].keys())

    elif datatype == "sensor_station":

        # INSTITUTION?
        # location is lon, lat, depth and type
        # e.g. {'coordinates': [-123.711083, 38.914556, 0.0], 'type': 'Point'}
        lon, lat, depth = results["data"]["location"]["coordinates"]
        keys = ["minLongitude", "minLatitude", "maxLongitude", "maxLatitude"]
        metadata.update(dict(zip(keys, [lon, lat, lon, lat])))

        # e.g. 106793
        metadata["internal_id"] = results["data"]["id"]

        # variables, standard_names (or at least parameterNames)
        figs = results["data"]["figures"]

        out = {
            subPlot["datasetVariableId"]: {
                "parameterGroupLabel": fig["label"],
                "parameterGroupId": fig["parameterGroupId"],
                "datasetVariableId": subPlot["datasetVariableId"],
                "parameterId": subPlot["parameterId"],
                "label": subPlot["label"],
                "deviceId": subPlot["deviceId"],
            }
            for fig in figs
            for plot in fig["plots"]
            for subPlot in plot["subPlots"]
        }
        metadata["variables_details"] = out
        metadata["variables"] = list(out.keys())

        # include datumConversion info if present
        if len(results["data"]["datumConversions"]) > 0:
            metadata["datumConversions"] = results["data"]["datumConversions"]

        filter = f"%7B%22stations%22:%5B%22{metadata['internal_id']}%22%5D%7D"
        baseurl = "https://sensors.axds.co/api"
        metadata_url = f"{baseurl}/metadata/filter/custom?filter={filter}"
        metadata["metadata_url"] = metadata_url

        # also save units here

        # 1 or 2?
        metadata["version"] = results["data"]["version"]

    return metadata


def make_label(label: str, units: Optional[str] = None, use_units: bool = True) -> str:
    """making column name

    Parameters
    ----------
    label : str
        variable label to use in column header
    units : Optional[str], optional
        units to use in column name, if not None, by default None
    use_units : bool, optional
        Users can choose not to include units in column name, by default True

    Returns
    -------
    str
        string to use as column name
    """

    if units is None or not use_units:
        return f"{label}"
    else:
        return f"{label} [{units}]"


def make_filter(internal_id: int, parameterGroupId: Optional[int] = None) -> str:
    """Make filter for Axiom Sensors API.

    Parameters
    ----------
    internal_id : int
        internal id for station. Not the dataset_id or uuid.
    parameterGroupId : Optional[int], optional
        Parameter Group ID to narrow search, by default None

    Returns
    -------
    str
        filter to use in station metadata and data access
    """

    filter = f"%7B%22stations%22:%5B%22{internal_id}%22%5D%7D"

    if parameterGroupId is not None:
        filter += f"%2C%22parameterGroups%22%3A%5B{parameterGroupId}%5D%7D"

    return filter


def make_data_url(
    filter: str,
    start_time: str,
    end_time: str,
    binned: bool = False,
    bin_interval: Optional[str] = None,
) -> str:
    """Create url for accessing sensor data, raw or binned.

    Parameters
    ----------
    filter : str
        get this from ``make_filter()``; contains station and potentially variable info.
    start_time : str
        e.g. "2022-1-1". Needs to be interpretable by pandas ``Timestamp``.
    end_time : str
        e.g. "2022-1-2". Needs to be interpretable by pandas ``Timestamp``.
    binned : bool, optional
        True for binned data, False for raw, by default False.
    bin_interval : Optional[str], optional
        If ``binned=True``, input the binning interval to return. Options are hourly, daily, weekly, monthly, yearly.

    Returns
    -------
    str
        URL from which to access data.
    """

    # handle start and end dates (maybe this should happen in cat?)
    start_date = pd.Timestamp(start_time).strftime("%Y-%m-%dT%H:%M:%S")
    end_date = pd.Timestamp(end_time).strftime("%Y-%m-%dT%H:%M:%S")

    if binned:
        return f"{baseurl}/observations/filter/custom/binned?filter={filter}&start={start_date}Z&end={end_date}Z&binInterval={bin_interval}"
    else:
        return f"{baseurl}/observations/filter/custom?filter={filter}&start={start_date}Z&end={end_date}Z"


def make_metadata_url(filter: str) -> str:
    """Make url for finding metadata

    Parameters
    ----------
    filter : str
        filter for Sensors API. Use ``make_filter`` to make this.

    Returns
    -------
    str
        url for metadata.
    """
    return f"{baseurl}/metadata/filter/custom?filter={filter}"


def make_search_docs_url(dataset_id: str) -> str:
    """Url for Axiom Search docs.

    Parameters
    ----------
    dataset_id : str
        dataset_id or uuid.

    Returns
    -------
    str
        Url for finding Axiom Search docs
    """
    return f"https://search.axds.co/v2/docs?verbose=false&id={dataset_id}"


def response_from_url(url: str) -> Union[list, dict]:
    """Return response from url.

    Parameters
    ----------
    url : str
        URL to check.

    Returns
    -------
    list, dict
        should be a list or dict depending on the url
    """
    return requests.get(url, headers=search_headers).json()
