"""Utils to run."""

from importlib.metadata import PackageNotFoundError, version
from typing import Optional, Union

import cf_pandas as cfp
import pandas as pd
import requests

from nested_lookup import nested_lookup
from shapely import wkt


search_headers = {"Accept": "application/json"}
baseurl = "https://sensors.axds.co/api"
contexturl = "http://oikos.axds.co/rest/context"


def _get_version() -> str:
    """Fixes circular import issues."""
    try:
        __version__ = version("ocean-model-skill-assessor")
    except PackageNotFoundError:
        # package is not installed
        __version__ = "unknown"

    return __version__


def available_names() -> list:
    """Return available parameterNames for variables.

    Returns
    -------
    list
        parametersNames, which are a superset of standard_names.
    """

    resp = response_from_url(contexturl)
    assert isinstance(resp, dict)  # for mypy
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

    resp = response_from_url(contexturl)
    assert isinstance(resp, dict)  # for mypy
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

    # want unique but ordered returned
    return list(zip(*sorted(set(zip(pglabels, pgids)))))


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

    resp = response_from_url(contexturl)
    assert isinstance(resp, dict)  # for mypy
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

    # want unique but ordered returned
    return list(zip(*sorted(set(zip(pglabels, pgids)))))


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

    # mostly matching names in intake-erddap
    metadata = {}
    keys = ["uuid", "label", "description"]
    new_names = ["uuid", "title", "summary"]
    for new_name, key in zip(new_names, keys):
        found = [value for value in nested_lookup(key, results) if value is not None]
        if len(found) > 0:
            metadata[new_name] = found[0]  # take first instance

    new_name = "minTime"
    found_dict = nested_lookup("start", results, wild=True, with_keys=True)
    for key, values in found_dict.items():
        if values == [None]:
            continue
        if len(values) == 1:
            metadata[new_name] = values[0]
        elif len(values) > 1:
            metadata[new_name] = min(values)

    new_name = "maxTime"
    found_dict = nested_lookup("end", results, wild=True, with_keys=True)
    for key, values in found_dict.items():
        if values == [None]:
            continue
        if len(values) == 1:
            metadata[new_name] = values[0]
        elif len(values) > 1:
            metadata[new_name] = min(values)

    if datatype == "platform2":
        metadata["institution"] = nested_lookup("institution", results)
        metadata["geospatial_bounds"] = nested_lookup("geospatial_bounds", results)[0]

        p1 = wkt.loads(metadata["geospatial_bounds"])
        keys = ["minLongitude", "minLatitude", "maxLongitude", "maxLatitude"]
        metadata.update(dict(zip(keys, p1.bounds)))

        metadata["variables_details"] = nested_lookup("variables", results)
        metadata["variables"] = nested_lookup("standard_name", results)

    elif datatype == "sensor_station":

        # location is lon, lat, depth and type
        # e.g. {'coordinates': [-123.711083, 38.914556, 0.0], 'type': 'Point'}
        lon, lat, depth = nested_lookup("location", results)[0]["coordinates"]
        keys = ["minLongitude", "minLatitude", "maxLongitude", "maxLatitude"]
        metadata.update(dict(zip(keys, [lon, lat, lon, lat])))

        # e.g. 106793
        metadata["internal_id"] = int(
            [value for value in nested_lookup("id", results) if value is not None][0]
        )

        metadata["variables_details"] = nested_lookup("figures", results)[0]
        metadata["variables"] = list(set(nested_lookup("datasetVariableId", results)))

        metadata["datumConversions"] = nested_lookup("datumConversions", results)[0]

        filter = f"%7B%22stations%22:%5B%22{metadata['internal_id']}%22%5D%7D"
        baseurl = "https://sensors.axds.co/api"
        metadata_url = f"{baseurl}/metadata/filter/custom?filter={filter}"
        metadata["metadata_url"] = metadata_url

        # 1 or 2?
        metadata["version"] = nested_lookup("version", results)[0]

        # name on other sites, esp for ERDDAP
        metadata["foreignNames"] = list(
            set(nested_lookup("foreignName", results, wild=True))
        )

    return metadata


def check_station(metadata: dict, verbose: bool) -> bool:
    """Whether to keep station or not.

    Parameters
    ----------
    metadata : dict
        metadata about station.
    verbose : bool, optional
        Set to True for helpful information.

    Returns
    -------
    bool
        True to keep station, False to skip.
    """

    keep = True
    # don't save Camera sensor data for now
    if "webcam" in metadata["variables"]:
        keep = False
        if verbose:
            print(f"UUID {metadata['uuid']} is a webcam and should be skipped.")

    # these are IOOS ERDDAP and were setup to be different stations so we can see which stations
    # are successfully being served through IOOS RAs. It duplicates the data (purposely)
    elif "ism-" in metadata["uuid"]:
        keep = False
        if verbose:
            print(
                f"UUID {metadata['uuid']} is a duplicate station from IOOS and should be skipped."
            )

    return keep


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
        internal id for station. Not the uuid.
    parameterGroupId : Optional[int], optional
        Parameter Group ID to narrow search, by default None

    Returns
    -------
    str
        filter to use in station metadata and data access
    """

    filter = f"%7B%22stations%22%3A%5B%22{internal_id}%22%5D"

    if parameterGroupId is not None:
        filter += f"%2C%22parameterGroups%22%3A%5B{parameterGroupId}%5D"

    # add ending }
    filter += "%7D"

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


def make_search_docs_url(
    internal_id: Optional[int] = None, uuid: Optional[str] = None
) -> str:
    """Url for Axiom Search docs.

    Uses whichever of internal_id and uuid is not None to formulate url.

    Parameters
    ----------
    internal_id : Optional[int], optional
        Internal station id for Axiom. Not the UUID.
    uuid : str
        uuid for station.

    Returns
    -------
    str
        Url for finding Axiom Search docs
    """
    if internal_id is not None:
        return f"https://search.axds.co/v2/docs?verbose=false&id=sensor_station:{internal_id}"
    elif uuid is not None:
        return f"https://search.axds.co/v2/docs?verbose=false&id={uuid}"
    else:
        raise KeyError("Correct key was not input for return")


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
