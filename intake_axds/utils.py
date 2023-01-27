"""Utils to run."""

from importlib.metadata import PackageNotFoundError, version
from typing import Optional

import cf_pandas as cfp
import requests


search_headers = {"Accept": "application/json"}


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


def return_docs_response(dataset_id: str) -> dict:
    """Return request response to docs url in json.

    Parameters
    ----------
    dataset_id : str
        ID for dataset.
    """

    url_docs_base = "https://search.axds.co/v2/docs?verbose=true"
    url = f"{url_docs_base}&id={dataset_id}"
    return requests.get(url, headers=search_headers).json()[0]
