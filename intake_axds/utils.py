"""Utils to run."""

from typing import Optional, Tuple

import cf_pandas as cfp
import requests


def return_parameter_options() -> Tuple:
    """Find category options for ERDDAP server.

    Parameters
    ----------
    server : str
        ERDDAP server address, for example: "https://erddap.sensors.ioos.us/erddap"
    category : str, optional
        ERDDAP category for filtering results. Default is "standard_name" but another good option is
        "variableName".

    Returns
    -------
    DataFrame
        Column "Category" contains all options for selected category on server. Column "URL" contains
        the link for search results for searching for a given category value.
    """

    # find parameterName options for AXDS. These are a superset of standard_names
    resp = requests.get("http://oikos.axds.co/rest/context")
    resp.raise_for_status()
    data = resp.json()
    params = data["parameters"]
    names = [i["parameterName"] for i in params]
    group_params = data["parameterGroups"]

    return params, names, group_params


def match_key_to_parameter(
    key_to_match: str,
    criteria: Optional[dict] = None,
) -> list:
    """Find Parameter Group values that match key_to_match.

    Currently only first value used.

    Parameters
    ----------
    key_to_match : str
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

    params, names, group_params = return_parameter_options()

    # select parameterName that matches selected key
    var = cfp.match_criteria_key(names, key_to_match, criteria)

    # find parametergroupid that matches var
    pgids = [i["idParameterGroup"] for i in params if i["parameterName"] == var[0]]
    pgid = pgids[0]

    # find parametergroup label to match id
    pglabels = [i["label"] for i in group_params if i["id"] == pgid]

    return pglabels
