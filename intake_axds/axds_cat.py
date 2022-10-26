"""
Set up a catalog for Axiom assets.
"""


from typing import Dict, Optional, Union

import pandas as pd
import requests

from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake.source.csv import CSVSource
from intake_xarray.netcdf import NetCDFSource

from . import __version__
from .utils import match_key_to_parameter


search_headers = {"Accept": "application/json"}


class AXDSCatalog(Catalog):
    """
    Makes data sources out of all datasets for a given AXDS data type.

    Have this cover all data types for now, then split out.
    """

    name = "axds_cat"
    version = __version__

    def __init__(
        self,
        datatype: str,
        outtype: str = "dataframe",
        keys_to_match: Optional[str] = None,
        kwargs_search: Dict[str, Union[str, int, float]] = None,
        page_size: int = 10,
        **kwargs,
    ):
        """Initialize an Axiom Catalog.

        Parameters
        ----------
        datatype : str
            Axiom data type. Currently only "platform2" but eventually also "layer_group".
        outtype : str
            Type of output. Probably will be "dataframe" or "xarray".
        keys_to_match : str, optional
            Name of key to match with system-available variable parameterNames using criteria. Currently only 1 at a time.
        kwargs_search : dict, optional
            Contains search information if desired. Keys include: "max_lon", "max_lat", "min_lon", "min_lat", "min_time", "max_time".
        page_size : int, optional
            Number of results. Default is 1000 but fewer is faster.
        """
        self.datatype = datatype
        self.url_docs_base = "https://search.axds.co/v2/docs?verbose=true"
        self.page_size = page_size
        self.outtype = outtype
        self.search_url = None

        if kwargs_search is not None:
            checks = [
                ["min_lon", "max_lon", "min_lat", "max_lat"],
                ["min_time", "max_time"],
            ]
            for check in checks:
                if any(key in kwargs_search for key in check) and not all(
                    key in kwargs_search for key in check
                ):
                    raise ValueError(
                        f"If any of {check} are input, they all must be input."
                    )
        else:
            kwargs_search = {}
        self.kwargs_search = kwargs_search

        if keys_to_match is not None:
            # Currently just take first match, but there could be more than one.
            self.pglabel = match_key_to_parameter(keys_to_match)[0]
        else:
            self.pglabel = None

        super(AXDSCatalog, self).__init__(**kwargs)

    def _load(self):
        """Find all dataset ids and create catalog."""

        self.url_search_base = f"https://search.axds.co/v2/search?portalId=-1&page=1&pageSize={self.page_size}&verbose=true"

        url = f"{self.url_search_base}&type={self.datatype}"

        if self.kwargs_search.keys() >= {"max_lon", "min_lon", "min_lat", "max_lat"}:
            url_add_box = (
                f'&geom={{"type":"Polygon","coordinates":[[[{self.kwargs_search["min_lon"]},{self.kwargs_search["min_lat"]}],'
                + f'[{self.kwargs_search["max_lon"]},{self.kwargs_search["min_lat"]}],'
                + f'[{self.kwargs_search["max_lon"]},{self.kwargs_search["max_lat"]}],'
                + f'[{self.kwargs_search["min_lon"]},{self.kwargs_search["max_lat"]}],'
                + f'[{self.kwargs_search["min_lon"]},{self.kwargs_search["min_lat"]}]]]}}'
            )
            url += f"{url_add_box}"

        if self.kwargs_search.keys() >= {"max_time", "min_time"}:
            # convert input datetime to seconds since 1970
            startDateTime = (
                pd.Timestamp(self.kwargs_search["min_time"]).tz_localize("UTC")
                - pd.Timestamp("1970-01-01 00:00").tz_localize("UTC")
            ) // pd.Timedelta("1s")
            endDateTime = (
                pd.Timestamp(self.kwargs_search["max_time"]).tz_localize("UTC")
                - pd.Timestamp("1970-01-01 00:00").tz_localize("UTC")
            ) // pd.Timedelta("1s")

            # search by time
            url_add_time = f"&startDateTime={startDateTime}&endDateTime={endDateTime}"

            url += f"{url_add_time}"

        if self.pglabel is not None:

            # search by variable
            url += f"&tag=Parameter+Group:{self.pglabel}"

        res = requests.get(url, headers=search_headers).json()

        self.search_url = url

        self._entries = {}

        for results in res["results"]:
            dataset_id = results["uuid"]

            description = f"AXDS dataset_id {dataset_id} of datatype {self.datatype}"

            # Find urlpath
            if self.datatype == "platform2":
                url = f"{self.url_docs_base}&id={dataset_id}"
                res2 = requests.get(url, headers=search_headers).json()
                if self.outtype == "dataframe":
                    urlpath = res2[0]["data"]["resources"]["files"]["data.csv.gz"][
                        "url"
                    ]
                    plugin = CSVSource  # 'csv'
                elif self.outtype == "xarray":
                    key = [
                        key
                        for key in res2[0]["data"]["resources"]["files"].keys()
                        if ".nc" in key
                    ][0]
                    urlpath = res2[0]["data"]["resources"]["files"][key]["url"]
                    plugin = NetCDFSource  # 'netcdf'
            elif self.datatype == "layer_group":
                pass

            args = {
                # 'dataset_id': dataset_id,
                "urlpath": urlpath,
            }

            entry = LocalCatalogEntry(
                dataset_id,
                description,
                plugin,
                True,
                args,
                {},
                {},
                {},
                "",
                getenv=False,
                getshell=False,
            )
            entry._metadata = {
                "info_url": f"{self.url_docs_base}&id={dataset_id}",
                "dataset_id": dataset_id,
            }
            # entry._plugin = [AXDSSource]
            entry._plugin = [plugin]

            self._entries[dataset_id] = entry
