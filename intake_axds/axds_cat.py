"""
Set up a catalog for Axiom assets.
"""


from datetime import datetime
from typing import List, MutableMapping, Optional, Sequence, Tuple, Union

import pandas as pd

from cf_pandas import astype
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake.source.csv import CSVSource
from intake_parquet.source import ParquetSource

from . import __version__
from .axds import AXDSSensorSource
from .utils import (
    load_metadata,
    match_key_to_parameter,
    match_std_names_to_parameter,
    response_from_url,
)


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
        keys_to_match: Optional[Union[str, list]] = None,
        standard_names: Optional[Union[str, list]] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        search_for: Optional[Union[str, List[str]]] = None,
        kwargs_search: MutableMapping[
            str, Union[str, int, float, Sequence[Union[str, None]]]
        ] = None,
        query_type: str = "union",
        qartod: Union[bool, int, List[int]] = False,
        use_units: bool = True,
        binned: bool = False,
        bin_interval: Optional[str] = None,
        page_size: int = 10,
        verbose: bool = False,
        name: str = "catalog",
        description: str = "Catalog of Axiom assets.",
        metadata: dict = None,
        ttl: Optional[int] = None,
        **kwargs,
    ):
        """Initialize an Axiom Catalog.

        only datatype sensor_station uses the following parameters: qartod, use_units, binned, bin_interval

        datatype of sensor_station skips webcam data.

        Parameters
        ----------
        datatype : str
            Axiom data type. Currently "platform2" or "sensor_station" but eventually also "module". Platforms and sensors are returned as dataframe containers.
        keys_to_match : str, list, optional
            Name of keys to match with system-available variable parameterNames using criteria. To filter search by variables, either input keys_to_match and a vocabulary or input standard_names. Results from multiple values will be combined according to ``query_type``.
        standard_names : str, list, optional
            Standard names to select from Axiom search parameterNames. If more than one is input, the search is for a logical OR of datasets containing the standard_names. To filter search by variables, either input keys_to_match and a vocabulary or input standard_names. Results from multiple values will be combined according to ``query_type``.
        bbox : tuple of 4 floats, optional
            For explicit geographic search queries, pass a tuple of four floats in the `bbox` argument. The bounding box parameters are `(min_lon, min_lat, max_lon, max_lat)`.
        start_time : str, optional
            For explicit search queries for datasets that contain data after `start_time`. Must include end_time if include start_time.
        end_time : str, optional
            For explicit search queries for datasets that contain data before `end_time`. Must include start_time if include end_time.
        search_for : str, list of strings, optional
            For explicit search queries for datasets that any contain of the terms specified in this keyword argument. Results from multiple values will be combined according to ``query_type``.
        kwargs_search : dict, optional
            Keyword arguments to input to search on the server before making the catalog. Options are:
            * to search by bounding box: include all of min_lon, max_lon, min_lat, max_lat: (int, float). Longitudes must be between -180 to +180.
            * to search within a datetime range: include both of min_time, max_time: interpretable datetime string, e.g., "2021-1-1"
            * to search using a textual keyword: include `search_for` as a string or list of strings. Results from multiple values will be combined according to ``query_type``.
        query_type : str, default "union"
            Specifies how the catalog should apply the query parameters. Choices are
            ``"union"`` or ``"intersection"``. If the ``query_type`` is set to
            ``"intersection"``, then the set of results will be the intersection of
            each individual query made to ERDDAP. This is equivalent to a logical
            AND of the results. If the value is ``"union"`` then the results will be
            the union of each resulting dataset. This is equivalent to a logical OR.
        qartod : bool, int, list, optional
            Whether to return QARTOD agg flags when available, which is only for sensor_stations. Can instead input an int or a list of ints representing the _qa_agg flags for which to return data values. More information about QARTOD testing and flags can be found here: https://cdn.ioos.noaa.gov/media/2020/07/QARTOD-Data-Flags-Manual_version1.2final.pdf. Only used by datatype "sensor_station".

            Examples of ways to use this input are:

            * ``qartod=True``: Return aggregate QARTOD flags as a column for each data variable.
            * ``qartod=False``: Do not return any QARTOD flag columns.
            * ``qartod=1``: nan any data values for which the aggregated QARTOD flags are not equal to 1.
            * ``qartod=[1,3]``: nan any data values for which the aggregated QARTOD flags are not equal to 1 or 3.

            Flags are:

            * 1: Pass
            * 2: Not Evaluated
            * 3: Suspect
            * 4: Fail
            * 9: Missing Data

        use_units : bool, optional
            If True include units in column names. Syntax is "standard_name [units]". If False, no units. Then syntax for column names is "standard_name". This is currently specific to sensor_station only. Only used by datatype "sensor_station".
        binned : bool, optional
            True for binned data, False for raw, by default False. Only used by datatype "sensor_station".
        bin_interval : Optional[str], optional
            If ``binned=True``, input the binning interval to return. Options are hourly, daily, weekly, monthly, yearly. If bin_interval is input, binned is set to True. Only used by datatype "sensor_station".
        page_size : int, optional
            Number of results. Fewer is faster. Note that default is 10. Note that if you want to make sure you get all available datasets, you should input a large number like 50000.
        verbose : bool, optional
            Set to True for helpful information.
        ttl : int, optional
            Time to live for catalog (in seconds). How long before force-reloading catalog. Set to None to not do this.
        name : str, optional
            Name for catalog.
        description : str, optional
            Description for catalog.
        metadata : dict, optional
            Metadata for catalog.
        kwargs:
            Other input arguments are passed to the intake Catalog class. They can includegetenv, getshell, persist_mode, storage_options, and user_parameters, in addition to some that are surfaced directly in this class.
        """

        self.datatype = datatype
        self.kwargs_search = kwargs_search
        self.page_size = page_size
        self.verbose = verbose
        self.qartod = qartod
        self.use_units = use_units
        self.kwargs_search = kwargs_search or {}
        self.query_type = query_type

        if bin_interval is not None:
            binned = True
        self.binned = binned
        self.bin_interval = bin_interval

        allowed_datatypes = ("platform2", "sensor_station")
        if datatype not in allowed_datatypes:
            raise KeyError(
                f"Datatype must be one of {allowed_datatypes} but is {datatype}"
            )

        if query_type == "intersection" and page_size == 10:
            if verbose:
                print(
                    "With `query_type` of 'intersection' you probably want to use a larger `page_size` than the default of 10."
                )

        # can instead input the kwargs_search outside of that dictionary
        if bbox is not None:
            if not isinstance(bbox, tuple):
                raise TypeError(
                    f"Expecting a tuple of four floats for argument bbox: {type(bbox)}"
                )
            if len(bbox) != 4:
                raise ValueError("bbox argument requires a tuple of four floats")
            self.kwargs_search["min_lon"] = bbox[0]
            self.kwargs_search["min_lat"] = bbox[1]
            self.kwargs_search["max_lon"] = bbox[2]
            self.kwargs_search["max_lat"] = bbox[3]

        if start_time is not None:
            if start_time in self.kwargs_search:
                raise KeyError("start_time defined explicitly and in kwargs_search.")

            if not isinstance(start_time, (str, datetime)):
                raise TypeError(
                    f"Expecting a datetime for start_time argument: {repr(start_time)}"
                )
            # if isinstance(start_time, str):
            #     start_time = pd.Timestamp(start_time)#.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
            self.kwargs_search["min_time"] = start_time

        if end_time is not None:
            if end_time in self.kwargs_search:
                raise KeyError("end_time defined explicitly and in kwargs_search.")
            if not isinstance(end_time, (str, datetime)):
                raise TypeError(
                    f"Expecting a datetime for end_time argument: {repr(end_time)}"
                )
            # if isinstance(end_time, str):
            #     end_time = pd.Timestamp(end_time)#.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")
            self.kwargs_search["max_time"] = end_time

        if search_for is not None:
            if "search_for" in self.kwargs_search:
                raise KeyError("search_for defined explicitly and in kwargs_search.")
            if not isinstance(search_for, (str, list)):
                raise TypeError(
                    f"Expecting string or list of strings for search_for argument: {repr(search_for)}"
                )
            self.kwargs_search["search_for"] = search_for
        if "search_for" not in self.kwargs_search:
            self.kwargs_search["search_for"] = [None]
        else:
            if isinstance(self.kwargs_search["search_for"], str):
                self.kwargs_search["search_for"] = [self.kwargs_search["search_for"]]

        check = ["min_lon", "max_lon", "min_lat", "max_lat"]
        if any(key in self.kwargs_search for key in check) and not all(
            key in self.kwargs_search for key in check
        ):
            raise ValueError(f"If any of {check} are input, they all must be input.")

        if "min_lon" in self.kwargs_search and "max_lon" in self.kwargs_search:
            min_lon, max_lon = (
                self.kwargs_search["min_lon"],
                self.kwargs_search["max_lon"],
            )
            if isinstance(min_lon, (int, float)) and isinstance(max_lon, (int, float)):
                if abs(min_lon) > 180 or abs(max_lon) > 180:
                    raise ValueError(
                        "`min_lon` and `max_lon` must be in the range -180 to 180."
                    )

        # input keys_to_match OR standard_names but not both
        if keys_to_match is not None and standard_names is not None:
            raise ValueError(
                "Input either `keys_to_match` or `standard_names` but not both."
            )

        self.pglabels: List[Union[str, None]]
        if keys_to_match is not None:
            self.pglabels = match_key_to_parameter(astype(keys_to_match, list))
        elif standard_names is not None:
            self.pglabels = match_std_names_to_parameter(astype(standard_names, list))
        else:
            self.pglabels = [None]

        # Put together catalog-level stuff
        if metadata is None:
            metadata = {}
            metadata["kwargs_search"] = self.kwargs_search
            metadata["pglabels"] = self.pglabels
            # metadata["qartod"] = qartod
            # metadata["use_units"] = use_units

        super(AXDSCatalog, self).__init__(
            **kwargs, ttl=ttl, name=name, description=description, metadata=metadata
        )

    def search_url(
        self, pglabel: Optional[str] = None, text_search: Optional[str] = None
    ) -> str:
        """Set up one url for searching.

        Parameters
        ----------
        pglabel : Optional[str], optional
            Parameter Group Label (not ID), by default None
        text_search : Optional[str], optional
            free text search, by default None

        Returns
        -------
        str
            URL to use to search Axiom systems.
        """

        self.url_search_base = f"https://search.axds.co/v2/search?portalId=-1&page=1&pageSize={self.page_size}&verbose=true"

        url = f"{self.url_search_base}&type={self.datatype}"

        assert isinstance(self.kwargs_search, dict)
        if self.kwargs_search.keys() >= {
            "max_lon",
            "min_lon",
            "min_lat",
            "max_lat",
        }:
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

        if text_search is not None:
            url += f"&query={text_search}"

        # search by variable
        if pglabel is not None:
            url += f"&tag=Parameter+Group:{pglabel}"

        # if requests.get(url).status_code != 200:
        #     raise ValueError("")

        if self.verbose:
            print(f"search url: {url}")

        return url

    def get_search_urls(self) -> list:
        """Gather all search urls for catalog.

        Inputs that can have more than one search_url are pglabels and search_for list.

        Returns
        -------
        list
            List of search urls.
        """
        assert isinstance(self.kwargs_search, dict)
        search_urls = [
            self.search_url(pglabel, text_search)
            for pglabel in self.pglabels
            for text_search in self.kwargs_search["search_for"]
        ]

        return search_urls

    def _load_all_results(self) -> list:
        """Combine results from multiple search urls using query_type.

        Returns
        -------
        list
            list of results from potentially multiple searches.

        Raises
        ------
        ValueError
            if no results found.
        """

        combined_results = []
        first_loop = True
        for search_url in self.get_search_urls():
            res = response_from_url(search_url)
            if "results" not in res:
                raise ValueError(
                    f"No results were returned for the search. Search url: {search_url}."
                )

            assert isinstance(res, dict)

            if self.verbose:
                print(
                    f"For search url {search_url}, number of results found: {len(res['results'])}. Page size: {self.page_size}."
                )

            if self.query_type == "union":  # logical OR
                combined_results.extend(res["results"])

            elif self.query_type == "intersection":  # logical AND
                if first_loop:  # initialize
                    combined_results = res["results"]
                    first_loop = False
                else:
                    # compare uuids from first search with uuids from next search
                    uuids_before = [dataset["uuid"] for dataset in combined_results]
                    uuids_now = [dataset["uuid"] for dataset in res["results"]]

                    overlapping_uuids = set(uuids_before).intersection(set(uuids_now))

                    combined_results = [
                        dataset
                        for dataset in combined_results
                        if dataset["uuid"] in overlapping_uuids
                    ]

        if self.verbose:
            unique = set([res["uuid"] for res in combined_results])
            print(
                f"Total number of results found for page_size {self.page_size} over {len(self.get_search_urls())} different searches with query_type {self.query_type}: {len(combined_results)}, with unique results: {len(unique)}."
            )

        return combined_results

    def _load(self):
        """Find all dataset ids and create catalog."""

        results = self._load_all_results()

        self._entries = {}
        for result in results:
            dataset_id = result["uuid"]

            # don't repeat an entry (it won't actually allow you to, but probably saves time not to try)
            if dataset_id in self._entries:
                continue

            if self.verbose:
                print(f"Dataset ID: {dataset_id}")

            # # quick check if OPENDAP is in the access methods for this uuid, otherwise move on
            # if self.datatype == "module":
            #     # if opendap is not in the access methods at the module level, then we assume it
            #     # also isn't at the layer_group level, so we will not check each layer_group
            #     if "OPENDAP" not in results["data"]["access_methods"]:
            #         if self.verbose:
            #             print(
            #                 f"Cannot access module {dataset_id} via opendap so no source is being made for it.",
            #                 UserWarning,
            #             )
            #         # warnings.warn(f"Cannot access module {dataset_id} via opendap so no source is being made for it.", UserWarning)
            #         continue
            #     if "DEPRECATED" in results["data"]["label"]:
            #         if self.verbose:
            #             print(
            #                 f"Skipping module {dataset_id} because label says it is deprecated.",
            #                 UserWarning,
            #             )
            #         continue

            description = f"AXDS dataset_id {dataset_id} of datatype {self.datatype}"

            metadata = load_metadata(self.datatype, result)

            # don't save Camera sensor data for now
            if "webcam" in metadata["variables"]:
                if self.verbose:
                    print(f"Dataset_id {dataset_id} is a webcam so is being skipped.")
                continue

            # Find urlpath
            if self.datatype == "platform2":
                # use parquet if available, otherwise csv
                try:
                    key = [
                        key
                        for key in result["source"]["files"].keys()
                        if ".parquet" in key
                    ][0]
                    urlpath = result["source"]["files"][key]["url"]
                    plugin = ParquetSource
                except Exception:
                    urlpath = result["source"]["files"]["data.csv.gz"]["url"]
                    plugin = CSVSource

                args = {
                    "urlpath": urlpath,
                }

            # this Source has different arg requirements
            elif self.datatype == "sensor_station":
                args = {
                    "dataset_id": dataset_id,
                    "internal_id": metadata["internal_id"],
                    "start_time": self.kwargs_search.get("min_time", None),
                    "end_time": self.kwargs_search.get("max_time", None),
                    # "kwargs_search": self.kwargs_search,
                    "qartod": self.qartod,
                    "use_units": self.use_units,
                    "binned": self.binned,
                    "bin_interval": self.bin_interval,
                }
                plugin = AXDSSensorSource

            # elif self.datatype == "module":
            #     plugin = NetCDFSource  # 'netcdf'

            #     # modules are the umbrella and contain 1 or more layer_groups
            #     # pull out associated layer groups uuids to make sure to capture them
            #     layer_group_uuids = list(docs["data"]["layer_group_info"].keys())

            #     # pull up docs for each layer_group to get urlpath
            #     # can only get a urlpath if it is available on opendap
            #     urlpaths = []  # using this to see if there are ever multiple urlpaths
            #     for layer_group_uuid in layer_group_uuids:
            #         docs_lg = return_docs_response(layer_group_uuid)
            # docs_log = response_from_url(make_search_docs_url(layer_group_uuid))

            #         if "OPENDAP" in docs_lg["data"]["access_methods"]:
            #             urlpath = docs_lg["source"]["layers"][0][
            #                 "thredds_opendap_url"
            #             ].removesuffix(".html")
            #             urlpaths.append(urlpath)

            #     # only want unique urlpaths
            #     urlpaths = list(set(urlpaths))
            #     if len(urlpaths) > 1:
            #         if self.verbose:
            #             print(
            #                 f"Several urlpaths were found for module {dataset_id} so no source is being made for it."
            #             )
            #         # warnings.warn(f"Several urlpaths were found for module {dataset_id} so no source is being made for it.", UserWarning)
            #         continue
            #         # raise ValueError(f"the layer_groups for module {dataset_id} have different urlpaths.")
            #     elif len(urlpaths) == 0:
            #         if self.verbose:
            #             print(
            #                 f"No urlpath was found for module {dataset_id} so no source is being made for it."
            #             )
            #         # warnings.warn(f"No urlpath was found for module {dataset_id} so no source is being made for it.", UserWarning)
            #         continue
            #     else:
            #         urlpath = urlpaths[0]

            entry = LocalCatalogEntry(
                name=dataset_id,
                description=description,
                driver=plugin,
                direct_access="allow",
                args=args,
                metadata=metadata,
                # True,
                # args,
                # {},
                # {},
                # {},
                # "",
                # getenv=False,
                # getshell=False,
            )

            entry._plugin = [plugin]

            self._entries[dataset_id] = entry
