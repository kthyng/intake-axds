"""
Set up a catalog for Axiom assets.
"""


from operator import itemgetter
from typing import List, MutableMapping, Optional, Tuple, Union

import pandas as pd
import requests
from datetime import datetime
from cf_pandas import astype
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake.source.csv import CSVSource
from intake_parquet.source import ParquetSource
from shapely import wkt

from . import __version__
from .utils import match_key_to_parameter, match_std_names_to_parameter
from .axds import AXDSSensorSource


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
        datatype: str = "platform2",
        keys_to_match: Optional[Union[str, list]] = None,
        standard_names: Optional[Union[str, list]] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        start_time: Optional[Union[datetime, str]] = None,
        end_time: Optional[Union[datetime, str]] = None,
        search_for: Optional[str] = None,
        kwargs_search: MutableMapping[str, Union[str, int, float]] = None,
        qartod: Union[bool,int,List[int]] = False,
        use_units: bool = True,
        page_size: int = 10,
        verbose: bool = False,
        name: str = "catalog",
        description: str = "Catalog of Axiom assets.",
        metadata: dict = None,
        ttl: int = 86400,
        **kwargs,
    ):
        """Initialize an Axiom Catalog.
        
        datatype of sensor_station skips webcam data.

        Parameters
        ----------
        datatype : str
            Axiom data type. Currently only "platform2" but eventually also "module". Platforms will be returned as dataframe containers.
        keys_to_match : str, list, optional
            Name of keys to match with system-available variable parameterNames using criteria. To filter search by variables, either input keys_to_match and a vocabulary or input standard_names.
        standard_names : str, list, optional
            Standard names to select from Axiom search parameterNames. To filter search by variables, either input keys_to_match and a vocabulary or input standard_names.
        bbox : tuple of 4 floats, optional
            For explicit geographic search queries, pass a tuple of four floats in the `bbox` argument. The bounding box parameters are `(min_lon, min_lat, max_lon, max_lat)`.
        start_time : str, datetime, optional
            For explicit search queries for datasets that contain data after `start_time`. Must include end_time if include start_time.
        end_time : str, datetime, optional
            For explicit search queries for datasets that contain data before `end_time`. Must include start_time if include end_time.
        search_for : str, optional
            For explicit search queries for datasets that any contain of the terms specified in this keyword argument.
        kwargs_search : dict, optional
            Keyword arguments to input to search on the server before making the catalog. Options are:
            * to search by bounding box: include all of min_lon, max_lon, min_lat, max_lat: (int, float). Longitudes must be between -180 to +180.
            * to search within a datetime range: include both of min_time, max_time: interpretable datetime string, e.g., "2021-1-1"
            * to search using a textual keyword: include `search_for` as a string.
        qartod : bool, int, list, optional
            Whether to return QARTOD agg flags when available, which is only for sensor_stations. Can instead input an int or a list of ints representing the _qa_agg flags for which to return data values. More information about QARTOD testing and flags can be found here: https://cdn.ioos.noaa.gov/media/2020/07/QARTOD-Data-Flags-Manual_version1.2final.pdf. 
            
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
            If True include units in column names. Syntax is "standard_name [units]". If False, no units. Then syntax for column names is "standard_name". This is currently specific to sensor_station only.
        page_size : int, optional
            Number of results. Fewer is faster. Note that default is 10. Note that if you want to make sure you get all available datasets, you should input a large number like 50000.
        verbose : bool, optional
            Set to True for helpful information.
        ttl : int, optional
            Time to live for catalog (in seconds). How long before force-reloading catalog. Set to None to not do this. Currently default is set to a large number because the available version of intake does not have a change to accept None.
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
        self.url_docs_base = "https://search.axds.co/v2/docs?verbose=true"
        self.kwargs_search = kwargs_search
        self.page_size = page_size
        self.verbose = verbose
        self.qartod = qartod
        self.use_units = use_units
        self.kwargs_search = kwargs_search or {}
        
        allowed_datatypes = ("platform2", "sensor_station")
        if datatype not in allowed_datatypes:
            raise KeyError(f"Datatype must be one of {allowed_datatypes} but is {datatype}")

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
            # if end_time is None:
            #     raise ValueError("Since start_time is not None, end_time also must not be None.")
            if not isinstance(start_time, (str, datetime)):
                raise TypeError(
                    f"Expecting a datetime for start_time argument: {repr(start_time)}"
                )
            # if isinstance(start_time, str):
            #     start_time = pd.Timestamp(start_time)#.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
            self.kwargs_search["min_time"] = start_time# f"{start_time:%Y-%m-%dT%H:%M:%S}"

        if end_time is not None:
            # if start_time is None:
            #     raise ValueError("Since end_time is not None, start_time also must not be None.")
            if not isinstance(end_time, (str, datetime)):
                raise TypeError(
                    f"Expecting a datetime for end_time argument: {repr(end_time)}"
                )
            # if isinstance(end_time, str):
            #     end_time = pd.Timestamp(end_time)#.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")
            self.kwargs_search["max_time"] = end_time# f"{end_time:%Y-%m-%dT%H:%M:%S}"

        if search_for is not None:
            if not isinstance(search_for, str):
                raise TypeError(
                    f"Expecting string for search_for argument: {repr(search_for)}"
                )
            self.kwargs_search["search_for"] = search_for


        # if self.kwargs_search is not None:
        checks = [
            ["min_lon", "max_lon", "min_lat", "max_lat"],
            ["min_time", "max_time"],
        ]
        for check in checks:
            if any(key in self.kwargs_search for key in check) and not all(
                key in self.kwargs_search for key in check
            ):
                raise ValueError(
                    f"If any of {check} are input, they all must be input."
                )

        if "min_lon" in self.kwargs_search and "max_lon" in self.kwargs_search:
            min_lon, max_lon = self.kwargs_search["min_lon"], self.kwargs_search["max_lon"]
            if isinstance(min_lon, (int, float)) and isinstance(
                max_lon, (int, float)
            ):
                if abs(min_lon) > 180 or abs(max_lon) > 180:
                    raise ValueError(
                        "`min_lon` and `max_lon` must be in the range -180 to 180."
                    )

        # else:
        #     kwargs_search = {}
        # self.kwargs_search = kwargs_search

        # input keys_to_match OR standard_names but not both
        if keys_to_match is not None and standard_names is not None:
            raise ValueError(
                "Input either `keys_to_match` or `standard_names` but not both."
            )

        self.pglabels: Optional[list] = None
        if keys_to_match is not None:
            self.pglabels = match_key_to_parameter(astype(keys_to_match, list))
        elif standard_names is not None:
            self.pglabels = match_std_names_to_parameter(astype(standard_names, list))

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

    def search_url(self, pglabel: Optional[str] = None) -> str:
        """Set up url for search."""

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

        if "search_for" in self.kwargs_search:
            url += f"&query={self.kwargs_search['search_for']}"

        # if self.pglabel is not None:

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

        Returns
        -------
        list
            List of search urls.
        """

        if self.pglabels is not None:
            search_urls = []
            for pglabel in self.pglabels:
                # import pdb; pdb.set_trace()
                search_urls.append(self.search_url(pglabel))
        else:
            search_urls = [self.search_url()]

        return search_urls

    def _load_metadata(self, results) -> dict:  #: Dict[str, str]
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

        # items = ["institution", "geospatial_bounds"]
        # values = itemgetter(*items)(results["source"]["meta"]["attributes"])
        # metadata.update(dict(zip(items, values)))
        # import pdb; pdb.set_trace()
        if self.datatype == "platform2":
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
        
        elif self.datatype == "sensor_station":
            
            # INSTITUTION?
            # location is lon, lat, depth and type
            # e.g. {'coordinates': [-123.711083, 38.914556, 0.0], 'type': 'Point'}
            lon, lat, depth = results["source"]["location"]["coordinates"]
            keys = ["minLongitude", "minLatitude", "maxLongitude", "maxLatitude"]
            metadata.update(dict(zip(keys, [lon, lat, lon, lat])))
            
            # internal id?
            # e.g. 106793
            metadata["internal_id"] = results["source"]["id"]
            
            # Parameter group IDs is probably closest to variables
            # e.g. [6, 7, 8, 9, 25, 26, 186]
            # results["source"]["parameterGroupIds"]
            
            # variables, standard_names (or at least parameterNames)
            # HERE SAVE VARIABLE NAMES
            figs = results["source"]["figures"]
            
            # add a section of metadata that has all details for API            
            
            # KEEP VARIABLES NAMES
            # out = [(fig["label"], fig["parameterGroupId"]) for fig in figs]
            # import pdb; pdb.set_trace()
            out = {subPlot["datasetVariableId"]: {"parameterGroupLabel": fig["label"], 
                                                  "parameterGroupId": fig["parameterGroupId"], 
                                                  "datasetVariableId": subPlot["datasetVariableId"], 
                                                  "parameterId": subPlot["parameterId"],
                                                  "label": subPlot["label"],
                                                  "deviceId": subPlot["deviceId"]}
                   for fig in figs for plot in fig["plots"] for subPlot in plot["subPlots"]}
            metadata["variables_details"] = out
            metadata["variables"] = list(out.keys())
            
            # include datumConversion info if present
            if len(results["data"]["datumConversions"]) > 0:
                metadata["datumConversions"] = results["data"]["datumConversions"]
            
            # # out = [(fig["label"], fig["parameterGroupId"], subPlot["datasetVariableId"], subPlot["parameterId"], subPlot["label"], subPlot["deviceId"]) for fig in figs for plot in fig["plots"] for subPlot in plot["subPlots"]]
            # # out = [(fig["label"], fig["parameterGroupId"], subPlot["datasetVariableId"], subPlot["parameterId"], subPlot["label"], subPlot["deviceId"]) for fig in figs for subPlot in fig["plots"][0]["subPlots"]]
            # pglabels, pgids, variables, parameterIds, labels, deviceIds = zip(*out)
            # metadata["pglabels"] = list(pglabels)
            # metadata["pgids"] = list(pgids)
            # metadata["variables"] = list(variables)
            # metadata["parameterIds"] = list(parameterIds)
            # metadata["labels"] = list(labels)
            # metadata["deviceIds"] = list(deviceIds)
            
            filter = f"%7B%22stations%22:%5B%22{metadata['internal_id']}%22%5D%7D"
            baseurl = "https://sensors.axds.co/api"
            metadata_url = f"{baseurl}/metadata/filter/custom?filter={filter}"
            metadata["metadata_url"] = metadata_url

            # also save units here
            
            # 1 or 2?
            metadata["version"] = results["data"]["version"]
            # import pdb; pdb.set_trace()

        return metadata

    def _load_all_results(self):

        all_results = []
        for search_url in self.get_search_urls():
            res = requests.get(search_url, headers=search_headers).json()
            if "results" not in res:
                raise ValueError(
                    f"No results were returned for the search. Search url: {search_url}."
                )

            if self.verbose:
                print(
                    f"For search url {search_url}, number of results found: {len(res['results'])}. Page size: {self.page_size}."
                )

            all_results.extend(res["results"])
        return all_results

    def _load(self):
        """Find all dataset ids and create catalog."""

        results = self._load_all_results()

        if self.verbose:
            unique = set([res["uuid"] for res in results])
            print(
                f"Total number of results found: {len(results)}, but unique results: {len(unique)}."
            )

        self._entries = {}
        for result in results:
            dataset_id = result["uuid"]

            # don't repeat an entry (it won't actually allow you to, but probably saves time not to try)
            if dataset_id in self._entries:
                continue

            if self.verbose:
                print(f"Dataset ID: {dataset_id}")
                
            # # don't include V1 stations
            # if result["data"]["version"] == 1:
            #     if self.verbose:
            #         print(f"Station with dataset_id {dataset_id} is V1 so is being skipped.")
            #     continue

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
            
            metadata = self._load_metadata(result)
            
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
                args = {"dataset_id": dataset_id,
                        "internal_id": metadata["internal_id"],
                        "kwargs_search": self.kwargs_search,
                        "qartod": self.qartod,
                        "use_units": self.use_units,
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
            #     # import pdb; pdb.set_trace()

            #     # # gather metadata — this is at the module level. Is it different by layer_group?
            #     # max_lat, min_lat = results["data"]["max_lat"], results["data"]["min_lat"]
            #     # max_lng, min_lng = results["data"]["max_lng"], results["data"]["min_lng"]
            #     # slug = results["data"]["model"]["slug"]
            #     # start_time, end_time = results["data"]["start_time_utc"], results["data"]["end_time_utc"]
            #     # # there are description and label too but are they the same for module and layer_group?


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
            # entry._metadata = {
            #     # "info_url": f"{self.url_docs_base}&id={dataset_id}",
            #     "dataset_id": dataset_id,
            # }

            entry._plugin = [plugin]

            self._entries[dataset_id] = entry
