from intake.source import base
# from . import __version__
# from erddapy import ERDDAP
import requests
import pandas as pd

search_headers = {"Accept": "application/json"}
baseurl = "https://sensors.axds.co/api"

def make_label(label, units):
    """make label."""
    if units is None:
        return f"{label}"
    else:
        return f"{label} [{units}]"


class AXDSSensorSource(base.DataSource):
    """
    sensor_station only
    
    Parameters
    ----------
    dataset_id: str
    variables: list

    Returns
    -------
    Dataframe
    """
    name = 'axds-sensor-dataframe'
    version = '0.0.1'
    container = 'dataframe'
    partition_access = True
    
    
    def __init__(self, dataset_id, kwargs_search, internal_id, metadata={}):
        self.url_search_base = "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10000&verbose=true"
        self.url_docs_base = "https://search.axds.co/v2/docs?verbose=true"
        self.dataset_id = dataset_id
        self.kwargs_search = kwargs_search
        self.internal_id = internal_id
#         self.axds_type = 'platform2'

#         self._variables = variables
        self._dataframe = None

        super(AXDSSensorSource, self).__init__(metadata=metadata)
    
    def _get_filters(self):
        """Return appropriate filter for stationid.
        
        What filter form to use depends on if V1 or V2.
        
        For V1, use each parameterGroupId only once to make a filter since all data of that type will be read in together.
        
        Following Sensor API
        https://admin.axds.co/#!/sensors/api/overview
        """
        filters = []
        # import pdb; pdb.set_trace()
        if self.metadata["version"] == 1:
            pgids = [self.metadata["variables_details"][var]["parameterGroupId"] for var in self.metadata["variables_details"]]
            for pgid in list(set(pgids)):
                filters.append(f"%7B%22stations%22%3A%5B{self.internal_id}%5D%2C%22parameterGroups%22%3A%5B{pgid}%5D%7D")
        else:
            filters.append(f"%7B%22stations%22:%5B%22{self.internal_id}%22%5D%7D")
            # filters.append(f"%7B%22stations%22%3A%5B{self.internal_id}%5D")
        return filters
    
    def _load_to_dataframe(self, url):
        """load from raw data url to DataFrame.
        
        For V1 stations the result of a call to this function will be one of potentially many calls for data, but there will be only one loop below in a call.
        For V2 stations the result of a call to this function will be the only call for data, but there may be several loops in the call.
        """
        
        data_raw = requests.get(url, headers=search_headers).json()
        # import pdb; pdb.set_trace()
        # check for presence of any data
        if len(data_raw["data"]["groupedFeeds"]) == 0:
            self._dataframe = None
            import pdb; pdb.set_trace()
            return
        
        # loop over the data feeds and read the data into DataFrames
        # link to other metadata as needed
        dfs = []
        for feed in data_raw["data"]["groupedFeeds"]:
            columns = {}  # all non-index columns in dataframe
            indices = {}  # indices for dataframe
            
            # indices first: time and potentially z
            feed_ind = feed["metadata"]["time"]
            feed_ind["column_name"] = make_label(feed_ind["label"], feed_ind.get("units", None)) 
            indices[feed_ind["index"]] = feed_ind
            
            # in this case, z or depth is also an index
            if feed["metadata"]["z"] is not None:
                feed_ind = feed["metadata"]["z"]
                feed_ind["column_name"] = make_label(feed_ind["label"], feed_ind.get("units", None)) 
                indices[feed_ind["index"]] = feed_ind

            # These should never be non-None for sensors
            if feed["metadata"]["lon"] is not None or feed["metadata"]["lat"] is not None:
                lon, lat = feed["metadata"]["lon"], feed["metadata"]["lat"]
                raise ValueError(f"lon/lat should be None for sensors but are {lon}, {lat}.")
            
            # add data columns
            data_cols = {val["index"]: val for val in feed["metadata"]["values"]}
            # match variable name from metadata (standard_name) to be column name
            for index in data_cols:
                # variable name
                label = [var for var in self.metadata["variables_details"] if self.metadata["variables_details"][var]["deviceId"] == data_cols[index]["deviceId"]][0]
                data_cols[index]["label"] = label
                # column name
                data_cols[index]["column_name"] = make_label(label, data_cols[index].get("units",None))

            columns.update(data_cols)
                        
            # whether or not to read in QARTOD Aggregation flags is chosen at the catalog level in axds_cat.py
            # columns only includes the QA column info if qartod is True
            # or, include QARTOD columns but then remove data rows based on the flag values.
            qartod = self.cat.metadata["qartod"]
            if isinstance(qartod, (str,list)) or qartod:
            
                # add qartod columns
                qa_cols = {val["index"]: val for val in feed["metadata"]["qcAgg"]}
                # match variable name using deviceId from metadata (standard_name) to be column name
                for index in qa_cols:
                    label = [var for var in self.metadata["variables_details"] if self.metadata["variables_details"][var]["deviceId"] == qa_cols[index]["deviceId"]][0]
                    qa_cols[index]["label"] = f"{label}_qc_agg"
                    qa_cols[index]["column_name"] = qa_cols[index]["label"]
                    # import pdb; pdb.set_trace()
                    # match deviceId between data_col and qa_col to get column name associated with qa_col
                    name = [data_cols[ind]["column_name"] for ind in data_cols if data_cols[ind]["deviceId"] == qa_cols[index]["deviceId"]][0]
                    qa_cols[index]["data_name"] = name
                    columns.update(qa_cols)
        
            # col_names = [make_label(columns[i]["label"], columns[i].get("units",None)) for i in list(columns)]
            # ind_names = [make_label(indices[i]["label"], indices[i].get("units",None)) for i in list(indices)]
            
            col_names = [columns[i]["column_name"] for i in list(columns)]
            ind_names = [indices[i]["column_name"] for i in list(indices)]
            
            # import pdb; pdb.set_trace()  
            # do this in steps in case we are dropping QA columns
            df = pd.DataFrame(feed["data"])
            icolumns_to_keep = list(indices) + list(columns)
            df = df[icolumns_to_keep]
            df.columns = ind_names + col_names
            df.set_index(ind_names, inplace=True)

            # nan data for which QARTOD flags shouldn't be included
            if isinstance(qartod, (str,list)):
                if isinstance(qartod,str):
                    qartod = [qartod]
                
                for ind in qa_cols:
                    data_name = qa_cols[ind]["data_name"]  # data column name
                    qa_name = qa_cols[ind]["column_name"]  # qa column name
                    
                    for qa in qartod:
                        df.loc[df[qa_name] != qa, data_name] = pd.NA
                
                # drop qartod columns
                df.drop(labels=qa_name, axis=1, inplace=True)
                                
            dfs.append(df)
        # import pdb; pdb.set_trace()
        df = pd.concat(dfs, axis=1)
        return df

    def _load(self):
        """How to load in a specific station once you know it by dataset_id"""
        
        if "min_time" not in self.kwargs_search or "max_time" not in self.kwargs_search:
            # raise KeyError("`min_time` and `max_time` are required to load data.")
            # if self.verbose:
            #     print(f"min_time and max_time not input in kwargs_search, using times from metadata: {self.metadata['minTime']}, {self.metadata['maxTime']}.")
            min_time, max_time = self.metadata['minTime'], self.metadata['maxTime']
        else:
            min_time, max_time = self.kwargs_search["min_time"], self.kwargs_search["max_time"]
        
        # handle start and end dates (maybe this should happen in cat?)
        start_date = pd.Timestamp(min_time).strftime("%Y-%m-%dT%H:%M:%S")
        end_date = pd.Timestamp(max_time).strftime("%Y-%m-%dT%H:%M:%S")
        
        # # get variable names and matching parameter IDs for dataset
        # varnames, parids = self.metadata["variables"], self.metadata["parameterIds"]
        
        filters = self._get_filters()
        
        dfs = []
        for filter in filters:
            data_raw_url = f"{baseurl}/observations/filter/custom?filter={filter}&start={start_date}Z&end={end_date}Z"
            dfs.append(self._load_to_dataframe(data_raw_url))
        
            
            # data_raw = requests.get(data_raw_url, headers=search_headers).json()
        # move all dataframes with 2 indices to the front of the list for the join
        # [dfs.insert(0, dfs.pop(i)) for i, df in enumerate(dfs) if df.index.nlevels == 2]
        # this expands all the possible indices in the dfs in the list
        # df = df.join(dfs[1], how='outer', sort=True)
        # df = df.join(dfs[2], how='outer', sort=True)

        df = dfs[0]
        # this gets different and I think better results than dfs[0].join(dfs[1:], how="outer", sort=True)
        # even though they should probably return the same thing.
        for i in range(1, len(dfs)):
            df = df.join(dfs[i], how='outer', sort=True) 
        # import pdb; pdb.set_trace()
        # df = dfs[0].join(dfs[1:], how="inner")
        # dfs[0].join(dfs[1], how='outer', sort=True).join(dfs[2], how='outer', sort=True).join(dfs[3], how='outer', sort=True).join(dfs[4], how='outer', sort=True).join(dfs[5], how='outer', sort=True).join(dfs[6], how='outer', sort=True)
            
            
            
        
        # # Following Sensor API
        # # https://admin.axds.co/#!/sensors/api/overview
        # filter = f"%7B%22stations%22%3A%5B{self.internal_id}%5D%2C%22parameterGroups%22%3A%5B{pgid}%5D%7D"
        # # filter = f"%7B%22stations%22:%5B%22{self.internal_id}%22%5D%7D%2C%22parameterGroups%22%3A%5B{pgid}%5D%2C%22"
        # # baseurl = "https://sensors.axds.co/api"
        # # metadata_url = f"{baseurl}/metadata/filter/custom?filter={filter}"
        
        # data_raw_url = f"{baseurl}/observations/filter/custom?filter={filter}&start={start_date}Z&end={end_date}Z"
        # # data_raw_url = f"{baseurl}/observations/filter/custom/parameterGroup/22/?filter={filter}&start={start_date}Z&end={end_date}Z"
        # # data_binned_url = f"{baseurl}/observations/filter/custom/binned?filter={filter}&start={start_date}Z&end={end_date}Z&binInterval={interval}"
        
        # data_raw = requests.get(data_raw_url, headers=search_headers).json()
        
        # # check for presence of any data
        # if len(data_raw["data"]["groupedFeeds"]) == 0:
        #     self._dataframe = None
        #     import pdb; pdb.set_trace()
        #     return

        # # loop over the data feeds and read the data into DataFrames
        # # link to other metadata as needed
        
        # dfs = []
        # for feed in data_raw["data"]["groupedFeeds"]:
        #     columns = {}
        #     # time as index
        #     # root = feed["metadata"]["time"]
        #     # index, label, units = root["index"], root["label"], root["units"]
        #     columns[feed["metadata"]["time"]["index"]] = feed["metadata"]["time"]
        #     # index_label = f"{label} [{units}]"
            
        #     # add data columns
        #     data_cols = {val["index"]: val for val in feed["metadata"]["values"]}
        #     # match variable name in as label (or could use label)
        #     for index in data_cols:
        #         which_index = [i for i, parid in enumerate(self.metadata["parameterIds"]) if data_cols[index]["parameterId"] == parid][0]
        #         data_cols[index]["label"] = self.metadata["variables"][which_index]            
        #     columns.update(data_cols)
        #     # import pdb; pdb.set_trace()
            
        #     # add qartod columns
        #     columns.update({val["index"]: val for val in feed["metadata"]["qcAgg"]})
                            
        #     # not sure if these are ever non-None
        #     if feed["metadata"]["lon"] is not None or feed["metadata"]["z"] is not None:
        #         import pdb; pdb.set_trace()
            
        #     # col_names = [make_label("", columns[i]["units"]) for i in range(len(columns))]
        #     col_names = [make_label(columns[i]["label"], columns[i].get("units",None)) for i in range(len(columns))]
            
        #     df = pd.DataFrame(feed["data"], columns=col_names)
        #     # import pdb; pdb.set_trace()
        #     df.set_index(df.columns[0], inplace=True)
            
        #     dfs.append(df)

        self._dataframe = df
            
        

        # dfs = []
        # for varname, parid in zip(varnames, parids):
            
        #     # when pulling out the data, I need to match the values: index with parameterId and units to the
        #     # resulting dataframe
            
        #     # import pdb; pdb.set_trace()
        #     output = [feed["data"] for feed in data_raw["data"]["groupedFeeds"] if feed["metadata"]["values"][0]["parameterId"] == parid][0]

        #     timekey, varkey, flagkey = "time [UTC]", varname, "flags"
        #     df = pd.DataFrame(output)#, columns=[timekey, varkey, flagkey])
        #     if len(df.columns) > 3:
        #         import pdb; pdb.set_trace()
        #     # df.set_index(timekey, inplace=True)
        #     dfs.append(df)
        # import pdb; pdb.set_trace()
        # # url = f"{self.url_docs_base}&id={self.dataset_id}"
        
        # res = requests.get(url, headers=search_headers).json()
        # urlpath = res[0]['data']['resources']['files']['data.csv.gz']['url']
        
        # self._dataframe = pd.read_csv(urlpath)        

    def _get_schema(self):
        if self._dataframe is None:
            # TODO: could do partial read with chunksize to get likely schema from
            # first few records, rather than loading the whole thing
            self._load()
        return base.Schema(datashape=None,
                           dtype=self._dataframe.dtypes,
                           shape=self._dataframe.shape,
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        if self._dataframe is None:
            self._load_metadata()
        return self._dataframe

    def read(self):
        return self._get_partition(None)

    def _close(self):
        self._dataframe = None


# class AxdsPlatformDataframeSource(base.DataSource):
#     """
#     platform2 only
    
#     Parameters
#     ----------
#     dataset_id: str
#     variables: list

#     Returns
#     -------
#     Dataframe
#     """
#     name = 'axds-platform-dataframe'
#     version = '0.0.1'
#     container = 'dataframe'
#     partition_access = True
    
    
#     def __init__(self, dataset_id, metadata={}):
#         self.url_search_base = "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10000&verbose=true"
#         self.url_docs_base = "https://search.axds.co/v2/docs?verbose=true"
#         self.dataset_id = dataset_id
# #         self.axds_type = 'platform2'

# #         self._variables = variables
#         self._dataframe = None

#         super(AxdsPlatformDataframeSource, self).__init__(metadata=metadata)

#     def _load(self):
#         """How to load in a specific station once you know it by dataset_id"""

#         url = f"{self.url_docs_base}&id={self.dataset_id}"
        
#         res = requests.get(url, headers=search_headers).json()
#         urlpath = res[0]['data']['resources']['files']['data.csv.gz']['url']
        
#         self._dataframe = pd.read_csv(urlpath)        

#     def _get_schema(self):
#         if self._dataframe is None:
#             # TODO: could do partial read with chunksize to get likely schema from
#             # first few records, rather than loading the whole thing
#             self._load()
#         return base.Schema(datashape=None,
#                            dtype=self._dataframe.dtypes,
#                            shape=self._dataframe.shape,
#                            npartitions=1,
#                            extra_metadata={})

#     def _get_partition(self, _):
#         if self._dataframe is None:
#             self._load_metadata()
#         return self._dataframe

#     def read(self):
#         return self._get_partition(None)

#     def _close(self):
#         self._dataframe = None


# class AxdsPlatformXarraySource(base.DataSource):
#     """
#     platform2 only
    
#     Parameters
#     ----------
#     dataset_id: str
#     variables: list

#     Returns
#     -------
#     xarray
#     """
#     name = 'axds-platform-xarray'
#     version = '0.0.1'
#     container = 'xarray'
#     partition_access = True
    
    
#     def __init__(self, dataset_id, metadata={}):
#         self.url_search_base = "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10000&verbose=true"
#         self.url_docs_base = "https://search.axds.co/v2/docs?verbose=true"
#         self.dataset_id = dataset_id
# #         self.axds_type = 'platform2'

# #         self._variables = variables
#         self._dataframe = None

#         super(AxdsPlatformDataframeSource, self).__init__(metadata=metadata)

#     def _load(self):
#         """How to load in a specific station once you know it by dataset_id"""

#         url = f"{self.url_docs_base}&id={self.dataset_id}"
        
#         res = requests.get(url, headers=search_headers).json()

#         key = [
#             key
#             for key in res[0]['data']['resources']["files"].keys()
#             if ".nc" in key
#         ][0]
#         urlpath = res[0]['data']['resources']["files"][key]["url"]


#         # urlpath = res[0]['data']['resources']['files']['data.csv.gz']['url']
        
#         self._dataframe = pd.read_csv(urlpath)        

#     def _get_schema(self):
#         if self._dataframe is None:
#             # TODO: could do partial read with chunksize to get likely schema from
#             # first few records, rather than loading the whole thing
#             self._load()
#         return base.Schema(datashape=None,
#                            dtype=self._dataframe.dtypes,
#                            shape=self._dataframe.shape,
#                            npartitions=1,
#                            extra_metadata={})

#     def _get_partition(self, _):
#         if self._dataframe is None:
#             self._load_metadata()
#         return self._dataframe

#     def read(self):
#         return self._get_partition(None)

#     def _close(self):
#         self._dataframe = None