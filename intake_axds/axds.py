from typing import Optional
from intake.source import base
# from . import __version__
# from erddapy import ERDDAP
import requests
import pandas as pd

from .utils import load_metadata, response_from_url, make_metadata_url, make_filter, make_label, make_data_url, make_search_docs_url


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
    name = 'axds-sensor'
    version = '0.0.1'
    container = 'dataframe'
    partition_access = True
    
    
    def __init__(self, internal_id=None, dataset_id=None, start_time=None, end_time=None, qartod: bool = False, use_units: bool = True, metadata=None):
        
        if internal_id is None and dataset_id is None:
            raise ValueError("internal_id and dataset_id cannot both be None. Input one of them.")
    
        self.url_search_base = "https://search.axds.co/v2/search?portalId=-1&page=1&pageSize=10000&verbose=true"
        self.url_docs_base = "https://search.axds.co/v2/docs?verbose=true"
        self.dataset_id = dataset_id
        self.start_time = start_time
        self.end_time = end_time
        self.internal_id = internal_id
        self.qartod = qartod
        self.use_units = use_units
        
        # need dataset_id to get metadata
        if self.dataset_id is None:
            res = response_from_url(make_metadata_url(make_filter(self.internal_id)))
            self.dataset_id = res["data"]["stations"][0]["uuid"]
        # need internal_id to get data
        elif self.internal_id is None:
            self.internal_id = response_from_url(make_search_docs_url(self.dataset_id))[0]["id"]

        self._dataframe = None
        
        metadata = metadata or {}
        metadata["dataset_id"] = self.dataset_id

        # this is what shows in the source if you print it
        self._captured_init_kwargs.update({
            "internal_id": self.internal_id,
            "dataset_id": self.dataset_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "qartod": self.qartod,
            "use_units": self.use_units,
        })

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
                filters.append(make_filter(self.internal_id, pgid))
                # filters.append(f"%7B%22stations%22%3A%5B{self.internal_id}%5D%2C%22parameterGroups%22%3A%5B{pgid}%5D%7D")
        else:
            filters.append(make_filter(self.internal_id))
            # filters.append(f"%7B%22stations%22:%5B%22{self.internal_id}%22%5D%7D")
            # filters.append(f"%7B%22stations%22%3A%5B{self.internal_id}%5D")
        return filters
    
    def _load_to_dataframe(self, url):
        """load from raw data url to DataFrame.
        
        For V1 stations the result of a call to this function will be one of potentially many calls for data, but there will be only one loop below in a call.
        For V2 stations the result of a call to this function will be the only call for data, but there may be several loops in the call.
        """
        data_raw = response_from_url(url)

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
            feed_ind["column_name"] = make_label(feed_ind["label"], feed_ind.get("units", None), use_units=self.use_units) 
            indices[feed_ind["index"]] = feed_ind
            
            # in this case, z or depth is also an index
            if feed["metadata"]["z"] is not None:
                feed_ind = feed["metadata"]["z"]
                feed_ind["column_name"] = make_label(feed_ind["label"], feed_ind.get("units", None), use_units=self.use_units) 
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
                data_cols[index]["column_name"] = make_label(label, data_cols[index].get("units",None), use_units=self.use_units)

            columns.update(data_cols)
                        
            # whether or not to read in QARTOD Aggregation flags is chosen at the catalog level in axds_cat.py
            # columns only includes the QA column info if qartod is True
            # or, include QARTOD columns but then remove data rows based on the flag values.
            qartod = self.qartod
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
        # df = pd.concat(dfs, axis=1)
        # dfs[0].join(dfs[1:], how='outer', sort=True)

        df = dfs[0]
        # this gets different and I think better results than dfs[0].join(dfs[1:], how="outer", sort=True)
        # even though they should probably return the same thing.
        for i in range(1, len(dfs)):
            df = df.join(dfs[i], how='outer', sort=True) 
        # import pdb; pdb.set_trace()
        return df

    def _load(self):
        """How to load in a specific station once you know it by dataset_id"""
        
        # get extended metadata which we need both for reading the data and as metadata
        result = response_from_url(make_search_docs_url(self.dataset_id))[0]
        self.metadata.update(load_metadata("sensor_station", result))

        start_time = self.start_time or self.metadata['minTime']
        end_time = self.end_time or self.metadata['maxTime']
        
        filters = self._get_filters()
        
        dfs = []
        for filter in filters:
            self.data_raw_url = make_data_url(filter, start_time, end_time)
            # data_raw_url = f"{baseurl}/observations/filter/custom?filter={filter}&start={start_date}Z&end={end_date}Z"
            dfs.append(self._load_to_dataframe(self.data_raw_url))

        df = dfs[0]
        # this gets different and I think better results than dfs[0].join(dfs[1:], how="outer", sort=True)
        # even though they should probably return the same thing.
        for i in range(1, len(dfs)):
            df = df.join(dfs[i], how='outer', sort=True) 

        self._dataframe = df

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
