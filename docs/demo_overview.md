---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3.10.8 ('intake-axds')
  language: python
  name: python3
---

# Overview

Use `intake-axds` to create intake catalogs containing sources in Axiom databases representing datasets. You can search in time and space as well as by variable and text to narrow to datasets for your project, then easily read in the data.

```{code-cell} ipython3
import intake
```

## Datatypes

The default page size is 10, so requesting a datatype without any other input arguments will return the first 10 datasets of that datatype. The input argument `page_size` controls the maximum number of entries in the catalog.


### Sensors (fixed location dataset like buoys)

Access sensor datasets by creating an AXDS catalog with `datatype="sensor_station"`. Note that webcam data is ignored.

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype="sensor_station", page_size=10)
len(cat)
```

See what search was performed with `.get_search_urls()`.

```{code-cell} ipython3
cat.get_search_urls()
```

See catalog-level metadata:

```{code-cell} ipython3
cat
```

What sources make up the catalog?

```{code-cell} ipython3
list(cat)
```

See source-level metadata for first source in catalog:

```{code-cell} ipython3
cat[list(cat)[0]]
```

Read data from first source in catalog. Note that since no start time or stop time was entered, the full data range will be read in, along with all available variables. The output is a DataFrame.

```{code-cell} ipython3
cat[list(cat)[0]].read()
```

#### Sensor-specific options

Options that are specific to sensors are QARTOD, units, and binning.

##### QARTOD

All time series available for sensors optionally come with an aggregate QARTOD flag time series.

By default, QARTOD flags are not returned, but will be returned if `qartod=True` is input to the call for catalog. Alternatively, a user can select that values that correspond to specific flags should be returned (with other values nan'ed out) with an input like `qartod=[1,2]` to only return the values that either pass the QARTOD tests or were not tested. Is not available if `binned==True`.

Flags are:

* 1: Pass
* 2: Not Evaluated
* 3: Suspect
* 4: Fail
* 9: Missing Data

More information on QARTOD is available [here](https://cdn.ioos.noaa.gov/media/2020/07/QARTOD-Data-Flags-Manual_version1.2final.pdf).

##### Units

By defaults units will be returned, syntax is "standard_name [units]". If False, no units will be included and then the syntax for column names is "standard_name".

##### Binning

By default, raw data for sensors is returned. However, binned data can instead by returned by entering `binned=True` and `bin_interval` options of hourly, daily, weekly, monthly, yearly. If `bin_interval` is input, binned is set to True.


##### Examples

For example, the following would return data columns as well as associated QARTOD columns, without units in the column names:

```
cat = intake.open_axds_cat(datatype="sensor_station", qartod=True, use_units=False)
```

This example would return data columns binned monthly:

```
cat = intake.open_axds_cat(datatype="sensor_station", bin_interval="monthly")
```

+++

### Platforms (traveling sensor, like gliders)

Access platforms datasets by creating an AXDS catalog with `datatype="platform2"`. Everything should work the same as demonstrated for sensors.

Data is output into a DataFrame for platforms. It is accessed by parquet file if available and otherwise by csv.

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype="platform2")
list(cat)
```

See source-level metadata for first source in catalog:

```{code-cell} ipython3
cat[list(cat)[0]]
```

## Filter in time and space

When setting up an AXDS intake catalog, you can narrow your search in time and space. The longitude values `min_lon` and `max_lon` should be in the range -180 to 180. You can search through the `kwargs_search` keyword or you can search explicitly using `bbox` (min_lon, min_lat, max_lon, max_lat) and `start_time` and `end_time`.

```{code-cell} ipython3
kw = {
    "min_lon": -180,
    "max_lon": -158,
    "min_lat": 50,
    "max_lat": 66,
    "min_time": '2015-1-1',
    "max_time": '2015-1-2',
}

cat = intake.open_axds_cat(datatype='sensor_station', kwargs_search=kw, page_size=5)
len(cat)
```

```{code-cell} ipython3
cat[list(cat)[0]]
```

## Filter with keyword(s)

You can also narrow your search by one or more keywords, by passing a string or list of strings with `kwargs_search["search_for"]` or explicitly using `search_for`. If you input more than one string, be aware that the multiple searches required will be combined according to `query_type`, either as a logical OR if `query_type=="union"` or as a logical AND if `query_type=="intersection"`.

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype='platform2', search_for=["whale", "bering"],
                           query_type="intersection", page_size=1000)
len(cat)
```

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype='platform2', search_for=["whale", "bering"],
                           query_type="union", page_size=1000)
len(cat)
```

## Filter by variable

This section describes two approaches for searching by variable. As with `search_for`, how multiple variable requests are combined depends on the input choice of `query_type`. However, in the case of variables there are three options for `query_type`:

* `query_type=="union"` logical OR
* `query_type=="intersection"` as a logical AND
* `query_type=="intersection_constrained"` as a logical AND but also only the requested variables are returned.

+++

### Select variable(s) to search for by standard_name

Check available standard names with:

```{code-cell} ipython3
import intake_axds

standard_names = intake_axds.utils.available_names()
len(standard_names), standard_names[:5]
```

Make a catalog of sensors that contain either of the standard_names input.

```{code-cell} ipython3
std_names = ["sea_water_practical_salinity", "sea_water_temperature"]
cat = intake.open_axds_cat(datatype="sensor_station", standard_names=std_names,
                           query_type="union")
cat[list(cat)[0]].metadata["variables"]
```

Make a catalog of sensors that contain both of the standard_names input.

```{code-cell} ipython3
std_names = ["sea_water_practical_salinity", "sea_water_temperature"]
cat = intake.open_axds_cat(datatype="sensor_station", standard_names=std_names,
                           query_type="intersection", page_size=100)
cat[list(cat)[0]].metadata["variables"]
```

Make a catalog of sensors that contain both of the standard_names input but then also only return those two variable types. All variables available in the dataset will still be present in the metadata, but only values for those requested will be returned in the DataFrame. We can look at the catalog metadata to see the parameterGroupIds and parameterGroupLables that will be used in data collection.

```{code-cell} ipython3
std_names = ["sea_water_practical_salinity", "sea_water_temperature"]
cat = intake.open_axds_cat(datatype="sensor_station", standard_names=std_names,
                           query_type="intersection_constrained", page_size=100)
cat
```

If you request standard_names that aren't present in the system, you will be told (cell commented out but will return exception and say that they aren't present).

```{code-cell} ipython3
# std_names = "sea_water_surface_salinity"
# cat = intake.open_axds_cat(datatype="sensor_station", standard_names=std_names)
```

### Select variable(s) to search for by custom vocabulary

Instead of selecting the exact standard_names to search on, you can set up a collections of regular expressions to match on the variables you want. This is particularly useful if you are running with several different searches and ultimately will need to select data variables from datasets using a generic name.

#### Set up vocabulary

One way to set up a custom vocabulary is with a helper class from `cf-pandas` (see more information in the [docs](https://cf-pandas.readthedocs.io/en/latest/index.html)). Choose a nickname for each variable you want to be able to match on, like "temp" for matching sea water temperature variables, then set up the regular expressions you want to "count" as your variable "temp" — you can use the "Reg" class from `cf-pandas` to write these expressions easily. The following example shows setting up a custom vocabulary for identifying variables of "temp", "salt", and "ssh".

```{code-cell} ipython3
import cf_pandas as cfp

nickname = "temp"
vocab = cfp.Vocab()

# define a regular expression to represent your variable
reg = cfp.Reg(include="temp", exclude=["air","qc","status","atmospheric"])

# Make an entry to add to your vocabulary
vocab.make_entry(nickname, reg.pattern(), attr="name")

vocab.make_entry("salt", cfp.Reg(include="sal", exclude=["soil","qc","status"]).pattern(), attr="name")
vocab.make_entry("ssh", cfp.Reg(include=["sea_surface_height","surface_elevation"], exclude=["qc","status"]).pattern(), attr="name")

# what does the vocabulary look like?
vocab.vocab
```

You can use your custom vocab with a context manager, as in the following example. Alternatively, you can set the vocabulary up so all commands will know about it:

```
cf_xarray.set_options(custom_criteria=vocab.vocab)  # for cf-xarray
cfp.set_options(custom_criteria=vocab.vocab)  # for cf-pandas
```

```{code-cell} ipython3
with cfp.set_options(custom_criteria=vocab.vocab):
    cat = intake.open_axds_cat(datatype="platform2", keys_to_match=["temp","salt"])
cat[list(cat)[0]].metadata["variables"]
```

## Catalog metadata and options

Can provide metadata at the catalog level with input arguments `name`, `description`, and `metadata` to override the defaults.

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype="platform2", name="Catalog name", description="This is the catalog.", page_size=1,
                           metadata={"special entry": "platforms"})
cat
```

### ttl

The default `ttl` argument, or time before force-reloading the catalog, is `None`, but can be overridden by inputting a value:

```{code-cell} ipython3
cat.ttl is None
```

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype="platform2", page_size=1, ttl=60)
cat.ttl
```

### Verbose

Get information as the catalog function runs.

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype="sensor_station", verbose=True, page_size=1)
```

## Sensor Source

You can use the intake `AXDSSensorSource` directly with `intake.open_axds_sensor` if you know the `dataset_id` (UUID) or the `internal_id` (Axiom station id). Alternatively, you can search using `intake.open_axds_cat` for a sensor if you know the dataset_id and search for it with "search_for".

Note that only some metadata will be available until the dataset is read in, at which point the full metadata is also read in.

```{code-cell} ipython3
source = intake.open_axds_sensor(internal_id=110532, bin_interval="monthly")
source
```

```{code-cell} ipython3
source.read()
```

If you prefer a catalog approach for a known dataset_id, you can do that like this:

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype="sensor_station", search_for="ism-aoos-noaa_nos_co_ops_9469439",
                           verbose=True)
```

```{code-cell} ipython3
cat[list(cat)[0]]
```

You can request only specific data variable(s) be returned directly in the Sensor Source, though you need to know the parameterGroupId. You could access this by running the desired source once and looking at the metadata to select the IDs you want to use. For example using the information from the previous catalog listed immediately above, we could set up the following:

```{code-cell} ipython3
source = intake.open_axds_sensor(internal_id=110532, bin_interval="monthly", only_pgids=[47])
source
```
