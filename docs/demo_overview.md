---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3.9.13 ('intake-axds_docs')
  language: python
  name: python3
---

# Overview

```{code-cell} ipython3
import intake
```

## Defaults

The default (and currently only) data type is "platform2".
The default page size is 10, so requesting platforms without any other input arguments will return the first 10 datasets. The input argument `page_size` controls the maximum number of entries in the catalog.

```{code-cell} ipython3
cat = intake.open_axds_cat()
len(cat)
```

```{code-cell} ipython3
cat.get_search_urls()
```

```{code-cell} ipython3
cat
```

```{code-cell} ipython3
cat[list(cat)[0]]
```

## Filter in time and space

The longitude values `min_lon` and `max_lon` should be in the range -180 to 180.

```{code-cell} ipython3
kw = {
    "min_lon": -180,
    "max_lon": -158,
    "min_lat": 50,
    "max_lat": 66,
    "min_time": '2015-1-1',
    "max_time": '2015-1-2',
}

cat = intake.open_axds_cat(datatype='platform2', kwargs_search=kw, page_size=1000)
len(cat)
```

## Additionally filter with keyword

```{code-cell} ipython3
kw = {
    "min_lon": -180,
    "max_lon": -158,
    "min_lat": 50,
    "max_lat": 66,
    "min_time": '2015-1-1',
    "max_time": '2020-1-1',
    "search_for": "humpback"
}

cat = intake.open_axds_cat(datatype='platform2', kwargs_search=kw, page_size=1000)
len(cat)
```

## Output container is dataframe for platforms

For dataframes, the data by default comes from csv files, but can be accessed by parquet files instead. The `dataframe_filetype` argument controls this. About half of the datasets are not available from parquet files.

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype='platform2')
source_name = list(cat)[0]
cat[source_name].read()
```

## Catalog metadata and options

Can provide metadata at the catalog level with input arguments `name`, `description`, and `metadata` to override the defaults.

```{code-cell} ipython3
cat = intake.open_axds_cat(name="Catalog name", description="This is the catalog.", page_size=1,
                           metadata={"special entry": "platforms"})
cat
```

The default `ttl` argument, or time before force-reloading the catalog, is `None`, but can be overridden by inputting a value:

```{code-cell} ipython3
cat.ttl is None
```

```{code-cell} ipython3
cat = intake.open_axds_cat(page_size=1, ttl=60)
cat.ttl
```

## Verbose

Get information as the catalog function runs.

```{code-cell} ipython3
cat = intake.open_axds_cat(verbose=True)
```

## Variable handling

This section describes two approaches for if you want to only return datasets that contain certain variables.

+++

### Select variable(s) to search for by standard_name

+++

#### Check available standard names

What standard_names are available in the Axiom system?

```{code-cell} ipython3
import intake_axds

standard_names = intake_axds.utils.available_names()
len(standard_names), standard_names[:5]
```

#### Create catalog of platforms that contain any of the standard_names input.

```{code-cell} ipython3
std_names = ["sea_water_practical_salinity","sea_water_temperature"]
cat = intake.open_axds_cat(standard_names=std_names)
```

```{code-cell} ipython3
list(cat)
```

### Select variable(s) to search for by custom vocabulary

Instead of selecting the exact standard_names so search on, you can set up a series of regular expressions to match on the variables you want. This is particularly useful if you are running with several different searches and ultimately will need to select data variables from datasets using a generic name.

#### Set up vocabulary

One way to set up a custom vocabulary is with a helper class from `cf-pandas` (see more information in the [docs](https://cf-pandas.readthedocs.io/en/latest/index.html)):

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


vocab.make_entry("T", cfp.Reg(include=["time"]).pattern(), attr="name")
vocab.make_entry("longitude", cfp.Reg(include=["lon"]).pattern(), attr="name")
vocab.make_entry("latitude", cfp.Reg(include=["lat"]).pattern(), attr="name")

# what does the vocabulary look like?
vocab.vocab
```

Use vocab with context manager, as in the following example. Alternatively, you can set the vocabulary up so all commands will know about it:

```
cf_xarray.set_options(custom_criteria=vocab.vocab)  # for cf-xarray
cfp.set_options(custom_criteria=vocab.vocab)  # for cf-pandas
```

```{code-cell} ipython3
with cfp.set_options(custom_criteria=vocab.vocab):
    cat = intake.open_axds_cat(keys_to_match=["temp","salt"])
```

This catalog contains the same datasets as the other variable example since they are demonstrating two approaches to the same end goal.

```{code-cell} ipython3
list(cat)
```

```{code-cell} ipython3

```
