---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3.10.6 ('intake-axds')
  language: python
  name: python3
---

# Overview

```{code-cell} ipython3
import intake
```

## First 10

The default page size is 10, so requesting platforms without any other input arguments will return the first 10 datasets. The input argument `page_size` controls the maximum number of entries in the catalog.

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype='platform2')
len(cat)
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
    "max_time": '2020-1-1',
}

cat = intake.open_axds_cat(datatype='platform2', kwargs_search=kw)
cat
```

## Output container type

+++

### Dataframe

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype='platform2', outtype='dataframe')
source_name = list(cat)[0]
cat[source_name].read()
```

### xarray

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype='platform2', outtype='xarray')
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

```{code-cell} ipython3
cat = intake.open_axds_cat(verbose=True)
```

```{code-cell} ipython3

```
