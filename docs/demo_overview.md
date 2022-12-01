---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Overview

```{code-cell} ipython3
import intake
```

## First 10

The default page size is 10, so requesting platforms without any other input arguments will return the first 10 datasets.

```{code-cell} ipython3
cat = intake.open_axds_cat(datatype='platform2')
len(cat)
```

## Filter in time and space

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
len(cat)
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
