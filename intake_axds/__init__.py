from pkg_resources import DistributionNotFound, get_distribution

from .axds_cat import AXDSCatalog

try:
    __version__ = get_distribution("intake-axds").version
except DistributionNotFound:
    # package is not installed
    __version__ = "unknown"

