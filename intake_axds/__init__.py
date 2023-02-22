"""
intake-axds: Intake approach for Axiom assets.
"""

import intake  # noqa: F401, need this to eliminate possibility of circular import

# from .axds_cat import AXDSCatalog
from .utils import _get_version, available_names, match_key_to_parameter  # noqa: F401


__version__ = _get_version()
