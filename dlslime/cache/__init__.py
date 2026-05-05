"""DLSlimeCache Python service/client wrappers."""

from dlslime._slime_c import cache as ccache
from .client import CacheClient, discover_cache_url, get_json, post_json
from .service import (
    CacheHttpServer,
    CacheService,
    DEFAULT_CACHE_MR_NAME,
    DEFAULT_MEMORY_SIZE,
    DEFAULT_SLAB_SIZE,
    NanoCtrlRegistration,
    resolve_host_for_registration,
)
from .types import (
    assignment_from_json,
    assignment_to_json,
    assignment_to_tuple,
    manifest_to_json,
    stats_to_json,
)

AssignmentManifest = ccache.AssignmentManifest
CacheServer = ccache.CacheServer
CacheStats = ccache.CacheStats
Extent = ccache.Extent
Manifest = ccache.Manifest

__all__ = [
    "AssignmentManifest",
    "CacheClient",
    "CacheHttpServer",
    "CacheServer",
    "CacheService",
    "CacheStats",
    "DEFAULT_CACHE_MR_NAME",
    "DEFAULT_MEMORY_SIZE",
    "DEFAULT_SLAB_SIZE",
    "Extent",
    "Manifest",
    "NanoCtrlRegistration",
    "assignment_from_json",
    "assignment_to_json",
    "assignment_to_tuple",
    "ccache",
    "discover_cache_url",
    "get_json",
    "manifest_to_json",
    "post_json",
    "resolve_host_for_registration",
    "stats_to_json",
]
