from typing import Any, Dict

from encrypted_search.models.level_info import LevelInfo
from encrypted_search.types import LevelInfos, LookupTable


def levels_to_json(levels: LevelInfos) -> Dict[str, Any]:
    """Utility method to serialize a `dict` of `LevelInfo` objects.

    Args:
        levels: Mapping from level indices to corresponding LevelInfo objects

    Returns:
        JSON-like dict; the serialized form of `levels`.
    """

    return {str(i): __level_info_to_json(level) for i, level in levels.items()}


def __level_info_to_json(level: LevelInfo) -> Dict[str, Any]:
    return {
        "array_size": level.array_size,
        "large_bucket_size": level.large_bucket_size,
        "number_of_large_buckets": level.number_of_large_buckets,
        "small_bucket_size": level.small_bucket_size,
        "number_of_buckets": level.number_of_buckets,
        "large_chunk_size": level.large_chunk_size,
    }


def lookup_table_to_json(lookup_table: LookupTable) -> Dict[str, Any]:
    """Utility method to serialize a lookup table.

    Args:
        lookup_table: Map from a keyword to corresponding location in the datastore

    Returns:
        JSON-like dict; the serialized form of `lookup_table`.
    """

    return {
        keyword: [location.to_json() for location in locations]
        for keyword, locations in lookup_table.items()
    }
