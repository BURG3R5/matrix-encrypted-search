from typing import Any, Dict

from encrypted_search.index import EncryptedIndex
from encrypted_search.models.location import Location


def index_from_json(json: Dict[str, Any]) -> EncryptedIndex:
    """Deserializes JSON map into an `EncryptedIndex` object.

    Args:
        json: Serialized data in the form of a `dict`

    Returns:
        Deserialized `EncryptedIndex` object.
    """
    encrypted_index = EncryptedIndex([])

    encrypted_index.s = json["s"]
    encrypted_index.L = json["L"]
    encrypted_index.keywords = json["keywords"]

    raw_datastore = json["datastore"]
    encrypted_index.datastore = {
        int(level_index): level
        for level_index, level in raw_datastore.items()
    }

    raw_lookup_table = json["lookup_table"]
    encrypted_index.lookup_table = {
        keyword: [Location.from_json(location) for location in locations]
        for keyword, locations in raw_lookup_table.items()
    }

    return encrypted_index
