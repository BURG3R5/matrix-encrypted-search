from typing import Dict, List, Set, Tuple

from .models.location import Location
from .types import LookupTable
from .utils.normalizer import normalize


class EncryptedSearch:
    """Representation of the scope in which encrypted searches can be executed.

        A search is divided into three steps:
            1. Lookup: Find relevant locations in all lookup tables in the scope of this search.
            2. Fetch: Get relevant files from content repositories. Must be completed by external client.
            3. Locate: Collect and shortlist event ids within fetched files.

        Args:
            lookup_tables: Tuple of lookup tables that define the scope of the search

        Examples:
            >>> index1 = EncryptedIndex([{"event": "data here"}])
            >>> index2 = EncryptedIndex([{"other": "index"}])
            >>> lookup_tables = (index1.lookup_table, index2.lookup_table)
            >>> search = EncryptedSearch(lookup_tables)
            >>> lookup_results = search.lookup("search query here")
            >>> fetched_files = {uri: your_fetch_method(uri) for uri in lookup_results}
            >>> event_ids = search.locate(fetched_files)
        """

    __locations: Dict[str, List[Location]]
    __lookup_tables: Tuple[LookupTable]

    def __init__(self, lookup_tables: Tuple[LookupTable, ...]):
        self.__lookup_tables = lookup_tables
        self.__locations = {}

    def lookup(self, query: str) -> Set[str]:
        """Finds relevant locations in all lookup tables and returns significant MXC URIs.

        Args:
            query: Search query string

        Returns:
            Set of MXC URIs pointing to files that contain the relevant locations.
        """

        locations: List[Location] = []
        mxc_uris = set()
        tokens = normalize(query)

        # Get locations
        for token in tokens:
            for lookup_table in self.__lookup_tables:
                locations += lookup_table.get(token, [])

        # Collect MXC URIs and fill __locations dict.
        for location in locations:
            mxc_uris.add(location.mxc_uri)
            if location.mxc_uri in self.__locations:
                self.__locations[location.mxc_uri].append(location)
            else:
                self.__locations[location.mxc_uri] = [location]

        return mxc_uris
