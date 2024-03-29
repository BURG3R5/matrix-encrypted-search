from collections import defaultdict
from typing import Dict, Iterable, List, Set, cast

from .models.location import Location
from .types import Bucket, FetchedFiles, Level, LookupTable
from .utils.normalizer import normalize_surface


class EncryptedSearch:
    """Representation of the scope in which encrypted searches can be executed.

    A search is divided into three steps:
        1. Lookup: Find relevant locations in all lookup tables in the scope of this search.
        2. Fetch: Get relevant files from content repositories. Must be completed by external client.
        3. Locate: Collect and shortlist doc ids within fetched files.

    Args:
        lookup_tables: Tuple of lookup tables that define the scope of the search

    Examples:
        >>> lookup_tables = (lookup_table1, lookup_table2, ...)
        >>> search = EncryptedSearch(lookup_tables)
        >>> lookup_results = search.lookup("search query here")
        >>> fetched_files = {uri: your_fetch_method(uri) for uri in lookup_results}
        >>> event_ids = search.locate(fetched_files)
    """

    __locations: Dict[str, Dict[str, List[Location]]]
    __lookup_tables: Iterable[LookupTable]

    def __init__(self, lookup_tables: Iterable[LookupTable]):
        self.__lookup_tables = lookup_tables

    def lookup(self, query: str) -> Set[str]:
        """Finds relevant locations in all lookup tables and returns significant MXC URIs.

        Args:
            query: Search query string

        Returns:
            Set of MXC URIs pointing to files that contain the relevant locations.
        """

        self.__locations = defaultdict(lambda: defaultdict(list))
        locations: LookupTable = defaultdict(list)
        mxc_uris = set()
        tokens = normalize_surface(query)

        # Get locations
        for token in tokens:
            for lookup_table in self.__lookup_tables:
                locations[token].extend(lookup_table.get(token, []))

        # Collect MXC URIs and fill __locations dict.
        for keyword, kw_locations in locations.items():
            for location in kw_locations:
                mxc_uris.add(location.mxc_uri)
                self.__locations[keyword][location.mxc_uri].append(location)
        return mxc_uris

    def locate(self, fetched_files: FetchedFiles) -> Set[str]:
        """Finds relevant docs ids within fetched files.

        Args:
            fetched_files: Mapping from MXC URIs to file contents fetched from those URIs.

        Returns:
            Set of doc ids containing the tokens in the search query.
        """

        doc_ids = []
        for keyword in self.__locations:
            kw_doc_ids = set()
            for uri, file_data in fetched_files.items():
                locations_in_this_file = self.__locations[keyword][uri]
                for location in locations_in_this_file:
                    bucket: Bucket
                    if isinstance(file_data[0], str):
                        bucket = cast(Bucket, file_data)
                    else:
                        bucket = cast(Level, file_data)[location.bucket_index]
                    chunk = bucket[location.
                                   start_of_chunk:location.start_of_chunk +
                                   location.chunk_length]
                    kw_doc_ids |= set(chunk)
            if kw_doc_ids != set():
                doc_ids.append(kw_doc_ids)
        if len(doc_ids) == 0:
            return set()
        elif len(doc_ids) == 1:
            return doc_ids[0]
        return doc_ids[0].intersection(*doc_ids)
