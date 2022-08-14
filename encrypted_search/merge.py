from collections import defaultdict
from typing import Callable, Dict, List, Set, Tuple

from encrypted_search.index import EncryptedIndex
from encrypted_search.models.location import Location
from encrypted_search.types import Bucket, FileData, InvertedIndex, LookupTable


class IndexMerge:
    """A transformer class that exposes methods to merge several encrypted indices into one.

    Args:
        lookup_tables: Tuple of lookup tables that define the scope of the search

    Keyword Args:
        s: Int parameter that determines the space/read efficiency tradeoff
        L: Int parameter that determines the locality

    Examples:
        >>> lookup_tables = (lookup_table1, lookup_table2, ...)
        >>> index_merger = IndexMerge(lookup_tables)
        >>> for mxc_uris, callback in index_merger:
        >>>     for mxc_uri in mxc_uris:
        >>>         file_data = your_fetch_method(mxc_uri)
        >>>         callback(mxc_uri, file_data)
        >>> merged_encrypted_index = index_merger.encrypted_index
    """

    encrypted_index: EncryptedIndex

    __inverted_index: InvertedIndex
    __keywords: Set[str]
    __lookup_tables: Tuple[LookupTable, ...]
    __remaining_keywords: Set[str]

    def __init__(self, lookup_tables: Tuple[LookupTable, ...], **kwargs):
        self.__lookup_tables = lookup_tables

        # Extract a common set of keywords
        self.__keywords = set.union(*(set(lt.keys()) for lt in lookup_tables))

        # Semi-initialize the final encrypted index
        self.encrypted_index = EncryptedIndex([],
                                              s=kwargs.get('s', 2),
                                              L=kwargs.get('L', 1))
        self.encrypted_index.keywords = self.__keywords

    def __next__(self) -> Tuple[Set[str], Callable[[str, FileData], None]]:
        """Provides the next set of MXC URIs to fetch in order to merge indices.

        Returns:
            A tuple of the form (U, CB), where — U is a set of MXC URIs to fetch and CB is a callback function to update after each fetch operation.
        """

        if not self.__remaining_keywords:
            self.__distribute_index()
            raise StopIteration

        # Choose a random keyword
        keyword = self.__remaining_keywords.pop()

        # Collect remote locations where its doc ids can be found.
        locations: Dict[str, List[Location]] = defaultdict(list)
        for lookup_table in self.__lookup_tables:
            if keyword not in lookup_table: continue
            for location in lookup_table[keyword]:
                locations[location.mxc_uri].append(location)

        # Callback to be called after data is fetched from the homeserver.
        def callback(mxc_uri: str, file_data: FileData):
            """Callback to update merged index with data fetched from `mxc_uri`.

            Args:
                mxc_uri: URI which was fetched from.
                file_data: Bucket/Level-like object found at the URI.
            """

            # Get all locations associated with current uri
            current_locations = locations[mxc_uri]

            # For each relevant location
            for loc in current_locations:
                # Extract a bucket from the fetched data
                bucket: Bucket
                if hasattr(loc, "bucket_index"):
                    bucket = file_data[loc.bucket_index]
                else:
                    bucket = file_data

                # Extract a chunk from the bucket
                chunk = bucket[loc.start_of_chunk:loc.start_of_chunk +
                               loc.chunk_length]

                # Add the doc ids in the chunk to the inverted index
                self.__inverted_index[keyword] |= set(chunk)

        return set(locations.keys()), callback

    def __iter__(self) -> "IndexMerge":
        """Initializes loop variables and returns iterator object.

        Called automatically by the `in` keyword when loop starts.

        Returns:
            The same object.
        """

        self.__inverted_index = {}
        self.__remaining_keywords = self.__keywords.copy()
        return self

    def __distribute_index(self):
        """Converts merged inverted index into encrypted index."""

        self.encrypted_index._EncryptedIndex__levels = self.encrypted_index.calc_params(
            self.__inverted_index)
        self.encrypted_index.distribute(self.__inverted_index)
