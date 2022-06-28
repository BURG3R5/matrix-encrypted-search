import secrets
from math import ceil, log
from typing import List, Set, Tuple

from .models.level_info import LevelInfo
from .models.location import Location
from .types import Corpus, Datastore, Event, InvertedIndex, LevelInfos, LookupTable
from .utils.normalizer import normalize


class EncryptedIndex:
    """A searchable, structurally-encrypted index.

    To prevent data leakage, we transform a simple inverted index into two structures: a collection of arrays and a lookup table. This class sets up this scheme and exposes the data to be searched upon.

    Args:
        events: List of Matrix room events to be indexed

    Keyword Args:
        s: Int parameter that determines the space/read efficiency tradeoff
        L: Int parameter that determines the locality

    Attributes:
        datastore: Three dimensional array-like structure containing document ids according to the structuring scheme
        lookup_table: Map from a keyword to corresponding location in the datastore
        keywords: Set of all the normalized tokens present in the corpus

        size: sum(len(inverted_index[w]): w∈keywords)
        s: Int parameter that determines the space/read efficiency tradeoff
        L: Int parameter that determines the locality
    """

    datastore: Datastore
    lookup_table: LookupTable
    keywords: Set[str]

    s: int
    L: int
    size: int

    __levels: LevelInfos

    def __init__(self, events: List[Event], **kwargs):
        # Pre-setup
        documents, keywords = self.parse(events)
        inverted_index = self.invert(documents, keywords)

        # Set parameters
        self.s = kwargs.get('s', 2)
        self.L = kwargs.get('L', 1)
        self.keywords = keywords
        self.__levels = self.calc_params(inverted_index)

        # Setup
        self.distribute(inverted_index)

    @staticmethod
    def parse(events: List[Event]) -> Tuple[Corpus, Set[str]]:
        """Transforms raw Matrix room events into normalized and simplified documents.

        Args:
            events: List of Matrix room events to be indexed

        Returns:
            A tuple of the form (D, K), where — D is a mapping from document ids to the set of keywords present in each document and K is a set of all the normalized tokens present in the corpus.
        """

        documents: Corpus = {}
        keywords: Set[str] = set()
        for event in events:
            if event["type"] == "m.room.message" \
                    and "content" in event \
                    and "body" in event["content"]:
                event_id: str = event["event_id"]
                content: str = event["content"]["body"]
                tokens = normalize(content)
                keywords |= tokens
                documents[event_id] = tokens
        return documents, keywords

    @staticmethod
    def invert(documents: Corpus, keywords: Set[str]) -> InvertedIndex:
        """Converts a normalized corpus of documents into an inverted index.

        Args:
            documents: Mapping from document ids to the set of keywords present in each document
            keywords: Set of all the normalized tokens present in the corpus

        Returns:
            An inverted index i.e. a mapping from keywords to documents that contain them.
        """

        inverted_index: InvertedIndex = {
            keyword: set()
            for keyword in keywords
        }
        for doc_id, doc_content in documents.items():
            for token in doc_content:
                inverted_index[token].add(doc_id)
        return inverted_index

    def calc_params(self, inverted_index: InvertedIndex) -> LevelInfos:
        """Calculates index-wide parameters and level-specific parameters based on s, L and the inverted index.

        Args:
            inverted_index: Mapping from keywords to documents that contain them
        """

        self.size = sum(len(arr) for arr in inverted_index.values())

        # Determine populated levels
        if self.size == 0:
            return {}
        l0 = ceil(log(self.size, 2))
        p = ceil(l0 / self.s)
        level_indices = {l0 - i for i in range(0, p * self.s, p)}
        if self.L > 1:
            level_indices.add(0)

        # Determine parameters of various structures on each level
        levels = {l: LevelInfo(l, self.size) for l in level_indices}
        return levels

    def distribute(self, inverted_index: InvertedIndex) -> None:
        """Fill the datastore and lookup_table with values from the inverted index according to the SSE scheme.

        Args:
            inverted_index: Mapping from keywords to documents that contain them
        """
        # Initialize structures
        self.lookup_table = {keyword: [] for keyword in self.keywords}
        self.datastore = {
            level_index: [[] for _ in range(level.number_of_buckets)]
            for level_index, level in self.__levels.items()
        }

        # Initialize helper
        bucket_capacity = dict()
        for i, l in self.__levels.items():
            bucket_capacity[i] = [
                l.large_bucket_size for _ in range(l.number_of_large_buckets)
            ]
            if l.small_bucket_size != 0:
                bucket_capacity[i].append(l.small_bucket_size)

        for keyword in self.keywords:
            docs = list(inverted_index[keyword])
            n = len(docs)

            # Determine level
            bound = log(n / self.L, 2)
            level_index = min(l for l in self.__levels if l >= bound)
            level = self.__levels[level_index]

            # Divide into chunks
            chunks = [
                docs[i:i + level.large_chunk_size]
                for i in range(0, len(docs), level.large_chunk_size)
            ]
            for chunk in chunks:
                # Choose bucket
                possible_buckets = [
                    index for index, capacity in enumerate(
                        bucket_capacity[level_index]) if capacity >= len(chunk)
                ]
                chosen_bucket = secrets.choice(possible_buckets)

                # Update helper
                prev_len = len(self.datastore[level_index][chosen_bucket])
                chunk_length = len(chunk)
                bucket_capacity[level_index][chosen_bucket] -= chunk_length

                # Append chunk
                self.datastore[level_index][chosen_bucket] += chunk
                self.lookup_table[keyword].append(
                    Location(
                        is_remote=False,
                        level_index=level_index,
                        bucket_index=chosen_bucket,
                        start_of_chunk=prev_len,
                        chunk_length=chunk_length,
                    ))
