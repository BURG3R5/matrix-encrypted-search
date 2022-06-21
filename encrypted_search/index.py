from math import ceil, log
from typing import Dict, List, Set, Tuple

from .models.level_info import LevelInfo
from .utils.normalizer import normalize
from .utils.types import corpus_type, event_type, index_type


class EncryptedIndex:
    """A searchable, structurally-encrypted index.

    To prevent data leakage, we transform a simple inverted index into two structures: a collection of arrays and a lookup table. This class sets up this scheme and exposes the data to be searched upon.

    Args:
        events: List of Matrix room events to be indexed

    Keyword Args:
        s: Int parameter that determines the space/read efficiency tradeoff
        L: Int parameter that determines the locality

    Attributes:
        database: Three dimensional array containing document ids according to the structuring scheme
        lookup_table: Map from a keyword to corresponding location in the database

        size: sum(len(inverted_index[w]): w∈keywords)
        s: Int parameter that determines the space/read efficiency tradeoff
        L: Int parameter that determines the locality
    """

    s: int
    L: int
    size: int

    __levels: Dict[int, LevelInfo]

    def __init__(self, events: List[event_type], **kwargs):
        # Pre-setup
        documents, keywords = self.parse(events)
        inverted_index = self.invert(documents, keywords)

        # Set parameters
        self.s = kwargs.get('s', 2)
        self.L = kwargs.get('L', 1)
        self.calculate_parameters(inverted_index)

    @staticmethod
    def parse(events: List[event_type]) -> Tuple[corpus_type, Set[str]]:
        """Transforms raw Matrix room events into normalized and simplified documents.

        Args:
            events: List of Matrix room events to be indexed

        Returns:
            A tuple of the form (D, K), where — D is a mapping from document ids to the set of keywords present in each document and K is a set of all the normalized tokens present in the corpus.
        """

        documents: corpus_type = {}
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
    def invert(documents: corpus_type, keywords: Set[str]) -> index_type:
        """Converts a normalized corpus of documents into an inverted index.

        Args:
            documents: Mapping from document ids to the set of keywords present in each document
            keywords: Set of all the normalized tokens present in the corpus

        Returns:
            An inverted index i.e. a mapping from keywords to documents that contain them.
        """

        inverted_index: index_type = {keyword: set() for keyword in keywords}
        for doc_id, doc_content in documents.items():
            for token in doc_content:
                inverted_index[token].add(doc_id)
        return inverted_index

    def calculate_parameters(self, inverted_index: index_type) -> None:
        """Calculates index-wide parameters and level-specific parameters based on s, L and the inverted index.

        Args:
            inverted_index: Mapping from keywords to documents that contain them
        """

        self.size = sum(len(arr) for arr in inverted_index.values())

        # Determine populated levels
        if self.size == 0:
            self.__levels = {}
            return
        l0 = ceil(log(self.size, 2))
        p = ceil(l0 / self.s)
        level_indices = {l0 - i for i in range(0, p * self.s, p)}
        if self.L > 1:
            level_indices.add(0)

        # Determine parameters of various structures on each level
        self.__levels = {l: LevelInfo(l, self.size) for l in level_indices}
