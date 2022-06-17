from typing import List, Set, Tuple

from nltk import word_tokenize
from nltk.corpus import stopwords

from utils.types import corpus_type, event_type, index_type


class EncryptedIndex:
    """A searchable, structurally-encrypted index.

    To prevent data leakage, we transform a simple inverted index into two structures: a collection of arrays and a lookup table. This class sets up this scheme and exposes the data to be searched upon.

    Args:
        events: List of Matrix room events to be indexed

    Attributes:
        database: Three dimensional array containing document ids according to the structuring scheme
        lookup_table: Map from a keyword to corresponding location in the database
    """

    def __init__(self, events: List[event_type]):
        documents, keywords = self.parse(events)
        inverted_index = self.invert(documents, keywords)

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
                for punctuation in "!()-[]{};:, <>./?@#$%^&*_~'\"\\":
                    content = content.replace(punctuation, " ")
                content = content.lower()
                tokens: Set[str] = (set(word_tokenize(content)) -
                                    set(stopwords.words("english")))
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
