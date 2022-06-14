from typing import List, Set, Tuple

from nltk import word_tokenize
from nltk.corpus import stopwords

from utils.types import corpus_type, event_type


class EncryptedIndex:
    """A searchable obfuscated index.

    To prevent data leakage, we obfuscate a simple inverted index into two structures: a collection of arrays and a lookup table. This class sets up this scheme and exposes the data to be searched upon.

    Args:
        events: List of Matrix room events to be indexed

    Attributes:
        database: Three dimensional array containing document ids according to obfuscation scheme
        lookup_table: Map from a keyword to corresponding location in the database
    """

    def __init__(self, events: List[event_type]):
        documents, keywords = self.parse(events)

    @staticmethod
    def parse(events: List[event_type]) -> Tuple[corpus_type, Set[str]]:
        """Transforms raw Matrix room events into normalized and simplified documents.

        Args:
            events: List of Matrix room events to be indexed

        Returns:
            A tuple of the form (D, K), where — D is a mapping from document ids to the set of keywords present in each document and K is a set of all the normalized tokens present in the corpus.
        """

        documents: corpus_type = {}
        for event in events:
            if (event["type"] == "m.room.message" and "content" in event
                    and "body" in event["content"]):
                event_id: str = event["event_id"]
                content: str = event["content"]["body"]
                for punctuation in "!()-[]{};:, <>./?@#$%^&*_~'\"\\":
                    content = content.replace(punctuation, " ")
                content = content.lower()
                tokens: Set[str] = (set(word_tokenize(content)) -
                                    set(stopwords.words("english")))
                documents[event_id] = tokens
        keywords: Set[str] = set.union(*(documents.values()))
        return documents, keywords
