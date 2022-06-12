from typing import List, Set, Tuple

from nltk import word_tokenize
from nltk.corpus import stopwords

from utils.types import corpus_type, event_type


class EncryptedIndex:

    def __init__(self, events: List[event_type]):
        documents, keywords = self.parse(events)

    @staticmethod
    def parse(events: List[event_type]) -> Tuple[corpus_type, Set[str]]:
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
