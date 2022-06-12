from typing import List, Set

from nltk import word_tokenize
from nltk.corpus import stopwords

from utils.types import corpus_type, event_type


class EncryptedIndex:

    def __init__(self, events: List[event_type]):
        documents = self.parse(events)
        keywords: Set[str] = set.union(*(documents.values()))

    def parse(self, events: List[event_type]) -> corpus_type:
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
        return documents
