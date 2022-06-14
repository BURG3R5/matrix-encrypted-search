import unittest

from encrypted_search.index import EncryptedIndex
from utils.test_helpers import get_test_data


class EncryptedIndexTest(unittest.TestCase):

    def test_parse(self):
        cases = (
            "basic",  # Basic tokenization
            "punctuation",  # Remove punctuation
            "case_insensitive",  # Ignore case
            "other_events",  # Ignore other events
            "stopwords",  # Exclude stopwords
        )
        for case_name in cases:
            raw_test_data = get_test_data("index/parse", case_name)
            events = raw_test_data["events"]
            expected_documents = {
                k: set(v)
                for k, v in raw_test_data["documents"].items()
            }
            expected_keywords = set(raw_test_data["keywords"])

            documents, keywords = EncryptedIndex.parse(events)

            self.assertEqual(documents, expected_documents)
            self.assertEqual(keywords, expected_keywords)


if __name__ == "__main__":
    unittest.main()
