import unittest

from encrypted_search.index import EncryptedIndex
from utils.test_helpers import get_test_case


class EncryptedIndexTest(unittest.TestCase):

    def test_parse(self):
        cases = (
            "basic",  # Basic tokenization
            "punctuation",  # Remove punctuation
            "case_insensitive",  # Ignore case
            "other_events",  # Ignore case
            "stopwords",  # Exclude stopwords
        )
        for case_name in cases:
            events, expected_documents, expected_keywords = get_test_case(
                "index/parse",
                case_name,
            )
            documents, keywords = EncryptedIndex.parse(events)
            self.assertEqual(documents, expected_documents)
            self.assertEqual(keywords, expected_keywords)


if __name__ == "__main__":
    unittest.main()
