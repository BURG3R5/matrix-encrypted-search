import unittest

from encrypted_search.index import EncryptedIndex

from .utils.serializers import levels_to_json
from .utils.test_helpers import get_test_data


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

    def test_invert(self):
        cases = (
            "basic",  # Trivial example
            "real",  # Slightly more realistic example
        )
        for case_name in cases:
            raw_test_data = get_test_data("index/invert", case_name)
            documents = {
                k: set(v)
                for k, v in raw_test_data["documents"].items()
            }
            keywords = set(raw_test_data["keywords"])
            expected_inverted_index = {
                k: set(v)
                for k, v in raw_test_data["inverted_index"].items()
            }

            inverted_index = EncryptedIndex.invert(documents, keywords)

            self.assertEqual(inverted_index, expected_inverted_index)

    def test_calculate_parameters(self):
        cases = (
            "basic",  # Simple example
            "real",  # Slightly more realistic example
        )
        for case_name in cases:
            raw_test_data = get_test_data("index/calc_params", case_name)
            expected_size = raw_test_data["size"]
            inverted = {
                k: set(v)
                for k, v in raw_test_data["inverted_index"].items()
            }
            for s in range(1, 5):
                for L in range(1, 3):
                    expected_levels = raw_test_data[f"levels({s})({L})"]

                    encrypted_index = EncryptedIndex([], s=s, L=L)
                    levels = encrypted_index.calc_params(inverted)
                    serialized_levels = levels_to_json(levels)

                    self.assertEqual(expected_size, encrypted_index.size)
                    self.assertEqual(expected_levels, serialized_levels)


if __name__ == "__main__":
    unittest.main()
