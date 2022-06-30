import unittest

from encrypted_search.models.location import Location
from encrypted_search.search import EncryptedSearch

from .utils.test_helpers import get_test_data


class EncryptedSearchTest(unittest.TestCase):

    def test_lookup(self):
        cases = (
            "single",  # Single index
            "multiple",  # Multiple indices
        )
        for case_name in cases:
            raw_test_data = get_test_data("search/lookup", case_name)
            lookup_tables = tuple({
                keyword:
                [Location.from_json(location) for location in locations]
                for keyword, locations in lookup_table.items()
            } for lookup_table in raw_test_data["lookup_tables"])
            keywords = {
                keyword
                for lookup_table in raw_test_data["lookup_tables"]
                for keyword in lookup_table
            }
            expected_results = {
                keyword: set(raw_test_data["results"][keyword])
                for keyword in keywords
            }

            search = EncryptedSearch(lookup_tables)
            results = {keyword: search.lookup(keyword) for keyword in keywords}

            self.assertEqual(expected_results, results)


if __name__ == "__main__":
    unittest.main()
