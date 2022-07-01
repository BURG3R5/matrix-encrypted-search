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

    def test_locate(self):
        # TODO: Add "mixed" test case, where files contain both buckets and levels.
        cases = (
            "single",  # Single index
            "multiple",  # Multiple indices
        )
        for case_name in cases:
            raw_test_data = get_test_data("search/locate", case_name)
            for keyword, keyword_test_data in raw_test_data.items():
                locations = {
                    mxc_uri: [
                        Location.from_json(serialized_location)
                        for serialized_location in serialized_locations
                    ]
                    for mxc_uri, serialized_locations in
                    keyword_test_data["locations"].items()
                }
                fetched_files = keyword_test_data["fetched_files"]
                expected_doc_ids = keyword_test_data["doc_ids"]

                search = EncryptedSearch(())
                search._EncryptedSearch__locations = locations
                doc_ids = search.locate(fetched_files)

                self.assertEqual(set(expected_doc_ids), doc_ids)


if __name__ == "__main__":
    unittest.main()
