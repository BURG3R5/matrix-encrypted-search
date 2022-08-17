import unittest

from encrypted_search.merge import IndexMerge
from tests.utils.deserializers import lookup_table_from_json

from .utils.test_helpers import get_test_data


class IndexMergeTest(unittest.TestCase):

    def test_constructor(self):
        cases = (
            "small",  # Small dataset
            "large",  # Relatively large dataset
        )
        for case_name in cases:
            raw_test_data = get_test_data("merge/constructor", case_name)
            lookup_tables = tuple(raw_test_data["lookup_tables"])
            expected_keywords = set(raw_test_data["keywords"])

            index_merge = IndexMerge(lookup_tables, s=4, L=2)

            self.assertEqual(expected_keywords,
                             index_merge._IndexMerge__keywords)

    def test_iterator(self):
        self.maxDiff = None
        cases = (
            "small",  # Small dataset
            "large",  # Relatively large dataset
        )
        for case_name in cases:
            raw_test_data = get_test_data("merge/iterator", case_name)
            lookup_tables = tuple(
                lookup_table_from_json(lt)
                for lt in raw_test_data["lookup_tables"])
            server_storage = raw_test_data["server_storage"]
            index_merge = IndexMerge(lookup_tables, s=4, L=2)
            expected_inverted_index = {
                keyword: set(doc_ids)
                for keyword, doc_ids in
                raw_test_data["inverted_index"].items()
            }

            for mxc_uris, callback in index_merge:
                for mxc_uri in mxc_uris:
                    file_data = server_storage[mxc_uri]
                    callback(mxc_uri, file_data)

            self.assertEqual(expected_inverted_index,
                             index_merge._IndexMerge__inverted_index)


if __name__ == "__main__":
    unittest.main()
