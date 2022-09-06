import unittest

from encrypted_search.index import EncryptedIndex
from encrypted_search.merge import IndexMerge
from encrypted_search.search import EncryptedSearch
from encrypted_search.storage import IndexStorage

from .utils.mock_homeserver import MockHomeserver
from .utils.test_helpers import get_test_data


class IntegrationTest(unittest.TestCase):

    def setUp(self) -> None:
        self.homeserver = MockHomeserver()

    def test_single_index(self):
        cases = (
            "small",  # Small index
            "large",  # Relatively large index
        )
        for case in cases:
            raw_test_data = get_test_data("integration/single", case)
            events = raw_test_data["events"]
            expected_search_results = raw_test_data["search_results"]

            # Create the basic structurally encrypted index
            encrypted_index = EncryptedIndex(events)

            # Prepare the index for upload
            upload_ready_index = IndexStorage(encrypted_index, 100)
            # Upload each file and save the generated uri in the lookup table
            for file_data, callback in upload_ready_index:
                mxc_uri = self.homeserver.upload(file_data)
                callback(mxc_uri)
            # Update the lookup table
            upload_ready_index.update_lookup_table()
            lookup_table = upload_ready_index.lookup_table

            # Cleanup
            del encrypted_index, upload_ready_index

            # Search and assert
            index_search = EncryptedSearch((lookup_table, ))
            for query, expected_result in expected_search_results.items():
                mxc_uris = index_search.lookup(query)
                fetched_files = {
                    mxc_uri: self.homeserver.fetch(mxc_uri)
                    for mxc_uri in mxc_uris
                }
                search_result = index_search.locate(fetched_files)

                self.assertEqual(
                    set(expected_result),
                    search_result,
                    f"Search results for '{query}' failed for test case {case}.",
                )

    def test_multiple_indices(self):
        raw_test_data = get_test_data("integration", "multiple")
        expected_search_results = raw_test_data["search_results"]
        lookup_tables = []

        # Create and upload two indices
        for i in range(2):
            events = raw_test_data["events"][i]
            encrypted_index = EncryptedIndex(events)

            upload_ready_index = IndexStorage(encrypted_index, 100)
            for file_data, callback in upload_ready_index:
                mxc_uri = self.homeserver.upload(file_data)
                callback(mxc_uri)
            upload_ready_index.update_lookup_table()
            lookup_tables.append(upload_ready_index.lookup_table)

            del encrypted_index, upload_ready_index

        # Search and assert
        index_search = EncryptedSearch(lookup_tables)
        for query, expected_result in expected_search_results.items():
            mxc_uris = index_search.lookup(query)
            fetched_files = {
                mxc_uri: self.homeserver.fetch(mxc_uri)
                for mxc_uri in mxc_uris
            }
            search_result = index_search.locate(fetched_files)

            self.assertEqual(
                set(expected_result),
                search_result,
                f"Search results for '{query}' failed.",
            )

    def test_merge_indices(self):
        # Reuse test data from `multiple` case
        raw_test_data = get_test_data("integration", "multiple")
        expected_search_results = raw_test_data["search_results"]
        lookup_tables = []

        # Create and upload two indices
        for i in range(2):
            events = raw_test_data["events"][i]
            encrypted_index = EncryptedIndex(events)

            upload_ready_index = IndexStorage(encrypted_index, 100)
            for file_data, callback in upload_ready_index:
                mxc_uri = self.homeserver.upload(file_data)
                callback(mxc_uri)
            upload_ready_index.update_lookup_table()
            lookup_tables.append(upload_ready_index.lookup_table)

            del encrypted_index, upload_ready_index

        # Merge two indices into one encrypted index
        index_merger = IndexMerge(lookup_tables)
        uris_to_delete = set()
        for mxc_uris, callback in index_merger:
            uris_to_delete |= mxc_uris
            for mxc_uri in mxc_uris:
                file_data = self.homeserver.fetch(mxc_uri)
                callback(mxc_uri, file_data)
        index_merger.distribute_new_index()
        merged_encrypted_index = index_merger.encrypted_index

        # Delete previous files from homeserver
        self.homeserver.delete_all(uris_to_delete)

        # Upload merged index
        upload_ready_index = IndexStorage(merged_encrypted_index, 100)
        for file_data, callback in upload_ready_index:
            mxc_uri = self.homeserver.upload(file_data)
            callback(mxc_uri)
        upload_ready_index.update_lookup_table()
        lookup_table = upload_ready_index.lookup_table

        # Cleanup
        del merged_encrypted_index, upload_ready_index, lookup_tables

        # Search and assert
        index_search = EncryptedSearch((lookup_table, ))
        for query, expected_result in expected_search_results.items():
            mxc_uris = index_search.lookup(query)
            fetched_files = {
                mxc_uri: self.homeserver.fetch(mxc_uri)
                for mxc_uri in mxc_uris
            }
            search_result = index_search.locate(fetched_files)

            self.assertEqual(
                set(expected_result),
                search_result,
                f"Search results for '{query}' failed.",
            )


if __name__ == '__main__':
    unittest.main()
