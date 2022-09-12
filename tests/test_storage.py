import ast
import unittest

from encrypted_search.index import EncryptedIndex
from encrypted_search.storage import IndexStorage

from .utils.deserializers import index_from_json
from .utils.serializers import lookup_table_to_json
from .utils.test_helpers import get_test_data


class IndexSearchTest(unittest.TestCase):

    def setUp(self) -> None:
        index = EncryptedIndex([])
        self.storage = IndexStorage(index, 200)

    def test_segregate_levels(self):
        event_id = '$vBjjKk3mrIEPHvoIedB4Cc9kL0tWLKNXYgCRb2HAyGs'
        levels = {
            3: [[]] * 4,  # Short empty level
            0: [[]] * 51,  # Long empty level
            2: [[event_id] * 2, [event_id]],  # Short filled level
            1: [[event_id] * 2, [event_id],
                [event_id] * 3],  # Long filled level
        }

        whole_level_files, rouge_levels = self.storage._IndexStorage__segregate_levels(
            levels)

        self.assertEqual(whole_level_files, {2: levels[2], 3: levels[3]})
        self.assertEqual(rouge_levels, {0: levels[0], 1: levels[1]})

    def test_split_large_levels(self):
        event_id = '$2c0y92QwXRjRAcKbZ2MK2QR4kMxxHSlACrFrtz0A0HE'
        large_levels = {
            0: [[]] * 51,  # Empty level with normal buckets
            1: [[event_id] * 2, [event_id],
                [event_id] * 3],  # Filled level with normal buckets
            2: [[event_id] * 7, [event_id],
                [event_id] * 3],  # Filled level with one large bucket
            3: [[event_id] * 5, [event_id] * 3, [event_id], [event_id] * 6,
                [event_id],
                [event_id] * 2],  # Filled level with many large buckets
        }
        expected_fractions_of_level_files = {
            (0, 0): large_levels[0][:49],
            (0, 49): large_levels[0][49:],
            (1, 0): large_levels[1][:2],
            (1, 2): large_levels[1][2:],
            (2, 1): large_levels[2][1:],
            (3, 1): large_levels[3][1:3],
            (3, 4): large_levels[3][4:],
        }
        expected_large_buckets = {
            (2, 0): large_levels[2][0],
            (3, 0): large_levels[3][0],
            (3, 3): large_levels[3][3],
        }

        fractions_of_level_files, large_buckets = self.storage._IndexStorage__split_large_levels(
            large_levels)

        self.assertEqual(fractions_of_level_files,
                         expected_fractions_of_level_files)
        self.assertEqual(large_buckets, expected_large_buckets)

    def test_split_large_buckets(self):
        event_id1 = '$2c0y92QwXRjRAcKbZ2MK2QR4kMxxHSlACrFrtz0A0HE'
        event_id2 = '$A0TaO8FhcLKym6An3fSnA_rU1P9oODG5JxNjLomITVE'
        large_buckets = {
            (0, 0): [event_id1, event_id1, event_id1, event_id2,
                     event_id1],  # Just larger than the limit
            (1, 1): [event_id1] * 7 +
            [event_id2] * 6,  # Multiple times larger than the limit
        }
        expected_fractions_of_bucket_files = {
            (0, 0, 0): [event_id1] * 3,
            (0, 0, 3): [event_id2, event_id1],
            (1, 1, 0): [event_id1] * 4,
            (1, 1, 4): [event_id1] * 3 + [event_id2],
            (1, 1, 8): [event_id2] * 4,
            (1, 1, 12): [event_id2],
        }

        fractions_of_bucket_files = self.storage._IndexStorage__split_large_buckets(
            large_buckets)

        self.assertEqual(fractions_of_bucket_files,
                         expected_fractions_of_bucket_files)

    def test_estimate_json_size(self):
        event_id = '$A0TaO8FhcLKym6An3fSnA_rU1P9oODG5JxNjLomITVE'
        cases = (
            ([[]] * 13, 52),  # Empty level
            ([[event_id]] + [[]] * 9, 86),  # Singleton level
            ([[event_id] * 2] * 5, 490),  # Filled level
            ([], 2),  # Empty bucket
            ([event_id], 48),  # Singleton bucket
            ([event_id] * 7, 336),  # Filled bucket
        )
        for case in cases:
            self.assertEqual(
                case[1],
                IndexStorage._IndexStorage__estimate_json_size(case[0]))

    def test_constructor(self):
        cases = {
            "small": range(100, 600, 100),  # Small dataset
            "large":
            (1000, 3000, 5000, 7000, 11000),  # Relatively large dataset
        }
        for case_name, cutoff_sizes in cases.items():
            raw_test_data = get_test_data("storage/constructor", case_name)
            encrypted_index = index_from_json(raw_test_data["encrypted_index"])
            for cutoff_size in cutoff_sizes:
                expected_files = {
                    ast.literal_eval(identifier): data
                    for identifier, data in
                    raw_test_data[f"files({cutoff_size})"].items()
                }

                storage = IndexStorage(encrypted_index, cutoff_size)

                self.assertEqual(expected_files, storage._IndexStorage__files)

    def test_iterator(self):
        cases = {
            "small": (100, 300, 500),  # Small dataset
            "large": (1000, 4000, 7000, 10000),  # Relatively large dataset
        }
        for case_name, cutoff_sizes in cases.items():
            raw_test_data = get_test_data("storage/iterator", case_name)
            for cutoff_size in cutoff_sizes:
                files = {
                    ast.literal_eval(identifier): data
                    for identifier, data in raw_test_data[str(cutoff_size)]
                    ["files"].items()
                }
                expected_uris = {
                    ast.literal_eval(identifier): uri
                    for identifier, uri in raw_test_data[str(cutoff_size)]
                    ["mxc_uris_map"].items()
                }
                expected_repository = raw_test_data[str(
                    cutoff_size)]["repository"]
                storage = IndexStorage(EncryptedIndex([]), cutoff_size)
                storage._IndexStorage__files = files

                repository = {}
                for file_data, callback in storage:
                    identifier = f"file_uri/{len(repository) + 1}"
                    repository[identifier] = file_data
                    callback(identifier)

                self.assertEqual(expected_uris,
                                 storage._IndexStorage__mxc_uris_map)
                self.assertEqual(expected_repository, repository)

    def test_update_lookup_table(self):
        cases = {
            "small": (100, 300, 500),  # Small dataset
            "large": (1000, 4000, 7000, 10000),  # Relatively large dataset
        }
        for case_name, cutoff_sizes in cases.items():
            raw_test_data = get_test_data("storage/update_lookup_table",
                                          case_name)
            for cutoff_size in cutoff_sizes:
                encrypted_index = index_from_json(
                    raw_test_data["encrypted_index"])
                mxc_uris_map = {
                    ast.literal_eval(identifier): uri
                    for identifier, uri in raw_test_data[str(cutoff_size)]
                    ["mxc_uris_map"].items()
                }
                storage = IndexStorage(encrypted_index, cutoff_size)
                storage._IndexStorage__mxc_uris_map = mxc_uris_map

                storage.update_lookup_table()

                self.assertEqual(
                    raw_test_data[str(cutoff_size)]["final_lookup_table"],
                    lookup_table_to_json(storage.lookup_table),
                )


if __name__ == "__main__":
    unittest.main()
