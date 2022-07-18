import json
from abc import ABC
from math import ceil
from typing import IO, Callable, Dict, Tuple, Union

from .index import EncryptedIndex
from .types import (
    Bucket,
    Datastore,
    FileData,
    FileIdentifier,
    FilesMap,
    FractionsOfBucketFiles,
    FractionsOfLevelFiles,
    LargeBuckets,
    LargeLevels,
    Level,
    WholeLevelFiles,
)


class IndexStorage:

    __cutoff_size: int
    __encrypted_index: EncryptedIndex
    __files: FilesMap
    __remaining_files: FilesMap
    __mxc_uris_map: Dict[FileIdentifier, str]

    def __init__(self, encrypted_index: EncryptedIndex, cutoff_size: int):
        self.__encrypted_index = encrypted_index
        self.__cutoff_size = cutoff_size
        self.__mxc_uris_map = {}

        # Get files
        whole_level_files, large_levels = self.__segregate_levels(
            encrypted_index.datastore)
        fractions_of_level_files, large_buckets = self.__split_large_levels(
            large_levels)
        fractions_of_bucket_files = self.__split_large_buckets(large_buckets)
        self.__files = {
            **whole_level_files,
            **fractions_of_level_files,
            **fractions_of_bucket_files
        }

    def __segregate_levels(
        self,
        levels: Datastore,
    ) -> Tuple[WholeLevelFiles, LargeLevels]:
        files: WholeLevelFiles = {}
        large_levels: LargeLevels = {}

        for l, level in levels.items():
            level_size = self.__estimate_json_size(level)
            if level_size < self.__cutoff_size:
                files[l] = level
            else:
                large_levels[l] = level

        return files, large_levels

    def __split_large_levels(
        self,
        large_levels: LargeLevels,
    ) -> Tuple[FractionsOfLevelFiles, LargeBuckets]:

        def divide_level(l: int, level: Level):
            """Divides level into appropriately sized blobs of buckets, reporting extra-large buckets separately.

            Args:
                l: Index of level being split
                level: Data stored in the level to be split

            Returns:
                Tuple of the form (FOL, LB) where — FOL are fractions that the level has been divided into, and LB are extra-large buckets.
            """

            fractions = {}
            temp_large_buckets = {}

            f = 0
            current_fraction: Level = []
            size_so_far = 0

            for b, bucket in enumerate(level):
                bucket_size = self.__estimate_json_size(bucket)
                if bucket_size >= self.__cutoff_size:
                    # Add bucket to large buckets
                    temp_large_buckets[l, b] = bucket

                    # Save the fraction collected so far
                    if len(current_fraction) > 0:
                        fractions[l, f] = current_fraction

                    # Reset current fraction
                    f = b + 1
                    current_fraction = []
                    size_so_far = 0
                elif size_so_far + bucket_size + 2 < self.__cutoff_size:
                    # Add bucket to current fraction
                    current_fraction.append(bucket)
                    size_so_far += bucket_size + 2
                else:
                    # Save the fraction collected so far
                    fractions[l, f] = current_fraction

                    # Reset current fraction
                    f = b
                    current_fraction = [bucket]
                    size_so_far = bucket_size

            # Save remaining fraction
            if len(current_fraction) > 0:
                fractions[l, f] = current_fraction

            return fractions, temp_large_buckets

        files: FractionsOfLevelFiles = {}
        large_buckets: LargeBuckets = {}

        for level_index, level_data in large_levels.items():
            fractions_of_level_files, current_large_buckets = divide_level(
                level_index,
                level_data,
            )
            files.update(fractions_of_level_files)
            large_buckets.update(current_large_buckets)

        return files, large_buckets

    def __split_large_buckets(
        self,
        large_buckets: LargeBuckets,
    ) -> FractionsOfBucketFiles:
        files: FractionsOfBucketFiles = {}

        # Iterate over large buckets
        for (l, b), bucket in large_buckets.items():
            # Calculate size of fractions
            number_of_fractions = ceil(
                self.__estimate_json_size(bucket) / self.__cutoff_size)
            fraction_length = ceil(len(bucket) / number_of_fractions)

            # Divide bucket into fractions and save fractions
            for i in range(number_of_fractions):
                f = i * fraction_length
                files[l, b, f] = bucket[f:f + fraction_length]

        return files

    def __next__(self) -> Tuple[FileData, Callable[[str], None]]:
        if not self.__remaining_files:
            raise StopIteration

        identifier, data = self.__remaining_files.popitem()

        def callback(uri: str):
            self.__mxc_uris_map[identifier] = uri

        return data, callback

    def __iter__(self):
        self.__remaining_files = self.__files.copy()
        return self

    @staticmethod
    def __estimate_json_size(data: Union[Bucket, Level]):
        fakefile = _FakeFile()
        json.dump(data, fakefile)
        return fakefile.size


class _FakeFile(IO, ABC):

    def __init__(self, size=0):
        self.size = size

    def write(self, string):
        self.size += len(string)
