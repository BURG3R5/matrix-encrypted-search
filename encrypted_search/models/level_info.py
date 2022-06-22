class LevelInfo:
    """Data about a level in the index.

    Args:
        * level_index
        * size_of_index

    Attributes:
        * array_size
        * large_bucket_size
        * number_of_large_buckets
        * small_bucket_size
        * number_of_buckets
        * large_chunk_size
    """

    def __init__(self, level_index: int, size_of_index: int):
        self.array_size = 2 * (size_of_index + 2**level_index)

        # Buckets
        self.large_bucket_size = 2**(level_index + 1)
        self.number_of_large_buckets, self.small_bucket_size = divmod(
            self.array_size,
            self.large_bucket_size,
        )
        self.number_of_buckets = (self.number_of_large_buckets +
                                  int(self.small_bucket_size != 0))

        # Chunks
        self.large_chunk_size = 2**level_index
