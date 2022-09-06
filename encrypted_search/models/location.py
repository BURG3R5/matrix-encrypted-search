from typing import Any, Dict, Optional

from ..exceptions import LocationFormatError


class Location:
    """Common model for the two types of location data that can be stored in the `lookup_table` of an `EncryptedIndex`.

    Attributes:
        mxc_uri: [if remote datastore] URI of the Matrix Content Repository file where the level or bucket is stored
        level_index: [if local datastore] Index of the level being located
        bucket_index: If the above data leads to a level, the index of the bucket being located
        start_of_chunk: Starting index of the chunk being located
        chunk_length: Length of the chunk being located
    """

    is_remote: bool
    level_index: Optional[int]
    mxc_uri: Optional[str]
    bucket_index: Optional[int]
    start_of_chunk: int
    chunk_length: int

    def __init__(self, is_remote: bool, **kwargs):
        """Common model for the two types of location data that can be stored in the `lookup_table` of an `EncryptedIndex`.

        Args:
            is_remote: Whether the location refers to a remote file

        Keyword Args:
            mxc_uri: [if remote datastore] URI of the Matrix Content Repository file where the level or bucket is stored
            level_index: [if local datastore] Index of the level being located
            bucket_index: If the above data leads to a level, the index of the bucket being located
            start_of_chunk: Starting index of the chunk being located
            chunk_length: Length of the chunk being located

        Raises:
            LocationFormatError:
                To locate a remote datastore an MXC URI is necessary. To locate a local datastore, both level index and bucket index are necessary.
        """

        if is_remote:
            self.mxc_uri = kwargs.get("mxc_uri")
            if self.mxc_uri is None:
                raise LocationFormatError(
                    "Matrix Content Repository URI not provided for remote datastore location"
                )
        else:
            self.level_index = kwargs.get("level_index")
            if self.level_index is None:
                raise LocationFormatError(
                    "Level or bucket index not provided for local datastore location"
                )

        if "bucket_index" in kwargs and kwargs["bucket_index"] is not None:
            self.bucket_index = int(kwargs["bucket_index"])

        self.is_remote = is_remote
        self.start_of_chunk = int(kwargs["start_of_chunk"])
        self.chunk_length = int(kwargs["chunk_length"])

    @classmethod
    def from_json(cls, json: Dict[str, Any]) -> "Location":
        """Deserializes JSON into a `Location` object.

        Args:
            json: Serialized data in the form of a `dict`

        Returns:
            Deserialized `Location` object.
        """

        return cls(
            is_remote="mxc_uri" in json,
            mxc_uri=json.get("mxc_uri"),
            level_index=json.get("level_index"),
            bucket_index=json.get("bucket_index"),
            start_of_chunk=json.get("start_of_chunk"),
            chunk_length=json.get("chunk_length"),
        )

    def to_json(self) -> Dict[str, Any]:
        """Serializes a `Location` object into a JSON.

        Returns:
            Serialized data in the form of a `dict`.
        """

        serialized = {"is_remote": self.is_remote}
        optional_attributes = ("mxc_uri", "level_index", "bucket_index",
                               "start_of_chunk", "chunk_length")
        for attr in optional_attributes:
            if hasattr(self, attr):
                serialized[attr] = getattr(self, attr)
        return serialized

    def __eq__(self, other):
        return (self.is_remote == other.is_remote
                and self.level_index == other.level_index
                and self.mxc_uri == other.mxc_uri
                and self.bucket_index == other.bucket_index
                and self.start_of_chunk == other.start_of_chunk
                and self.chunk_length == other.chunk_length)

    def __hash__(self):
        t = (
            self.is_remote,
            self.level_index,
            self.mxc_uri,
            self.bucket_index,
            self.start_of_chunk,
            self.chunk_length,
        )
        return hash(t)

    def __repr__(self):
        if self.is_remote:
            return f"({self.mxc_uri}, {self.bucket_index if hasattr(self, 'bucket_index') else None}, {self.start_of_chunk}, {self.chunk_length})"
        else:
            return f"({self.level_index}, {self.bucket_index}, {self.start_of_chunk}, {self.chunk_length})"
