from typing import Any, Dict, List, Set, Tuple, Union

from .models.level_info import LevelInfo
from .models.location import Location

# index.parse
Event = Dict[str, Any]
Corpus = Dict[str, Set[str]]

# index.invert
InvertedIndex = Dict[str, Set[str]]

# index.calc_params
LevelInfos = Dict[int, LevelInfo]

# index.distribute
Bucket = List[str]
Level = List[Bucket]
Datastore = Dict[int, Level]
LookupTable = Dict[str, List[Location]]

# storage.__segregate_levels
WholeLevelFiles = Dict[int, Level]
LargeLevels = Dict[int, Level]

# storage.__split_large_levels
FractionsOfLevelFiles = Dict[Tuple[int, int], Level]
LargeBuckets = Dict[Tuple[int, int], Bucket]

# storage.__split_large_buckets
FractionsOfBucketFiles = Dict[Tuple[int, int, int], Bucket]

# storage.__next__
FileIdentifier = Union[int, Tuple[int, int], Tuple[int, int, int]]
FileData = Union[Level, Bucket]

# storage.__init__
FilesMap = Dict[FileIdentifier, FileData]

# search.locate
FetchedFiles = Dict[str, Union[Bucket, Level]]
