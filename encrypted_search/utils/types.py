from typing import Any, Dict, List, NamedTuple, Set

from ..models.level_info import LevelInfo

Event = Dict[str, Any]
Corpus = Dict[str, Set[str]]
InvertedIndex = Dict[str, Set[str]]
LevelInfos = Dict[int, LevelInfo]
Datastore = Dict[int, List[List[str]]]
Location = NamedTuple(
    'Location',
    [
        ('level_index', int),
        ('bucket_index', int),
        ('start_of_chunk', int),
        ('chunk_length', int),
    ],
)
LookupTable = Dict[str, List[Location]]
