from typing import Any, Dict, List, Set

from .models.level_info import LevelInfo
from .models.location import Location

Event = Dict[str, Any]
Corpus = Dict[str, Set[str]]
InvertedIndex = Dict[str, Set[str]]
LevelInfos = Dict[int, LevelInfo]
Datastore = Dict[int, List[List[str]]]
LookupTable = Dict[str, List[Location]]
