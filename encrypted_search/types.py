from typing import Any, Dict, List, Set, Union

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

# search.locate
LookupResults = Dict[str, Union[Bucket, Level]]
