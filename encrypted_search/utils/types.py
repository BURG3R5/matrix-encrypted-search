from typing import Any, Dict, List, Set, Tuple

from ..models.level_info import LevelInfo

event_type = Dict[str, Any]
corpus_type = Dict[str, Set[str]]
index_type = Dict[str, Set[str]]
levels_type = Dict[int, LevelInfo]
database_type = Dict[int, List[List[str]]]
lookup_table_type = Dict[str, List[Tuple[int, int, int, int]]]
