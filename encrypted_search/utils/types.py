from typing import Any, Dict, Set

from ..models.level_info import LevelInfo

event_type = Dict[str, Any]
corpus_type = Dict[str, Set[str]]
index_type = Dict[str, Set[str]]
levels_type = Dict[int, LevelInfo]
