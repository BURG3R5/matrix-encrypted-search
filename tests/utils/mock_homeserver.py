from random import random
from typing import Any, Dict, Iterable


class MockHomeserver:

    def __init__(self):
        self.files: Dict[str, Any] = {}

    def upload(self, file: Any) -> str:
        uri = str(random())[2:]
        self.files[uri] = file
        return uri

    def fetch(self, uri: str) -> Any:
        return self.files[uri]

    def delete_all(self, uris_to_delete: Iterable[str]):
        for uri in uris_to_delete:
            del self.files[uri]
