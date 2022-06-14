import json
import os
from typing import Any


def get_test_data(test: str, case: str) -> Any:
    """Fetches data for a test case.

    Data is stored in JSON files like tests/data/<class-name>/<method-name>/<case>.json

    Args:
        test: String of the form A/B where A is the name of the class and B is the name of the method to be tested
        case: String representing which particular scenario is being tested

    Returns:
        Raw data for the requested test case.
    """

    file_path = f"tests/data/{test}/{case}.json"  # Run from root
    if os.path.exists(file_path):
        with open(file_path, encoding="utf-8") as file:
            return json.load(file)
    raise IOError(f"File 'tests/data/{test}/{case}.json' can't be found")
