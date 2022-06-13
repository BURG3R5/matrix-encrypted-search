import json
import os


def get_test_case(test: str, case: str):
    file_path = f"tests/data/{test}/{case}.json"  # Run from root
    if os.path.exists(file_path):
        with open(file_path, encoding="utf-8") as file:
            raw_case = json.load(file)
            return (raw_case["events"],
                    {k: set(v)
                     for k, v in raw_case["documents"].items()},
                    set(raw_case["keywords"]))
    raise IOError(f"File 'tests/data/{test}/{case}.json' can't be found")
