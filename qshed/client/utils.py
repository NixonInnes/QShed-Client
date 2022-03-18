import hashlib
from collections.abc import MutableMapping
from typing import Dict


def string_hash(string: str) -> str:
    hash_obj = hashlib.sha256(bytes(string, "utf-8"))
    return hash_obj.hexdigest()


def _flatten_dict_gen(d, parent_key: str, sep: str):
    for k, v in d.items():
        k = k.replace(sep, "").replace(" ", "_")
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            yield from flatten_dict(v, new_key, sep=sep).items()
        else:
            yield new_key, v


def flatten_dict(d: MutableMapping, parent_key: str = "", sep: str = ".") -> Dict:
    return dict(_flatten_dict_gen(d, parent_key, sep))
