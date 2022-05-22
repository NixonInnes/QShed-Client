import hashlib
from collections.abc import MutableMapping
from typing import Dict
from functools import lru_cache, wraps
from datetime import datetime, timedelta
import zlib
import json, base64


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


def timed_lru_cache(seconds: int, maxsize: int = None):
    def wrapper(wrapped_func):
        func = lru_cache(maxsize=maxsize)(wrapped_func)
        func.__lifetime = timedelta(seconds=seconds)
        func.__expiration = datetime.utcnow() + func.__lifetime

        @wraps(func)
        def inner(*args, **kwargs):
            if (now:=datetime.utcnow()) >= func.__expiration:
                func.cache_clear()
                func.__expiration = now + func.__lifetime
            return func(*args, **kwargs)
        return inner
    return wrapper



def zip_str(s):
    return base64.b64encode(
        zlib.compress(
            s.encode("utf-8")
        )
    ).decode("ascii")


def unzip_str(s):
    try:
        return zlib.decompress(base64.b64decode(s)).decode("utf-8")
    except:
        raise RuntimeError("Could not decode/unzip the contents")

