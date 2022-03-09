import hashlib

def string_hash(string) -> str:
    hash_obj = hashlib.sha256(bytes(string, "utf-8"))
    return hash_obj.hexdigest()