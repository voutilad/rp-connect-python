"""
Serializer module for converting data to JSON or Pickles.
"""
import json
import pickle

def to_json_string(obj) -> str:
    """
    Convert object obj to JSON string.
    :param obj: object to serialize
    :return: string of JSON
    """
    return json.dumps(obj)


def to_json_bytes(obj) -> bytes:
    """
    Convert object obj to JSON encoded to bytes.
    :param obj: object to serialize
    :return: bytes of encoded JSON
    """
    return json.dumps(obj).encode()


def to_pickle(obj) -> bytes:
    """
    Convert object obj to Pickle representation.
    :param obj: object to serialize
    :return: pickled object in bytes
    """
    return pickle.dumps(obj)

