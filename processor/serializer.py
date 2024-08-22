"""
Take the user's data and serialize it to JSON, encoded into Python bytes in a
local value called "result". If something fails, set result to None.

This module assumes the following state:
  locals:
    root -- Object originally defined in globals.py, possibly modified by user.
  globals:
     Root -- Class defined in globals.py.
"""
import json


class RootEncoder(json.JSONEncoder):
    """Custom JSON encoder wrapper for a Root."""
    def default(self, o):
        return o.to_dict()

try:
    if root:
        t = type(root)
        if t is dict or t is list or t is tuple:
            encoder = None
        else:
            encoder = RootEncoder
        result = json.dumps(root, cls=encoder).encode()

except NameError as n:
    # 'root' isn't defined in our scope.
    raise n
except (AttributeError, ValueError, RecursionError, TypeError):
    # Something amiss in json.dumps!
    result = None

try:
    if meta:
        meta_result = json.dumps(meta)
except NameError as n:
    # 'meta' not in our scope.
    raise n
except (AttributeError, ValueError, RecursionError, TypeError):
    # Something amiss in json.dumps!
    meta_result = None
