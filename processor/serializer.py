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
    if type(root) is Root:
        result = json.dumps(root, cls=RootEncoder).encode()
    else:
        # YOLO.
        result = json.dumps(root).encode()
except NameError:
    # 'root' isn't defined in our scope.
    result = None
except (AttributeError, ValueError, RecursionError, TypeError):
    # Something amiss in json.dumps!
    result = None

