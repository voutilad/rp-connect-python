"""
Take the input's data and serialize it to JSON, encoded into Python bytes in a
local value called "result". If something fails, set result to None.

This module assumes the following state:
  globals:
    message -- object to serialize
"""
import json
global message

try:
    result = json.dumps(message).encode()
except NameError:
    # 'message' isn't defined in our scope.
    pass
except (AttributeError, ValueError, RecursionError, TypeError):
    # Something amiss in json.dumps!
    result = None