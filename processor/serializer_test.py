import unittest
from importlib.util import find_spec

class Dummy:
    """Stubbed class that is not serializable."""
    pass

class FakeRoot:
    """Stubbed class to test the RootEncoder assumptions."""
    def __init__(self, data):
        self.data = dict(data)

    def add(self, key, value):
        self.data[key] = value

    def to_dict(self):
        return self.data

class ProcessorSerializerTest(unittest.TestCase):
    """
    Tests for the serialization helper code.
    """
    @classmethod
    def setUpClass(cls):
        spec = find_spec("serializer")
        with open(spec.origin) as f:
            cls.source = "\n".join(f.readlines())

    def test_serializing_popo(self):
        """Test serializing plain old python objects: lists, tuples, and dicts."""
        tests = {
            "lists": (
                ["Dave", "Cora", "Maple"],
                '["Dave", "Cora", "Maple"]'),
            "tuples":
                (("Dave", "Cora", "Maple"),
                 '["Dave", "Cora", "Maple"]'),
            "dicts":
                ({"names": ["Dave", "Cora", "Maple"]},
                 '{"names": ["Dave", "Cora", "Maple"]}'),
        }
        for test, data in tests.items():
            _globals = {"Root": Dummy()}
            _locals = {"root": data[0]}
            exec(self.source, _globals, _locals)

            self.assertTrue("result" in _locals, "Should have set a local 'result'.")
            result = _locals["result"]
            self.assertEqual(type(result), bytes, "Result should be in bytes.")
            self.assertEqual(result.decode(), data[1], "Bytes should decode to expected JSON output.")

    def test_serializing_root(self):
        """Test special handling for Root types."""
        root = FakeRoot({"name": "Dave", "inner": FakeRoot({"numbers": (1, 2, 3)})})
        _globals = {"Root": FakeRoot}
        _locals = {"root": root}
        exec(self.source, _globals, _locals)

        self.assertTrue("result" in _locals, "Should have set a local 'result'.")
        result = _locals["result"]
        self.assertEqual(type(result), bytes, "Result should be in bytes.")
        self.assertEqual(result.decode(),
                         '{"name": "Dave", "inner": {"numbers": [1, 2, 3]}}',
                         "Bytes should decode to expected JSON output.")

    def test_serialization_issue(self):
        """Something wrong with our Root class should result in a None result."""
        _globals = {"Root": Dummy}
        _locals = {"root": Dummy()}
        exec(self.source, _globals, _locals)

        self.assertTrue("result" in _locals, "Should have set a local 'result'.")
        result = _locals["result"]
        self.assertIsNone(result, "Result should be None.")

    def test_value_error(self):
        """If the user puts unserializable data in our Root class, we should get a None."""
        root = FakeRoot({"name": "Dave", "inner": Dummy()})
        _globals = {"Root": FakeRoot}
        _locals = {"root": root}
        exec(self.source, _globals, _locals)

        self.assertTrue("result" in _locals, "Should have set a local 'result'.")
        result = _locals["result"]
        self.assertIsNone(result, "Result should be None.")

    def test_serializing_meta(self):
        """
        Similar to handling root, but a bit simpler. meta should only be a
        dict of POPO.
        """
        metadata = {
            "name": "Dave",
            "list": [1, 2, 3],
            "tuple": (1, "two"),
            "dict": {"last": "Voutila"},
        }
        _globals = {"Root": Dummy}
        _locals = {"root": Dummy(), "meta": metadata}
        exec(self.source, _globals, _locals)

        # We shouldn't have any root result.
        self.assertTrue("result" in _locals, "Should have set a local 'result'.")
        result = _locals["result"]
        self.assertIsNone(result, "Result should be None.")

        self.assertTrue("meta_result" in _locals,
                        "Should have set a local 'meta_result'.")
        meta_result = _locals["meta_result"]
        self.assertEqual(type(meta_result), str,
                         "meta_result should be a string.")
        self.assertEqual(
            meta_result,
            '{"name": "Dave", "list": [1, 2, 3], "tuple": [1, "two"], "dict": {"last": "Voutila"}}',
            "Bytes should decode to expected JSON output."
        )


if __name__ == "__main__":
    unittest.main()
