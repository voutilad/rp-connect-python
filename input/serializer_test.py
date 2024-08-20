import unittest
from importlib.util import find_spec


class InputSerializerTest(unittest.TestCase):
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
            _globals = {"message": data[0]}
            _locals = {}
            exec(self.source, _globals, _locals)

            self.assertTrue("result" in _locals, "Should have set a global 'result'.")
            result = _locals["result"]
            self.assertEqual(type(result), bytes, "Result should be in bytes.")
            self.assertEqual(result.decode(), data[1], "Bytes should decode to expected JSON output.")


if __name__ == "__main__":
    unittest.main()