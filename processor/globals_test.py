import unittest

from globals import Root

class TestRoot(unittest.TestCase):
    """
    Tests for our Root implementation's behavior related to data insertion.
    """

    def test_attribute_assignment(self):
        root = Root()
        root.name = "Dave"
        self.assertEqual(root.name, "Dave", "Should allow assigning first level attributes.")

    def test_multi_level_attribute_assignment(self):
        root = Root()
        root.name.first = "Dave"
        self.assertEqual(root.name.first, "Dave",
                         "Should allow assigning first level attributes.")
        name = root.name
        self.assertEqual(type(name), Root,
                         "Should fill in missing level as another Root instance.")

    def test_key_based_assignment(self):
        root = Root()
        root["name"] = "Dave"
        self.assertEqual(root["name"], "Dave",
                         "Should allow setting by key like a dict.")
        self.assertEqual(root.name, "Dave",
                         "Data inserted by key should appear as attributes.")

    def test_multi_level_key_based_assignment(self):
        root = Root()
        root["name"]["first"] = "Dave"
        self.assertEqual(root["name"]["first"], "Dave",
                         "Should allow multi-level key-based assignment.")
        self.assertEqual(root.name.first, "Dave",
                         "Multi-level key-based assignments should appear as attributes.")

if __name__ == "__main__":
    unittest.main()
