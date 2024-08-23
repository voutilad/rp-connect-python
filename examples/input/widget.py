"""
Example python module.
"""
class Widget:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return f"Widget<{self.value}>"

