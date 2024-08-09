"""
Creates an environment similar to the one available in Bloblang.
"""
global __content__


def content():
    """
    Provides access to a messages data as bytes, similar to Bloblang's
    `content()` function.
    :return: bytes
    """
    global __content__
    return __content__


class Root:
    """
    Provides an experience similar to Bloblang's `root` object, allowing
    for dynamic creation of object hierarchy.
    """
    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__
        self.__dict__[name] = Root()
        return self.__dict__[name]

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def __str__(self):
        return str(self.__dict__)

    def to_dict(self):
        """
        Provide access to the raw data backing this Root instance.
        :return: a dict of the instance's data
        """
        return self.__dict__

# Pre-create an empty "root" instance for the user.
root = Root()
