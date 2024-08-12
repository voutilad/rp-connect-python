"""
Creates an environment similar to the one available in Bloblang.
"""
global __content      # bytes of the service.Message
global __metadata     # our metadata callback function implemented in Go
global __message_addr # the virtual address of a service.Message

def content():
    """
    Provides access to a message's data as bytes, similar to Bloblang's
    `content()` function.
    :return: bytes
    """
    global __content
    return __content


def metadata(key = ""):
    """
    Provides access to a message's metadata, similar to Bloblang's
    `metadata()` function.
    :param key: optional key for retrieving a particular metadata value.
    :return: metadata from Redpanda Connect
    """
    global __metadata
    global __message_addr
    value = __metadata(__message_addr, key)
    if value == "":
        # This is our "no such value for key" response.
        return None
    return value


class Root:
    """
    Provides an experience similar to Bloblang's `root` object, allowing
    for dynamic creation of object hierarchy.
    """
    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        self.__dict__[name] = Root()
        return self.__dict__[name]

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, item):
        return self.__getattr__(item)

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
