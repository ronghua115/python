class Singleton(object):
    _instance = None  # Keep instance reference

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance


class LimitedInstances(object):
    _instances = []  # Keep track of instance reference
    limit = 5

    def __new__(cls, *args, **kwargs):
        if not len(cls._instances) <= cls.limit:
            raise RuntimeError("Count not create instance. Limit %s reached" % cls.limit)
        instance = object.__new__(cls, *args, **kwargs)
        cls._instances.append(instance)
        return instance

    def __del__(self):
        # Remove instance from _instances
        self._instance.remove(self)


def create_instance():
    # Do what ever you want to determine if instance can be created
    return True


class CustomizeInstance(object):

    def __new__(cls, a, b):
        if not create_instance():
            raise RuntimeError("Count not create instance")
        instance = super(CustomizeInstance, cls).__new__(cls, a, b)
        instance.a = a
        return instance

    def __init__(self, a, b):
        pass


class AbstractClass(object):

    def __new__(cls, a, b):
        instance = super(AbstractClass, cls).__new__(cls)
        instance.__init__(a, b)
        return 3

    def __init__(self, a, b):
        print("Initializing Instance", a, b)
