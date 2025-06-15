import abc


class BaseEndpoint:

    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    @property
    def endpoint_info(self):
        raise NotImplementedError

    @property
    def mr_info(self):
        raise NotImplementedError

    @abc.abstractmethod
    def initialize(self, *args, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def register_memory_region(self, mr_key: str, addr: int, offset: int, length: int):
        raise NotImplementedError

    @abc.abstractmethod
    def register_remote_memory_region(self):
        raise NotImplementedError

    @abc.abstractmethod
    def reload_memory_pool(self, mr_key: str, remote_mr_info):
        raise NotImplementedError

    @abc.abstractmethod
    def connect(self, endpoint_info):
        raise NotImplementedError

    @abc.abstractmethod
    def read_batch(self):
        raise NotImplementedError
