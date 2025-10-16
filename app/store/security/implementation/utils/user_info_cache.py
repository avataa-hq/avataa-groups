from abc import ABC, abstractmethod

from cachetools import TTLCache


class UserInfoCacheInterface(ABC):
    @abstractmethod
    def set(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def get(self, key):
        raise NotImplementedError

    @abstractmethod
    def __getitem__(self, item):
        raise NotImplementedError

    @abstractmethod
    def __setitem__(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def __delitem__(self, key):
        raise NotImplementedError


class UserInfoCache(UserInfoCacheInterface):
    def __init__(self, ttl: int = 60):
        self._cache = TTLCache(maxsize=500, ttl=ttl)

    def set(self, key, value):
        self._cache[key] = value

    def get(self, key):
        return self._cache.get(key)

    def __getitem__(self, item):
        return self._cache[item]

    def __setitem__(self, key, value):
        self._cache[key] = value

    def __delitem__(self, key):
        del self._cache[key]
