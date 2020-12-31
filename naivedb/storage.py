import json
import os
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any

__version__ = '1.0'
__author__ = 'Riccardo De Zen'
__email__ = 'riccardodezen98@gmail.com'


class Storage(ABC):
    """
    Base class for storing data to file. Assumes data is a Dictionary of Dictionaries.
    Subclasses may decide to implement some ways to cache the data and/or access single values.
    """

    @abstractmethod
    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """
        Read the data in the storage.

        :return: The data in the storage. Should return `None` if no data is found.
        """
        raise NotImplementedError(f"{self.__class__} is abstract.")

    @abstractmethod
    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        """
        Write the given data in the storage.

        :param data: Any Dictionary.
        """
        raise NotImplementedError(f"{self.__class__} is abstract.")

    @abstractmethod
    def __getitem__(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single item from the storage.

        :param key: The key of the item.
        :return: The item.
        """
        raise NotImplementedError(f"{self.__class__} is abstract.")

    @abstractmethod
    def __setitem__(self, key: str, value: Dict[str, Any]):
        """
        Set the value of a single item, the updated value is then written in the underlying storage.

        :param key: Key of the item.
        :param value: The new value for the item.
        """
        raise NotImplementedError(f"{self.__class__} is abstract.")

    def close(self) -> None:
        """
        Optional. Close file handles and other resources.
        """
        pass


class JSONStorage(Storage):
    """
    Naive storage interface, uses `json` module to serialize. This implies data must be of the types accepted by the
    JSON format.
    """

    def __init__(self, path: str, mode='r+', **kwargs):
        """
        :param path: The path to the file.
        :param mode: The mode with which to access the file.
        :param kwargs: Any arguments that you wish to pass to `json.dumps`.
        """
        # Store params.
        self._mode = mode
        self._kwargs = kwargs

        # Prepare file.
        self._can_write = any([c in mode for c in ('a', 'w', '+')])
        self._file = open(path, mode)

    def close(self):
        self._file.close()

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        self._file.seek(0, os.SEEK_END)
        size = self._file.tell()

        # Empty file, return None
        if size == 0:
            return None

        self._file.seek(0)
        return json.load(self._file)

    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        # If the database cannot write raises an Exception.
        if not self._can_write:
            raise IOError(f"Cannot write, access mode is \'{self._mode}\'.")

        self._file.seek(0)
        self._file.write(json.dumps(data, **self._kwargs))

        # Ensure the file has been written
        self._file.flush()
        os.fsync(self._file.fileno())

        # Truncate if file got shorter
        self._file.truncate()

    def __getitem__(self, key: str) -> Optional[Dict[str, Any]]:
        # Return an item after reading the file.
        data = self.read()
        return data[key] if data else None

    def __setitem__(self, key: str, value: Dict[str, Any]):
        # Reads the whole file and rewrites everything.
        data = self.read()
        data[key] = value
        self.write(data)


class MemoryStorage(Storage):
    """
    Storage class keeping a dictionary in memory. Useful for testing.
    """

    def __init__(self):
        # Data in the storage.
        self._data = None

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        return self._data

    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Dict[str, Any]:
        return self._data[key]

    def __setitem__(self, key: str, value: Dict[str, Any]):
        self._data[key] = value


class ItemStorage(Storage):
    """
    Wrapper Storage, used to cache the file to avoid reading the whole database every time part of the data needs to be
    updated. This class assumes the underlying storage is not modified by other external sources.
    """

    def __init__(self, storage: Storage):
        """
        :param storage: The `Storage` to wrap.
        """
        self._storage = storage
        self._data = storage.read()

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """
        :return: The data from the cache.
        """
        return self._data

    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        """
        :param data: The data to write.
        """
        self._storage.write(data)
        self._data = data

    def __getitem__(self, key: str) -> Optional[Dict[str, Any]]:
        return self._data[key] if self._data else None

    def __setitem__(self, key: str, value: Dict[str, Any]):
        """
        If writing the new data to the underlying storage fails, the changes are reverted.

        :param key: Key of the item.
        :param value: Value of the item.
        :raises Exception: If any exception is raised when writing.
        """
        # Save old value
        old_none = object()
        old_value = self._data.get(key, old_none)

        try:
            # Try to write
            self._data[key] = value
            self._storage.write(self._data)
        except Exception as e:
            # Restore old value
            if old_value is old_none:
                self._data.pop(key, None)
            else:
                self._data[key] = old_value
            raise e
