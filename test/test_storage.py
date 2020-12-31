import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from typing import Optional

from naivedb.storage import Storage, JSONStorage, MemoryStorage, ItemStorage

__version__ = '1.0'
__author__ = 'Riccardo De Zen'
__email__ = 'riccardodezen98@gmail.com'

EXAMPLE_JSON = {'key': {'another_key': 'value'}}
EXAMPLE_STRING = 'Lorem Ipsum'


@contextmanager
def temp_file(content: Optional[str] = None) -> str:
    """
    :param content: The content of the file. If `None` the file will be empty.
    :return: Full name of the temporary file.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        file = os.path.join(tempdir, 'temp.json')
        with open(file, 'w+') as f:
            if content is not None:
                f.write(content)
        yield file


class StorageTest(unittest.TestCase):

    def test_abstract(self):
        self.assertRaises(TypeError, Storage)


class JSONStorageTest(unittest.TestCase):

    def test_begins_none(self):
        with temp_file() as file:
            storage = JSONStorage(file)
            data = storage.read()
            storage.close()
            self.assertIsNone(data)

    def test_can_read(self):
        with temp_file(json.dumps(EXAMPLE_JSON)) as file:
            storage = JSONStorage(file, mode='r')
            data = storage.read()
            storage.close()
            # Original Dict must be equal to read dict.
            self.assertEqual(EXAMPLE_JSON, data)

    def test_can_write(self):
        with temp_file() as file:
            storage = JSONStorage(file, mode='r+')
            storage.write(EXAMPLE_JSON)
            storage.close()
            # Dict in the file must be equal to input.
            with open(file) as f:
                self.assertEqual(json.load(f), EXAMPLE_JSON)

    def test_cannot_write(self):
        for mode in ('r', 'r+', 'w', 'w+', 'a', 'a+'):
            with temp_file() as file:
                storage = JSONStorage(file, mode)
                if not any([c in mode for c in ('a', 'w', '+')]):
                    # Storage should not be able to write.
                    self.assertRaises(IOError, storage.write, (EXAMPLE_JSON,))
                    storage.close()
                else:
                    # Storage should be able to write.
                    storage.write(EXAMPLE_JSON)
                    storage.close()
                    with open(file) as f:
                        self.assertEqual(json.load(f), EXAMPLE_JSON)

    def test_get_item(self):
        with temp_file() as file:
            storage = JSONStorage(file)
            storage.write(EXAMPLE_JSON)
            self.assertEqual(storage['key'], EXAMPLE_JSON['key'])
            storage.close()

    def test_set_item(self):
        new_item = {'sub_key': 'sub_value'}
        with temp_file(json.dumps(EXAMPLE_JSON)) as file:
            storage = JSONStorage(file)
            storage['key'] = new_item
            self.assertEqual(storage['key'], new_item)
            storage.close()


class MemoryStorageTest(unittest.TestCase):

    def read_and_write(self):
        data = EXAMPLE_JSON
        m = MemoryStorage()
        m.write(data)
        self.assertEqual(m.read(), data)

    def test_begins_none(self):
        m = MemoryStorage()
        self.assertIsNone(m.read())


class ItemStorageTest(unittest.TestCase):

    def setUp(self) -> None:
        self.under = MemoryStorage()
        self.storage = ItemStorage(self.under)

    def test_read(self):
        # Storage is not supposed to listen to the underlying.
        self.under.write(EXAMPLE_JSON)
        self.assertNotEqual(self.storage.read(), EXAMPLE_JSON)

    def test_write(self):
        self.storage.write(EXAMPLE_JSON)
        self.assertEqual(self.under.read(), EXAMPLE_JSON)

    def test_get_item(self):
        self.storage.write(EXAMPLE_JSON)
        self.assertEqual(self.storage['key'], EXAMPLE_JSON['key'])

    def test_set_item(self):
        new_item = {'sub_key': 'sub_value'}
        self.storage.write(EXAMPLE_JSON)
        self.storage['key'] = new_item
        self.assertEqual(self.storage['key'], new_item)

    def test_begins_none(self):
        self.assertIsNone(self.storage.read())
