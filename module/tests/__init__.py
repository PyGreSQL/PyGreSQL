"""PyGreSQL test suite.

You can specify your local database settings in LOCAL_PyGreSQL.py.

"""

import unittest


def discover():
    loader = unittest.TestLoader()
    suite = loader.discover('.')
    return suite