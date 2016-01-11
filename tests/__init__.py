"""PyGreSQL test suite.

You can specify your local database settings in LOCAL_PyGreSQL.py.

"""

try:
    import unittest2 as unittest  # for Python < 2.7
except ImportError:
    import unittest


def discover():
    loader = unittest.TestLoader()
    suite = loader.discover('.')
    return suite