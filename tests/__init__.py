"""PyGreSQL test suite.

You can specify your local database settings in LOCAL_PyGreSQL.py.
"""

import faulthandler
import os
import unittest

# Abort runaway test runs: if PYGRESQL_TEST_TIMEOUT is set to a number of
# seconds (tox sets it), dump the tracebacks of all threads and exit when
# the tests are still running after that time. This also works when the
# tests hang in C code or wait for the database server.
_timeout = int(os.environ.get('PYGRESQL_TEST_TIMEOUT') or 0)
if _timeout > 0:
    faulthandler.dump_traceback_later(_timeout, exit=True)

if not (hasattr(unittest, 'skip')
        and hasattr(unittest.TestCase, 'setUpClass')
        and hasattr(unittest.TestCase, 'skipTest')
        and hasattr(unittest.TestCase, 'assertIn')):
    raise ImportError('Please install a newer version of unittest')


def discover():
    loader = unittest.TestLoader()
    suite = loader.discover('.')
    return suite
