################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import unittest
from unittest.mock import MagicMock

from pypaimon.catalog.catalog_lock import CatalogLock
from pypaimon.catalog.lock import Lock
from pypaimon.common.identifier import Identifier


class TestLockEmpty(unittest.TestCase):

    def test_empty_lock_runs_callable_directly(self):
        lock = Lock.empty()
        result = lock.run_with_lock(lambda: 42)
        self.assertEqual(result, 42)

    def test_empty_lock_propagates_exception(self):
        lock = Lock.empty()
        with self.assertRaises(ValueError):
            lock.run_with_lock(lambda: (_ for _ in ()).throw(ValueError("test")))


class TestLockClose(unittest.TestCase):

    def test_close_delegates_to_catalog_lock(self):
        mock_catalog_lock = MagicMock(spec=CatalogLock)
        identifier = Identifier("my_db", "my_table")
        lock = Lock.from_catalog(mock_catalog_lock, identifier)
        lock.close()
        mock_catalog_lock.close.assert_called_once()

    def test_close_empty_lock_is_noop(self):
        lock = Lock.empty()
        lock.close()  # Should not raise


class TestLockFromCatalog(unittest.TestCase):

    def test_delegates_to_catalog_lock_with_bound_identifier(self):
        mock_catalog_lock = MagicMock(spec=CatalogLock)
        mock_catalog_lock.run_with_lock.return_value = "result"
        identifier = Identifier("my_db", "my_table")

        lock = Lock.from_catalog(mock_catalog_lock, identifier)
        result = lock.run_with_lock(lambda: "inner")

        self.assertEqual(result, "result")
        mock_catalog_lock.run_with_lock.assert_called_once()
        args = mock_catalog_lock.run_with_lock.call_args
        self.assertEqual(args[0][0], "my_db")
        self.assertEqual(args[0][1], "my_table")
        # The third argument is the callable
        self.assertTrue(callable(args[0][2]))

    def test_catalog_lock_exception_propagates(self):
        mock_catalog_lock = MagicMock(spec=CatalogLock)
        mock_catalog_lock.run_with_lock.side_effect = RuntimeError("lock failed")
        identifier = Identifier("my_db", "my_table")

        lock = Lock.from_catalog(mock_catalog_lock, identifier)
        with self.assertRaises(RuntimeError) as ctx:
            lock.run_with_lock(lambda: "inner")
        self.assertIn("lock failed", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
