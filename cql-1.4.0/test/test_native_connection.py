# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# to configure behavior, define $CQL_TEST_HOST to the destination address
# for native connections, and $CQL_TEST_NATIVE_PORT to the associated port.

import os
import unittest
import contextlib
from thrift.transport import TTransport
import test_cql
from test_prepared_queries import MIN_THRIFT_FOR_CQL_3_0_0_FINAL
from test_connection import with_keyspace, TEST_HOST, randstring, cql

TEST_NATIVE_PORT = int(os.environ.get('CQL_TEST_NATIVE_PORT', '8000'))

class TestNativeConnection(unittest.TestCase):
    def setUp(self):
        self.randstr = randstring()
        self.with_keyspace = lambda curs, ver: with_keyspace(self.randstr, curs, ver)

    def test_connecting_with_cql_version(self):
        # 2.0.0 won't be supported by binary protocol
        self.assertRaises(cql.ProgrammingError,
                          cql.connect, TEST_HOST, TEST_NATIVE_PORT,
                          native=True, cql_version='2.0.0')

    def test_connecting_with_keyspace(self):
        # this conn is just for creating the keyspace
        conn = cql.connect(TEST_HOST, TEST_NATIVE_PORT, native=True)
        curs = conn.cursor()
        with self.with_keyspace(curs, conn.cql_version) as ksname:
            curs.execute('create table blah1_%s (a int primary key, b int);' % self.randstr)
            conn2 = cql.connect(TEST_HOST, TEST_NATIVE_PORT, keyspace=ksname,
                                native=True, cql_version=conn.cql_version)
            curs2 = conn2.cursor()
            curs2.execute('select * from blah1_%s;' % self.randstr)
            conn2.close()

    def test_execution_fails_after_close(self):
        conn = cql.connect(TEST_HOST, TEST_NATIVE_PORT, native=True)
        curs = conn.cursor()
        with self.with_keyspace(curs, conn.cql_version) as ksname:
            curs.execute('create table blah (a int primary key, b int);')
            curs.execute('select * from blah;')
        conn.close()
        self.assertRaises(cql.ProgrammingError, curs.execute, 'select * from blah;')

    def try_basic_stuff(self, conn):
        curs = conn.cursor()
        with self.with_keyspace(curs, conn.cql_version) as ksname:
            curs.execute('create table moo (a text primary key, b int, c float);')
            curs.execute("insert into moo (a, b, c) values (:d, :e, :f);",
                         {'d': 'hi', 'e': 1234, 'f': 1.234});
            qprep = curs.prepare_query("select * from moo where a = :fish;")
            curs.execute_prepared(qprep, {'fish': 'hi'})
            res = curs.fetchall()
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0][0], 'hi')
            self.assertEqual(res[0][1], 1234)
            self.assertAlmostEqual(res[0][2], 1.234)

    def test_connecting_without_compression(self):
        conn = cql.connect(TEST_HOST, TEST_NATIVE_PORT, native=True, compression=False)
        self.assertEqual(conn.compressor, None)
        self.try_basic_stuff(conn)

    def test_connecting_with_compression(self):
        try:
            import snappy
        except ImportError:
            if hasattr(unittest, 'skipTest'):
                unittest.skipTest('Snappy compression not available')
            else:
                return
        conn = cql.connect(TEST_HOST, TEST_NATIVE_PORT, native=True, compression=True)
        self.assertEqual(conn.compressor, snappy.compress)
        self.try_basic_stuff(conn)
