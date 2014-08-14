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
# for Thrift connections, and $CQL_TEST_PORT to the associated port.

import unittest
import contextlib
from thrift.transport import TTransport
import test_cql
from test_prepared_queries import MIN_THRIFT_FOR_CQL_3_0_0_FINAL

cql = test_cql.cql
TEST_HOST = test_cql.TEST_HOST
TEST_PORT = test_cql.TEST_PORT
randstring = test_cql.randstring
del test_cql

@contextlib.contextmanager
def with_keyspace(randstr, cursor, cqlver):
    ksname = randstr + '_conntest_' + cqlver.encode('ascii').replace('.', '_')
    if cqlver.startswith('2.'):
        cursor.execute("create keyspace '%s' with strategy_class='SimpleStrategy'"
                       " and strategy_options:replication_factor=1;" % ksname)
        cursor.execute("use '%s'" % ksname)
        yield ksname
        cursor.execute("use system;")
        cursor.execute("drop keyspace '%s'" % ksname)
    elif cqlver == '3.0.0-beta1': # for cassandra 1.1
        cursor.execute("create keyspace \"%s\" with strategy_class='SimpleStrategy'"
                       " and strategy_options:replication_factor=1;" % ksname)
        cursor.execute('use "%s"' % ksname)
        yield ksname
        cursor.execute('use system;')
        cursor.execute('drop keyspace "%s"' % ksname)
    else:
        cursor.execute("create keyspace \"%s\" with replication = "
                       "{'class': 'SimpleStrategy', 'replication_factor': 1};" % ksname)
        cursor.execute('use "%s"' % ksname)
        yield ksname
        cursor.execute('use system;')
        cursor.execute('drop keyspace "%s"' % ksname)

class TestConnection(unittest.TestCase):
    def setUp(self):
        self.randstr = randstring()
        self.with_keyspace = lambda curs, ver: with_keyspace(self.randstr, curs, ver)

    def test_connecting_with_cql_version(self):
        conn = cql.connect(TEST_HOST, TEST_PORT, cql_version='2.0.0')
        curs = conn.cursor()
        with self.with_keyspace(curs, '2.0.0'):
            # this should only parse in cql 2
            curs.execute('create table foo (a int primary key) with comparator = float;')
        conn.close()

        if conn.remote_thrift_version >= MIN_THRIFT_FOR_CQL_3_0_0_FINAL:
            cqlver = '3.0.0'
        else:
            cqlver = '3.0.0-beta1'
        conn = cql.connect(TEST_HOST, TEST_PORT, cql_version=cqlver)
        curs = conn.cursor()
        with self.with_keyspace(curs, cqlver):
            # this should only parse in cql 3
            curs.execute('create table foo (a int, b text, c timestamp, primary key (a, b));')

    def test_connecting_with_keyspace(self):
        # this conn is just for creating the keyspace
        conn = cql.connect(TEST_HOST, TEST_PORT, cql_version='2.0.0')
        curs = conn.cursor()
        with self.with_keyspace(curs, '2.0.0') as ksname:
            curs.execute('create table blah1_%s (a int primary key, b int);' % self.randstr)
            conn2 = cql.connect(TEST_HOST, TEST_PORT, keyspace=ksname, cql_version='3.0.0')
            curs2 = conn2.cursor()
            curs2.execute('select * from blah1_%s;' % self.randstr)
            conn2.close()
        with self.with_keyspace(curs, '2.0.0') as ksname:
            curs.execute('create table blah2_%s (a int primary key, b int);' % self.randstr)
            conn3 = cql.connect(TEST_HOST, TEST_PORT, keyspace=ksname, cql_version='3.0.0')
            curs3 = conn3.cursor()
            curs3.execute('select * from blah2_%s;' % self.randstr)
            conn3.close()

    def test_execution_fails_after_close(self):
        conn = cql.connect(TEST_HOST, TEST_PORT, cql_version='3.0.0')
        curs = conn.cursor()
        with self.with_keyspace(curs, '3.0.0') as ksname:
            curs.execute('create table blah (a int primary key, b int);')
            curs.execute('select * from blah;')
        conn.close()
        self.assertRaises(cql.ProgrammingError, curs.execute, 'select * from blah;')
