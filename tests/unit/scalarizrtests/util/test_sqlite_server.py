'''
Created on Mar 21, 2012

@author: dmitry
'''

import threading
import sqlite3
import os

from scalarizr.util import sqlite_server, system2
from scalarizr.util.software import which

from nose.tools import assert_raises, nottest
from nose.tools import nottest

import mock
import sys


DATABASE = '/tmp/sqlite_server_test.db'
SQLITE3 = None
CONN = None


def setup():
    global CONN, SQLITE3, SERVER_THREAD

    SQLITE3 = which('sqlite3')

    def creator():
        return sqlite3.Connection(database=DATABASE)

    t = sqlite_server.SQLiteServerThread(creator)
    t.setDaemon(True)
    t.start()
    sqlite_server.wait_for_server_thread(t)

    CONN = t.connection



def teardown(cls):
    os.remove(DATABASE)


class TestConnectionProxy(object):

    def test_executescript(self):
        script = '''
DROP TABLE IF EXISTS test_execute_script;
CREATE TABLE test_execute_script (
"id" INTEGER PRIMARY KEY,
"script_name" TEXT,
"return_code" INTEGER
);
INSERT INTO test_execute_script VALUES (1, '/usr/bin/python', -9);
'''
        CONN.executescript(script)

        query_result = system2("sqlite3 %s 'select script_name from test_execute_script'" % DATABASE, shell=True)[0].strip()
        assert query_result == '/usr/bin/python'


    def test_cursor(self):
        cur = CONN.cursor()
        assert type(cur) == sqlite_server.CursorProxy


class TestCursorProxy(object):
    @classmethod
    def setup_class(cls):
        script = '''
CREATE TABLE test_clients (
"id" INTEGER PRIMARY KEY,
"name" TEXT,
"age" INTEGER
);
INSERT INTO test_clients VALUES (1, 'Mr. First', 36);
INSERT INTO test_clients VALUES (2, 'Mr. Seconds', 41);
CREATE TABLE test_clients_update (
"id" INTEGER PRIMARY KEY,
"name" TEXT,
"age" INTEGER
);
INSERT INTO test_clients_update VALUES (1, 'Mr. First', 36);
'''
        CONN.executescript(script)

    def test_execute(self):
        cur = CONN.cursor()
        ret = cur.execute('SELECT * FROM test_clients WHERE id = ?', (2, ))
        assert type(ret) == sqlite_server.CursorProxy

    def test_execute_update(self):
        cur = CONN.cursor()
        cur.execute('UPDATE test_clients_update SET age = ? WHERE id = ?', [37, 1])
        assert cur.rowcount == 1

    def test_fetchone(self):
        cur = CONN.cursor()
        cur.execute('SELECT * FROM test_clients WHERE id = ?', (2, ))
        assert cur.fetchone() == (2, 'Mr. Seconds', 41)
        assert cur.fetchone() is None


    def test_fetchall(self):
        cur = CONN.cursor()
        cur.execute('SELECT * FROM test_clients ORDER BY id')
        assert cur.fetchall() == [(1, u'Mr. First', 36), (2, u'Mr. Seconds', 41)]
        assert cur.fetchall() is None


    def test_rowcount(self):
        pass

    def test_close(self):
        pass

    ''''
    def test_operate_closed_cursor(self):
            cur = CONN.cursor()
            cur.close()
            # XXX: This wasn't works. method silently dies by timeout
            assert_raises(Exception, cur.execute, 'SELECT * FROM sqlite_main')
    '''


class TestSQLiteServer(object):

    def test_threadsafity(self):
        num_threads = 3
        threads = [None]*num_threads

        def work(i):
            def inner():
                try:
                    cur = CONN.cursor()
                    cur.execute('CREATE TABLE t%d (id INTEGER PRIMARY KEY, name TEXT)' % i)
                    assert cur.rowcount == -1
                    cur.execute('INSERT INTO t%d VALUES (?, ?)' % i, [None, 'Mister'])
                    cur.execute('SELECT * FROM t%d' % i)
                    assert cur.fetchone() == (1, 'Mister')
                except:
                    threads[i][1] = sys.exc_info()
            return inner

        for i in range(0, num_threads):
            thread = threading.Thread(target=work(i))
            threads[i] = [thread, None]
            thread.start()

        for i in range(0, num_threads):
            threads[i][0].join()

        for i in range(0, num_threads):
            if threads[i][1]:
                raise threads[i][1][0], threads[i][1][1], threads[i][1][2]


    def test_invalid_query(self):
        cur = CONN.cursor()
        assert_raises(sqlite3.OperationalError, cur.execute, 'U KNOW SQL!')
        self.assert_requests_accepting()


    def test_invalid_server_method_name(self):
        cur = CONN.cursor()
        assert_raises(AttributeError, cur._call, 'unknown')
        self.assert_requests_accepting()


    def test_client_gone_during_server_method_call(self):
        pass


    def assert_requests_accepting(self):
        cur = CONN.cursor()
        cur.execute('select 1')
        assert cur.fetchone() == (1, )
