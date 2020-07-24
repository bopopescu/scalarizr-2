from __future__ import with_statement
'''
Created on Mar 14, 2012

@author: marat
'''

import time
import Queue
import threading
import weakref
import logging
from weakref import WeakValueDictionary

from scalarizr.util import wait_until
import sys
import sqlite3

LOG = logging.getLogger(__name__)
GLOBAL_TIMEOUT = 30

class Proxy(object):


    def __init__(self, tasks_queue):
        '''
        Ingoing tasks queue. Item is a tuple(method, client_hash, args, kwds)
        Outgoing result.
        '''
        self.result = None
        self.error = None
        self.tasks_queue = tasks_queue
        self.result_available = threading.Event()
        self.hash = self.__hash__()

    def _call(self, method, args=None, kwds=None, wait=True):
        self.result_available.clear()
        self.tasks_queue.put((method, self.__hash__(), args, kwds))
        if wait:
            self.result_available.wait(GLOBAL_TIMEOUT)
        try:
            if self.error:
                raise self.error[0], self.error[1]
            else:
                return self.result
        finally:
            self.error = None
            self.result = None


class CursorProxy(Proxy):

    def __init__(self, tasks_queue):
        super(CursorProxy, self).__init__(tasks_queue)
        self._execute_result = None
        self._call('cursor_create', [self])


    def execute(self, sql, parameters=None):
        args = [sql]
        if parameters:
            args += [parameters]
        self._execute_result = self._call('cursor_execute', args)

        if not self._execute_result:
            self._execute_result = dict(data=[], rowcount=0)

        # Temporary
        #LOG.debug('Execute result: %s', self._execute_result)

        self._execute_result['iter'] = iter(self._execute_result['data'] or [None])
        return self


    def fetchone(self):
        try:
            return self._execute_result['iter'].next()
        except StopIteration:
            return None


    def fetchall(self):
        try:
            return self._execute_result['data']
        finally:
            self._execute_result['data'] = None

    @property
    def rowcount(self):
        return self._execute_result['rowcount']


    def close(self):
        pass
        #self._call('cursor_delete', wait=False)


    __del__ = close


class ConnectionProxy(Proxy):

    def cursor(self):
        cp = CursorProxy(self.tasks_queue)
        return cp


    def commit(self):
        # no worries, autocommit is set
        pass


    def executescript(self, sql):
        return self._call('conn_executescript', [sql])




    def _get_row_factory(self):
        return self._call('conn_get_row_factory')


    def _set_row_factory(self,f):
        return self._call('conn_set_row_factory', [f])


    row_factory = property(_get_row_factory, _set_row_factory)


    def _get_text_factory(self):
        return self._call('conn_get_text_factory')


    def _set_text_factory(self,f):
        return self._call('conn_set_text_factory', [f])


    text_factory = property(_get_text_factory, _set_text_factory)



class SqliteServer(object):

    def __init__(self, conn_creator):
        self._main_conn = conn_creator()
        self._main_conn.isolation_level = None
        self._single_conn_proxy = None
        self._clients = WeakValueDictionary()
        self._cursors = {}


    def connect(self):
        if not self._single_conn_proxy:
            self._single_conn_proxy = ConnectionProxy(Queue.Queue())
            self._clients[self._single_conn_proxy.__hash__()] = self._single_conn_proxy
        return self._single_conn_proxy


    def serve_forever(self):
        while True:
            # TODO: what about to create connection here and periodically check it's health
            # This will allow us to remove SQLiteServerThread class

            job = self._single_conn_proxy.tasks_queue.get()
            #LOG.debug('job: %s', job)
            try:
                result = error = None
                try:
                    if type(job) != tuple or len(job) != 4:
                        raise TypeError('Expected tuple(method, hash, args, kwds)')
                    method, hash, args, kwds = '_%s' % job[0], job[1], job[2] or [], job[3] or {}
                    result = getattr(self, method)(hash, *args, **kwds)
                except:
                    error = sys.exc_info()
                finally:
                    # If client stil exists
                    #LOG.debug('Result: %s, Error: %s', result, error)
                    if hash in self._clients:

                        self._clients[hash].result = result
                        self._clients[hash].error = error
                        self._clients[hash].result_available.set()
                    else:
                        LOG.debug('result is ready but client disconnected (client: %s)', hash)
            except:
                LOG.warning('Recoverable error in SQLite server loop', exc_info=sys.exc_info())


    def _cursor_create(self, hash, proxy):
        """
        self._cursors[hash] = self._main_conn.cursor()
        return self._cursors[hash]
        """
        #LOG.debug('create cursor %s', hash)
        self._clients[hash] = proxy


    def _cursor_delete(self, hash):
        """
        result = None
        if hash in self._cursors:
                result = self._cursors[hash].close()
                del self._cursors[hash]
        return result
        """
        if hash in self._clients:
            #LOG.debug('delete cursor %s', hash)
            del self._clients[hash]


    def _cursor_execute(self, hash, *args, **kwds):
        cur = self._main_conn.cursor()
        try:
            cur.execute(*args, **kwds)
            return {
                    'data': cur.fetchall(),
                    'rowcount': cur.rowcount
            }
        finally:
            cur.close()


    def _cursor_fetchone(self, hash):
        result = None
        if hash in self._cursors:
            result = self._cursors[hash].fetchone()
        return result


    def _cursor_fetchall(self, hash):
        result = None
        if hash in self._cursors:
            result = self._cursors[hash].fetchall()
        return result


    def _cursor_rowcount(self, hash):
        result = None
        if hash in self._cursors:
            result = self._cursors[hash].rowcount
        return result


    def _conn_set_row_factory(self, hash, f):
        self._main_conn.row_factory = f


    def _conn_set_text_factory(self, hash, f):
        self._main_conn.text_factory = f


    def _conn_get_row_factory(self, hash):
        return self._main_conn.row_factory


    def _conn_get_text_factory(self, hash):
        return self._main_conn.text_factory


    def _conn_executescript(self, hash, sql):
        return self._main_conn.executescript(sql)


    def _conn_execute(self, hash, *args, **kwds):
        cur = self._cursor_create(hash, self._single_conn_proxy)
        try:
            cur.execute(*args)
        finally:
            self._cursor_delete(hash)


    def _conn_fetchall(self, hash, *args, **kwds):
        cur = self._cursor_create(hash, self._single_conn_proxy)
        try:
            cur.execute(*args)
            return cur.fetchall()
        finally:
            self._cursor_delete(hash)


    def _conn_fetchone(self, hash, *args, **kwds):
        cur = self._cursor_create(hash, self._single_conn_proxy)
        try:
            cur.execute(*args)
            return cur.fetchone()
        finally:
            self._cursor_delete(hash)


class _NULL(object):
    pass

class SQLiteServerThread(threading.Thread):

    ready = None
    connection = None
    conn_creator = None

    def __init__(self, conn_creator):
        self.ready = False
        self.conn_creator = conn_creator
        threading.Thread.__init__(self)

    def run(self):
        server = SqliteServer(self.conn_creator)
        self.connection = server.connect()
        self.ready = True
        server.serve_forever()


def wait_for_server_thread(t):
    wait_until(lambda: t.ready == True, sleep = 0.1)
