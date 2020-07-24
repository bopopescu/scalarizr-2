'''
Created on July 23, 2010

@author: shaitanich
'''
import unittest
import os
from scalarizr.util import init_tests
from scalarizr.bus import bus
from scalarizr.handlers import memcached


class _Bunch(dict):
    __getattr__, __setattr__ = dict.get, dict.__setitem__


class _QueryEnv:
    def list_roles(self, behaviour=None):
        return [_Bunch(
                behaviour = "app",
                name = "nginx",
                hosts = [_Bunch(index='1',replication_main="1",internal_ip="8.8.8.8",external_ip="192.168.1.93")]
                )]


class Test(unittest.TestCase):

    def testName(self):
        bus.queryenv_service = _QueryEnv()
        m = memcached.MemcachedHandler()
        m.on_before_host_up()
        m.on_HostUp()


if __name__ == "__main__":
    init_tests()
    unittest.main()
