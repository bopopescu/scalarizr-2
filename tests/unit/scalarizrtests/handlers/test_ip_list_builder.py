'''
@author: Dmytro Korsakov
'''
import unittest
import os
import ConfigParser
from scalarizr.bus import bus
from scalarizr.handlers import ip_list_builder
from scalarizr.messaging import Message
from szr_unittest import RESOURCE_PATH
from scalarizr.config import ScalarizrCnf

class _Bunch(dict):
    __getattr__, __setattr__ = dict.get, dict.__setitem__

class _QueryEnv:
    def list_roles(self, role_name=None, behaviour=None):
        return [_Bunch(
                behaviour = ["mysql"],
                name = "mysql-lvm",
                hosts = [_Bunch(
                        index='1',
                        replication_main=True,
                        internal_ip="127.0.0.1",
                        external_ip="192.168.1.92"
                        ),
                        _Bunch(
                        index='2',
                        replication_main=False,
                        internal_ip="127.0.0.2",
                        external_ip="192.168.1.93"
                        )
                        ]
                )]


class TestIpListBuilder(unittest.TestCase):

    def setUp(self):
        bus.etc_path = os.path.join(RESOURCE_PATH, 'etc')
        cnf = ScalarizrCnf(bus.etc_path)
        cnf.load_ini('ip_list_builder')
        bus.cnf = cnf

        bus.queryenv_service = _QueryEnv()
        bus.define_events("before_host_up", "init")

        self.ip_lb = ip_list_builder.IpListBuilder()

    def test_host_is_replication_main(self):
        is_replication_main = self.ip_lb._host_is_replication_main('127.0.0.1', 'mysql-lvm')
        self.assertTrue(is_replication_main)

    def _on_HostUpDown(self,internal_ip, prefix):
        role_alias = 'mysql'
        role_name = 'mysql-lvm'
        role_dir = self.ip_lb._base_path + os.sep + role_name
        role_file = role_dir + os.sep + internal_ip
        mysql_dir = self.ip_lb._base_path + os.sep + role_alias + prefix #
        mysql_file = mysql_dir + os.sep + internal_ip
        msg = Message(body=dict(role_name=role_name, behaviour=[role_alias], local_ip=internal_ip))
        self.ip_lb.on_HostUp(msg)

        self.assertTrue(os.path.exists(role_dir) and os.path.isdir(role_dir))
        self.assertTrue(os.path.exists(role_file) and os.path.isfile(role_file))
        self.assertTrue(os.path.exists(mysql_dir) and os.path.isdir(mysql_dir))
        self.assertTrue(os.path.exists(mysql_file) and os.path.isfile(mysql_file))
        self.ip_lb.on_HostDown(msg)
        self.assertFalse(os.path.exists(role_dir))
        self.assertFalse(os.path.exists(role_file))
        self.assertFalse(os.path.exists(mysql_dir))
        self.assertFalse(os.path.exists(mysql_file))

    def test_on_HostUpDown1(self):
        self._on_HostUpDown(internal_ip = '127.0.0.1', prefix = "-main")


    def test_on_HostUpDown2(self):
        self._on_HostUpDown(internal_ip = '127.0.0.2', prefix = "-subordinate")

if __name__ == "__main__":
    unittest.main()
