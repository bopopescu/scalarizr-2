'''
Created on June, 25 2010

@author: Dmytro Korsakov
'''
import unittest
from scalarizr.util import init_tests
from scalarizr.bus import bus
from scalarizr.handlers import cassandra
from scalarizr.linux import mount
from xml.dom.minidom import parse
from scalarizr.platform import ec2

platform = ec2.Ec2Platform()

file = open('/tmp/keys')
access = {}
access['account_id'] = file.readline().strip()
access['key_id']     = file.readline().strip()
access['key']        = file.readline().strip()
print access
file.close()
platform.set_access_data(access)
bus.platform = platform

class Test(unittest.TestCase):

    def setUp(self):
        config = bus.config
        self._storage_path = config.get('behaviour_cassandra','storage_path')
        self._storage_conf = config.get('behaviour_cassandra','storage_conf')

    def _tearDown(self):
        mount.umount("/dev/sdb1", clean_fstab=True)

    def testName(self):
        class _Bunch(dict):
            __getattr__, __setattr__ = dict.get, dict.__setitem__

        class _QueryEnv:
            def list_role_params(self, role_name=None):
                return _Bunch(cassandra_data_storage_engine = "eph"
)
            def list_roles(self, behaviour):
                return [_Bunch(
                                behaviour = "cassandra",
                                name = "cassandra-node-1",
                                hosts = [_Bunch(index='1',replication_main="1",internal_ip="192.168.1.93",external_ip="8.8.8.8")]
                                ),
                                _Bunch(
                                behaviour = "cassandra",
                                name = "cassandra-node-2",
                                hosts = [_Bunch(index='2',replication_main="0",internal_ip=None,external_ip="8.8.8.9")]
                                )]
        class _Message:
            def __init__(self):
                self.storage_conf_url = 's3://szr-test/storage-conf.xml'
                self.snapshot_url        = 's3://szr-test/cassandra.tar.gz'
                self.storage_size        = 1

        bus.queryenv_service = _QueryEnv()
        C = cassandra.CassandraHandler()
        C.on_before_host_up(_Message())

        xml = parse(self._storage_conf)
        data = xml.documentElement

        log_entry = data.getElementsByTagName("CommitLogDirectory")
        self.assertEqual(log_entry[0].firstChild.nodeValue, C.commit_log_directory)

        data_entry = data.getElementsByTagName("DataFileDirectory")
        self.assertEqual(data_entry[0].firstChild.nodeValue, C.data_file_directory)

    def _test_fstab(self):
        fstab = mount.fstab("/etc/fstab")
        entries = fstab.list_entries()
        self.assertEqual(entries[1].device, "/dev/sda1")
        self.assertEqual(entries[1].mpoint, "/")
        self.assertEqual(entries[1].fstype, "ext4")
        self.assertEqual(entries[1].options, "errors=remount-ro")
        self.assertEqual(entries[1].value, "/dev/sda1      /                       ext4 errors=remount-ro 0        1")


class _Platform:
    def get_block_device_mapping(self):
        return dict(ephemeral0 ="sdb1")
    def get_private_ip(self):
        return '10.251.90.34'
if __name__ == "__main__":
    init_tests()
    unittest.main()
