
import mock

import sys


eph_host_init_response_new_main = {
        'server_index': '1',
        'db_type': 'percona',
        'percona': {
                'replication_main': 1,
                'volume_config': {
                        'type': 'eph',
                },
                'snapshot_config': None,
                'root_password': None,
                'repl_password': None,
                'stat_password': None,
                'log_file': None,
                'log_pos': None
        }
}

eph_host_init_response_respawn_main = {
        'server_index': '1',
        'db_type': 'percona',
        'percona': {
                'replication_main': 1,
                'volume_config': {
                        'type': 'eph',
                        'mpoint': '/mnt/dbstorage',
                        'device': '/dev/percona/data'
                },
                'snapshot_config': {
                        'type': 'eph',
                        'id': 'eph-snap-12345678'
                },
                'root_password': 'Q9OgJxYf19ygFHpRprL',
                'repl_password': 'MGVmwVu6CkaKvwr4g7mT',
                'stat_password': 'ngATsFuvHFhKR1rAybLr',
                'log_file': 'binlog.000003',
                'log_pos': '107'
        }
}

eph_host_init_response_subordinate = {
        'local_ip': '10.146.34.58',
        'remote_ip': '176.34.6.168',
        'db_type': 'percona',
        'percona': {
                'replication_main': 1,
                'volume_config': {
                        'type': 'eph',
                        'id': 'eph-vol-2a3bd1c8'
                },
                'snapshot_config': {
                        'type': 'eph',
                        'id': 'eph-snap-fe9ac9ce'
                }
        }
}

eph_new_main_up = {
        'behaviour': ['percona'],
        'local_ip': '10.146.34.58',
        'remote_ip': '176.34.6.168',
        'db_type': 'percona',
        'percona': {
                'replication_main': '1',
                'snapshot_config': {'type': 'eph'},
                'root_password': 'zcuDiVum9hDvx1v97Ac5',
                'repl_password': 'cumLityXgnJv5JgaxmXA',
                'stat_password': 'UVVkn7ygMEMt7WktIqoy',
                'log_file': 'binlog.000003',
                'log_pos': '107'
        }
}

class NodeMock(dict):

    def __init__(self, *args, **kwds):
        super(NodeMock, self).__init__(
                        percona={},
                        behavior=['percona'])

    def __setitem__(self, key, value):
        if key == 'replication_main':
            value = int(key)
        super(NodeMock, self).__setitem__(key, value)


@mock.patch.dict('scalarizr.node.__node__', {'percona': {}, 'behavior': ['percona']})
@mock.patch.multiple('scalarizr.handlers.mysql2',
                                bus=mock.DEFAULT,
                                mysql_svc=mock.DEFAULT,
                                MysqlCnfController=mock.DEFAULT,
                                ServiceCtlHandler=mock.DEFAULT)
@mock.patch.multiple('scalarizr.storage2',
                                volume=mock.DEFAULT,
                                snapshot=mock.DEFAULT)
@mock.patch.multiple('scalarizr.services.backup',
                                backup=mock.DEFAULT,
                                restore=mock.DEFAULT)
class TestMysqlHandler(object):


    def test_main_new(self, **kwds):
        snapshot = mock.MagicMock(
                        name='main storage snapshot',
                        type='eph',
                        id='eph-snap-12345678')
        restore = mock.Mock(
                        name='main restore',
                        type='snap_mysql',
                        snapshot=snapshot,
                        log_file='binlog.000003',
                        log_pos='107')
        backup = mock.Mock(
                        name='main backup',
                        **{'run.return_value': restore})
        kwds['backup'].configure_mock(return_value=backup)

        from scalarizr.handlers import mysql2

        hdlr = mysql2.MysqlHandler()
        mock.patch.object(hdlr, '_storage_valid', return_value=False)

        hir = mock.Mock(**eph_host_init_response_new_main)
        host_up = mock.Mock()
        hdlr.on_host_init_response(hir)
        hdlr.on_before_host_up(host_up)

        __mysql__ = mysql2.__mysql__

        # datadir moved
        hdlr.mysql.move_mysqldir_to.assert_called_once_with(__mysql__['storage_dir'])
        # restore created
        backup.run.assert_called_once_with()
        # data stored in __mysql__
        assert __mysql__['root_password']
        assert __mysql__['repl_password']
        assert __mysql__['stat_password']
        assert 'backup' in __mysql__
        assert 'volume' in __mysql__
        assert 'restore' in __mysql__
        # HostUp message is valid
        assert host_up.db_type == 'percona'
        assert host_up.percona['log_file'] == restore.log_file
        assert host_up.percona['log_pos'] == restore.log_pos
        assert int(host_up.percona['replication_main']) == 1
        assert host_up.percona['root_password'] == __mysql__['root_password']
        assert host_up.percona['repl_password'] == __mysql__['repl_password']
        assert host_up.percona['stat_password'] == __mysql__['stat_password']
        assert 'volume_config' in host_up.percona
        assert 'snapshot_config' in host_up.percona


    def test_main_respawn(self, **kwds):
        return
        from scalarizr.handlers import mysql2

        hdlr = mysql2.MysqlHandler()
        mock.patch.object(hdlr, '_storage_valid', return_value=True)

        hir = mock.Mock(**eph_host_init_response_respawn_main)
        hdlr.on_host_init_response(hir)

        __mysql__ = mysql2.__mysql__
        assert (__mysql__['replication_main']) == 1
        assert __mysql__['restore']
        kwds['restore'].assert_called_with(
                        type='snap_mysql',
                        snapshot=mock.ANY,
                        volume=mock.ANY)
        __mysql__['restore'].type = 'snap_mysql'


        host_up = mock.Mock()
        hdlr.on_before_host_up(host_up)

        assert host_up.db_type == 'percona'
        assert host_up.percona['log_file']
        assert host_up.percona['log_pos']
        assert int(host_up.percona['replication_main']) == 1
        assert host_up.percona['root_password'] == __mysql__['root_password']
        assert host_up.percona['repl_password'] == __mysql__['repl_password']
        assert host_up.percona['stat_password'] == __mysql__['stat_password']
        assert 'volume_config' in host_up.percona
        assert 'snapshot_config' in host_up.percona


    def test_main_respawn_from_snapshot(self, **kwds):
        pass


    def test_subordinate(self, **kwds):
        pass


    def test_create_data_bundle(self, **kwds):
        pass


    def test_create_backup(self, **kwds):
        pass


    def test_subordinate_to_main(self, **kwds):
        pass


    def test_new_main_up(self, **kwds):
        pass


class TestMysqlHandlerXtrabackup(object):
    def test_main_new(self):
        pass

    def test_main_respawn(self):
        pass


class TestMysqlHandlerEphToLvm(object):
    def test_migrate_on_restart(self):
        pass

    def test_create_data_bundle(self):
        pass
