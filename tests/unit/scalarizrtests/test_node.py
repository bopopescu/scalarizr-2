import os

import mock

from scalarizr import node
from nose.tools import raises


class TestCompound(object):
    def test_plain_key(self):
        store = mock.MagicMock(spec=node.Store)
        store.__len__.return_value = 1
        store.__getitem__.return_value = 'aaa'

        main = node.Compound({'plain_key': store})

        assert main['plain_key'] == 'aaa'
        store.__getitem__.assert_called_with('plain_key')


    def test_re_key(self):
        store = {
                'root_password': 'qqq',
                'stat_password': 'ppp'
        }

        main = node.Compound({'*_password': store})

        assert main['root_password'] == 'qqq'
        assert main['stat_password'] == 'ppp'
        try:
            main['undefined_password']
            assert 0, 'Expected KeyError'
        except KeyError:
            pass


    def test_enum_key(self):
        values = {
                'server_id': '14593',
                'platform': 'ec2'
        }
        def getitem(key):
            return values[key]
        store = mock.MagicMock(spec=node.Store)
        store.__len__.return_value = len(values)
        store.__getitem__.side_effect = getitem

        main = node.Compound({'server_id,platform': store})

        assert main['server_id'] == '14593'
        assert main['platform'] == 'ec2'


    def test_set_undefined_key(self):
        main = node.Compound()

        main['key1'] = 'ooo'
        assert main['key1'] == 'ooo'


    def test_update(self):
        mysql = node.Compound({
                'behavior': 'percona'
        })
        mysql.update({
                'replication_main': '1'
        })

        assert 'replication_main' in mysql
        assert mysql['replication_main'] == '1'


class TestJson(object):
    def setup(self):
        self.fixtures_dir = os.path.dirname(__file__) + '/../fixtures'
        filename = self.fixtures_dir + '/node.json'
        self.store = node.Json(filename, mock.Mock())


    def teardown(self):
        for name in ('node-test-set-dict.json', 'node-test-set-object.json'):
            filename = os.path.join(self.fixtures_dir, name)
            if os.path.isfile(filename):
                os.remove(filename)


    def test_get(self):
        val = self.store['any_key']

        assert val
        self.store.fn.assert_called_with(
                        type='eph',
                        id='eph-vol-592f4b8c',
                        size='80%')


    def test_set_dict(self):
        data = {'type': 'lvm', 'vg': 'mysql'}

        self.store.filename = self.fixtures_dir + '/node-test-set-dict.json'
        self.store['any_key'] = data


    def test_set_object(self):
        class _Data(object):
            def __init__(self, data):
                self.data = data
            def config(self):
                return self.data

        data = {'type': 'lvm', 'vg': 'mysql'}
        self.store.filename = self.fixtures_dir + '/node-test-set-object.json'
        self.store['any_key'] = _Data(data)


class TestIni(object):
    def setup(self):
        filename = os.path.dirname(__file__) + '/../fixtures/node.ini'
        self.store = node.Ini(filename, 'mysql')


    def test_get(self):
        assert self.store['root_password'] == 'Q9OgJxYf19ygFHpRprLF'


    @raises(KeyError)
    def test_get_nosection(self):
        self.store.section = 'undefined'
        self.store['log_file']

    @raises(KeyError)
    def test_get_nooption(self):
        self.store['undefined_option']

    @mock.patch('__builtin__.open')
    def test_set(self, open):
        with mock.patch.object(self.store, '_reload') as reload:
            self.store.ini = mock.Mock()
            self.store['new_option'] = 1
            self.store.ini.set.assert_called_with(self.store.section, 'new_option', '1')
            assert self.store.ini.write.call_count == 1

    def test_set_new_file(self):
        filename = os.path.dirname(__file__) + '/../fixtures/node_new.ini'
        self.store.filename = filename
        try:
            self.store['root_password'] = 'abs'
            assert self.store['root_password'] == 'abs'
        finally:
            if os.path.exists(filename):
                os.remove(filename)
