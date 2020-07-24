'''
Created on 17.06.2010

@author: spike
'''

import unittest
import os
import signal, pexpect, re
from scalarizr.util import init_tests, ping_service
from scalarizr.bus import bus
from scalarizr.handlers import mysql
from scalarizr.util import system, initd, disttool, cryptotool, configtool
from scalarizr.platform.ec2 import Ec2Platform
from subprocess import Popen, PIPE, STDOUT
import time, shutil, hashlib, pwd


class _Volume:
    def __init__(self):
        self.id = 'test_id'
        self._messages = []

class _MysqlHandler(mysql.MysqlHandler):
    def _create_storage(self, vol_id, mnt_point):
        pass
    def _detach_delete_volume(self, volume):
        pass
    def _create_volume_from_snapshot(self, snap_id):
        return _Volume()
    def _mount_device(self, devname, mnt_point):
        pass
    def send_message(self, message):
        self._messages.append(message)
    def _create_ebs_snapshot(self):
        pass
    def _take_main_volume(self, volume_id, ec2_conn=None):
        return _Volume()

LOCAL_IP = '12.34.56.78'

class Test(unittest.TestCase):

    def setUp(self):
        system('mkdir /mnt/tmpdir')
        system('rsync -a /var/lib/mysql/ /mnt/tmpdir/mysql-data')
        system('rsync -a /var/log/mysql/binarylog/ /mnt/tmpdir/mysql-misc')
        system('cp -pr /etc/mysql/ /tmp/mysqletc/')
        system('cp -pr /var/lib/mysql /tmp/mysqldata/')

    def tearDown(self):
        initd.stop("mysql")
        system('cp /etc/mysql/my.cnf /tmp/etc'+time.strftime("%d%b%H%M%S", time.gmtime()))
        system('rm -rf /etc/mysql/')
        system('rm -rf /var/lib/mysql')
        system('cp -pr /tmp/mysqletc/ /etc/mysql/ ')
        system('cp -pr /tmp/mysqldata/ /var/lib/mysql ')
        system('rm -rf /tmp/mysql*')
        system('rm -rf /mnt/dbstorage/*')
        system('rm -rf /mnt/tmpdir/*')
        initd.start("mysql")
        config = bus.config
        section = configtool.get_behaviour_section_name(mysql.BEHAVIOUR)
        try:
            config.remove_option(section, mysql.OPT_ROOT_USER)
            config.remove_option(section, mysql.OPT_ROOT_PASSWORD)
            config.remove_option(section, mysql.OPT_REPL_USER)
            config.remove_option(section, mysql.OPT_REPL_PASSWORD)
            config.remove_option(section, mysql.OPT_STAT_USER)
            config.remove_option(section, mysql.OPT_STAT_PASSWORD)
        except:
            pass

    def test_users(self):
        bus.queryenv_service = _QueryEnv()
        bus.platform = _Platform()
        handler = _MysqlHandler()
        handler._stop_mysql()
        root_password, repl_password, stat_password = handler._add_mysql_users(mysql.ROOT_USER,
                                                                                                                                                   mysql.REPL_USER,
                                                                                                                                                   mysql.STAT_USER)
        handler._stop_mysql()
        myd = handler._start_mysql_skip_grant_tables()
        for user, password in {mysql.ROOT_USER: root_password,
                                                   mysql.REPL_USER: repl_password,
                                                   mysql.STAT_USER: stat_password}.items():
            myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
            out,err = myclient.communicate("SELECT Password from mysql.user where User='"+user+"'")
            hashed_pass = re.search ('Password\n(.*)', out).group(1)
            self.assertEqual(hashed_pass, mysql_password(password))
        os.kill(myd.pid, signal.SIGTERM)

    def test_create_snapshot(self):
        bus.queryenv_service = _QueryEnv()
#               bus.platform = Ec2Platform()
        bus.platform = _Platform()
        handler = _MysqlHandler()

        root_password, repl_password, stat_password = handler._add_mysql_users(mysql.ROOT_USER,
                                                                                                                                                   mysql.REPL_USER,
                                                                                                                                                   mysql.STAT_USER)
        handler._move_mysql_dir('log_bin', '/var/log/mysql/binarylog/binary.log', 'mysqld')
        handler._replication_init()
        snap_id, log_file, log_pos = handler._create_snapshot(mysql.ROOT_USER, root_password)
        sql = pexpect.spawn('/usr/bin/mysql -u' + mysql.ROOT_USER + ' -p' + root_password)
        sql.expect('mysql>')
        sql.sendline('SHOW MASTER STATUS;\n')
        sql.expect('mysql>')
        lines = sql.before
        # Retrieve log file and log position
        log_row = re.search(re.compile('^\|\s*([\w-]*\.\d*)\s*\|\s*(\d*)', re.MULTILINE), lines)
        if log_row:
            true_log_file = log_row.group(1)
            true_log_pos = log_row.group(2)
        sql.close()
        self.assertEqual(log_file, true_log_file)
        self.assertEqual(log_pos, true_log_pos)
        file = open('/etc/mysql/farm-replication.cnf')
        self.assertEqual('[mysqld]\nserver-id\t\t=\t1\nmain-connect-retry\t\t=\t15\n', file.read())
        file.close()


    def test_on_before_host_up(self):
        bus.queryenv_service = _QueryEnv()
#               bus.platform = Ec2Platform()
        bus.platform = _Platform()
        handler = _MysqlHandler()
        message = _Message()
        config = bus.config
        config.set(configtool.SECT_GENERAL, mysql.OPT_REPLICATION_MASTER, '1')
        handler.on_before_host_up(message)
        self.assertTrue(os.path.exists('/mnt/dbstorage/mysql-data'))
        self.assertTrue(os.path.exists('/mnt/dbstorage/mysql-misc'))
        mysql_user      = pwd.getpwnam("mysql")
        self.assertEqual(mysql_user.pw_uid, os.stat('/mnt/dbstorage/mysql-data')[4])
        self.assertEqual(mysql_user.pw_gid, os.stat('/mnt/dbstorage/mysql-data')[5])
        self.assertEqual(mysql_user.pw_uid, os.stat('/mnt/dbstorage/mysql-misc')[4])
        self.assertEqual(mysql_user.pw_gid, os.stat('/mnt/dbstorage/mysql-misc')[5])
        handler.on_before_host_up(message)
        datadir, log_bin = extract_datadir_and_log()
        self.assertEqual(datadir, '/mnt/dbstorage/mysql-data/')
        self.assertEqual(log_bin, '/mnt/dbstorage/mysql-misc/binlog.log')

    def test_on_mysql_newmain_up(self):
        bus.queryenv_service = _QueryEnv()
        bus.platform = _Platform()
        config = bus.config
        sect_name = configtool.get_behaviour_section_name(mysql.BEHAVIOUR)
        config.set(sect_name, mysql.OPT_REPLICATION_MASTER, '0')
        handler = _MysqlHandler()
        root_pass, repl_pass, stat_pass = handler._add_mysql_users(mysql.ROOT_USER, mysql.REPL_USER, mysql.STAT_USER)
        handler._update_config( {mysql.OPT_ROOT_PASSWORD : root_pass,
                                                         mysql.OPT_REPL_PASSWORD : repl_pass,
                                                         mysql.OPT_STAT_PASSWORD : stat_pass})
        message = _Message()
        if disttool.is_redhat_based():
            daemon = "/usr/libexec/mysqld"
        else:
            daemon = "/usr/sbin/mysqld"
        initd.stop("mysql")
        myd = Popen([daemon, '--defaults-file=/etc/mysql2/my.cnf', '--skip-grant-tables'], stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        ping_service(LOCAL_IP, 3306, 5)
        myclient = pexpect.spawn('/usr/bin/mysql -h'+LOCAL_IP)
        myclient.expect('mysql>')
        repl_password = re.sub('[^\w]','', cryptotool.keygen(20))
        sql = "update mysql.user set password = PASSWORD('"+repl_password+"') where user = '"+mysql.REPL_USER+"';"
        myclient.sendline(sql)
        myclient.expect('mysql>')
        result = myclient.before
        if re.search('ERROR', result):
            os.kill(myd.pid, signal.SIGTERM)
            raise BaseException("Cannot update user", result)
        myclient.sendline('FLUSH TABLES WITH READ LOCK;')
        myclient.expect('mysql>')
#               system('cp -pr /var/lib/mysql /var/lib/backmysql')
#               system('rm -rf /var/lib/mysql && cp -pr /var/lib/mysql2 /var/lib/mysql')
        myclient.sendline('SHOW MASTER STATUS;')
        myclient.expect('mysql>')
        # retrieve log file and position
        try:
            main_status = myclient.before.split('\r\n')[4].split('|')
        except:
            raise BaseException("Cannot get main status")
        finally:
            myclient.sendline('UNLOCK TABLES;')
            os.kill(myd.pid, signal.SIGTERM)
        myd = Popen([daemon, '--defaults-file=/etc/mysql2/my.cnf'], stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        ping_service(LOCAL_IP, 3306, 5)
        message.log_file = main_status[1].strip()
        message.log_pos = main_status[2].strip()
        message.repl_user = mysql.REPL_USER
        message.repl_password = repl_password
        message.root_password = root_pass
        handler.on_Mysql_NewMainUp(message)
        os.kill(myd.pid, signal.SIGTERM)
        initd.stop("mysql")
        system ('rm -rf /var/lib/mysql && cp -pr /var/lib/backmysql /var/lib/mysql && rm -rf /var/lib/backmysql')
        config.set(sect_name, mysql.OPT_REPLICATION_MASTER, '1')

    def test_on_before_host_up_subordinate_ebs(self):
        bus.queryenv_service = _QueryEnv()
        bus.platform = _Platform()
        message = _Message()
        config = bus.config
        sect_name = configtool.get_behaviour_section_name(mysql.BEHAVIOUR)
        config.set(sect_name, mysql.OPT_REPLICATION_MASTER, '0')
        bus.queryenv_service.storage = 'ebs'
        handler = _MysqlHandler()
        handler.on_before_host_up(message)
        config.set(sect_name, mysql.OPT_REPLICATION_MASTER, '1')
        datadir, log_bin = extract_datadir_and_log()
        self.assertEqual(datadir, '/mnt/dbstorage/mysql-data/')
        self.assertEqual(log_bin, '/mnt/dbstorage/mysql-misc/binlog.log')

    def test_on_before_host_up_subordinate_eph(self):
        bus.queryenv_service = _QueryEnv()
        bus.platform = _Platform()
        message = _Message()
        config = bus.config
        sect_name = configtool.get_behaviour_section_name(mysql.BEHAVIOUR)
        config.set(sect_name, mysql.OPT_REPLICATION_MASTER, '0')
        bus.queryenv_service.storage = 'eph'
        handler = _MysqlHandler()
        handler.on_before_host_up(message)
        config.set(sect_name, mysql.OPT_REPLICATION_MASTER, '1')
        datadir, log_bin = extract_datadir_and_log()
        self.assertEqual(datadir, '/mnt/dbstorage/mysql-data/')
        self.assertEqual(log_bin, '/mnt/dbstorage/mysql-misc/binlog.log')

    def test_on_Mysql_PromoteToMain(self):
        bus.queryenv_service = _QueryEnv()
        bus.platform = _Platform()
        config = bus.config
        sect_name = configtool.get_behaviour_section_name(mysql.BEHAVIOUR)
        config.set(sect_name, mysql.OPT_REPLICATION_MASTER, '0')
        message = _Message()
        message.root_password = '123'
        message.repl_password = '456'
        message.stat_password = '789'
        handler = _MysqlHandler()
        handler.on_Mysql_PromoteToMain(message)

def mysql_password(str):
    pass1 = hashlib.sha1(str).digest()
    pass2 = hashlib.sha1(pass1).hexdigest()
    return "*" + pass2.upper()

def extract_datadir_and_log():
    if disttool.is_redhat_based():
        my_cnf_file = "/etc/my.cnf"
    else:
        my_cnf_file = "/etc/mysql/my.cnf"
    file = open(my_cnf_file)
    mycnf = file.read()
    file.close()
    datadir = re.search(re.compile('^\s*datadir\s*=\s*(.*)$', re.MULTILINE), mycnf).group(1)
    log_bin = re.search(re.compile('^\s*log_bin\s*=\s*(.*)$', re.MULTILINE), mycnf).group(1)
    return datadir, log_bin

class _Bunch(dict):
    __getattr__, __setattr__ = dict.get, dict.__setitem__

class _QueryEnv:

    def __init__(self):
        self.storage = 'ebs'

    def list_role_params(self, role_name):
        return _Bunch(
                mysql_data_storage_engine = self.storage,
                mysql_main_ebs_volume_id = 'test-id',
                ebs_snap_id = 'test_snap_id'
                )

class _Platform:
    def get_instance_id(self):
        pass

    def get_block_device_mapping(self):
        return {'ephemeral0' : 'eph0'}

class _Message:
    def __init__(self):
        self.repl_user = None
        self.repl_password = None
        self.log_file = None
        self.log_pos  = None
        self.mysql_repl_user = None
        self.mysql_repl_password = None
        self.mysql_stat_password = None
        self.mysql_stat_user = None
        self.local_ip = LOCAL_IP
        self.mysql_replication_main = 1

if __name__ == "__main__":
    init_tests()
    unittest.main()
