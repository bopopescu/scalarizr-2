from __future__ import with_statement
'''
Created on Nov 8, 2011

@author: dmitry
'''
from __future__ import with_statement

import os
import logging
import re
import subprocess
import signal
import pymysql
import random
import pwd
import threading
import time
import shutil
import socket
import errno


from pymysql import cursors

from scalarizr.config import BuiltinBehaviours
from scalarizr.services import  BaseService, ServiceError, BaseConfig, lazy, PresetProvider
from scalarizr.util import system2, disttool, firstmatched, initdv2, wait_until, PopenError, software
from scalarizr.util.initdv2 import wait_sock, InitdError
from scalarizr.linux.coreutils import chown_r
from scalarizr.libs import metaconf
from scalarizr.linux.rsync import rsync


LOG = logging.getLogger(__name__)

MYSQL_DEFAULT_PORT=3306
MYSQL_PATH  = '/usr/bin/mysql' # old mysql_path
MYCNF_PATH      = '/etc/mysql/my.cnf' if disttool.is_ubuntu() else '/etc/my.cnf'
MYSQLD_PATH = firstmatched(lambda x: os.access(x, os.X_OK), ('/usr/sbin/mysqld', '/usr/libexec/mysqld'))
MYSQLDUMP_PATH = '/usr/bin/mysqldump'
DEFAULT_DATADIR = "/var/lib/mysql"
DEFAULT_OWNER = "mysql"
STORAGE_DATA_DIR = "mysql-data"
STORAGE_BINLOG = "mysql-misc/binlog"
SU_EXEC = '/bin/su'
BASH = '/bin/bash'
PRESET_FNAME = 'my.cnf'

BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.MYSQL


class MySQL(BaseService):

    service = None
    my_cnf = None
    _instance = None


    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MySQL, cls).__new__(
                                                    cls, *args, **kwargs)
        return cls._instance


    def __init__(self):
        self._objects = {}
        self.service = initdv2.lookup(SERVICE_NAME)
        if not os.path.exists(MYCNF_PATH):
            if disttool.is_redhat_based() and os.path.exists('/usr/share/mysql/my-medium.cnf'):
                shutil.copy('/usr/share/mysql/my-medium.cnf', MYCNF_PATH)
            else:
                fp = open(MYCNF_PATH, 'w')
                fp.write('[mysqld]')
                fp.close()


    def _init_replication(self, main=True):
        LOG.info('Initializing replication')
        server_id = 1 if main else int(random.random() * 100000)+1
        self.my_cnf.server_id = server_id
        self.my_cnf.delete_options(['mysqld/bind-address', 'mysqld/skip-networking'])


    def init_main(self):
        pass

    def init_subordinate(self):
        pass

    def _init_service(self):
        pass

    def change_main_to(self):
        # client.change_main_to
        # check_replication_health and wait
        pass

    def check_replication_health(self):
        # set subordinate status
        # on fail get status from error.log
        pass

    @property
    def version(self):
        #5.1/5.5
        #percona/mysql
        pass


    def move_mysqldir_to(self, storage_path):
        LOG.info('Moving mysql dir to %s' % storage_path)
        for directive, dirname in (
                        ('mysqld/log_bin', os.path.join(storage_path,STORAGE_BINLOG)),
                        ('mysqld/datadir', os.path.join(storage_path,STORAGE_DATA_DIR) + '/')
                        ):

            dest = os.path.dirname(dirname)
            if os.path.isdir(dest):
                LOG.info('No need to move %s to %s: already in place.' % (directive, dest))
            else:
                os.makedirs(dest)

                raw_value = self.my_cnf.get(directive)
                LOG.debug('directive %s:%s' % (directive, raw_value))
                if raw_value:
                    src_dir = os.path.dirname(raw_value + "/") + "/"
                    LOG.debug('source path: %s' % src_dir)
                    if os.path.isdir(src_dir) and src_dir != dest:
                        try:
                            if not system2((software.which('selinuxenabled'), ), raise_exc=False)[2]:
                                if not system2((software.which('getsebool'), 'mysqld_disable_trans'), raise_exc=False)[2]:
                                    LOG.debug('Make SELinux rule for rsync')
                                    system2((software.which('setsebool'), '-P', 'mysqld_disable_trans', '1'))
                        except LookupError:
                            pass

                        LOG.info('Copying mysql directory \'%s\' to \'%s\'', src_dir, dest)
                        rsync(src_dir, dest, archive=True, exclude=['ib_logfile*', '*.sock'])

            self.my_cnf.set(directive, dirname)
            chown_r(dest, "mysql", "mysql")
            # Adding rules to apparmor config
            if disttool.is_debian_based():
                _add_apparmor_rules(dest)


    def flush_logs(self, data_dir):
        LOG.info('Flushing logs')
        if not os.path.exists(data_dir):
            return

        info_files = ['relay-log.info', 'main.info']
        files = os.listdir(data_dir)

        for file in files:
            if file in info_files or file.find('relay-bin') != -1:
                os.remove(os.path.join(data_dir, file))


    def _get_my_cnf(self):
        return self._get('my_cnf', MySQLConf.find)


    def _set_my_cnf(self, obj):
        self._set('my_cnf', obj)


    my_cnf = property(_get_my_cnf, _set_my_cnf)


class MySQLClient(object):
    _pool = dict()

    def __init__(self, user=None, passwd=None, db=None):
        self.db = db
        self.user = user
        self.passwd = passwd or ''


    def test_connection(self):
        LOG.debug('Checking MySQL service status')
        try:
            self.fetchone('SELECT 1')
        except pymysql.err.OperationalError, e:
            if 'Access denied for user' in str(e):
                return True
            elif "Can't connect to MySQL server on":
                return False
        except BaseException, e:
            LOG.debug('test_connection returned error: %s' % e)
            raise
        return True


    def list_databases(self):
        out = self.fetchall('SHOW DATABASES')
        LOG.debug('databases: %s' % str(out))
        databases = [db[0] for db in out if db]
        if 'information_schema' in databases:
            databases.remove('information_schema')
        if 'performance_schema' in databases:
            databases.remove('performance_schema')
        return databases


    def start_subordinate(self):
        return self.fetchone('START SLAVE')


    def stop_subordinate(self):
        return self.fetchone("STOP SLAVE")


    def reset_subordinate(self):
        return self.fetchone("RESET SLAVE")


    def stop_subordinate_io_thread(self):
        return self.fetchone("STOP SLAVE IO_THREAD")


	def start_subordinate_io_thread(self):
		return self.fetchone("START SLAVE IO_THREAD")
	
	
    def lock_tables(self):
        return self.fetchone('FLUSH TABLES WITH READ LOCK')


    def unlock_tables(self):
        return self.fetchone('UNLOCK TABLES')


    def create_user(self, login, host, password, privileges=None):
        priv_count = self._priv_count()
        if not privileges:
            '''
            XXX: temporary solution for mysql55
            '''
            cmd = "INSERT INTO mysql.user VALUES('%s','%s',PASSWORD('%s')" % (host, login, password) + ",'Y'"*priv_count
            if len(self.fetchdict("select * from mysql.user LIMIT 1;")) == 42:
                cmd += ",'','','','',0,0,0,0,'',''"
            else:
                cmd += ",''"*4 +',0'*4
            cmd += ");"
        else:
            cmd = "INSERT INTO mysql.user (Host, User, Password, %s) VALUES ('%s','%s',PASSWORD('%s'), %s);" \
                            % (', '.join(privileges), host,login,password, ', '.join(["'Y'"]*len(privileges)))
        self.fetchone(cmd)
        self.flush_privileges()


    def remove_user(self, login, host):
        return self.fetchone("DELETE FROM mysql.user WHERE User='%s' and Host='%s'" % (login, host))


    def user_exists(self, login, host):
        ret = self.fetchone("select User,Host from mysql.user where User='%s' and Host='%s'" % (login, host))
        result = True if ret and len(ret)==2 and ret[0]==login and ret[1]==host else False
        LOG.debug('user_exists query returned value: %s for user %s on host %s. User exists: %s' % (str(ret), login, host, str(result)))
        return result

    def set_user_password(self, username, host, password):
        return self.fetchone("UPDATE mysql.user SET Password=PASSWORD('%s') WHERE User='%s' AND Host='%s';" % (password, username, host))

    def flush_privileges(self):
        return self.fetchone("FLUSH PRIVILEGES")

    def change_main_to(self, host, user, password, log_file, log_pos):
        return self.fetchone('CHANGE MASTER TO MASTER_HOST="%(host)s", \
                                        MASTER_USER="%(user)s", \
                                        MASTER_PASSWORD="%(password)s", \
                                        MASTER_LOG_FILE="%(log_file)s", \
                                        MASTER_LOG_POS=%(log_pos)s, \
                                        MASTER_CONNECT_RETRY=15;' % vars())


    def subordinate_status(self):
        ret = self.fetchdict("SHOW SLAVE STATUS")
        LOG.debug('subordinate status: %s' % str(ret))
        if ret:
            return ret
        else:
            raise ServiceError('SHOW SLAVE STATUS returned empty set. Subordinate is not started?')


    def main_status(self):
        out = self.fetchdict('SHOW MASTER STATUS')
        log_file, log_pos = None, None
        if out:
            log_file, log_pos = out['File'], out['Position']
        return (log_file, log_pos)


    def reset_main(self):
        return self.fetchone("RESET MASTER")


    def show_global_variables(self):
        d = {}
        raw = self.fetchdict('SHOW GLOBAL VARIABLES', fetch_one=False)
        LOG.debug('global variables: %s' % str(raw))
        for row in raw:
            name = row['Variable_name']
            val = row['Value']
            d[name] = val
        return d

    def check_password(self, user, password):
        hash_pairs = self.fetchall("SELECT PASSWORD('%s') AS hash, Password AS valid_hash FROM mysql.user WHERE mysql.user.User = '%s';" %
                        (password, user))

        for pair in hash_pairs:
            if pair[0] != pair[1]:
                return False

        return True


    def version(self):
        return self.fetchone('SELECT VERSION()')

    def reconnect(self):
        self._pool[self.creds] = pymysql.connect(host="127.0.0.1", user=self.user, passwd=self.passwd, db=self.db)

    @property
    def creds(self):
        return (self.user, self.passwd, self.db)

    def get_connection(self, force=False):
        if force or not self.creds in self._pool:
            self.reconnect()
        return self._pool[self.creds]


    def _priv_count(self):
        res = self.fetchdict("select * from mysql.user LIMIT 1;")
        return len([r for r in res.keys() if r.endswith('priv')])


    def _fetch(self, query, cursor_type = None, fetch_one=False):
        conn = self.get_connection()
        cur = conn.cursor(cursor_type)
        LOG.debug(query)
        try:
            cur.execute(query)
        except (pymysql.err.Error, pymysql.err.OperationalError, socket.error, IOError), e:
            #catching mysqld restarts (e.g. sgt)
            if type(e) == pymysql.err.Error or e.args[0] in (2013,32,errno.EPIPE):
                try:
                    conn = self.get_connection(force=True)
                    cur = conn.cursor(cursor_type)
                    cur.execute(query)
                except socket.error, err:
                    if err.args[0] == 32:
                        raise ServiceError('Scalarizr was unable to connect to mysql with user %s: (%s)' % (self.user, str(err)))
            else:
                raise
        res = cur.fetchone() if fetch_one else cur.fetchall()
        return res


    def fetchdict(self, query, fetch_one=True):
        return self._fetch(query, cursors.DictCursor, fetch_one)


    def fetchall(self, query):
        return self._fetch(query)


    def fetchone(self, query):
        return self._fetch(query, fetch_one=True)


class MySQLUser(object):

    login = None
    password = None
    host = None
    privileges = None

    def __init__(self, client, login, password=None, host=None, privileges=None):
        self.cli = client
        self.login = login
        self.password = password
        self.host = host
        self.privileges = privileges


    def create(self):
        if self.exists():
            raise ServiceError('Unable to create user %s@%s: already exists.')
        LOG.debug('Creating user %s on host %s with password %s and privileges %s' % (self.login, self.host, self.password, self.privileges))
        self.cli.create_user(self.login, self.host, self.password, self.privileges)
        return self


    def check_password(self):
        if not self.exists():
            return False
        return self.cli.check_password(self.login, self.password)


    def exists(self):
        result = False
        try:
            result = self.cli.user_exists(self.login, self.host)
        except pymysql.err.OperationalError, e:
            raise ServiceError(str(e))
        return result


    def remove(self):
        return self.cli.remove_user(self.login, self.host)



class DataDir(object):

    def __init__(self):
        pass

    def move_to(self, dst):
        pass


class MySQLConf(BaseConfig):

    config_type = 'mysql'
    config_name = 'my.cnf'


    @classmethod
    def find(cls):
        return cls(MYCNF_PATH)


    def __init__(self, path, autosave=True):
        super(MySQLConf, self).__init__(path, autosave=True)
        self.data = metaconf.Configuration(self.config_type)
        if os.path.exists(self.path):
            self.data.read(self.path)
        try:
            self.data.options('mysqld')
        except metaconf.NoPathError:
            self.data.add('mysqld')
        finally:
            # WHY?
            self.data = None


    def _get_datadir(self):
        return self.get('mysqld/datadir')


    def _set_datadir(self, path):
        if not path:
            raise BaseException('Datadir value cannot be empty')
        self.set('mysqld/datadir', path)


    def _get_log_bin(self):
        return self.get('mysqld/log_bin')


    def _set_log_bin(self, path):
        self.set('mysqld/log_bin', path)


    def _get_server_id(self):
        return self.get('mysqld/server-id')


    def _set_server_id(self, id):
        self.set('mysqld/server-id', id)


    def _get_bind_address(self):
        return self.get('mysqld/bind-address')


    def _set_bind_address(self, addr):
        self.set('mysqld/bind-address', addr)


    def _get_skip_networking(self):
        return self.get('mysqld/skip-networking')


    def _set_skip_networking(self, val):
        self.set('mysqld/skip-networking', val)


    def _get_expire_logs_days(self):
        return self.get('mysqld/expire_logs_days')


    def _set_expire_logs_days(self, val):
        self.set('mysqld/expire_logs_days', val)


    def _get_skip_locking(self):
        return self.get('mysqld/skip-locking')


    def _set_skip_locking(self, val):
        self.set('mysqld/skip-locking', val)


    def _get_read_only(self):
        return self.get('mysqld/read_only')


    def _set_read_only(self, val):
        self.set('mysqld/read_only', val)

    def _get_socket(self):
        return self.get('mysqld/socket')


    def _set_socket(self, path):
        self.set('mysqld/socket', path)

    def _get_pid_file(self):
        return self.get('mysqld/pid-file')


    def _set_pid_file(self, path):
        self.set('mysqld/pid-file', path)

    log_bin = property(_get_log_bin, _set_log_bin)
    server_id = property(_get_server_id, _set_server_id)
    bind_address = property(_get_bind_address, _set_bind_address)
    skip_networking = property(_get_skip_networking, _set_skip_networking)
    skip_locking = property(_get_skip_locking, _set_skip_locking)
    expire_logs_days = property(_get_expire_logs_days, _set_expire_logs_days)
    datadir  = property(_get_datadir, _set_datadir)
    read_only = property(_get_read_only, _set_read_only)
    datadir_default = DEFAULT_DATADIR
    socket = property(_get_socket, _set_socket)
    pid_file = property(_get_pid_file, _set_pid_file)



class MySQLDump(object):

    host = None
    port = None

    def __init__(self, root_user=None, root_password=None):
        self.root_user = root_user or 'root'
        self.root_password = root_password or ''

    def create(self, dbname, filename, opts=None, mysql_upgrade=True):
        _opts = opts and list(opts) or []
        LOG.debug('Dumping database %s to %s' % (dbname, filename))
        _opts = [MYSQLDUMP_PATH, '-u', self.root_user, '--password='+self.root_password] + opts + ['--databases']
        with open(filename, 'w') as fp:
            system2(_opts + [dbname], stdout=fp)


class RepicationWatcher(threading.Thread):


    _state = None
    _client = None
    _main_host = None
    _repl_user = None
    _repl_password = None


    WATCHER_RUNNING = 'running'
    WATCHER_STOPPED = 'stopped'
    TIMEOUT = 60


    def __init__(self, client, main_host, repl_user, repl_password):
        super(RepicationWatcher, self).__init__()
        self._client = client
        self.change_main_host(main_host, repl_user, repl_password)


    def change_main_host(self, host, user, password):
        self.suspend()
        self._main_host = host
        self._repl_user = user
        self._repl_password = password
        self.resume()


    def start(self):
        self.resume()
        while True:
            if self._state == self.WATCHER_RUNNING:
                r_status = None
                try:
                    r_status = self._client.subordinate_status()
                except ServiceError, e:
                    LOG.error(e)

                if not r_status:
                    time.sleep(self.TIMEOUT)

                elif r_status['Subordinate_IO_Running'] == 'Yes' and r_status['Subordinate_SQL_Running'] == 'Yes':
                    time.sleep(self.TIMEOUT)

                elif r_status and r_status['Subordinate_SQL_Running'] == 'No' and \
                                        'Relay log read failure: Could not parse relay log event entry' in r_status['Last_Error']:
                    self.repair_relaylog(r_status['Relay_Main_Log_File'], r_status['Exec_Main_Log_Pos'])
                    time.sleep(self.TIMEOUT)
                else:
                    self.suspend()
                    msg = 'Replication is broken. Subordinate_IO_Running=%s, Subordinate_SQL_Running=%s, Last_Error=%s' % (
                                                    r_status['Subordinate_IO_Running'],
                                                    r_status['Subordinate_SQL_Running'],
                                                    r_status['Last_Error']
                                                    )
                    LOG.error(msg)

    def repair_relaylog(self, log_file, log_pos):
        LOG.info('Repairing relay log')
        try:
            self._client.stop_subordinate()
            self._client.reset_subordinate()
            self._client.change_main_to(self._main_host, self._repl_user, self._repl_password, log_file, log_pos)
            self._client.sart_subordinate()
        except BaseException, e:
            self.suspend()
            LOG.error(e)
        else:
            LOG.info('Relay log has been repaired')


    def resume(self):
        self._state = self.WATCHER_RUNNING


    def suspend(self):
        self._state = self.WATCHER_STOPPED



class MysqlInitScript(initdv2.ParametrizedInitScript):

    socket_file = None
    cli = None
    sgt_pid_path = '/tmp/mysqld-sgt.pid'


    @lazy
    def __new__(cls, *args, **kws):
        obj = super(MysqlInitScript, cls).__new__(cls, *args, **kws)
        cls.__init__(obj)
        return obj


    def __init__(self):
        self.mysql_cli = MySQLClient()


        if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
            initd_script = ('/usr/sbin/service', 'mysql')
        else:
            initd_script = firstmatched(os.path.exists, ('/etc/init.d/mysqld', '/etc/init.d/mysql'))

        pid_file = None
        try:
            out = system2("my_print_defaults mysqld", shell=True, silent=True)
            m = re.search("--pid[-_]file=(.*)", out[0], re.MULTILINE)
            if m:
                pid_file = m.group(1)
            m = re.search("--socket=(.*)", out[0], re.MULTILINE)
            if m:
                self.socket_file = m.group(1)
        except:
            pass

        initdv2.ParametrizedInitScript.__init__(self, SERVICE_NAME,
                        initd_script, pid_file, socks=[initdv2.SockParam(MYSQL_DEFAULT_PORT, timeout=3600)])


    def _start_stop_reload(self, action):
        ''' XXX: Temporary ugly hack (Ubuntu 1004 upstart problem - Job is already running)'''
        try:
            args = [self.initd_script] \
                            if isinstance(self.initd_script, basestring) \
                            else list(self.initd_script)
            args.append(action)
            out, err, returncode = system2(args, close_fds=True, preexec_fn=os.setsid)
        except PopenError, e:
            if 'Job is already running' not in str(e):
                raise InitdError("Popen failed with error %s" % (e,))

        if action == 'restart':
            if err and 'stop: Job has already been stopped: mysql' in err:
                return True
            else:
                LOG.debug('waiting for mysql process')
                wait_until(lambda: MYSQLD_PATH in system2(('ps', '-G', DEFAULT_OWNER, '-o', 'command', '--no-headers'))[0]
                        , timeout=10, sleep=1)

        if action == 'start' and disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
            try:
                LOG.debug('waiting for mysql process')
                wait_until(lambda: MYSQLD_PATH in system2(('ps', '-G', DEFAULT_OWNER, '-o', 'command', '--no-headers'))[0]
                                        , timeout=10, sleep=1)
                LOG.debug('waiting for debian-start finish')
                out = system2('ps axo pid,command --noheaders | grep /etc/mysql/debian-start', shell=True)[0].strip()
                if out:
                    pid = out.split('\n')[0].split(' ')[0]
                    wait_until(lambda: not os.path.exists('/proc/%s' % pid), sleep=1)
            except:
                self._start_stop_reload('restart')
                return True

        if self.socks and (action != "stop" and not (action == 'reload' and not self.running)):
            for sock in self.socks:
                wait_sock(sock)

        return True


    def status(self):
        return initdv2.Status.RUNNING if self.mysql_cli.test_connection() else initdv2.Status.NOT_RUNNING


    def start(self):
        '''
        Commented, cause Dima said this code is useless

        # FIXME: This condition here because of the following fixme
        if os.listdir('/mnt/dbstorage/mysql-data'):

                # FIXME: It's not a good place to test mysql configuration
                # This code fails when datadir is empty, whereas init script detects this and start gracefully
                mysql_cnf_err_re = re.compile('Unknown option|ERROR')
                stderr = system2('%s --user=mysql --help' % MYSQLD_PATH, shell=True, silent=True)[1]
                if re.search(mysql_cnf_err_re, stderr):
                        raise Exception('Error in mysql configuration detected. Output:\n%s' % stderr)
        '''

        if not self.running:
            try:
                LOG.info("Starting mysql")
                initdv2.ParametrizedInitScript.start(self)
                LOG.debug("mysql started")
            except:
                if self._is_sgt_process_exists():
                    LOG.warning('MySQL service is running with skip-grant-tables mode.')
                elif not self.running:
                    raise


        try:
            LOG.info("Starting mysql")
            initdv2.ParametrizedInitScript.start(self)
            LOG.debug("mysql started")
        except:
            if self._is_sgt_process_exists():
                LOG.warning('MySQL service is running with skip-grant-tables mode.')
            elif not self.running:
                raise


    def stop(self, reason=None):
        initdv2.ParametrizedInitScript.stop(self)


    def restart(self, reason=None):
        initdv2.ParametrizedInitScript.restart(self)

    def reload(self, reason=None):
        initdv2.ParametrizedInitScript.reload(self)


    def _is_sgt_process_exists(self):
        try:
            out = system2(('ps', '-G', DEFAULT_OWNER, '-o', 'command', '--no-headers'))[0]
            return MYSQLD_PATH in out and 'skip-grant-tables' in out
        except:
            return False


    def start_skip_grant_tables(self):
        pid_dir = '/var/run/mysqld/'
        if not os.path.isdir(pid_dir):
            os.makedirs(pid_dir, mode=0755)
            mysql_user      = pwd.getpwnam(DEFAULT_OWNER)
            os.chown(pid_dir, mysql_user.pw_uid, -1)
        if not self._is_sgt_process_exists():
            args = [MYSQLD_PATH, '--user=mysql', '--skip-grant-tables', '--pid-file=%s' % self.sgt_pid_path]
            LOG.debug('Starting mysqld with a skip-grant-tables')
            subprocess.Popen(args, stdin=subprocess.PIPE,stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
            wait_until(lambda: self._is_sgt_process_exists(), timeout=10, sleep=1)
        wait_until(lambda: self.running, timeout=600, sleep=1)


    def stop_skip_grant_tables(self):
        if self._is_sgt_process_exists() and os.path.exists(self.sgt_pid_path):
            sgt_pid = open(self.sgt_pid_path).read().strip()
            if sgt_pid:
                LOG.debug('Stopping mysqld with a skip-grant-tables')
                os.kill(int(sgt_pid), signal.SIGTERM)
                wait_until(lambda: not self._is_sgt_process_exists(), timeout=10, sleep=1)
                wait_until(lambda: not self.running, timeout=600, sleep=1)
            else:
                LOG.warning('Unable to stop mysql running with skip-grant-tables. PID not found.')
        else:
            LOG.debug('Skip stopping mysqld with a skip-grant-tables')



def _add_apparmor_rules(directory):
    if not os.path.exists('/etc/init.d/apparmor'):
        return
    try:
        file = open('/etc/apparmor.d/usr.sbin.mysqld', 'r')
    except IOError, e:
        pass
    else:
        app_rules = file.read()
        file.close()
        if not re.search (directory, app_rules):
            file = open('/etc/apparmor.d/usr.sbin.mysqld', 'w')
            if os.path.isdir(directory):
                app_rules = re.sub(re.compile('(.*)(})([^}]*)', re.S), '\\1\n'+directory+'/ r,\n'+'\\2\\3', app_rules)
                app_rules = re.sub(re.compile('(.*)(})([^}]*)', re.S), '\\1'+directory+'/** rwk,\n'+'\\2\\3', app_rules)
            else:
                app_rules = re.sub(re.compile('(.*)(})([^}]*)', re.S), '\\1\n'+directory+' r,\n'+'\\2\\3', app_rules)
            file.write(app_rules)
            file.close()
            apparmor_initd = initdv2.ParametrizedInitScript('apparmor', '/etc/init.d/apparmor')
            try:
                apparmor_initd.reload()
            except InitdError, e:
                LOG.error('Cannot restart apparmor. %s', e)


class MySQLPresetProvider(PresetProvider):

    def __init__(self):
        service = initdv2.lookup(SERVICE_NAME)
        config_mapping = {PRESET_FNAME:MySQLConf(MYCNF_PATH)}
        PresetProvider.__init__(self, service, config_mapping)


initdv2.explore(SERVICE_NAME, MysqlInitScript)
