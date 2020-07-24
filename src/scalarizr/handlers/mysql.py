from __future__ import with_statement
'''
Created on 14.06.2010

@author: spike
@author: marat
'''

from __future__ import with_statement

# Core
from scalarizr import config
from scalarizr.bus import bus
from scalarizr.storage import Storage, StorageError, Snapshot, Volume, transfer
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.service import CnfController, _CnfManifest
from scalarizr.messaging import Messages
from scalarizr.handlers import HandlerError, ServiceCtlHandler, prepare_tags
from scalarizr.platform import UserDataOptions

# Libs
from scalarizr.libs.metaconf import Configuration, MetaconfError, NoPathError, \
        ParseError
from scalarizr.util import system2, disttool, \
        firstmatched, initdv2, software, wait_until, cryptotool,\
        PopenError
from scalarizr.handlers import operation
from scalarizr.util.software import which
from scalarizr.linux import iptables
from scalarizr.linux import coreutils
from scalarizr.linux.rsync import rsync
from scalarizr.util.initdv2 import ParametrizedInitScript, wait_sock, InitdError

# Stdlibs
import logging, os, re, sys, tarfile, tempfile
import time, pwd, random, shutil
import glob
import string
import ConfigParser


# Extra
import pexpect
import signal
from scalarizr.handlers import operation


SU_EXEC = '/bin/su'
BASH = '/bin/bash'

BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.MYSQL
CNF_SECTION = BEHAVIOUR
CNF_NAME = BEHAVIOUR

LOG = logging.getLogger(__name__)

MYSQLD = which('mysqld')
MYSQL_CLI = which('mysql')
MYSQLDUMP = which('mysqldump')
MYCNF = '/etc/my.cnf' if disttool.is_redhat_based() else '/etc/mysql/my.cnf'

change_main_timeout = 30


class MysqlInitScript(initdv2.ParametrizedInitScript):
    socket_file = None
    def __init__(self):

        if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
            initd_script = ('/usr/sbin/service', 'mysql')
        else:
            initd_script = firstmatched(os.path.exists, ('/etc/init.d/mysqld', '/etc/init.d/mysql'))

        pid_file = None
        try:
            out = system2("my_print_defaults mysqld", shell=True)
            m = re.search("--pid[-_]file=(.*)", out[0], re.MULTILINE)
            if m:
                pid_file = m.group(1)
            m = re.search("--socket=(.*)", out[0], re.MULTILINE)
            if m:
                self.socket_file = m.group(1)
        except:
            pass

        initdv2.ParametrizedInitScript.__init__(self, SERVICE_NAME,
                        initd_script, pid_file, socks=[initdv2.SockParam(3306, timeout=3600)])
        '''
        timeout=60 is no chance
        when server starts after rebundle, mysql takes too long to start on the attached EBS storage.

        Scalarizr:
        2010-12-02 10:31:12,086 - INFO - scalarizr.handlers - Starting mysql

        MySQL:
        Version: '5.1.41-3ubuntu12.7-log'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306  (Ubuntu)
        101202 10:31:30 [Note] Plugin 'FEDERATED' is disabled.
        101202 10:31:31  InnoDB: Started; log sequence number 0 44556
        101202 10:31:31 [Note] Event Scheduler: Loaded 0 events

        Over 15 seconds! OMFG!!
        XXX: Requires investigation
        '''

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

        if action == 'start' and disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
            try:
                LOG.debug('waiting for mysql process')
                wait_until(lambda: MYSQLD in system2(('ps', '-G', 'mysql', '-o', 'command', '--no-headers'))[0]
                                        , timeout=10, sleep=1)
            except:
                self._start_stop_reload('restart')
                return True

        if self.socks and (action != "stop" and not (action == 'reload' and not self.running)):
            for sock in self.socks:
                wait_sock(sock)

#               if self.pid_file:
#                       if (action == "start" or action == "restart") and not os.path.exists(self.pid_file):
#                               raise InitdError("Cannot start %s. pid file %s doesn't exists" % (self.name, self.pid_file))
#                       if action == "stop" and os.path.exists(self.pid_file):
#                               raise InitdError("Cannot stop %s. pid file %s still exists" % (self.name, self.pid_file))

        return True


    def status(self):
        if self.socket_file:
            if os.path.exists(self.socket_file):
                if MYSQL_CLI:
                    try:
                        spawn_mysql_cli('root').close()
                        return initdv2.Status.RUNNING
                    except HandlerError, e:
                        if 'Access denied' in str(e):
                            return initdv2.Status.RUNNING
                    except pexpect.ExceptionPexpect, e:
                        if 'close() could not terminate the child using terminate()' in str(e):
                            return initdv2.Status.RUNNING
            else:
                return initdv2.Status.NOT_RUNNING

        return initdv2.ParametrizedInitScript.status(self)

    '''
    def stop(self):
            if not self.running:
                    return True
            initdv2.ParametrizedInitScript.stop(self)
    '''

initdv2.explore(SERVICE_NAME, MysqlInitScript)



class MysqlServiceConfigurator:
    pass

# Configuration options
# Private
OPT_ROOT_PASSWORD               = "root_password"
OPT_REPL_PASSWORD               = "repl_password"
OPT_STAT_PASSWORD       = "stat_password"
OPT_REPLICATION_MASTER  = "replication_main"
OPT_LOG_FILE                    = "log_file"
OPT_LOG_POS                             = "log_pos"
OPT_VOLUME_CNF                  = 'volume_config'
OPT_SNAPSHOT_CNF                = 'snapshot_config'
# Public
OPT_MYSQLD_PATH                 = 'mysqld_path'
OPT_MYSQL_PATH                  = 'mysql_path'
OPT_MYSQLDUMP_PATH              = 'mysqldump_path'
OPT_MYCNF_PATH                  = 'mycnf_path'
OPT_CHANGE_MASTER_TIMEOUT = 'change_main_timeout'

# System users
ROOT_USER                               = "scalr"
REPL_USER                               = "scalr_repl"
STAT_USER                               = "scalr_stat"
PMA_USER                                = "pma"

# Mysql storage constants
STORAGE_PATH                    = "/mnt/dbstorage"
STORAGE_DATA_DIR                = "mysql-data"
STORAGE_TMP_DIR                 = "tmp"
STORAGE_BINLOG                  = "mysql-misc/binlog"
STORAGE_VOLUME_CNF              = 'mysql.json'
STORAGE_SNAPSHOT_CNF    = 'mysql-snap.json'

BACKUP_CHUNK_SIZE               = 200*1024*1024

DEFAULT_DATADIR                 = "/var/lib/mysql"


def get_handlers ():
    return [MysqlHandler()]



class MySQL(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._client = None

    root_user = 'scalr'

    @property
    def root_password(self):
        return bus.cnf.rawini.get('mysql', 'root_password')

    @property
    def client(self):
        if not self._client:
            self._client = MySQLClient(lambda: (self.root_user, self.root_password))
        return self._client

    def dump_database(self, database, filename):
        self.logger.info('Dumping database %s', database)
        opts = config.split(bus.cnf.rawini.get('mysql', 'mysqldump_options'), ' ')
        opts = [MYSQLDUMP, '-u', self.root_user, '-p%s' % self.root_password] + opts + ['--databases']
        with open(filename, 'w') as fp:
            system2(opts + [database], stdout=fp)


class MySQLClient(object):
    def __init__(self, credentials_function):
        self._credentials_function = credentials_function

    def execute(self, query, vertical=False):
        user, passwd  = self._credentials_function()
        # q: why not send password in stdin?
        # a: on Ubuntu 8.04 (5.0.51a-3ubuntu5.4-log) mysql cli doesn't accept passwords in stdin
        return system2(('/usr/bin/mysql', '-u', user, '-p' + passwd, '--execute', query))[0]

    def fetchall(self, query):
        lines = self.execute(query).splitlines()
        headers = lines[0].split('\t')
        return tuple(dict(zip(headers, line.split('\t'))) for line in lines[1:])

    def fetchdict(self, query):
        pass



mysql = MySQL()



'''
Failover scenario:

1. Cloud support pluggable disks (ex: EBS)
+-------+              +------------------+                     +--------+
| Scalr |              | Subordinate1 -> Main |                     | Subordinate2 |
+-------+              +------------------+                     +--------+

     Mysql_PromoteToMain
       - root_password
       - repl_password
       - stat_password
       - volume_config
     -------------------------->

                                                 STOP SLAVE
                             vol = Storage(volume_config)
                                                 vol.detach() from Main
                                                 vol.attach() to Subordinate1
                                                 start mysql

     Mysql_PromoteToMainResult
       - volume_config
     <--------------------------

     Mysql_NewMainUp
       - root_password
       - repl_password
       - stat_password
     ---------------------------------------------------------------->
                                                                                                                 get current log_file, log_pos
                                                                                                                 CHANGE MASTER TO


2. Cloud has no pluggable disks (ex: Rackspace)
+-------+              +------------------+                     +--------+
| Scalr |              | Subordinate1 -> Main |                     | Subordinate2 |
+-------+              +------------------+                     +--------+

         Mysql_PromoteToMain
           - root_password
           - repl_password
           - stat_password
     --------------------------->

                             STOP SLAVE
                             RESET MASTER
                             start mysql
                             create snapshot

     Mysql_PromoteToMainResult
       - snapshot_config
       - log_file
       - log_pos
    <----------------------------

         Mysql_NewMainUp
           - local_ip
           - repl_password
           - snapshot_config
           - log_file
           - log_pos
        ----------------------------------------------------------------->
                                                               vol = Storage(snapshot)
                                                               CHANGE MASTER TO

'''

class MysqlMessages:
    CREATE_DATA_BUNDLE = "Mysql_CreateDataBundle"

    CREATE_DATA_BUNDLE_RESULT = "Mysql_CreateDataBundleResult"
    '''
    @ivar status: ok|error
    @ivar last_error
    @ivar snapshot_config
    @ivar log_file
    @ivar log_pos
    @ivar used_size
    '''

    CREATE_BACKUP = "Mysql_CreateBackup"

    CREATE_BACKUP_RESULT = "Mysql_CreateBackupResult"
    """
    @ivar status: ok|error
    @ivar last_error
    @ivar backup_urls: S3 URL
    """

    CREATE_PMA_USER = "Mysql_CreatePmaUser"
    """
    @ivar pma_server_ip: User host
    @ivar farm_role_id
    @ivar root_password
    """

    CREATE_PMA_USER_RESULT = "Mysql_CreatePmaUserResult"
    """
    @ivar status: ok|error
    @ivar last_error
    @ivar pma_user
    @ivar pma_password
    @ivar farm_role_id
    """

    PROMOTE_TO_MASTER       = "Mysql_PromoteToMain"
    """
    @ivar root_password: 'scalr' user password
    @ivar repl_password: 'scalr_repl' user password
    @ivar stat_password: 'scalr_stat' user password
    @ivar volume_config?: Main storage configuration
    """

    PROMOTE_TO_MASTER_RESULT = "Mysql_PromoteToMainResult"
    """
    @ivar status: ok|error
    @ivar last_error: Last error message in case of status = 'error'
    @ivar volume_config: Main storage configuration
    @ivar snapshot_config?
    @ivar log_file?
    @ivar log_pos?
    """

    NEW_MASTER_UP = "Mysql_NewMainUp"
    """
    @ivar behaviour
    @ivar local_ip
    @ivar remote_ip
    @ivar role_name
    @ivar repl_password
    @ivar snapshot_config?
    @ivar log_file?
    @ivar log_pos?
    """

    """
    Also MySQL behaviour adds params to common messages:

    = HOST_INIT_RESPONSE =
    @ivar mysql=dict(
            replication_main:     1|0
            root_password:                  'scalr' user password                                   (on subordinate)
            repl_password:                  'scalr_repl' user password                              (on subordinate)
            stat_password:                  'scalr_stat' user password                              (on subordinate)
            log_file:                               Binary log file                                                 (on subordinate)
            log_pos:                                Binary log file position                                (on subordinate)
            volume_config                   Main storage configuration                    (on main)
            snapshot_config                 Main storage snapshot                                 (both)
    )

    = HOST_UP =
    @ivar mysql=dict(
            root_password:                  'scalr' user password                                   (on main)
            repl_password:                  'scalr_repl' user password                              (on main)
            stat_password:                  'scalr_stat' user password                              (on main)
            log_file:                               Binary log file                                                 (on main)
            log_pos:                                Binary log file position                                (on main)
            volume_config:                  Current storage configuration                   (both)
            snapshot_config:                Main storage snapshot                                 (on main)
    )
    """


class MysqlCnfController(CnfController):
    _mysql_version = None
    _merged_manifest = None

    def __init__(self):
        self._init_script = initdv2.lookup(SERVICE_NAME)
        self._cnf = bus.cnf
        ini = self._cnf.rawini
        CnfController.__init__(self, BEHAVIOUR, MYCNF, 'mysql', {'ON':'1', 'TRUE':'1','OFF':'0','FALSE':'0'}) #TRUE,FALSE

    @property
    def _manifest(self):
        f_manifest = CnfController._manifest
        base_manifest = f_manifest.fget(self)
        path = self._manifest_path

        s = {}
        out = None

        if not self._merged_manifest:
            cmd = '%s --no-defaults --verbose --help' % MYSQLD
            out = system2('%s - mysql -s %s -c "%s"' % (SU_EXEC, BASH, cmd),shell=True, raise_exc=False,silent=True)[0]

        if out:
            raw = out.split('--------------------------------- -----------------------------')
            if raw:
                a = raw[-1].split('\n')
                if len(a) > 4:
                    b = a[1:-4]
                    for item in b:
                        c = item.split()
                        if len(c) > 1:
                            s[c[0].strip()] = ' '.join(c[1:]).strip()

        if s:
            m_config = Configuration('ini')
            if os.path.exists(path):
                m_config.read(path)

            for variable in base_manifest:
                name = variable.name
                dv_path = './%s/default-value' % name

                try:
                    old_value =  m_config.get(dv_path)
                    if name in s:
                        new_value = s[name]
                    else:
                        name = name.replace('_','-')
                        if name in s:
                            new_value = self.definitions[s[name]] if s[name] in self.definitions else s[name]
                            if old_value != new_value and new_value != '(No default value)':
                                LOG.debug('Replacing %s default value %s with precompiled value %s' % (name, old_value, new_value))
                                m_config.set(path=dv_path, value=new_value, force=True)
                except NoPathError, e:
                    pass
            m_config.write(path)

        self._merged_manifest = _CnfManifest(path)
        return self._merged_manifest

    def _start_service(self):
        if not hasattr(self, '_mysql_cnf_err_re'):
            self._mysql_cnf_err_re = re.compile('Unknown option|ERROR')
        stderr = system2('%s --user=mysql --help' % MYSQLD, shell=True)[1]
        if re.search(self._mysql_cnf_err_re, stderr):
            raise Exception('Error in mysql configuration detected. Output:\n%s' % stderr)

        if not self._init_script.running:
            try:
                LOG.info("Starting %s" % self.behaviour)
                self._init_script.start()
                LOG.debug("%s started" % self.behaviour)
            except:
                if not self._init_script.running:
                    raise

    def get_system_variables(self):
        vars = CnfController.get_system_variables(self)
        if self._init_script.running:
            try:
                mysql = self._get_connection()
                mysql.sendline('SHOW GLOBAL VARIABLES;')
                mysql.expect('mysql>')
                out = mysql.before
                raw_text = out.splitlines()
                text = raw_text[4:-3]
                vars = {}

                for line in text:
                    splitted_line = line.split('|')
                    name = splitted_line[1].strip()
                    value = splitted_line[2].strip()
                    vars[name] = value
            finally:
                if mysql:
                    mysql.close()
        return vars

    def apply_preset(self, preset):

        CnfController.apply_preset(self, preset)

    def _before_apply_preset(self):
        self.sendline = ''

    def _after_set_option(self, option_spec, value):
        LOG.debug('callback "_after_set_option": %s %s (Need restart: %s)'
                        % (option_spec, value, option_spec.need_restart))

        if value != option_spec.default_value and not option_spec.need_restart:
            LOG.debug('Preparing to set run-time variable %s to %s' % (option_spec.name, value))
            self.sendline += 'SET GLOBAL %s = %s; ' % (option_spec.name, value)


    def _after_remove_option(self, option_spec):
        if option_spec.default_value and not option_spec.need_restart:
            LOG.debug('Preparing to set run-time variable %s to default [%s]'
                                    % (option_spec.name,option_spec.default_value))
            '''
            when removing mysql options DEFAULT keyword must be used instead of
            self.sendline += 'SET GLOBAL %s = %s; ' % (option_spec.name, option_spec.default_value)
            '''
            self.sendline += 'SET GLOBAL %s = DEFAULT; ' % (option_spec.name)

    def _after_apply_preset(self):
        if not self._init_script.running:
            LOG.info('MySQL isn`t running, skipping process of applying run-time variables')
            return

        mysql = self._get_connection()
        try:
            if self.sendline and mysql:
                LOG.debug(self.sendline)
                mysql.sendline(self.sendline)
                index = mysql.expect(['mysql>', pexpect.EOF, pexpect.TIMEOUT])
                if 1==index or 2==index:
                    LOG.error('Cannot set global variables: %s' % mysql.before)
                else:
                    LOG.debug('All global variables has been set.')
            elif not self.sendline:
                LOG.debug('No global variables changed. Nothing to set.')
            elif not mysql:
                LOG.debug('No connection to MySQL. Skipping SETs.')
        finally:
            if mysql:
                mysql.close()

    def _get_version(self):
        if not self._mysql_version:
            info = software.software_info('mysql')
            self._mysql_version = info.version
        return self._mysql_version


    def _get_connection(self):
        szr_cnf = bus.cnf
        root_password = szr_cnf.rawini.get(CNF_SECTION, OPT_ROOT_PASSWORD)
        return spawn_mysql_cli(ROOT_USER, root_password)



def _reload_mycnf(f):
    def g(self, *args, **kwargs):
        self._mysql_config = Configuration('mysql')
        try:
            self._mysql_config.read(MYCNF)
        except (OSError, MetaconfError, ParseError), e:
            raise HandlerError('Cannot read mysql config %s : %s' % (MYCNF, str(e)))
        f(self, *args, **kwargs)
    return g

class MysqlHandler(ServiceCtlHandler):

    _mysql_config = None

    _queryenv = None
    """ @type _queryenv: scalarizr.queryenv.QueryEnvService """

    _platform = None
    """ @type _platform: scalarizr.platform.Ec2Platform """

    _cnf = None
    ''' @type _cnf: scalarizr.config.ScalarizrCnf '''

    _storage_path = _data_dir = _binlog_path = None
    """ Storage parameters """

    storage_vol = None

    def __init__(self):
        if not os.path.exists(MYCNF):
            if disttool.is_redhat_based() and os.path.exists('/usr/share/mysql/my-medium.cnf'):
                shutil.copy('/usr/share/mysql/my-medium.cnf', MYCNF)
            else:
                open(MYCNF, 'w').close()

        initd = initdv2.lookup(SERVICE_NAME)
        ServiceCtlHandler.__init__(self, SERVICE_NAME, initd, MysqlCnfController())

        bus.on(init=self.on_init, reload=self.on_reload)
        bus.define_events(
                'before_mysql_data_bundle',

                'mysql_data_bundle',

                # @param host: New main hostname
                'before_mysql_change_main',

                # @param host: New main hostname
                # @param log_file: log file to start from
                # @param log_pos: log pos to start from
                'mysql_change_main'

                'before_subordinate_promote_to_main',

                'subordinate_promote_to_main'
        )

        self._phase_mysql = 'Configure MySQL'
        self._phase_data_bundle = self._op_data_bundle = 'MySQL data bundle'
        self._phase_backup = self._op_backup = 'MySQL backup'
        self._step_upload_to_cloud_storage = 'Upload data to cloud storage'
        self._step_accept_scalr_conf = 'Accept Scalr configuration'
        self._step_patch_conf = 'Patch my.cnf configuration file'
        self._step_create_storage = 'Create storage'
        self._step_move_datadir = 'Move data directory to storage'
        self._step_create_users = 'Create Scalr users'
        self._step_restore_users = 'Restore Scalr users'
        self._step_create_data_bundle = 'Create data bundle'
        self._step_change_replication_main = 'Change replication Main'
        self._step_innodb_recovery = 'InnoDB recovery'
        self._step_collect_hostup_data = 'Collect HostUp data'

        self.on_reload()

    def on_init(self):
        bus.on("host_init_response", self.on_host_init_response)
        bus.on("before_host_up", self.on_before_host_up)
        bus.on("before_reboot_start", self.on_before_reboot_start)

        self._insert_iptables_rules()
        if self._cnf.state == ScalarizrState.BOOTSTRAPPING:
            self._stop_service('Configuring')

            # Add SELinux rule
            try:
                if not system2((software.which('selinuxenabled'), ), raise_exc=False)[2]:
                    if not system2((software.which('getsebool'), 'mysqld_disable_trans'), raise_exc=False)[2]:
                        system2((software.which('setsebool'), '-P', 'mysqld_disable_trans', '1'))
            except LookupError:
                pass


        elif self._cnf.state == ScalarizrState.RUNNING:
            # Creating self.storage_vol object from configuration
            storage_conf = Storage.restore_config(self._volume_config_path)
            storage_conf['tags'] = self.mysql_tags
            self.storage_vol = Storage.create(storage_conf)
            if not self.storage_vol.mounted():
                if not os.path.exists(self.storage_vol.mpoint):
                    os.makedirs(self.storage_vol.mpoint)
                self.storage_vol.mount()

            self._change_selinux_ctx()

            if int(self._get_ini_options(OPT_REPLICATION_MASTER)[0]):
                LOG.debug("Checking Scalr's MySQL system users presence.")
                root_password, repl_password, stat_password = self._get_ini_options(
                                OPT_ROOT_PASSWORD, OPT_REPL_PASSWORD, OPT_STAT_PASSWORD)
                try:
                    my_cli = spawn_mysql_cli(ROOT_USER, root_password, timeout=5)
                    mysqld=None
                except:
                    self._stop_service('Checking mysql users')
                    mysqld = spawn_mysqld()
                    self._ping_mysql()
                    my_cli = spawn_mysql_cli()

                try:
                    check_mysql_password(my_cli, ROOT_USER, root_password, host='localhost')
                    check_mysql_password(my_cli, ROOT_USER, root_password)
                    check_mysql_password(my_cli, REPL_USER, repl_password)
                    check_mysql_password(my_cli, STAT_USER, stat_password)
                    LOG.debug("Scalr's MySQL system users are present. Passwords are correct.")
                except ValueError:
                    LOG.warning("Scalr's MySQL system users were changed. Recreating.")
                    self._add_mysql_users(ROOT_USER, REPL_USER, STAT_USER,
                                                              root_password, repl_password, stat_password,
                                                              mysqld, my_cli)
                finally:
                    if mysqld:
                        term_mysqld(mysqld)
                    self._start_service()

    def on_reload(self):
        self._queryenv = bus.queryenv_service
        self._platform = bus.platform
        self._cnf = bus.cnf
        ini = self._cnf.rawini
        self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
        try:
            self._change_main_timeout = globals()['change_main_timeout'] = int(
                            ini.get(CNF_SECTION, OPT_CHANGE_MASTER_TIMEOUT) or '30')
        except ConfigParser.Error:
            self._change_main_timeout = globals()['change_main_timeout'] = 30

        self._storage_path = STORAGE_PATH
        self._data_dir = os.path.join(self._storage_path, STORAGE_DATA_DIR)
        self._tmp_dir = os.path.join(self._storage_path, STORAGE_TMP_DIR)
        self._binlog_base = os.path.join(self._storage_path, STORAGE_BINLOG)

        self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
        self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))

        f = '/sys/module/apparmor/parameters/enabled'
        self._apparmor_enabled = os.access(f, os.R_OK) and open(f).read().strip() in ('Y', '1')


    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return BEHAVIOUR in behaviour and (
                                message.name == Messages.BEFORE_HOST_TERMINATE
                        or      message.name == MysqlMessages.NEW_MASTER_UP
                        or      message.name == MysqlMessages.PROMOTE_TO_MASTER
                        or      message.name == MysqlMessages.CREATE_DATA_BUNDLE
                        or      message.name == MysqlMessages.CREATE_BACKUP
                        or      message.name == MysqlMessages.CREATE_PMA_USER
                        or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION)

    def get_initialization_phases(self, hir_message):
        if BEHAVIOUR in hir_message.body:
            steps = [self._step_accept_scalr_conf, self._step_create_storage]
            if hir_message.body['mysql']['replication_main'] == '1':
                steps.append(self._step_create_data_bundle)
            else:
                steps.append(self._step_change_replication_main)
            steps.append(self._step_collect_hostup_data)


            return {'before_host_up': [{
                    'name': self._phase_mysql,
                    'steps': steps
            }]}

    def on_BeforeHostTerminate(self, message):
        LOG.debug('Handling BeforeHostTerminate message from %s' % message.local_ip)
        if message.local_ip == self._platform.get_private_ip():
            LOG.info('Stopping %s service' % BEHAVIOUR)
            self._stop_service(reason='Server will be terminated')
            if not int(self._get_ini_options(OPT_REPLICATION_MASTER)[0]):
                LOG.info('Destroying volume %s' % self.storage_vol.id)
                self.storage_vol.destroy(remove_disks=True)
                LOG.info('Volume %s has been destroyed.' % self.storage_vol.id)


    def on_Mysql_CreatePmaUser(self, message):
        try:
            # Operation allowed only on Main server
            if not int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)):
                raise HandlerError('Cannot add pma user on subordinate. It should be a Main server')

            root_password, = self._get_ini_options(OPT_ROOT_PASSWORD)
            pma_server_ip = message.pma_server_ip
            farm_role_id  = message.farm_role_id

            LOG.info("Adding phpMyAdmin system user")



            # Connecting to mysql
            my_cli = spawn_mysql_cli(ROOT_USER, root_password)
            try:
                # Add user
                pma_password = cryptotool.pwgen(20)
                self._add_mysql_user(my_cli, PMA_USER, pma_password, pma_server_ip)
            finally:
                # Close connection
                my_cli.close()
                del(my_cli)

            LOG.info('PhpMyAdmin system user successfully added')

            # Notify Scalr
            self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
                    status       = 'ok',
                    pma_user         = PMA_USER,
                    pma_password = pma_password,
                    farm_role_id = farm_role_id,
            ))

        except (Exception, BaseException), e:
            LOG.exception(e)

            # Notify Scalr about error
            self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
                    status          = 'error',
                    last_error      =  str(e).strip(),
                    farm_role_id = farm_role_id
            ))


    @_reload_mycnf
    def on_Mysql_CreateBackup(self, message):

        # Retrieve password for scalr mysql user
        tmpdir = backup_path = None
        try:
            # Get databases list
            databases = list(row['Database'] for row in mysql.client.fetchall('SHOW DATABASES'))
            if 'information_schema' in databases:
                databases.remove('information_schema')

            op = operation(name=self._op_backup, phases=[{
                    'name': self._phase_backup,
                    'steps': ["Backup '%s'" % db for db in databases] + [self._step_upload_to_cloud_storage]
            }])
            op.define()

            with op.phase(self._phase_backup):

                # Defining archive name and path
                if not os.path.exists(self._tmp_dir):
                    os.makedirs(self._tmp_dir)
                backup_filename = '%s.tar.gz' % time.strftime('%Y-%m-%d-%H:%M:%S')
                backup_path = os.path.join(self._tmp_dir, backup_filename)

                # Creating archive
                backup = tarfile.open(backup_path, 'w:gz')

                # Dump all databases
                LOG.info("Dumping all databases")
                tmpdir = tempfile.mkdtemp(dir=self._tmp_dir)
                for db in databases:
                    with op.step("Backup '%s'" % db):
                        dump_path = os.path.join(tmpdir, db + '.sql')
                        mysql.dump_database(db, dump_path)
                        backup.add(dump_path, os.path.basename(dump_path))

                backup.close()

                with op.step(self._step_upload_to_cloud_storage):
                    # Creating list of full paths to archive chunks
                    if os.path.getsize(backup_path) > BACKUP_CHUNK_SIZE:
                        parts = [os.path.join(tmpdir, file) for file in coreutils.split(backup_path, backup_filename, BACKUP_CHUNK_SIZE , tmpdir)]
                    else:
                        parts = [backup_path]
                    sizes = [os.path.getsize(file) for file in parts]

                    cloud_storage_path = self._platform.scalrfs.backups('mysql')
                    self._logger.info("Uploading backup to cloud storage (%s)", cloud_storage_path)
                    trn = transfer.Transfer()
                    cloud_files = trn.upload(parts, cloud_storage_path)
                    self._logger.info("Mysql backup uploaded to cloud storage under %s/%s",
                                                    cloud_storage_path, backup_filename)

            result = list(dict(path=path, size=size) for path, size in zip(cloud_files, sizes))
            op.ok(data=result)

            # Notify Scalr
            self.send_message(MysqlMessages.CREATE_BACKUP_RESULT, dict(
                    status = 'ok',
                    backup_parts = result
            ))


        except (Exception, BaseException), e:
            LOG.exception(e)

            # Notify Scalr about error
            self.send_message(MysqlMessages.CREATE_BACKUP_RESULT, dict(
                    status = 'error',
                    last_error = str(e)
            ))

        finally:
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)
            if backup_path and os.path.exists(backup_path):
                os.remove(backup_path)


    def on_Mysql_CreateDataBundle(self, message):
        try:
            op = operation(name=self._op_data_bundle, phases=[{
                    'name': self._phase_data_bundle,
                    'steps': [self._step_create_data_bundle]
            }])
            op.define()

            with op.phase(self._phase_data_bundle):
                with op.step(self._step_create_data_bundle):

                    bus.fire('before_mysql_data_bundle')

                    # Creating snapshot
                    root_password, = self._get_ini_options(OPT_ROOT_PASSWORD)
                    snap, log_file, log_pos = self._create_snapshot(ROOT_USER, root_password, tags=self.mysql_tags)
                    used_size = coreutils.statvfs(self._storage_path)['used']

                    bus.fire('mysql_data_bundle', snapshot_id=snap.id)

                    # Notify scalr
                    msg_data = dict(
                            log_file=log_file,
                            log_pos=log_pos,
                            used_size='%.3f' % (float(used_size) / 1024 / 1024,),
                            status='ok'
                    )
                    msg_data.update(self._compat_storage_data(snap=snap))
                    self.send_message(MysqlMessages.CREATE_DATA_BUNDLE_RESULT, msg_data)

            op.ok()

        except (Exception, BaseException), e:
            LOG.exception(e)

            # Notify Scalr about error
            self.send_message(MysqlMessages.CREATE_DATA_BUNDLE_RESULT, dict(
                    status          ='error',
                    last_error      = str(e)
            ))

    def _innodb_recovery(self, storage_path=None):
        storage_path = storage_path or self._storage_path
        binlog_path     = os.path.join(storage_path, STORAGE_BINLOG)
        data_dir = os.path.join(storage_path, STORAGE_DATA_DIR),
        pid_file = os.path.join(storage_path, 'mysql.pid')
        socket_file = os.path.join(storage_path, 'mysql.sock')
        mysqld_safe_bin = software.whereis('mysqld_safe')[0]

        '''
        ndb_support = any(row['Engine'] == 'ndbcluster' and row['Support'] == 'YES'
                                        for row in mysql.client.fetchall('SHOW ENGINES'))
        '''

        LOG.info('Performing InnoDB recovery')
        mysqld_safe_cmd = (mysqld_safe_bin,
                '--socket=%s' % socket_file,
                '--pid-file=%s' % pid_file,
                '--datadir=%s' % data_dir,
                '--log-bin=%s' % binlog_path,
                '--skip-networking',
                '--skip-grant',
                '--bootstrap',
                '--skip-subordinate-start')
        '''
        if ndb_support:
                mysqld_safe_cmd += ('--skip-ndbcluster',)
        '''

        system2(mysqld_safe_cmd, stdin="select 1;")

    def _recreate_binlog_index(self, index_file, binlog_dir):
        with open(index_file, 'w+') as f:
            f.write('\n'.join(glob.glob(binlog_dir + '/binlog.[0-9]*')))

    def _data_bundle_description(self):
        pl = bus.platform
        return 'MySQL data bundle (farm: %s role: %s)' % (
                                pl.get_user_data(UserDataOptions.FARM_ID),
                                pl.get_user_data(UserDataOptions.ROLE_NAME))


    @_reload_mycnf
    def on_Mysql_PromoteToMain(self, message):
        """
        Promote subordinate to main
        @type message: scalarizr.messaging.Message
        @param message: Mysql_PromoteToMain
        """
        old_conf                = None
        new_storage_vol = None

        if not int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)):

            bus.fire('before_subordinate_promote_to_main')

            if bus.scalr_version >= (2, 2):
                main_storage_conf = message.body.get('volume_config')
            else:
                if 'volume_id' in message.body:
                    main_storage_conf = dict(type='ebs', id=message.body['volume_id'])
                else:
                    main_storage_conf = None

            tx_complete = False

            try:
                # Stop mysql
                if main_storage_conf and main_storage_conf['type'] != 'eph':
                    if self._init_script.running:
                        mysql = spawn_mysql_cli(ROOT_USER, message.root_password)
                        timeout = 180
                        try:
                            mysql.sendline("STOP SLAVE;")
                            mysql.expect("mysql>", timeout=timeout)
                        except pexpect.TIMEOUT:
                            raise HandlerError("Timeout (%d seconds) reached " 
                                            "while waiting for subordinate stop" % (timeout,))
                        finally:
                            mysql.close()
                        self._stop_service('Swapping storages to promote subordinate to main')

                    # Unplug subordinate storage and plug main one
                    #self._unplug_storage(subordinate_vol_id, self._storage_path)
                    old_conf = self.storage_vol.detach(force=True) # ??????
                    #main_vol = self._take_main_volume(main_vol_id)
                    #self._plug_storage(main_vol.id, self._storage_path)
                    new_storage_vol = self._plug_storage(self._storage_path, main_storage_conf)
                    # Continue if main storage is a valid MySQL storage
                    if self._storage_valid():
                        # Patch configuration files
                        self._move_mysql_dir('mysqld/log_bin', self._binlog_base)
                        self._move_mysql_dir('mysqld/datadir', self._data_dir + os.sep)
                        self._replication_init()
                        # Update behaviour configuration
                        updates = {
                                OPT_ROOT_PASSWORD : message.root_password,
                                OPT_REPL_PASSWORD : message.repl_password,
                                OPT_STAT_PASSWORD : message.stat_password,
                                OPT_REPLICATION_MASTER  : "1"
                        }
                        self._update_config(updates)
                        Storage.backup_config(new_storage_vol.config(), self._volume_config_path)

                        self._start_service()

                        # Send message to Scalr
                        msg_data = dict(status='ok')
                        msg_data.update(self._compat_storage_data(vol=new_storage_vol))
                        self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, msg_data)
                    else:
                        raise HandlerError("%s is not a valid MySQL storage" % self._storage_path)

                elif not main_storage_conf or main_storage_conf['type'] == 'eph':
                    self._start_service()
                    mysql = spawn_mysql_cli(ROOT_USER, message.root_password)
                    timeout = 180
                    try:
                        mysql.sendline("STOP SLAVE;")
                        mysql.expect("mysql>", timeout=timeout)
                        mysql.sendline("RESET MASTER;")
                        mysql.expect("mysql>", 20)
                        coreutils.remove(os.path.join(self._data_dir, 'relay-log.info'))
                        coreutils.remove(os.path.join(self._data_dir, 'main.info'))
                    except pexpect.TIMEOUT:
                        raise HandlerError("Timeout (%d seconds) reached " +
                                                "while waiting for subordinate stop and main reset." % (timeout,))
                    finally:
                        mysql.close()

                    updates = {
                            OPT_ROOT_PASSWORD : message.root_password,
                            OPT_REPL_PASSWORD : message.repl_password,
                            OPT_STAT_PASSWORD : message.stat_password,
                            OPT_REPLICATION_MASTER  : "1"
                    }
                    self._update_config(updates)

                    snap, log_file, log_pos = self._create_snapshot(ROOT_USER, message.root_password, tags=self.mysql_tags)
                    Storage.backup_config(snap.config(), self._snapshot_config_path)

                    # Send message to Scalr
                    msg_data = dict(
                            status="ok",
                            log_file = log_file,
                            log_pos = log_pos
                    )
                    msg_data.update(self._compat_storage_data(self.storage_vol, snap))
                    self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, msg_data)

                tx_complete = True
                bus.fire('subordinate_promote_to_main')

            except (Exception, BaseException), e:
                LOG.exception(e)
                self._stop_service()

                if new_storage_vol:
                    new_storage_vol.detach()
                # Get back subordinate storage
                if old_conf:
                    self._plug_storage(self._storage_path, old_conf)

                self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, dict(
                        status="error",
                        last_error=str(e)
                ))

                # Start MySQL
                self._start_service()

            if tx_complete and main_storage_conf and main_storage_conf['type'] != 'eph':
                # Delete subordinate EBS
                self.storage_vol.destroy(remove_disks=True)
                self.storage_vol = new_storage_vol
                Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
        else:
            LOG.warning('Cannot promote to main. Already main')

    @_reload_mycnf
    def on_Mysql_NewMainUp(self, message):
        """
        Switch replication to a new main server
        @type message: scalarizr.messaging.Message
        @param message:  Mysql_NewMainUp
        """
        is_repl_main, = self._get_ini_options(OPT_REPLICATION_MASTER)

        if int(is_repl_main):
            LOG.debug('Skip NewMainUp. My replication role is main')
            return
        mysql = message.body
        host = message.local_ip or message.remote_ip
        LOG.info("Switching replication to a new MySQL main %s", host)
        bus.fire('before_mysql_change_main', host=host)

        if 'snapshot_config' in mysql and mysql['snapshot_config']['type'] != 'eph':
            LOG.info('Reinitializing Subordinate from the new snapshot %s (log_file: %s log_pos: %s)',
                            message.snapshot_config['id'], message.log_file, message.log_pos)
            self._stop_service('Swapping storages to reinitialize subordinate')

            LOG.debug('Destroing old storage')
            self.storage_vol.destroy()
            LOG.debug('Storage destoyed')

            LOG.debug('Plugging new storage')
            vol = Storage.create(snapshot=message.snapshot_config.copy(), tags=self.mysql_tags)
            self._plug_storage(self._storage_path, vol)
            LOG.debug('Storage plugged')

            Storage.backup_config(vol.config(), self._volume_config_path)
            Storage.backup_config(message.snapshot_config, self._snapshot_config_path)
            self.storage_vol = vol
            log_file = message.log_file
            log_pos = message.log_pos

            self._start_service()

        my_cli = spawn_mysql_cli(ROOT_USER, message.root_password)

        if not 'snapshot_config' in mysql or mysql['snapshot_config']['type'] == 'eph':
            LOG.debug("Stopping subordinate i/o thread")
            my_cli.sendline("STOP SLAVE IO_THREAD;")
            my_cli.expect("mysql>")
            LOG.debug("Subordinate i/o thread stopped")

            LOG.debug("Retrieving current log_file and log_pos")
            my_cli.sendline("SHOW SLAVE STATUS\\G");
            my_cli.expect("mysql>")
            log_file = log_pos = None
            for line in my_cli.before.split("\n"):
                pair = map(str.strip, line.split(": ", 1))
                if pair[0] == "Main_Log_File":
                    log_file = pair[1]
                elif pair[0] == "Read_Main_Log_Pos":
                    log_pos = pair[1]
            LOG.debug("Retrieved log_file=%s, log_pos=%s", log_file, log_pos)

        self._change_main(
                host=host,
                user=REPL_USER,
                password=message.repl_password,
                log_file=log_file,
                log_pos=log_pos,
                timeout=self._change_main_timeout,
                my_cli=my_cli
        )

        LOG.debug("Replication switched")
        bus.fire('mysql_change_main', host=host, log_file=log_file, log_pos=log_pos)


    def on_before_reboot_start(self, *args, **kwargs):
        """
        Stop MySQL and unplug storage
        """
        self._stop_service('Instance is going to reboot')

    def on_host_init_response(self, message):
        """
        Check mysql data in host init response
        @type message: scalarizr.messaging.Message
        @param message: HostInitResponse
        """

        with bus.initialization_op as op:
            with op.phase(self._phase_mysql):
                with op.step(self._step_accept_scalr_conf):

                    if not message.body.has_key("mysql"):
                        raise HandlerError("HostInitResponse message for MySQL behaviour must have 'mysql' property")

                    dir = os.path.dirname(self._volume_config_path)
                    if not os.path.exists(dir):
                        os.makedirs(dir)

                    mysql_data = message.mysql.copy()
                    for key, file in ((OPT_VOLUME_CNF, self._volume_config_path),
                                                    (OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
                        if os.path.exists(file):
                            os.remove(file)
                        if key in mysql_data:
                            Storage.backup_config(mysql_data[key], file)
                            del mysql_data[key]

                    # Compatibility with Scalr <= 2.1
                    if bus.scalr_version <= (2, 1):
                        if 'volume_id' in mysql_data:
                            Storage.backup_config(dict(type='ebs', id=mysql_data['volume_id']), self._volume_config_path)
                            del mysql_data['volume_id']
                        if 'snapshot_id' in mysql_data:
                            if mysql_data['snapshot_id']:
                                Storage.backup_config(dict(type='ebs', id=mysql_data['snapshot_id']), self._snapshot_config_path)
                            del mysql_data['snapshot_id']

                    LOG.debug("Update mysql config with %s", mysql_data)
                    self._update_config(mysql_data)


    @_reload_mycnf
    def on_before_host_up(self, message):
        """
        Configure MySQL behaviour
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """


        try:
            out = system2("my_print_defaults mysqld", shell=True)
            result = re.search("--datadir=(.*)", out[0], re.MULTILINE)
            if result:
                datadir = result.group(1)
                if os.path.isdir(datadir) and not os.path.isdir(os.path.join(datadir, 'mysql')):
                    self._start_service()
                    self._stop_service('Autogenerating datadir')
        except:
            pass

        try:
            self._init_script.stop()
        except:
            pass

        repl = 'main' if int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)) else 'subordinate'
        if repl == 'main':
            bus.fire('before_mysql_configure', replication=repl)
            self._init_main(message)
        else:
            bus.fire('before_mysql_configure', replication=repl)
            self._init_subordinate(message)

        bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)


    def _change_selinux_ctx(self):
        if disttool.is_rhel():
            chcon = software.whereis('chcon')
            if not chcon:
                return
            LOG.debug('Changing SELinux file security context for new mysql home')
            system2((chcon[0], '-R', '-u', 'system_u', '-r',
                     'object_r', '-t', 'mysqld_db_t', os.path.dirname(STORAGE_PATH)), raise_exc=False)

    def _init_main(self, message):
        """
        Initialize MySQL main
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """
        LOG.info("Initializing MySQL main")

        with bus.initialization_op as op:
            with op.step(self._step_create_storage):
                # Plug storage
                volume_cnf = Storage.restore_config(self._volume_config_path)
                try:
                    snap_cnf = Storage.restore_config(self._snapshot_config_path)
                    volume_cnf['snapshot'] = snap_cnf
                except IOError:
                    pass
                self.storage_vol = self._plug_storage(mpoint=self._storage_path, vol=volume_cnf)
                Storage.backup_config(self.storage_vol.config(), self._volume_config_path)

                # Stop MySQL server
                #self._stop_service('Required by Main initialization process')
                self._flush_logs()

            msg_data = None
            storage_valid = self._storage_valid() # It's important to call it before _move_mysql_dir

            try:
                with op.step(self._step_move_datadir):
                    try:
                        datadir = self._mysql_config.get('mysqld/datadir')
                    except NoPathError:
                        """ There is no datadir in config """
                        datadir = DEFAULT_DATADIR
                        if not 'mysqld' in self._mysql_config.sections('./'):
                            self._mysql_config.add('mysqld', '')
                        self._mysql_config.add('mysqld/datadir', DEFAULT_DATADIR)
                    if not storage_valid and datadir.find(self._data_dir) == 0:
                        # When role was created from another mysql role it contains modified my.cnf settings
                        self._repair_original_mycnf()

                    # Patch configuration
                    self._move_mysql_dir('mysqld/datadir', self._data_dir + os.sep)
                    self._move_mysql_dir('mysqld/log_bin', self._binlog_base)
                    self._change_selinux_ctx()

                with op.step(self._step_patch_conf):
                    # Init replication
                    self._replication_init(main=True)

                # If It's 1st init of mysql main storage
                if not storage_valid:
                    self._copy_debian_cnf()

                    with op.step(self._step_create_users):
                        # Add system users
                        root_password, repl_password, stat_password = \
                                        self._add_mysql_users(ROOT_USER, REPL_USER, STAT_USER)

                    with op.step(self._step_create_data_bundle):
                        # Get binary logfile, logpos and create storage snapshot
                        snap, log_file, log_pos = self._create_snapshot(ROOT_USER, root_password, tags=self.mysql_tags)
                        Storage.backup_config(snap.config(), self._snapshot_config_path)

                    # Update HostUp message
                    msg_data = dict(
                            root_password=root_password,
                            repl_password=repl_password,
                            stat_password=stat_password,
                            log_file=log_file,
                            log_pos=log_pos
                    )
                    msg_data.update(self._compat_storage_data(self.storage_vol, snap))

                # If volume has mysql storage directory structure (N-th init)
                else:
                    # Get required configuration options
                    with op.step(self._step_innodb_recovery):
                        root_password, = self._get_ini_options(OPT_ROOT_PASSWORD)

                        self._copy_debian_cnf_back()
                        self._innodb_recovery()

                    with op.step(self._step_create_data_bundle):
                        # Create snapshot
                        snap, log_file, log_pos = self._create_snapshot(ROOT_USER, root_password, tags=self.mysql_tags)
                        Storage.backup_config(snap.config(), self._snapshot_config_path)

                    # Update HostUp message
                    msg_data = dict(
                            log_file=log_file,
                            log_pos=log_pos
                    )
                    msg_data.update(self._compat_storage_data(self.storage_vol, snap))


            except (BaseException, Exception):
                if not storage_valid and self._storage_path:
                    # Perform cleanup
                    # XXX: it's not safe. `storage_valid` can be calculated wrong and users data will totally lost.
                    # system2('rm -rf %s' % os.path.join(self._storage_path, '*'), shell=True, raise_exc=False)
                    pass
                raise

            with op.step(self._step_collect_hostup_data):
                if msg_data:
                    message.mysql = msg_data.copy()
                    try:
                        del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
                    except KeyError:
                        pass
                    self._update_config(msg_data)

    def _compat_storage_data(self, vol=None, snap=None):
        ret = dict()
        if bus.scalr_version >= (2, 2):
            if vol:
                ret['volume_config'] = vol.config()
            if snap:
                ret['snapshot_config'] = snap.config()
        else:
            if vol:
                ret['volume_id'] = vol.config()['id']
            if snap:
                ret['snapshot_id'] = snap.config()['id']
        return ret

    def _init_subordinate(self, message):
        """
        Initialize MySQL subordinate
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """
        LOG.info("Initializing MySQL subordinate")

        with bus.initialization_op as op:
            with op.step(self._step_create_storage):
                # Read required configuration options
                root_pass, repl_pass, log_file, log_pos = self._get_ini_options(
                                OPT_ROOT_PASSWORD, OPT_REPL_PASSWORD, OPT_LOG_FILE, OPT_LOG_POS)

                LOG.debug("Initialize subordinate storage")
                self.storage_vol = self._plug_storage(self._storage_path,
                                dict(snapshot=Storage.restore_config(self._snapshot_config_path)))
                Storage.backup_config(self.storage_vol.config(), self._volume_config_path)


            try:
                with op.step(self._step_patch_conf):
                    # Stop MySQL
                    #self._stop_service('Required by Subordinate initialization process')
                    self._flush_logs()

                    # Change configuration files
                    LOG.info("Changing configuration files")

                    if not 'datadir' in self._mysql_config.options('mysqld'):
                        """ Set default value for datadir """
                        self._mysql_config.add('mysqld/datadir', DEFAULT_DATADIR)


                with op.step(self._step_move_datadir):
                    self._move_mysql_dir('mysqld/datadir', self._data_dir)
                    self._move_mysql_dir('mysqld/log_bin', self._binlog_base)
                    self._change_selinux_ctx()
                    self._replication_init(main=False)
                    self._copy_debian_cnf_back()

                with op.step(self._step_innodb_recovery):
                    self._innodb_recovery()
                    self._start_service()

                with op.step(self._step_change_replication_main):
                    # Change replication main
                    main_host = None
                    LOG.info("Requesting main server")
                    while not main_host:
                        try:
                            main_host = list(host
                                    for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts
                                    if host.replication_main)[0]
                        except IndexError:
                            LOG.debug("QueryEnv respond with no mysql main. " +
                                            "Waiting %d seconds before the next attempt", 5)
                            time.sleep(5)

                    LOG.debug("Main server obtained (local_ip: %s, public_ip: %s)",
                                    main_host.internal_ip, main_host.external_ip)

                    host = main_host.internal_ip or main_host.external_ip
                    self._change_main(
                            host=host,
                            user=REPL_USER,
                            password=repl_pass,
                            log_file=log_file,
                            log_pos=log_pos,
                            mysql_user=ROOT_USER,
                            mysql_password=root_pass,
                            timeout=self._change_main_timeout
                    )

                with op.step(self._step_collect_hostup_data):
                    # Update HostUp message
                    message.mysql = self._compat_storage_data(self.storage_vol)
            except:
                exc_type, exc_value, exc_trace = sys.exc_info()
                raise exc_type, exc_value, exc_trace

    def _plug_storage(self, mpoint, vol):
        if not isinstance(vol, Volume):
            vol['tags'] = self.mysql_tags
            vol = Storage.create(vol)

        try:
            if not os.path.exists(mpoint):
                os.makedirs(mpoint)
            if not vol.mounted():
                vol.mount(mpoint)
        except StorageError, e:
            ''' XXX: Crapy. We need to introduce error codes from mount '''
            if 'you must specify the filesystem type' in str(e):
                vol.mkfs()
                vol.mount(mpoint)
            else:
                raise
        return vol

    @property
    def mysql_tags(self):
        is_main = bool(int(self._get_ini_options(OPT_REPLICATION_MASTER)[0]))
        return prepare_tags(BEHAVIOUR, db_replication_role=is_main)


    def _insert_iptables_rules(self):
        if iptables.enabled():
            iptables.FIREWALL.ensure([
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "3306"},
            ])


    def _get_ini_options(self, *args):
        ret = []
        for opt in args:
            try:
                ret.append(self._cnf.rawini.get(CNF_SECTION, opt))
            except ConfigParser.Error:
                err = 'Required configuration option is missed in mysql.ini: %s' % opt
                raise HandlerError(err)
        return tuple(ret)

    def _copy_debian_cnf_back(self):
        debian_cnf = os.path.join(self._storage_path, 'debian.cnf')
        if disttool.is_debian_based() and os.path.exists(debian_cnf):
            LOG.debug("Copying debian.cnf from storage to mysql configuration directory")
            shutil.copy(debian_cnf, '/etc/mysql/')


    def _copy_debian_cnf(self):
        if os.path.exists('/etc/mysql/debian.cnf'):
            LOG.debug("Copying debian.cnf file to mysql storage")
            shutil.copy('/etc/mysql/debian.cnf', self._storage_path)


    def _storage_valid(self, path=None):
        data_dir = os.path.join(path, STORAGE_DATA_DIR) if path else self._data_dir
        binlog_base = os.path.join(path, STORAGE_BINLOG) if path else self._binlog_base
        return os.path.exists(data_dir) and glob.glob(binlog_base + '*')


    def _create_snapshot(self, root_user, root_password, dry_run=False, tags=None):
        self._logger.info('Creating MySQL data bundle')
        was_running = self._init_script.running
        if not was_running:
            self._start_service()

        # Lock tables
        #mysql.client.execute('FLUSH TABLES WITH READ LOCK')
        root_password, = self._get_ini_options(OPT_ROOT_PASSWORD)
        my_cli = spawn_mysql_cli(ROOT_USER, root_password)
        try:
            my_cli.sendline('FLUSH TABLES WITH READ LOCK;')
            my_cli.expect('mysql>')

            system2('sync', shell=True)

            # Retrieve log file and log position
            log_file = log_pos = None
            if int(self._get_ini_options(OPT_REPLICATION_MASTER)[0]):
                my_cli.sendline('SHOW MASTER STATUS;')
                my_cli.expect('mysql>')


                # Retrieve log file and log position
                lines = my_cli.before
                log_row = re.search(re.compile('^\|\s*([\w-]*\.\d*)\s*\|\s*(\d*)', re.M), lines)
                if log_row:
                    log_file = log_row.group(1)
                    log_pos = log_row.group(2)
                else:
                    raise HandlerError('SHOW MASTER STATUS returns empty set. Main is not started?')

                '''
                try:
                        status = mysql.client.fetchall('SHOW MASTER STATUS')[0]
                except IndexError:
                        raise HandlerError('SHOW MASTER STATUS returns empty set. Main is not started?')
                else:
                        log_file, log_pos = status['File'], status['Position']
                '''
            else:
                my_cli.sendline('SHOW SLAVE STATUS \G')
                my_cli.expect('mysql>')
                lines = my_cli.before
                log_row = re.search(re.compile('Relay_Main_Log_File:\s*(.*?)$.*?Exec_Main_Log_Pos:\s*(.*?)$', re.M | re.S), lines)
                if log_row:
                    log_file = log_row.group(1).strip()
                    log_pos = log_row.group(2).strip()
                else:
                    raise HandlerError('SHOW SLAVE STATUS returns empty set. Subordinate is not started?')

            # Creating storage snapshot
            snap = None if dry_run else self._create_storage_snapshot(tags)
        finally:
            my_cli.sendline('UNLOCK TABLES;')
            my_cli.expect('mysql>')
            my_cli.terminate()
            if not was_running:
                self._stop_service('Restoring service`s state after making snapshot')

        wait_until(lambda: snap.state in (Snapshot.CREATED, Snapshot.COMPLETED, Snapshot.FAILED))
        if snap.state == Snapshot.FAILED:
            raise HandlerError('MySQL storage snapshot creation failed. See log for more details')

        LOG.info('MySQL data bundle created\n  snapshot: %s\n  log_file: %s\n  log_pos: %s',
                                        snap.id, log_file, log_pos)
        return snap, log_file, log_pos


    def _create_storage_snapshot(self, tags=None):
        LOG.info("Creating storage snapshot")
        tags = tags or dict()
        #tags.update({'storage': 'mysql'})
        try:
            return self.storage_vol.snapshot(self._data_bundle_description(), tags=tags)
        except StorageError, e:
            LOG.error("Cannot create MySQL data snapshot. %s", e)
            raise


    def _repair_original_mycnf(self):
        self._mysql_config.set('mysqld/datadir', '/var/lib/mysql')
        self._mysql_config.remove('mysqld/log_bin')


    def _add_mysql_users(self, root_user, repl_user, stat_user, root_pass=None, repl_pass=None, stat_pass=None, mysqld=None, my_cli=None):
        LOG.info("Adding mysql system users")

        should_term_mysqld = False
        if not my_cli:
            if not mysqld:
                self._stop_service('Changing access mode')
                mysqld = spawn_mysqld()
                self._ping_mysql()
                should_term_mysqld = True
            my_cli = spawn_mysql_cli()

        # Generate passwords
        root_password = root_pass if root_pass else cryptotool.pwgen(20)
        repl_password = repl_pass if repl_pass else cryptotool.pwgen(20)
        stat_password = stat_pass if stat_pass else cryptotool.pwgen(20)
        self._add_mysql_user(my_cli, root_user, root_password, '%')
        self._add_mysql_user(my_cli, root_user, root_password, 'localhost')
        self._add_mysql_user(my_cli, repl_user, repl_password, '%', ('Repl_subordinate_priv',))
        self._add_mysql_user(my_cli, stat_user, stat_password, '%', ('Repl_client_priv',))

        if should_term_mysqld:
            term_mysqld(mysqld)
            time.sleep(5)

        self._start_service()

        self._update_config(dict(
                root_password=root_password,
                repl_password=repl_password,
                stat_password=stat_password
        ))

        LOG.debug("MySQL system users added")
        return (root_password, repl_password, stat_password)


    def _add_mysql_user(self, my_cli, login, password, host, privileges=None):

        my_cli.sendline('SHOW COLUMNS FROM mysql.user\G')
        my_cli.expect('mysql>')
        res = my_cli.before
        if 'ERROR' in res:
            raise HandlerError("Can't get privileges columns count.")
        priv_count = len([line for line in res.split('\r\n') if line.strip().startswith('Field') and line.endswith('_priv')])
        LOG.debug("*_priv columns total: %s" % priv_count)

        if not privileges:
            cmd = "INSERT INTO mysql.user VALUES('%s','%s',PASSWORD('%s')" % (host, login, password) + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
        else:
            cmd = "INSERT INTO mysql.user (Host, User, Password, %s) VALUES ('%s','%s',PASSWORD('%s'), %s);" \
                            % (', '.join(privileges), host, login,password, ', '.join(["'Y'"]*len(privileges)))

        my_cli.sendline("DELETE FROM mysql.user WHERE User='%s' and Host='%s';" % (login, host))
        my_cli.expect('mysql>')
        my_cli.sendline(cmd)
        my_cli.expect('mysql>')
        res = my_cli.before
        if 'ERROR' in res:
            raise HandlerError("Error occured while adding user '%s' to MySQL user table.\n%s" % (login, res))
        my_cli.sendline("FLUSH PRIVILEGES;")
        my_cli.expect('mysql>')


    def _update_config(self, data):
        self._cnf.update_ini(BEHAVIOUR, {CNF_SECTION: data})


    @_reload_mycnf
    def _replication_init(self, main=True):
        # Create replication config
        self._mysql_config.set('mysqld/expire_logs_days', 10, force=True)
        server_id = 1 if main else int(random.random() * 100000)+1
        self._mysql_config.remove('mysqld/server-id')
        self._mysql_config.add('mysqld/server-id', server_id)
        # Patch networking
        for option in ['bind-address','skip-networking']:
            try:
                self._mysql_config.comment('mysqld/'+option)
            except:
                pass
        self.write_config()


    def _change_main(self, host, user, password, log_file, log_pos,
                                    my_cli=None, mysql_user=None, mysql_password=None,
                                    connect_retry=15, timeout=None):
        my_cli = my_cli or spawn_mysql_cli(mysql_user, mysql_password)
        LOG.info("Changing replication Main to server %s (log_file: %s, log_pos: %s)", host, log_file, log_pos)

        # Changing replication main
        my_cli.sendline('STOP SLAVE;')
        my_cli.expect('mysql>')
        my_cli.sendline('CHANGE MASTER TO MASTER_HOST="%(host)s", \
                                        MASTER_USER="%(user)s", \
                                        MASTER_PASSWORD="%(password)s", \
                                        MASTER_LOG_FILE="%(log_file)s", \
                                        MASTER_LOG_POS=%(log_pos)s, \
                                        MASTER_CONNECT_RETRY=%(connect_retry)s;' % vars())
        my_cli.expect('mysql>')

        # Starting subordinate
        my_cli.sendline('START SLAVE;')
        my_cli.expect('mysql>')
        status = my_cli.before
        if re.search(re.compile('ERROR', re.MULTILINE), status):
            raise HandlerError('Cannot start mysql subordinate: %s' % status)

        def subordinate_status():
            my_cli.sendline('SHOW SLAVE STATUS\G')
            my_cli.expect('mysql>')
            out = my_cli.before
            lines = map(string.strip, out.strip().split('\r\n')[2:-1])
            return dict(map(string.strip, line.split(':', 1)) for line in lines)

        try:
            time_until = time.time() + timeout
            status = None
            while time.time() <= time_until:
                status = subordinate_status()
                if status['Subordinate_IO_Running'] == 'Yes' and \
                        status['Subordinate_SQL_Running'] == 'Yes':
                    break
                time.sleep(5)
            else:
                if status:
                    if not status['Last_Error']:
                        logfile = firstmatched(lambda p: os.path.exists(p),
                                                                ('/var/log/mysqld.log', '/var/log/mysql.log'))
                        if logfile:
                            gotcha = '[ERROR] Subordinate I/O thread: '
                            size = os.path.getsize(logfile)
                            fp = open(logfile, 'r')
                            try:
                                fp.seek(max((0, size - 8192)))
                                lines = fp.read().split('\n')
                                for line in lines:
                                    if gotcha in line:
                                        status['Last_Error'] = line.split(gotcha)[-1]
                            finally:
                                fp.close()

                    msg = "Cannot change replication Main server to '%s'. "  \
                                    "Subordinate_IO_Running: %s, Subordinate_SQL_Running: %s, " \
                                    "Last_Errno: %s, Last_Error: '%s'" % (
                                    host, status['Subordinate_IO_Running'], status['Subordinate_SQL_Running'],
                                    status['Last_Errno'], status['Last_Error'])
                    raise HandlerError(msg)
                else:
                    raise HandlerError('Cannot change replication main to %s' % (host))
        finally:
            try:
                my_cli.close()
            except:
                os.kill(my_cli.pid, signal.SIGKILL)

        LOG.debug('Replication main is changed to host %s', host)


    def _ping_mysql(self):
        for sock in self._init_script.socks:
            wait_sock(sock)


    def _move_mysql_dir(self, directive=None, dirname = None):

        # Retrieveing mysql user from passwd
        mysql_user      = pwd.getpwnam("mysql")
        directory       = os.path.dirname(dirname)

        try:
            raw_value = self._mysql_config.get(directive)
            if not os.path.isdir(directory):
                os.makedirs(directory)
                src_dir = os.path.dirname(raw_value + "/") + "/"
                if os.path.isdir(src_dir):
                    set_se_path = software.whereis('setsebool')
                    if set_se_path:
                        LOG.debug('Make SELinux rule for rsync')
                        system2((set_se_path[0], 'rsync_disable_trans', 'on'), raise_exc=False)
                    LOG.info('Copying mysql directory \'%s\' to \'%s\'', src_dir, directory)
                    rsync(src_dir, directory, archive=True, exclude='ib_logfile*')
                    self._mysql_config.set(directive, dirname)
                else:
                    LOG.info('Mysql directory \'%s\' doesn\'t exist. Creating new in \'%s\'', src_dir, directory)
            else:
                self._mysql_config.set(directive, dirname)

        except NoPathError:
            LOG.debug('There is no such option "%s" in mysql config.' % directive)
            if not os.path.isdir(directory):
                os.makedirs(directory)

            self._mysql_config.add(directive, dirname)

        self.write_config()
        # Recursively setting new directory permissions

        os.chown(directory, mysql_user.pw_uid, mysql_user.pw_gid)
        try:
            for root, dirs, files in os.walk(directory):
                for dir in dirs:
                    os.chown(os.path.join(root , dir), mysql_user.pw_uid, mysql_user.pw_gid)
                for file in files:
                    os.chown(os.path.join(root, file), mysql_user.pw_uid, mysql_user.pw_gid)
        except OSError, e:
            LOG.error('Cannot chown Mysql directory %s', directory)

        LOG.debug('New permissions for mysql directory "%s" were successfully set.' % directory)

        # Adding rules to apparmor config
        if disttool.is_debian_based() and self._apparmor_enabled:
            _add_apparmor_rules(directory)


    def _flush_logs(self):
        if not os.path.exists(self._data_dir):
            return

        info_files = ['relay-log.info', 'main.info']
        files = os.listdir(self._data_dir)

        for file in files:
            if file in info_files or file.find('relay-bin') != -1:
                os.remove(os.path.join(self._data_dir, file))


    def write_config(self):
        self._mysql_config.write(MYCNF)


def spawn_mysqld():
    if not os.path.isdir('/var/run/mysqld'):
        os.makedirs('/var/run/mysqld', mode=0755)
        mysql_user      = pwd.getpwnam("mysql")
        os.chown('/var/run/mysqld', mysql_user.pw_uid, -1)
    try:
        LOG.debug('Spawning mysqld')
        return pexpect.spawn(MYSQLD + ' --user=mysql --skip-grant-tables')
    except pexpect.ExceptionPexpect, e:
        raise HandlerError('Cannot start mysqld. Error: %s' % e)
        pass

def term_mysqld(mysqld):
    LOG.debug('Terminating mysqld')
    mysqld.terminate(force=True)


def spawn_mysql_cli(user=None, password=None, timeout=30):
    try:
        cmd = MYSQL_CLI
        if user:
            cmd += ' -u ' + user
        if password:
            cmd += ' -p'
        LOG.debug('Spawning mysql client')
        exp = pexpect.spawn(cmd, timeout=timeout)

        if password:
            exp.expect('Enter password:')
            exp.sendline(password or '')

        exp.expect('mysql>')
        return exp
    except pexpect.ExceptionPexpect:
        raise HandlerError('Cannot start mysql client. Error: %s' % exp.before)

def get_mysql_version(my_cli):
    my_cli.sendline('SELECT VERSION();')
    my_cli.expect('mysql>')
    version_string = my_cli.before.strip().split('\r\n')[4].split('|')[1].strip()
    version = re.search('[\d\.]+', version_string)
    if not version:
        raise Exception("Can't obtain MySQL version.")
    return version.group(0)

def check_mysql_password(my_cli, user, password, host='%'):
    my_cli.sendline("SELECT PASSWORD('%s') AS hash, Password AS valid_hash FROM mysql.user WHERE mysql.user.User = '%s' and mysql.user.Host = '%s';" %
                            (password, user, host));
    my_cli.expect('mysql>')
    if not 'Empty set' in my_cli.before:
        hash, valid_hash = filter(None, map(string.strip, my_cli.before.strip().split('\r\n')[4].split('|')))
        if hash != valid_hash:
            raise ValueError("Password for user %s doesn't match." % user)
    else:
        raise ValueError("User %s doesn't exists" % (user))


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
            apparmor_initd = ParametrizedInitScript('apparmor', '/etc/init.d/apparmor')
            try:
                apparmor_initd.reload()
            except InitdError, e:
                LOG.error('Cannot restart apparmor. %s', e)
