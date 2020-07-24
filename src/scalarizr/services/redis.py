from __future__ import with_statement
'''
Created on Aug 10, 2011

@author: Dmytro Korsakov
'''

import os
import signal
import logging
import shutil

from scalarizr import storage2, node
from scalarizr.util import initdv2, system2, PopenError, wait_until, Singleton
from scalarizr.services import backup
from scalarizr.services import lazy, BaseConfig, BaseService, ServiceError, PresetProvider
from scalarizr.util import disttool, cryptotool, firstmatched
from scalarizr.linux.coreutils import chown_r
from scalarizr.libs.metaconf import Configuration, NoPathError


__redis__ = node.__node__['redis']
__redis__.update({
        'storage_dir': '/mnt/redisstorage'
})

SERVICE_NAME = CNF_SECTION = DEFAULT_USER = 'redis'

SU_EXEC = '/bin/su'
BASH    = '/bin/bash'

UBUNTU_BIN_PATH          = '/usr/bin/redis-server'
CENTOS_BIN_PATH          = '/usr/sbin/redis-server'
BIN_PATH = UBUNTU_BIN_PATH if disttool.is_ubuntu() else CENTOS_BIN_PATH

PRESET_FNAME = 'redis.conf'
UBUNTU_CONFIG_DIR = '/etc/redis'
CENTOS_CONFIG_DIR = '/etc/'
CONFIG_DIR = UBUNTU_CONFIG_DIR if disttool.is_ubuntu() else CENTOS_CONFIG_DIR
DEFAULT_CONF_PATH = os.path.join(CONFIG_DIR, 'redis.conf')


OPT_REPLICATION_MASTER  = "replication_main"

REDIS_CLI_PATH = '/usr/bin/redis-cli'
DEFAULT_DIR_PATH = '/var/lib/redis'

DEFAULT_PID_DIR = '/var/run/redis' if os.path.isdir('/var/run/redis') else '/var/run'
CENTOS_DEFAULT_PIDFILE = os.path.join(DEFAULT_PID_DIR, 'redis.pid')
UBUNTU_DEFAULT_PIDFILE = os.path.join(DEFAULT_PID_DIR, 'redis-server.pid')
DEFAULT_PIDFILE = UBUNTU_DEFAULT_PIDFILE if disttool.is_ubuntu() else CENTOS_DEFAULT_PIDFILE

REDIS_USER = 'redis'
DB_FILENAME = 'dump.rdb'
AOF_FILENAME = 'appendonly.aof'

AOF_TYPE = 'aof'
SNAP_TYPE = 'snapshotting'

MAX_CUSTOM_PROCESSES = 16
MAX_START_TIMEOUT = 30
DEFAULT_PORT = 6379
PORTS_RANGE = range(DEFAULT_PORT, DEFAULT_PORT+MAX_CUSTOM_PROCESSES)

LOG = logging.getLogger(__name__)


class RedisInitScript(initdv2.ParametrizedInitScript):
    socket_file = None

    @lazy
    def __new__(cls, *args, **kws):
        obj = super(RedisInitScript, cls).__new__(cls, *args, **kws)
        cls.__init__(obj)
        return obj

    def __init__(self):
        initd_script = None
        if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
            initd_script = ('/usr/sbin/service', 'redis-server')
        else:
            initd_script = firstmatched(os.path.exists, ('/etc/init.d/redis', '/etc/init.d/redis-server'))
        initdv2.ParametrizedInitScript.__init__(self, name=SERVICE_NAME,
                initd_script=initd_script)

    @property
    def _processes(self):
        return [p for p in get_redis_processes() if p == DEFAULT_CONF_PATH]


    def status(self):
        return initdv2.Status.RUNNING if self._processes else initdv2.Status.NOT_RUNNING

    def stop(self, reason=None):
        initdv2.ParametrizedInitScript.stop(self)
        wait_until(lambda: not self._processes, timeout=10, sleep=1)

    def restart(self, reason=None):
        self.stop()
        self.start()

    def reload(self, reason=None):
        initdv2.ParametrizedInitScript.restart(self)

    def start(self):
        initdv2.ParametrizedInitScript.start(self)
        wait_until(lambda: self._processes, timeout=10, sleep=1)
        redis_conf = RedisConf.find()
        password = redis_conf.requirepass
        cli = RedisCLI(password)
        wait_until(lambda: cli.test_connection(), timeout=10, sleep=1)


class Redisd(object):

    config_path = None
    redis_conf = None
    port = None
    cli = None

    name = 'redis-server'

    def __init__(self, config_path=None, port=None):
        self.config_path = config_path
        self.redis_conf = RedisConf(config_path)
        self.port = port or self.redis_conf.port
        self.cli = RedisCLI(self.redis_conf.requirepass, self.port)


    @classmethod
    def find(cls, config_obj=None, port=None):
        return cls(config_obj.path, port)


    def start(self):
        try:
            if not self.running:

                #TODO: think about moving this code elsewhere
                if self.port == DEFAULT_PORT:
                    base_dir = self.redis_conf.dir
                    snap_src = os.path.join(base_dir, DB_FILENAME)
                    snap_dst = os.path.join(base_dir, get_snap_db_filename(DEFAULT_PORT))
                    if os.path.exists(snap_src) and not os.path.exists(snap_dst):
                        shutil.move(snap_src, snap_dst)
                        self.redis_conf.dbfilename = snap_dst
                    aof_src = os.path.join(base_dir, AOF_FILENAME)
                    aof_dst = os.path.join(base_dir, get_aof_db_filename(DEFAULT_PORT))
                    if os.path.exists(aof_src) and not os.path.exists(aof_dst):
                        shutil.move(aof_src, aof_dst)
                        self.redis_conf.appendfilename = aof_dst


                LOG.debug('Starting %s on port %s' % (BIN_PATH, self.port))
                system2('%s %s -s %s -c "%s %s"'%(SU_EXEC, DEFAULT_USER, BASH, BIN_PATH, self.config_path), shell=True, close_fds=True, preexec_fn=os.setsid)
                wait_until(lambda: self.running, timeout=MAX_START_TIMEOUT)
                wait_until(lambda: self.cli.test_connection(), timeout=MAX_START_TIMEOUT)
                LOG.debug('%s process has been started.' % SERVICE_NAME)

        except PopenError, e:
            LOG.error('Unable to start redis process: %s' % e)
            raise initdv2.InitdError(e)


    def stop(self, reason=None):
        if self.running:
            LOG.info('Stopping redis server on port %s (pid %s). Reason: %s' % (self.port, self.pid, reason))
            os.kill(int(self.pid), signal.SIGTERM)
            wait_until(lambda: not self.running, timeout=MAX_START_TIMEOUT)


    def restart(self, reason=None, force=True):
        #force parameter is needed
        #for compatibility with lazyInitScript
        if self.running:
            self.stop()
        self.start()


    def reload(self, reason=None):
        self.restart()


    @property
    def running(self):
        process_matches = False
        for config_path in get_redis_processes():
            if config_path == self.config_path:
                process_matches = True
            elif config_path == DEFAULT_CONF_PATH and int(self.port) == DEFAULT_PORT:
                process_matches = True
        return process_matches


    @property
    def pid(self):
        pid = None
        pidfile = self.redis_conf.pidfile
        LOG.debug('Got pidfile %s from redis config %s' % (pidfile, self.redis_conf.path))

        if pidfile == get_pidfile(DEFAULT_PORT):
            if not os.path.exists(pidfile) or not open(pidfile).read().strip():
                LOG.debug('Pidfile is empty. Trying default pidfile %s' % DEFAULT_PIDFILE)
                pidfile = DEFAULT_PIDFILE

        if os.path.exists(pidfile):
            pid = open(pidfile).read().strip()
        else:
            LOG.debug('No pid found in pidfile %s' % pidfile)
        LOG.debug('Redis process on port %s has pidfile %s and pid %s' % (self.port, pidfile, pid))
        return pid


class RedisInstances(object):

    __metaclass__ = Singleton

    instances = None
    main = None
    persistence_type = None

    def __init__(self, main=False, persistence_type=SNAP_TYPE, use_passwords=True):
        self.main = main
        self.persistence_type = persistence_type
        self.use_passwords = use_passwords
        self.instances = []


    @property
    def ports(self):
        return [instance.port for instance in self.instances]


    @property
    def passwords(self):
        return [instance.password for instance in self.instances]


    def __iter__(self):
        return iter(self.instances)


    def get_processes(self):
        return [instance.service for instance in self.instances]


    def get_config_files(self):
        return [instance.redis_conf.path for instance in self.instances]


    def get_default_process(self):
        return self.get_instance(port=DEFAULT_PORT).service


    def get_instance(self, port=None):
        for instance in self.instances:
            if instance.port == port:
                return instance
        raise ServiceError('Redis instance with port %s not found' % port)


    def init_processes(self, num, ports=None, passwords=None):
        ports = ports or []
        passwords = passwords or []
        if not self.use_passwords:
            # Ignoring passwords from HostInitResponse if use_password=0
            passwords = [None for password in passwords]
        if len(ports) < num:
            diff = num-len(ports)
            LOG.debug("Passed ports: %s. Need to find %s more." % (str(ports), diff))
            additional_ports = [port for port in get_available_ports() if port not in ports]
            if len(additional_ports) < diff:
                raise ServiceError('Not enough free ports')

            LOG.debug("Found available ports: %s" % str(additional_ports))
            ports += additional_ports[:diff]

        if len(passwords) < len(ports):
            diff = len(ports) - len(passwords)
            if self.use_passwords:
                LOG.debug("Generating %s additional passwords for ports %s" % (diff, ports[-diff:]))
                additional_passwords= [cryptotool.pwgen(20) for port in ports[-diff:]]
                LOG.debug("Generated passwords: %s" % str(additional_passwords))
                passwords += additional_passwords
            else:
                LOG.debug("Setting  %s additional empty passwords for ports %s" % (diff, ports[-diff:]))
                passwords += [None for port in ports[-diff:]]

        assert len(ports) == len(passwords)

        creds = dict(zip(ports, passwords))
        LOG.debug("Initializing redis processes: %s" % str(creds))
        for port,password in creds.items():
            if port not in self.ports:
                create_redis_conf_copy(port)
                redis_process = Redis(self.main, self.persistence_type, port, password)
                self.instances.append(redis_process)
        LOG.debug('Total of redis processes: %d' % len(self.instances))


    def kill_processes(self, ports=[], remove_data=False):
        for instance in self.instances:
            if instance.port in ports:
                instance.service.stop()
                if remove_data and os.path.exists(instance.db_path):
                    os.remove(instance.db_path)
                self.instances.remove(instance)


    def start(self):
        for redis in self.instances:
            redis.service.start()


    def stop(self, reason = None):
        for redis in self.instances:
            redis.service.stop(reason)


    def restart(self, reason = None):
        for redis in self.instances:
            redis.service.restart(reason)


    def reload(self, reason = None):
        for redis in self.instances:
            redis.service.reload(reason)


    def save_all(self):
        for redis in self.instances:
            if redis.service.running:
                redis.redis_cli.save()


    def init_as_mains(self, mpoint):
        passwords = []
        ports = []
        for redis in self.instances:
            redis.init_main(mpoint)
            passwords.append(redis.password)
            ports.append(redis.port)
        return (ports, passwords)


    def init_as_subordinates(self, mpoint, primary_ip):
        passwords = []
        ports = []
        for redis in self.instances:
            passwords.append(redis.password)
            ports.append(redis.port)
            redis.init_subordinate(mpoint, primary_ip, redis.port)
        return (ports, passwords)


    def wait_for_sync(self,link_timeout=None,sync_timeout=None):
        #consider using threads
        for redis in self.instances:
            redis.wait_for_sync(link_timeout,sync_timeout)


class Redis(BaseService):

    _instance = None
    port = None
    password = None

    def __init__(self, main=False, persistence_type=SNAP_TYPE, port=DEFAULT_PORT, password=None):
        self._objects = {}
        self.is_replication_main = main
        self.persistence_type = persistence_type
        self.port = port
        self.password = password


    def init_main(self, mpoint):
        self.service.stop('Configuring main. Moving Redis db files')
        self.init_service(mpoint)
        self.redis_conf.mainauth = None
        self.redis_conf.subordinateof = None
        self.service.start()
        self.is_replication_main = True
        return self.current_password


    def init_subordinate(self, mpoint, primary_ip, primary_port):
        self.service.stop('Configuring subordinate')
        self.init_service(mpoint)
        self.change_primary(primary_ip, primary_port)
        self.service.start()
        self.is_replication_main = False
        return self.current_password


    def wait_for_sync(self,link_timeout=None,sync_timeout=None):
        LOG.info('Waiting for link with main')
        wait_until(lambda: self.redis_cli.main_link_status == 'up', sleep=3, timeout=link_timeout)
        LOG.info('Waiting for sync with main to complete')
        wait_until(lambda: not self.redis_cli.main_sync_in_progress, sleep=10, timeout=sync_timeout)
        LOG.info('Sync with main completed')


    def change_primary(self, primary_ip, primary_port):
        '''
        Currently redis subordinates cannot use existing data to catch main
        Instead they create another db file while performing full sync
        Wchich may potentially cause free space problem on redis storage
        And broke whole initializing process.
        So scalarizr removing all existing data on initializing subordinate
        to free as much storage space as possible.
        '''
        self.working_directory.empty()
        self.redis_conf.mainauth = self.password
        self.redis_conf.subordinateof = (primary_ip, primary_port)


    def init_service(self, mpoint):
        move_files = not self.working_directory.is_initialized(mpoint)
        self.working_directory.move_to(mpoint, move_files)
        self.redis_conf.requirepass = self.password
        self.redis_conf.daemonize = True
        self.redis_conf.dir = mpoint
        self.redis_conf.bind = None
        self.redis_conf.port = self.port
        self.redis_conf.dbfilename = get_snap_db_filename(self.port)
        self.redis_conf.appendfilename = get_aof_db_filename(self.port)
        self.redis_conf.pidfile = get_pidfile(self.port)
        if self.persistence_type == SNAP_TYPE:
            self.redis_conf.appendonly = False
        elif self.persistence_type == AOF_TYPE:
            self.redis_conf.appendonly = True
            self.redis_conf.save = {}


    @property
    def current_password(self):
        return self.redis_conf.requirepass


    @property
    def db_path(self):
        fname = self.redis_conf.dbfilename if not self.redis_conf.appendonly else self.redis_conf.appendfilename
        return os.path.join(self.redis_conf.dir, fname)


    def _get_redis_conf(self):
        return self._get('redis_conf', RedisConf.find, CONFIG_DIR, self.port)


    def _set_redis_conf(self, obj):
        self._set('redis_conf', obj)


    def _get_redis_cli(self):
        return self._get('redis_cli', RedisCLI.find, self.redis_conf)


    def _set_redis_cli(self, obj):
        self._set('redis_cli', obj)


    def _get_working_directory(self):
        return self._get('working_directory', WorkingDirectory.find, self.redis_conf)


    def _set_working_directory(self, obj):
        self._set('working_directory', obj)


    def _get_service(self):
        return self._get('service', Redisd.find, self.redis_conf, self.port)


    def _set_service(self, obj):
        self._set('service', obj)


    service = property(_get_service, _set_service)
    working_directory = property(_get_working_directory, _set_working_directory)
    redis_conf = property(_get_redis_conf, _set_redis_conf)
    redis_cli = property(_get_redis_cli, _set_redis_cli)


class WorkingDirectory(object):

    default_db_fname = DB_FILENAME

    def __init__(self, db_path=None, user = "redis"):
        self.db_path = db_path
        self.user = user


    @classmethod
    def find(cls, redis_conf):
        dir = redis_conf.dir
        if not dir:
            dir = DEFAULT_DIR_PATH

        db_fname = redis_conf.appendfilename if redis_conf.appendonly else redis_conf.dbfilename
        if not db_fname:
            db_fname = cls.default_db_fname
        return cls(os.path.join(dir,db_fname))


    def move_to(self, dst, move_files=True):
        new_db_path = os.path.join(dst, os.path.basename(self.db_path))

        if not os.path.exists(dst):
            LOG.debug('Creating directory structure for redis db files: %s' % dst)
            os.makedirs(dst)

        if move_files and os.path.exists(os.path.dirname(self.db_path)) and os.path.isfile(self.db_path):
            LOG.debug("copying db file %s into %s" % (os.path.dirname(self.db_path), dst))
            shutil.copyfile(self.db_path, new_db_path)

        LOG.debug("changing directory owner to %s" % self.user)
        chown_r(dst, self.user)
        self.db_path = new_db_path
        return new_db_path


    def is_initialized(self, path):
        # are the redis db files already in place?
        if os.path.exists(path):
            fnames = os.listdir(path)
            return os.path.basename(self.db_path) in fnames
        return False


    def empty(self):
        LOG.info('Emptying redis database dir %s' % os.path.dirname(self.db_path))
        try:
            for fname in os.listdir(os.path.dirname(self.db_path)):
                if fname.endswith('.rdb') or fname.startswith('appendonly'):
                    path = os.path.join(os.path.dirname(self.db_path), fname)
                    if os.path.isfile(path):
                        LOG.debug('Deleting redis db file %s' % path)
                        os.remove(path)
                    elif os.path.islink(path):
                        LOG.debug('Deleting link to redis db file %s' % path)
                        os.unlink(path)
        except OSError, e:
            LOG.error('Cannot empty %s: %s' % (os.path.dirname(self.db_path), e))


class BaseRedisConfig(BaseConfig):

    config_type = 'redis'

    def set(self, option, value, append=False):
        self._init_configuration()
        if value:
            if append:
                self.data.add(option, str(value))
            else:
                self.data.set(option,str(value), force=True)
        else:
            self.data.comment(option)
        self._cleanup(True)


    def set_sequential_option(self, option, seq):
        is_typle = type(seq) is tuple
        try:
            assert seq is None or is_typle
        except AssertionError:
            raise ValueError('%s must be a sequence (got %s instead)' % (option, seq))
        self.set(option, ' '.join(map(str,seq)) if is_typle else None)


    def get_sequential_option(self, option):
        raw = self.get(option)
        return raw.split() if raw else ()


    def get_list(self, option):
        self._init_configuration()
        try:
            value = self.data.get_list(option)
        except NoPathError:
            try:
                value = getattr(self, option+'_default')
            except AttributeError:
                value = ()
        self._cleanup(False)
        return value


    def get_dict_option(self, option):
        raw = self.get_list(option)
        d = {}
        for raw_value in raw:
            k,v = raw_value.split()
            if k and v:
                d[k] = v
        return d


    def set_dict_option(self, option, d):
        try:
            assert d is None or type(d)==dict
            #cleaning up
            #TODO: make clean process smarter using indexes
            for i in self.get_list(option):
                self.set(option+'[0]', None)

            #adding multiple entries
            for k,v in d.items():
                val = ' '.join(map(str,'%s %s'%(k,v)))
                self.set(option, val, append=True)
        except ValueError:
            raise ValueError('%s must be a sequence (got %s instead)' % (option, d))


class RedisConf(BaseRedisConfig):

    config_name = 'redis.conf'

    @classmethod
    def find(cls, config_dir=None, port=DEFAULT_PORT):
        conf_name = get_redis_conf_basename(port)
        conf_path = os.path.join(CONFIG_DIR, conf_name)
        return cls(os.path.join(config_dir, conf_name) if config_dir else conf_path)


    def _get_dir(self):
        return self.get('dir')


    def _set_dir(self, path):
        self.set_path_type_option('dir', path)


    def _get_bind(self):
        return self.get_sequential_option('bind')


    def _set_bind(self, list_ips):
        self.set_sequential_option('bind', list_ips)


    def _get_subordinateof(self):
        return self.get_sequential_option('subordinateof')


    def _set_subordinateof(self, conn_data):
        '''
        @tuple conndata: (ip,) or (ip, port)
        '''
        self.set_sequential_option('subordinateof', conn_data)


    def _get_mainauth(self):
        return self.get('mainauth')


    def _set_mainauth(self, passwd):
        self.set('mainauth', passwd)


    def _get_requirepass(self):
        return self.get('requirepass')


    def _set_requirepass(self, passwd):
        self.set('requirepass', passwd)


    def _get_appendonly(self):
        return True if self.get('appendonly') == 'yes' else False


    def _set_appendonly(self, on):
        assert on == True or on == False
        self.set('appendonly', 'yes' if on else 'no')


    def _get_dbfilename(self):
        return self.get('dbfilename')


    def _set_dbfilename(self, fname):
        self.set('dbfilename', fname)


    def _get_save(self):
        return self.get_dict_option('save')


    def _set_save(self, save_dict):
        self.set_dict_option('save', save_dict)


    def _get_pidfile(self):
        return self.get('pidfile')


    def _set_pidfile(self, pid):
        self.set('pidfile', pid)


    def _get_port(self):
        return self.get('port')


    def _set_port(self, number):
        self.set('port', number)


    def _get_logfile(self):
        return self.get('logfile')


    def _set_logfile(self, path):
        self.set('logfile', path)


    def _get_appendfilename(self):
        return self.get('appendfilename')


    def _set_appendfilename(self, path):
        self.set('appendfilename', path)


    def _get_daemonize(self):
        return self.get('daemonize')


    def _set_daemonize(self, yes=True):
        self.set('daemonize', 'yes' if yes else 'no')


    daemonize = property(_get_daemonize, _set_daemonize)
    appendfilename = property(_get_appendfilename, _set_appendfilename)
    pidfile = property(_get_pidfile, _set_pidfile)
    port = property(_get_port, _set_port)
    logfile = property(_get_logfile, _set_logfile)
    dir = property(_get_dir, _set_dir)
    save = property(_get_save, _set_save)
    bind = property(_get_bind, _set_bind)
    subordinateof = property(_get_subordinateof, _set_subordinateof)
    mainauth = property(_get_mainauth, _set_mainauth)
    requirepass = property(_get_requirepass, _set_requirepass)
    appendonly = property(_get_appendonly, _set_appendonly)
    dbfilename = property(_get_dbfilename, _set_dbfilename)
    dbfilename_default = DB_FILENAME
    appendfilename_default = AOF_FILENAME
    port_default = DEFAULT_PORT


class RedisCLI(object):

    port = None
    password = None
    path = REDIS_CLI_PATH


    class no_keyerror_dict(dict):
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return None


    def __init__(self, password=None, port=DEFAULT_PORT):
        self.port = port
        self.password = password

        if not os.path.exists(self.path):
            raise OSError('redis-cli not found')


    @classmethod
    def find(cls, redis_conf):
        return cls(redis_conf.requirepass, port=redis_conf.port)


    def execute(self, query, silent=False):
        if not self.password:
            full_query = query
        else:
            full_query = 'AUTH %s\n%s' % (self.password, query)
        try:
            out = system2([self.path, '-p', self.port], stdin=full_query,silent=True, warn_stderr=False)[0]

            #fix for redis 2.4 AUTH
            if 'Client sent AUTH, but no password is set' in out:
                out = system2([self.path], stdin=query,silent=True)[0]

            if out.startswith('ERR') or out.startswith('LOADING'):
                raise PopenError(out)

            elif out.startswith('OK\n'):
                out = out[3:]
            if out.endswith('\n'):
                out = out[:-1]
            return out
        except PopenError, e:
            if 'LOADING' in str(e):
                LOG.debug('Unable to execute query %s: Redis is loading the dataset in memory' % query)
            elif not silent:
                LOG.error('Unable to execute query %s with redis-cli: %s' % (query, e))
            raise


    def test_connection(self):
        try:
            self.execute('select (1)', silent=True)
        except PopenError, e:
            if 'LOADING' in str(e):
                return False
        return True


    @property
    def info(self):
        info = self.execute('info')
        LOG.debug('Redis INFO: %s' % info)
        d = self.no_keyerror_dict()
        if info:
            for i in info.strip().split('\n'):
                raw = i[:-1] if i.endswith('\r') else i
                if raw:
                    kv = raw.split(':')
                    if len(kv)==2:
                        key, val = kv
                        if key:
                            d[key] = val
        return d


    @property
    def aof_enabled(self):
        return True if self.info['aof_enabled']=='1' else False


    @property
    def bgrewriteaof_in_progress(self):
        return True if self.info['bgrewriteaof_in_progress']=='1' else False


    @property
    def bgsave_in_progress(self):
        return True if self.info['bgsave_in_progress']=='1' else False


    @property
    def changes_since_last_save(self):
        return int(self.info['changes_since_last_save'])


    @property
    def connected_subordinates(self):
        return int(self.info['connected_subordinates'])


    @property
    def last_save_time(self):
        return int(self.info['last_save_time'])


    @property
    def redis_version(self):
        return self.info['redis_version']


    @property
    def role(self):
        return self.info['role']


    @property
    def main_host(self):
        info = self.info
        if info['role']=='subordinate':
            return info['main_host']
        return None


    @property
    def main_port(self):
        info = self.info
        if info['role']=='subordinate':
            return int(info['main_port'])
        return None


    @property
    def main_link_status(self):
        info = self.info
        if info['role']=='subordinate':
            return info['main_link_status']
        return None


    def bgsave(self, wait_until_complete=True):
        if not self.bgsave_in_progress:
            self.execute('bgsave')
        if wait_until_complete:
            wait_until(lambda: not self.bgsave_in_progress, sleep=5, timeout=900)


    def bgrewriteaof(self, wait_until_complete=True):
        if not self.bgrewriteaof_in_progress:
            self.execute('bgrewriteaof')
        if wait_until_complete:
            wait_until(lambda: not self.bgrewriteaof_in_progress, sleep=5, timeout=900)


    def save(self):
        LOG.info('Flushing redis data to disk (cli on port %s)', self.port)
        self.bgrewriteaof() if self.aof_enabled else self.bgsave()


    @property
    def main_last_io_seconds_ago(self):
        info = self.info
        if info['role']=='subordinate':
            return int(info['main_last_io_seconds_ago'])
        return None


    @property
    def main_sync_in_progress(self):
        info = self.info
        if info['role']=='subordinate':
            return True if info['main_sync_in_progress']=='1' else False
        return False


class RedisPresetProvider(PresetProvider):


    def __init__(self):
        pass


    def get_preset(self, manifest):
        for provider in self.providers:
            if provider.service.port == DEFAULT_PORT:
                return provider.get_preset(manifest)


    def set_preset(self, settings, manifest):
        for provider in self.providers:
            for fname in settings:
                if fname == PRESET_FNAME:
                    provider.set_preset(settings, manifest)


    @property
    def providers(self):
        providers = []
        LOG.debug('Getting list of redis preset providers')
        for port in get_busy_ports():
            service = Redisd(get_redis_conf_path(port), int(port))
            config_mapping = {PRESET_FNAME: service.redis_conf}
            providers.append(PresetProvider(service, config_mapping))
        return providers


class RedisSnapBackup(backup.SnapBackup):
    def __init__(self, **kwds):
        super(RedisSnapBackup, self).__init__(**kwds)
        self.on(freeze=self.freeze)
        self._redis_instances = RedisInstances()


    def freeze(self, volume, state):
        system2('sync', shell=True)
        self._redis_instances.save_all()
        system2('sync', shell=True)


class RedisSnapRestore(backup.SnapRestore):
    def __init__(self, **kwds):
        super(RedisSnapRestore, self).__init__(**kwds)
        self.on(complete=self.complete)


    def complete(self, volume):
        vol = storage2.volume(volume)
        vol.mpoint = __redis__['storage_dir']
        vol.mount()

backup.backup_types['snap_redis'] = RedisSnapBackup
backup.restore_types['snap_redis'] = RedisSnapRestore


def get_snap_db_filename(port=DEFAULT_PORT):
    return 'dump.%s.rdb' % port

def get_aof_db_filename(port=DEFAULT_PORT):
    return 'appendonly.%s.aof' % port

def get_redis_conf_basename(port=DEFAULT_PORT):
    return 'redis.%s.conf' % port


def get_port(conf_path=DEFAULT_CONF_PATH):
    '''
    returns number from config filename
    e.g. 6380 from redis.6380.conf
    '''
    if conf_path == DEFAULT_CONF_PATH:
        return DEFAULT_PORT
    raw = conf_path.split('.')
    return int(raw[-2]) if len(raw) > 2 else None


def get_redis_conf_path(port=DEFAULT_PORT):
    return os.path.join(CONFIG_DIR, get_redis_conf_basename(port))


def get_log_path(port=DEFAULT_PORT):
    return '/var/log/redis/redis-server.%s.log' % port


def get_pidfile(port=DEFAULT_PORT):

    pid_file = os.path.join(DEFAULT_PID_DIR,'redis-server.%s.pid' % port)
    '''
    fix for ubuntu1004
    '''
    if not os.path.exists(pid_file):
        open(pid_file, 'w').close()
    chown_r(pid_file, 'redis')
    return pid_file


def create_redis_conf_copy(port=DEFAULT_PORT):
    if not os.path.exists(DEFAULT_CONF_PATH):
        raise ServiceError('Default redis config %s does not exist' % DEFAULT_CONF_PATH)
    dst = get_redis_conf_path(port)
    if not os.path.exists(dst):
        LOG.debug('Copying %s to %s.' % (DEFAULT_CONF_PATH,dst))
        shutil.copy(DEFAULT_CONF_PATH, dst)
    else:
        LOG.debug('%s already exists.' % dst)


def get_redis_processes():
    config_files = list()
    try:
        out = system2(('ps', '-G', 'redis', '-o', 'command', '--no-headers'))[0]
    except:
        out = ''
    if out:
        for line in out.split('\n'):
            words = line.split()
            if len(words) == 2 and words[0] == BIN_PATH:
                config_files.append(words[1])
    return config_files


def get_busy_ports():
    busy_ports = []
    args = ('ps', '-G', 'redis', '-o', 'command', '--no-headers')
    try:
        out = system2(args, silent=True)[0].split('\n')
        p = [x for x in out if x and BIN_PATH in x]
    except PopenError,e:
        p = []
    LOG.debug('Running redis processes: %s' % p)
    for redis_process in p:
        for port in PORTS_RANGE:
            conf_name = get_redis_conf_basename(port)
            if conf_name in redis_process:
                busy_ports.append(port)
            elif DEFAULT_PORT == port and DEFAULT_CONF_PATH in redis_process:
                busy_ports.append(port)
    LOG.debug('busy_ports: %s' % busy_ports)
    return busy_ports


def get_available_ports():
    busy_ports = get_busy_ports()
    available = [port for port in PORTS_RANGE if port not in busy_ports]
    LOG.debug("Available ports: %s" % available)
    return available
