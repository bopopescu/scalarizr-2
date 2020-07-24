from __future__ import with_statement
'''
Created on Sep 30, 2011

@author: Dmytro Korsakov
'''
import re
import os
import sys
import time
import glob
import shutil
import logging
import functools
import resource


from scalarizr.config import BuiltinBehaviours
from scalarizr.services import BaseConfig, BaseService, lazy
from scalarizr.libs.metaconf import Configuration
from scalarizr.util import disttool, system2, \
                                PopenError, wait_until, initdv2, software, \
                                firstmatched
from scalarizr.linux.coreutils import chown_r
import pymongo


MONGOD = software.which('mongod')
MONGO_CLI = software.which('mongo')
MONGO_DUMP = software.which('mongodump')
MONGOS = software.which('mongos')

ROUTER_DEFAULT_PORT = 27017
ARBITER_DEFAULT_PORT = 27020
REPLICA_DEFAULT_PORT = 27018
CONFIG_SERVER_DEFAULT_PORT = 27019

SERVICE_NAME = BuiltinBehaviours.MONGODB
STORAGE_PATH = "/mnt/mongodb-storage"

LOG_DIR = glob.glob('/var/log/mongo*')[0]
LOG_PATH_DEFAULT = os.path.join(LOG_DIR, 'mongodb.shardsrv.log') 
DEFAULT_UBUNTU_DB_PATH = '/var/lib/mongodb'
DEFAULT_CENTOS_DB_PATH = '/var/lib/mongo'
LOCK_FILE = 'mongod.lock'
DEFAULT_USER = 'mongodb' if disttool.is_ubuntu() else 'mongod'
SCALR_USER = 'scalr'
STORAGE_DATA_DIR = os.path.join(STORAGE_PATH, 'data')

UBUNTU_CONFIG_PATH = '/etc/mongodb.conf'
CENTOS_CONFIG_PATH = '/etc/mongod.conf'
CONFIG_PATH_DEFAULT = '/etc/mongodb.shardsrv.conf'
ARBITER_DATA_DIR = '/tmp/arbiter'
ARBITER_LOG_PATH = os.path.join(LOG_DIR, 'mongodb.arbiter.log') 
ARBITER_CONF_PATH = '/etc/mongodb.arbiter.conf'

CONFIG_SERVER_DATA_DIR = os.path.join(STORAGE_PATH, 'config_server')
CONFIG_SERVER_CONF_PATH = '/etc/mongodb.configsrv.conf'
CONFIG_SERVER_LOG_PATH = os.path.join(LOG_DIR, 'mongodb.configsrv.log') 

ROUTER_LOG_PATH = os.path.join(LOG_DIR, 'mongodb.router.log')

MAX_START_TIMEOUT = 600
MAX_STOP_TIMEOUT = 180


                                
class MongoDBDefaultInitScript(initdv2.ParametrizedInitScript):
    socket_file = None
    
    @lazy
    def __new__(cls, *args, **kws):
        obj = super(MongoDBDefaultInitScript, cls).__new__(cls, *args, **kws)
        cls.__init__(obj)
        return obj
                
    def __init__(self):
        initd_script = None
        if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
            initd_script = ('/usr/sbin/service', 'mongodb')
        else:
            initd_script = firstmatched(os.path.exists, ('/etc/init.d/mongodb', '/etc/init.d/mongod'))
        initdv2.ParametrizedInitScript.__init__(self, name=SERVICE_NAME, 
                        initd_script=initd_script)
        
    def status(self):
        '''
        By default Ubuntu automatically starts mongodb process on 27017 
        which is exactly the port number used by our router process.
        '''
        p = MongoCLI(port=ROUTER_DEFAULT_PORT)
        return initdv2.Status.RUNNING if p.has_connection else initdv2.Status.NOT_RUNNING

    def stop(self, reason=None):
        initdv2.ParametrizedInitScript.stop(self)
        wait_until(lambda: self.status() == initdv2.Status.NOT_RUNNING, timeout=MAX_STOP_TIMEOUT)

    def restart(self, reason=None):
        initdv2.ParametrizedInitScript.restart(self)

    def reload(self, reason=None):
        initdv2.ParametrizedInitScript.restart(self)
        
    def start(self):
        initdv2.ParametrizedInitScript.start(self)
        wait_until(lambda: self.status() == initdv2.Status.RUNNING, sleep=1)
        #error_text="In %s seconds after start Mongodb state still isn't 'Running'" % MAX_START_TIMEOUT)

        
initdv2.explore(SERVICE_NAME, MongoDBDefaultInitScript)
        
                
class MongoDB(BaseService):
    _arbiter = None
    _instance = None
    _config_server = None
    _mongod_noauth = None

    keyfile = None
    login = None
    password = None
    auth = True

    
    def __init__(self, keyfile=None):
        self.keyfile = keyfile
        self._objects = {}
        self._logger = logging.getLogger(__name__)
        self.default_init_script = initdv2.lookup(SERVICE_NAME)

                                                        
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MongoDB, cls).__new__(
                                                    cls, *args, **kwargs)
        return cls._instance


    @property
    def is_replication_main(self):
        res = self.cli.is_main()['ismain']
        self._logger.debug("Replication main: %s", res)
        return res


    def prepare(self, rs_name):
        '''
        Somehow shardsvr in Mongo 2.01 messes with our port option
        And since it is not required we decided to skip it for good.
        '''
        #self.config.shardsvr = True
        '''
        option nojournal is True on all 64bit platforms by default
        '''
        self._logger.debug('Preparing main config file')
        #self.config.nojournal = False
        self.config.replSet = rs_name
        self.config.dbpath = self.working_dir.create(STORAGE_DATA_DIR)
        self.config.logpath = LOG_PATH_DEFAULT
        self.config.port = REPLICA_DEFAULT_PORT
        self.config.logappend = True


    def _prepare_arbiter(self, rs_name):
        if os.path.isdir(ARBITER_DATA_DIR):
            shutil.rmtree(ARBITER_DATA_DIR)
        self._logger.debug('Creating datadir for arbiter: %s' % ARBITER_DATA_DIR)
        os.makedirs(ARBITER_DATA_DIR)
        chown_r(ARBITER_DATA_DIR, DEFAULT_USER)
        self._logger.debug("Preparing arbiter's config file")
        self.arbiter_conf.dbpath = ARBITER_DATA_DIR
        self.arbiter_conf.replSet = rs_name
        self.arbiter_conf.shardsvr = True
        self.arbiter_conf.port = ARBITER_DEFAULT_PORT
        self.arbiter_conf.logpath = ARBITER_LOG_PATH
        self.arbiter_conf.nojournal = True


    def _prepare_config_server(self):
        self._logger.debug('Preparing config server')
        if not os.path.exists(CONFIG_SERVER_DATA_DIR):
            os.makedirs(CONFIG_SERVER_DATA_DIR)
        chown_r(CONFIG_SERVER_DATA_DIR, DEFAULT_USER)
        '''
        configsvr changes the default port and turns on the diaglog, 
        a log that keeps every action the config database performs 
        in a replayable format, just in case.
        For mongo 1.8+ use --port 27019 and --journal (instead of --diaglog). 
        Journaling gives mostly the same effect as the diaglog with better performance.
        P.S. Assume that mongodb roles Scalr will be build on x64 platform only
        Wchich means journal option by default will be on.
        '''
        self.config_server_conf.configsvr = True
        self.config_server_conf.port = CONFIG_SERVER_DEFAULT_PORT
        self.config_server_conf.logpath = CONFIG_SERVER_LOG_PATH


    def initiate_rs(self):
        '''
        @return (host:port)
        '''
        self.cli.initiate_rs()
        wait_until(lambda: self.is_replication_main, sleep=5, logger=self._logger,
                                timeout=120, start_text='Wait until node becomes replication primary')          
        self._logger.debug('Server became replication main')


    def start_shardsvr(self):
        self.working_dir.unlock()
        self.working_dir.set_permissions()
        if self.auth:
            self._logger.info('Starting main mongod process with auth enabled')
        else:
            self._logger.info('Starting main mongod process with auth disabled')
        self.mongod.start()


    def start_arbiter(self):
        if self.arbiter.is_running:
            self.arbiter.stop()
        self._prepare_arbiter(self.config.replSet)
        self._logger.info('Starting mongo arbiter process')
        self.arbiter.start()


    def stop_arbiter(self):
        self.arbiter.stop(reason='Stopping mongo arbiter')
        

    def start_config_server(self):
        self._prepare_config_server()
        self._logger.info('Starting mongo config server')
        self.config_server.start()
        
        
    def stop_config_server(self):
        self.config_server.stop('Stopping mongo config server')
        
        
    def start_router(self, verbose = 0):
        self.stop_default_init_script()
        Mongos.set_keyfile(self.keyfile.path)
        Mongos.verbose = verbose
        try:
            Mongos.start()
        except:
            # XXX: Ugly workaround for those cases when for some reason mongos can't connect to mongo config server
            # and dies without trying to reconnect again (it's normal behaviour). Restart usually helps
            e = sys.exc_info()[1]
            if 'Timeout' in str(e) and not Mongos.is_running():
                Mongos.start()
            else:
                raise


    def stop_router(self):
        Mongos.stop()


    def restart_router(self):
        Mongos.restart()

                
    def stop_default_init_script(self):
        if self.default_init_script.running:
            try:
                if not self.router_cli.is_router_connection():
                    self.default_init_script.stop('Stopping default mongod service')
            except:
                self.default_init_script.stop('Stopping default mongod service')
                
                        
    def register_subordinate(self, ip, port=None):
        ret = self.cli.add_replica(ip, port)
        if ret['ok'] == '0':
            self._logger.error('Could not add replica %s to set: %s' % (ip, ret['errmsg']))
    
            
    def register_arbiter(self,ip,port=None):
        ret = self.cli.add_arbiter(ip, port or ARBITER_DEFAULT_PORT)
        if ret['ok'] == '0':
            self._logger.error('Could not add arbiter %s to set: %s' % (ip, ret['errmsg']))
            
            
    def unregister_subordinate(self,ip,port=None):
        ret = self.cli.remove_subordinate(ip, port)
        if ret['ok'] == '0':
            self._logger.error('Could not remove replica %s from set: %s' % (ip, ret['errmsg']))


    """
    def wait_for_sync(self):
            wait_until(lambda: self.status == 1 or self.status == 2, timeout=3600, sleep=2)
    """


    def remove_replset_info(self):
        self._logger.info("Removing previous replica set info")
        return self.cli.connection.local.system.replset.remove()


    @property
    def status(self):
        '''
        rs.status['myState']
        
        0        Starting up, phase 1 (parsing configuration)
        1        Primary
        2        Secondary
        3        Recovering (initial syncing, post-rollback, stale members)
        4        Fatal error
        5        Starting up, phase 2 (forking threads)
        6        Unknown state (member has never been reached)
        7        Arbiter
        8        Down
        9        Rollback
        '''
        self._logger.debug('Getting rs status')
        ret = self.cli.get_rs_status()
        if 'errmsg' in ret:
            self._logger.error('Could not get status of replica set: %s' % (ret['errmsg']))
        else:
            return int(ret['myState']) if 'myState' in ret else None
                    
                    
    @property
    def replicas(self):
        self._logger.debug('Querying list of replicas')
        ret = self.cli.is_main()
        rep_list = ret['hosts'] if 'hosts' in ret else []
        self._logger.debug('Current replicas are %s' % rep_list) 
        return rep_list


    @property
    def arbiters(self):
        self._logger.debug('Querying list of arbiters')
        ret = self.cli.is_main()
        arbiter_list = ret['arbiters'] if 'arbiters' in ret else []
        self._logger.debug('Current arbiters are %s' % arbiter_list) 
        return arbiter_list

    @property
    def primary_host(self):
        self._logger.debug('Getting current primary host')
        ret = self.cli.is_main()
        return ret['primary'] if 'primary' in ret else None


    @property
    def dbpath(self):
        return self.config.dbpath


    @property
    def arbiter(self):
        if not self._arbiter:
            self._arbiter = Mongod(ARBITER_CONF_PATH, self.keyfile.path, ARBITER_DATA_DIR, ARBITER_DEFAULT_PORT)
        return self._arbiter


    @property
    def config_server(self):
        if not self._config_server:
            self._config_server = Mongod(CONFIG_SERVER_CONF_PATH, self.keyfile.path, CONFIG_SERVER_DATA_DIR, \
                                                                     CONFIG_SERVER_DEFAULT_PORT)
        return self._config_server




    def disable_requiretty(self):
        '''
        requiretty      If set, sudo will only run when the user is logged in to a real tty.  
        When this flag is set, sudo can only be  run from a login session and not via other means 
        such  as cron(8) or cgi-bin scripts.  This flag is off by default on all systems but CentOS5.
        '''
        path = '/etc/sudoers'
        self._logger.debug('Disabling requiretty in %s' % path)
        if not disttool.is_ubuntu():
            orig = None
            with open(path, 'r') as fp:
                orig = fp.read()
            new = re.sub('Defaults\s+requiretty', '\n', orig)
            if new != orig:
                with open(path, 'w') as fp:
                    fp.write(new)


    def _get_mongod(self):
        """
        @rtype: Mongod
        """
        if not self.auth:
            if not self._mongod_noauth:
                self._mongod_noauth = Mongod(configpath=CONFIG_PATH_DEFAULT, keyfile=None, cli=self.cli)
            return self._mongod_noauth
        return self._get('mongod', Mongod.find, self.config, self.keyfile.path, self.cli)

    def _set_mongod(self, obj):
        self._set('mongod', obj)


    def _get_cli(self):
        if not self.auth:
            return MongoCLI(REPLICA_DEFAULT_PORT)
        return self._get('cli', MongoCLI.find, REPLICA_DEFAULT_PORT, SCALR_USER, self.password)

    def _set_cli(self, obj):
        self._set('cli', obj)


    def _get_working_directory(self):
        return self._get('working_directory', WorkingDirectory.find, self.config)
        
    def _set_working_directory(self, obj):
        self._set('working_directory', obj)


    def _get_config(self):
        return self._get('mongodb_conf', MongoDBConfig.find)

    def _set_config(self, obj):
        self._set('mongodb_conf', obj)
        
        
    def _get_arbiter_conf(self):
        return self._get('arbiter_config', ArbiterConf.find, '/etc')

    def _set_arbiter_conf(self, obj):
        self._set('arbiter_config', obj)
        
        
    def _get_cfg_srv_conf(self):
        return self._get('cfg_srv_config', ConfigServerConf.find, '/etc')

    def _set_cfg_srv_conf(self, obj):
        self._set('cfg_srv_config', obj)


    def _get_router_cli(self):
        return self._get('router_cli', MongoCLI.find, ROUTER_DEFAULT_PORT)

    def _set_router_cli(self, obj):
        self._set('router_cli', obj)

    def _get_configsrv_cli(self):
        return self._get('configsrv_cli', MongoCLI.find, CONFIG_SERVER_DEFAULT_PORT)

    def _set_configsrv_cli(self, obj):
        self._set('configsrv_cli', obj)



    cli = property(_get_cli, _set_cli)
    router_cli = property(_get_router_cli, _set_router_cli)
    configsrv_cli = property(_get_configsrv_cli, _set_configsrv_cli)
    mongod = property(_get_mongod, _set_mongod)
    working_dir = property(_get_working_directory, _set_working_directory)  
    config = property(_get_config, _set_config)
    arbiter_conf = property(_get_arbiter_conf, _set_arbiter_conf)
    config_server_conf = property(_get_cfg_srv_conf, _set_cfg_srv_conf)

    
class MongoDump(object):
        
    host = None
    port = None
    
    def __init__(self, host=None, port=None):
        self._logger = logging.getLogger(__name__)
        self.host = host
        self.port = port

    def create(self, dbname, dst):
        self._logger.debug('Dumping database %s to %s' % (dbname, dst))
        args = [MONGO_DUMP, '-d', dbname, '-o', dst]
        if self.host:
            args += ('-h', self.host if not self.port else "%s:%s" % (self.host, self.port))
        return system2(args)

        
class KeyFile(object):
        
    def __init__(self, path):
        self.path = path
                
    @classmethod
    def find(cls, path):
        return cls(path)

    def exists(self):
        return os.path.exists(self.path)

    def __repr__(self):
        return open(self.path).read().strip() if os.path.exists(self.path) else None


class WorkingDirectory(object):
        
    path = None
    user = DEFAULT_USER
    
    def __init__(self, path=None):
        self._logger = logging.getLogger(__name__)  
        self.path = path
        
    @classmethod
    def find(cls, mongo_conf):
        dir = mongo_conf.dbpath
        if not dir:
            dir = DEFAULT_UBUNTU_DB_PATH if disttool.is_ubuntu() else DEFAULT_CENTOS_DB_PATH
        return cls(dir) 

    def is_initialized(self, path): 
        if os.path.exists(path):
            fnames = os.listdir(path)
            return 'local.0' in fnames
        return False

    def is_locked(self):
        return os.path.exists(self.lock_path)

    def unlock(self):
        if self.is_locked():
            self._logger.debug('Lock was found in database directory %s. Last time Mongodb was not shut down properly.' % self.path)
            os.remove(self.lock_path)       
            
    def create(self, dst):
        if not os.path.exists(dst):
            self._logger.debug('Creating directory structure for mongodb files: %s' % dst)
            os.makedirs(dst)

        self.path = dst
        self.set_permissions()
        return dst

    def set_permissions(self):
        self._logger.debug("Changing working directory owner to %s" % self.user)
        chown_r(self.path, self.user)

    @property
    def lock_path(self):
        return os.path.join(self.path, LOCK_FILE)


class MongoDBConfig(BaseConfig):
        
    config_type = 'mongodb'
    config_name = os.path.basename(CONFIG_PATH_DEFAULT)
    
    @classmethod
    def find(cls, config_dir=None):
        #conf_path = UBUNTU_CONFIG_PATH if disttool.is_ubuntu() else CENTOS_CONFIG_PATH
        return cls(os.path.join(config_dir, cls.config_name) if config_dir else CONFIG_PATH_DEFAULT)

    def set(self, option, value):
        self._init_configuration()
        if value :
            self.data.set(option,value, force=True)
        else:
            self.data.remove(option)
        self._cleanup(save_data=True)

    def set_bool_option(self, option, value):
        try:
            assert value in (None, True, False)
        except AssertionError:
            raise ValueError('%s must be a boolean (got %s instead)' % (option, type(value)))
        value_to_set = None if value is None else str(value).lower()
        self.set(option, value_to_set)

    def get_bool_option(self, option):
        value = self.get(option)
        try:
            assert not value or value == 'true' or value == 'false'
        except AssertionError:
            raise ValueError('%s must be true or false (got %s instead)' % (option,  type(value)))
        return True if value == 'true' else False

    def _get_logpath(self):
        return self.get('logpath')

    def _set_logpath(self, path):
        self.set('logpath', path)    

    def _get_replSet(self):
        return self.get('replSet')

    def _set_replSet(self, name):
        self.set('replSet', name)    

    def _get_port(self):
        return self.get_numeric_option('port')

    def _set_port(self, number):
        self.set_numeric_option('port', number)    

    def _get_logappend(self):
        return self.get_bool_option('logappend')

    def _set_logappend(self, on=True):
        self.set_bool_option('logappend', on)    

    def _get_dbpath(self):
        return self.get('dbpath')

    def _set_dbpath(self, path):
        self.set_path_type_option('dbpath', path)

    def _get_nojournal(self):
        return self.get_bool_option('nojournal')

    def _set_nojournal(self, on=False):
        self.set_bool_option('nojournal', on)      

    def _get_nohttpinterface(self):
        return self.get_bool_option('nohttpinterface')

    def _set_nohttpinterface(self, on=False):
        self.set_bool_option('nohttpinterface', on) 

    def _get_rest(self):
        return self.get_bool_option('rest')

    def _set_rest(self, on=False):
        self.set_bool_option('rest', on)
        
    def _set_shardsvr(self, value):
        self.set_bool_option('shardsvr', value)

    def _get_shardsvr(self):
        return self.get_bool_option('shardsvr')

    shardsvr = property(_get_shardsvr, _set_shardsvr)
    rest = property(_get_rest, _set_rest)
    nohttpinterface = property(_get_nohttpinterface, _set_nohttpinterface)
    nojournal = property(_get_nojournal, _set_nojournal)
    dbpath = property(_get_dbpath, _set_dbpath)
    logappend = property(_get_logappend, _set_logappend)
    port = property(_get_port, _set_port)
    replSet = property(_get_replSet, _set_replSet)
    logpath = property(_get_logpath, _set_logpath)


class ArbiterConf(MongoDBConfig):
    config_name = 'mongodb.arbiter.conf'

    @classmethod
    def find(cls, config_dir='/etc'):
        return cls(os.path.join(config_dir, cls.config_name))


class ConfigServerConf(MongoDBConfig):
    config_name = 'mongodb.configsrv.conf'

    @classmethod
    def find(cls, config_dir='/etc'):
        return cls(os.path.join(config_dir, cls.config_name))
                                
    def _set_configsvr(self, value):
        self.set_bool_option('configsvr', value)

    def _get_configsvr(self):
        return self.get_bool_option('configsvr')

    configsvr = property(_get_configsvr, _set_configsvr)

  
def mongo_preexec_fn():
    unlimited = (resource.RLIM_INFINITY, resource.RLIM_INFINITY)
    resource.setrlimit(resource.RLIMIT_NOFILE, (64000, 64000))
    resource.setrlimit(resource.RLIMIT_NPROC, (32000, 32000))
    resource.setrlimit(resource.RLIMIT_FSIZE, unlimited)
    resource.setrlimit(resource.RLIMIT_CPU, unlimited)
    os.setsid()


class Mongod(object):   
    def __init__(self, configpath=None, keyfile=None, dbpath=None, port=None, cli=None, verbose=1):
        self._logger = logging.getLogger(__name__)
        self.configpath = configpath
        self.dbpath = dbpath
        self.keyfile = keyfile
        self.cli = cli or MongoCLI(port=port)
        self.port = port
        self.sock = initdv2.SockParam(self.port or REPLICA_DEFAULT_PORT)
        self.verbose = verbose
        
    @classmethod
    def find(cls, mongo_conf=None, keyfile=None, cli=None):
        #config_path = mongo_conf.path or UBUNTU_CONFIG_PATH if disttool.is_ubuntu() else CENTOS_CONFIG_PATH
        return cls(configpath=CONFIG_PATH_DEFAULT, keyfile=keyfile, cli=cli)

    @property
    def args(self):
        s = ['--fork']
        if self.configpath:
            s.append('--config=%s' % self.configpath)
        if self.dbpath:
            s.append('--dbpath=%s' % self.dbpath)
        if self.port:
            s.append('--port=%s' % self.port)
        if self.keyfile and os.path.exists(self.keyfile):
            chown_r(self.keyfile, DEFAULT_USER)
            s.append('--keyFile=%s' % self.keyfile)
        if self.verbose and isinstance(self.verbose, int) and 0<self.verbose<6:
            s.append('-'+'v'*self.verbose)

        return s




    def start(self):
        try:
            if not self.is_running:
                self._logger.debug('Starting %s' % MONGOD)
                system2(['sudo', '-u', DEFAULT_USER, MONGOD,] + self.args, close_fds=True, preexec_fn=mongo_preexec_fn)
                '''
                mongod process takes some time before it actualy starts accepting connections
                it can easily be as long as 160 seconds on a Large instance
                '''
                wait_until(lambda: self.is_running, timeout=MAX_START_TIMEOUT)
                wait_until(lambda: self.cli.has_connection, timeout=MAX_START_TIMEOUT)
                self._logger.debug('%s process has been started.' % MONGOD)
                
        except PopenError, e:
            self._logger.error('Unable to start mongod process: %s' % e)

    def stop(self, reason=None):
        if self.is_running:
            self._logger.info('Stopping %s: %s' % (MONGOD,reason))
            self.cli.shutdown_server()
            wait_until(lambda: not self.is_running, timeout=MAX_STOP_TIMEOUT)
            self._logger.debug('%s process has been stopped.' % MONGOD)

    def restart(self, reason=None):
        if self.is_running:
            self.stop(reason)
            self.start()    

    @property
    def is_running(self):
        try:
            initdv2.wait_sock(self.sock)
            return True
        except:
            return False


class Mongos(object):
    _logger = logging.getLogger(__name__)
    sock = initdv2.SockParam(ROUTER_DEFAULT_PORT)
    keyfile = None
    authenticated = False
    login = None
    password = None
    verbose = 0

    @classmethod
    def set_keyfile(cls, keyfile = None):
        cls.keyfile = keyfile


    @classmethod
    def auth(cls, login, password):
        cls.login = login
        cls.password = password


    @classmethod
    def get_cli(cls):
        if not hasattr(cls, '_cli'):
            cls._cli = MongoCLI(port=ROUTER_DEFAULT_PORT)
        if cls.login and cls.password and not cls.authenticated:
            cls._cli.auth(cls.login, cls.password)
            cls.authenticated = True
        return cls._cli


    @classmethod
    def start(cls):
        if not cls.is_running():
            cls._logger.info('Starting %s process' % MONGOS)
            args = ['sudo', '-u', DEFAULT_USER, MONGOS, '--fork',
                            '--logpath', ROUTER_LOG_PATH, '--configdb',
                            'mongo-0-0:%s' % CONFIG_SERVER_DEFAULT_PORT]
            if cls.keyfile and os.path.exists(cls.keyfile):
                chown_r(cls.keyfile, DEFAULT_USER)
                args.append('--keyFile=%s' % cls.keyfile)

            if cls.verbose and isinstance(cls.verbose, int) and 0<cls.verbose<6:
                args.append('-'+'v'*cls.verbose)


            if os.path.exists(ROUTER_LOG_PATH):
                chown_r(ROUTER_LOG_PATH, DEFAULT_USER)

            system2(args, close_fds=True, preexec_fn=mongo_preexec_fn)
            wait_until(lambda: cls.is_running, timeout=MAX_START_TIMEOUT)
            wait_until(lambda: cls.get_cli().has_connection, timeout=MAX_START_TIMEOUT)
            cls._logger.debug('%s process has been started.' % MONGOS)


    @classmethod
    def stop(cls):
        if cls.is_running():
            cls._logger.info('Stopping %s process' % MONGOS)
            cls.get_cli().shutdown_server()
            wait_until(lambda: not cls.is_running(), timeout=MAX_STOP_TIMEOUT)
            cls._logger.debug('%s process has been stopped.' % MONGOS)


    @classmethod
    def is_running(cls):
        try:
            initdv2.wait_sock(cls.sock)
            return True
        except:
            return False


    @classmethod
    def restart(cls):
        if cls.is_running:
            cls.stop()
            cls.start()


def autoreconnect(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        max_retry = 3
        attempts = 0
        while True:
            try:
                return f(*args, **kwargs)
            except pymongo.errors.AutoReconnect, e:
                attempts += 1
                if attempts >= max_retry:
                    raise e
                time.sleep(0.3)
    return wrapper


class MongoCLIMeta(type):
    """ Port-based multiton metaclass for MongoCLI """
    def __call__(cls, *args, **kwargs):
        port = args[0] if args else kwargs['port'] if 'port' in kwargs else REPLICA_DEFAULT_PORT
        if cls._instances.has_key(port):
            return cls._instances[port]

        cli = type.__call__(cls, *args, **kwargs)
        cls._instances[port] = cli
        return cli



class MongoCLI(object):

    __metaclass__ = MongoCLIMeta
    host = 'localhost'
    _instances = dict()

    def __init__(self, port=REPLICA_DEFAULT_PORT, login=SCALR_USER, password=None):
        self.port = port
        self.login = login
        self.password = password
        self._logger = logging.getLogger(__name__)
        self.sock = initdv2.SockParam(port)

    """
    def __call__(cls, *args, **kwargs):
            'MongoCLI gives only one connection per port'
            port = args[0] if args else kwargs['port'] if 'port' in kwargs else REPLICA_DEFAULT_PORT
            if port not in cls._instances:
                    cls._instances[port] = super(MongoCLI, cls).__call__(*args, **kwargs)
            return cls._instances[port]
    """

    @classmethod
    def find(cls, port=REPLICA_DEFAULT_PORT, login=SCALR_USER, password=None):
        return cls(port=port, login=login, password=password)


    def auth(self,login,password):
        self.login = login
        self.password = password
        
        
    @property
    def connection(self):
        if not hasattr(self, '_con'):
            self._logger.debug('creating pymongo connection to %s:%s' % (self.host,self.port))
            self._con = pymongo.Connection(self.host, self.port)

        if self.login and self.password and self.is_port_listening:
            try:
                self._con.admin.system.users.find().next()
            except:
                self._logger.debug('Authenticating connection on port %s as %s', self.port, self.login)
                self._con.admin.authenticate(self.login, self.password)
        return self._con


    @property
    def has_connection(self):
        '''
        MongoDB shell version: 2.0.0
        connecting to: test
        Thu Oct 20 13:17:21 Error: couldn't connect to server 127.0.0.1 shell/mongo.js:84
        exception: connect failed
        '''
        try:
            self.connection.database_names()
        except pymongo.errors.AutoReconnect, e:
            if "Connection refused" in str(e):
                return False
        except pymongo.errors.OperationFailure, e:
            if 'unauthorized' in str(e):
                pass
        except BaseException, e:
            self._logger.debug(e)
        return True


    @property
    def is_port_listening(self):
        try:
            initdv2.wait_sock(self.sock)
            return True
        except:
            return False

    @autoreconnect
    def list_database_names(self):
        self._logger.debug('Getting list of databases')
        return self.connection.database_names()

    @autoreconnect
    def initiate_rs(self):
        '''
    initializes replica set
    '''
        self._logger.info('Initializing replica set')
        if self.connection.local.system.replset.find_one():
            self._logger.debug('Replica set already initialized. Nothing to do')
            return
    
        res = self.connection.admin.command('replSetInitiate')
        if int(res['ok']) == 0:
            raise BaseException('Could not initialize replica set: %s' % res['errmsg'])


    @autoreconnect
    def add_shard(self, shard_name, rs_name, rs_members):
        self._logger.debug('Adding shard %s with members %s' % (shard_name, rs_members))
        host_str = '%s/%s' % (rs_name, ','.join(rs_members))
        return self.connection.admin.command('addshard', host_str, name=shard_name)


    def add_replica(self, ip, port=None, arbiter=False):

        port = port or REPLICA_DEFAULT_PORT
        host = "%s:%s" % (ip, port)
        self._logger.debug('Registering new %s %s', 'arbiter' if arbiter else 'replica', host)

        cfg = self.get_rs_config()

        if host in [member['host'] for member in cfg['members']]:
            self._logger.debug('Host %s is already in replica set.' % host)
            return
        new_member = {}
        new_member['host'] = host
        cfg['version'] = cfg['version'] + 1
        new_member['_id'] = max([m['_id'] for m  in cfg['members']]) + 1
        
        if arbiter:
            new_member['arbiterOnly'] = True
            
        cfg['members'].append(new_member)
        return self.rs_reconfig(cfg, force=True)


    @autoreconnect
    def is_main(self):
        self._logger.debug('Checking if node is main')
        return self.connection.admin.command('isMain')


    @autoreconnect
    def get_rs_status(self):
        return self.connection.admin.command('replSetGetStatus')


    @autoreconnect
    def get_rs_config(self):
        self._logger.debug('Getting rs config')
        rs_count = self.connection.local.system.replset.count()
        assert rs_count, "No replica set found" 
        return self.connection.local.system.replset.find_one()


    @autoreconnect
    def rs_reconfig(self, config, force=False):
        self._logger.debug('Reconfiguring replica set (config: %s)', config)
        try:
            ret = self.connection.admin.command("replSetReconfig", config)
            self._logger.debug('Mongo replSetReconfig answer: %s', ret)
        except:
            if force:
                self._logger.debug('Reconfiguring failed. Retrying with "force" argument')
                ret = self.connection.admin.command("replSetReconfig", config, force=force)
                self._logger.debug('Mongo replSetReconfig answer: %s', ret)
            else:
                raise
        return ret


    def add_arbiter(self,ip, port=None):
        self._logger.debug('Registering new arbiter %s' % ip)
        return self.add_replica(ip, port, arbiter=True)


    def remove_subordinate(self, ip, port=None):
        port = port or REPLICA_DEFAULT_PORT
        host_to_del = "%s:%s" % (ip, port)
                        
        self._logger.debug('Removing replica %s' % host_to_del)         

        cfg = self.get_rs_config()
        cfg['version'] = cfg['version'] + 1
        for member in cfg['members']:
            if member['host'] == host_to_del:
                cfg['members'].remove(member)
                return self.rs_reconfig(cfg, force=True)
        else:
            self._logger.warning("Host %s not found in replica set" % host_to_del)


    def shutdown_server(self):
        try:
            self._logger.debug('Shutting down service %s:%s' % (self.host,self.port))
            out = self.connection.admin.command('shutdown', force=True)

        except (pymongo.errors.AutoReconnect, pymongo.errors.OperationFailure), e:
            self._logger.debug('Could not shutdown server from the inside: %s',e)
            out = None
        return out


    @autoreconnect
    def sync(self, lock=False):
        self._logger.debug('Performing fsync on server')
        '''
        By default the command returns after synchronizing.
        '''
        ret = self.connection.admin.command('fsync', lock=lock)
        self._logger.debug('fsync is done.')
        return ret


    @autoreconnect
    def unlock(self):
        self._logger.debug('Requesting fsync unlock')
        ret = self.connection.admin['$cmd'].sys.unlock.find_one()
        if ret['ok'] != 1:
            raise Exception('Failed to get fsync unlock: %s' % ret.get('errmsg'))
        return ret


    @autoreconnect
    def stop_balancer(self):
        '''
        // connect to mongos (not a config server!)
        
        > use config
        > db.settings.update( { _id: "balancer" }, { $set : { stopped: true } } , true ); 
        >
        > // wait for any migrations that were in progress to finish
        > // "state" field is zero if no migrations in progress
        > while( db.locks.findOne({_id: "balancer"}).state ) { print("waiting..."); sleep(1000); }
        
        http://www.mongodb.org/display/DOCS/Backing+Up+Sharded+Cluster
        '''
        self._logger.info('Stopping balancer')
        self.connection.config.settings.update({'_id': 'balancer'}, {'stopped' : True}, True)
        self._logger.debug("Waiting until balancer finishes it's round")
        while self.connection.locks.find_one({'_id': "balancer"})['state']:
            time.sleep(1)
        self._logger.debug('Balancer has been stopped.')        


    @autoreconnect
    def start_balancer(self):
        '''
        >use config
        >db.settings.update( { _id: "balancer" }, { $set : { stopped: false } } , true );
        '''
        self._logger.info('Starting balancer')
        self.connection.config.settings.update({'_id': 'balancer'}, {'stopped' : False}, True)
        self._logger.debug('Balancer has been started.')


    @autoreconnect
    def create_or_update_admin_user(self, username, password):
        self._logger.info('Updating mongodb user %s on %s:%s' % (username, self.host, self.port))
        self.connection.admin.add_user(username, password)


    @autoreconnect
    def is_router_connection(self):
        return 'mongos' in self.connection.config.collection_names()


    @autoreconnect
    def flush_router_cfg(self):
        self.connection.admin.command('flushRouterConfig')


    @autoreconnect
    def list_cluster_databases(self):
        """ list databases with shard status """
        return list(self.connection.config.databases.find())


    @autoreconnect
    def remove_shard(self, shard_name):
        return self.connection.admin.command('removeshard', shard_name)


    @autoreconnect
    def move_primary(self, db_name, dest_shard):
        return self.connection.admin.command("moveprimary", db_name, to=dest_shard)


    @autoreconnect
    def step_down(self, seconds=1, force=False):
        return self.connection.admin.command('replSetStepDown', seconds, force=force)


    @autoreconnect
    def list_shards(self):
        return list(self.connection.config.shards.find())
