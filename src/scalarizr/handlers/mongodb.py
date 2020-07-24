from __future__ import with_statement
'''
Created on Sep 30, 2011

@author: Dmytro Korsakov
'''
from __future__ import with_statement

from scalarizr.linux import iptables, pkgmgr
from scalarizr.util import disttool, system2

import os
import sys
import time
import shutil
import urllib
import urllib2
import logging
import datetime
import threading

mgr = pkgmgr.package_mgr()

if disttool.is_redhat_based():
    if disttool.version_info()[0] >= 6:
        if mgr.info('python-pymongo').get('installed'):
            system2(('/usr/bin/yum', '-d0', '-y', 'erase', 'python-pymongo',
                             'python-bson'))
        if not mgr.info('pymongo').get('installed'):
            mgr.install('pymongo', mgr.info('pymongo')['candidate'])
    elif disttool.version_info()[0] == 5:
        if not mgr.info('python26-pymongo').get('installed'):
            mgr.install('python26-pymongo', mgr.info('python26-pymongo')['candidate'])
else:
    if not mgr.info('python-pymongo').get('installed'):
        # without python-bson explicit version won't work
        ver = mgr.info('python-pymongo')['candidate']
        mgr.install('python-pymongo', ver)
        mgr.install('python-bson', ver)

import pymongo

from scalarizr.bus import bus
from scalarizr.platform import PlatformFeatures
from scalarizr.messaging import Messages
from scalarizr.util import wait_until, Hosts, cryptotool

from scalarizr.config import BuiltinBehaviours, ScalarizrState, STATE
from scalarizr.handlers import ServiceCtlHandler, HandlerError
from scalarizr import storage2

from scalarizr.node import __node__
import scalarizr.services.mongodb as mongo_svc
from scalarizr.messaging.p2p import P2pMessageStore
from scalarizr.handlers import operation, prepare_tags

from scalarizr.api import mongodb as mongodb_api

__mongodb__ = __node__['mongodb']


BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.MONGODB

STORAGE_VOLUME_CNF              = 'mongodb.json'
STORAGE_SNAPSHOT_CNF    = 'mongodb-snap.json'
STORAGE_TMP_DIR                 = "tmp"

BACKUP_CHUNK_SIZE               = 200*1024*1024

HOSTNAME_TPL                    = "mongo-%s-%s"
RS_NAME_TPL                             = "rs-%s"
SHARD_NAME_TPL                  = "shard-%s"

HEARTBEAT_INTERVAL              = 60

CLUSTER_STATE_KEY               = "mongodb.cluster_state"
REMOVE_VOLUME_KEY               = "mongodb.remove_volume"
MONGO_VOLUME_CREATED    = "mongodb_created_volume_id"


        
def get_handlers():
    return (MongoDBHandler(), )


class MongoDBClusterStates:
    TERMINATING = 'terminating'
    RUNNING         = 'running'


class MongoDBMessages:

    CREATE_DATA_BUNDLE = "MongoDb_CreateDataBundle"
    
    CREATE_DATA_BUNDLE_RESULT = "MongoDb_CreateDataBundleResult"
    '''
    @ivar status: ok|error
    @ivar last_error
    @ivar snapshot_config
    @ivar used_size
    '''
    
    CREATE_BACKUP = "MongoDb_CreateBackup"
    
    CREATE_BACKUP_RESULT = "MongoDb_CreateBackupResult"
    """
    @ivar status: ok|error
    @ivar last_error
    @ivar backup_urls: S3 URL
    """

    """
    Also MongoDB behaviour adds params to common messages:
    
    = HOST_INIT_RESPONSE =
    @ivar MongoDB=dict(
            key_file                                 A key file with at least 6 Base64 characters
            volume_config                   Main storage configuration                    (on main)
            snapshot_config                 Main storage snapshot                          (both)
    )
    
    = HOST_UP =
    @ivar mysql=dict(
            root_password:                   'scalr' user password                                    (on main)
            repl_password:                   'scalr_repl' user password                             (on main)
            stat_password:                   'scalr_stat' user password                             (on main)
            log_file:                                Binary log file                                                        (on main) 
            log_pos:                                 Binary log file position                               (on main)
            volume_config:                  Current storage configuration                   (both)
            snapshot_config:                Main storage snapshot                                 (on main)              
    ) 
    """

    INT_CREATE_DATA_BUNDLE = "MongoDb_IntCreateDataBundle"
    
    INT_CREATE_DATA_BUNDLE_RESULT = "MongoDb_IntCreateDataBundle"
    
    INT_CREATE_BOOTSTRAP_WATCHER = "MongoDb_IntCreateBootstrapWatcher"
    
    INT_BOOTSTRAP_WATCHER_RESULT = "MongoDb_IntBootstrapWatcherResult"

    
    CLUSTER_TERMINATE = "MongoDb_ClusterTerminate"
    
    CLUSTER_TERMINATE_STATUS = "MongoDb_ClusterTerminateStatus"
    
    CLUSTER_TERMINATE_RESULT = "MongoDb_ClusterTerminateResult"

    INT_CLUSTER_TERMINATE = "MongoDb_IntClusterTerminate"
    
    INT_CLUSTER_TERMINATE_RESULT = "MongoDb_IntClusterTerminateResult"
    
    REMOVE_SHARD = "MongoDb_RemoveShard"
    
    REMOVE_SHARD_RESULT = "MongoDb_RemoveShardResult"
    
    REMOVE_SHARD_STATUS = "MongoDb_RemoveShardStatus"

    INT_BEFORE_HOST_UP = "MongoDB_IntBeforeHostUp"
    
    
class ReplicationState:
    INITIALIZED = 'initialized'
    STALE           = 'stale'


class TerminationState:
    FAILED = 'failed'
    UNREACHABLE = 'unreachable'
    TERMINATED = 'terminated'
    PENDING = 'pending_terminate'


class MongoDBHandler(ServiceCtlHandler):
    _logger = None
            
    _queryenv = None
    """ @type _queryenv: scalarizr.queryenv.QueryEnvService """
    
    _platform = None
    """ @type _platform: scalarizr.platform.Ec2Platform """
    
    _cnf = None
    ''' @type _cnf: scalarizr.config.ScalarizrCnf '''
    
    storage_vol = None
            
            
    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return BEHAVIOUR in behaviour and message.name in (
                        MongoDBMessages.CREATE_DATA_BUNDLE,
                        MongoDBMessages.CREATE_BACKUP,
                        MongoDBMessages.INT_CREATE_BOOTSTRAP_WATCHER,
                        MongoDBMessages.INT_CREATE_DATA_BUNDLE,
                        MongoDBMessages.INT_CREATE_DATA_BUNDLE_RESULT,
                        MongoDBMessages.CLUSTER_TERMINATE,
                        MongoDBMessages.INT_CLUSTER_TERMINATE,
                        MongoDBMessages.REMOVE_SHARD,
                        Messages.UPDATE_SERVICE_CONFIGURATION,
                        Messages.BEFORE_HOST_TERMINATE,
                        Messages.HOST_DOWN,
                        Messages.HOST_INIT,
                        Messages.HOST_UP)
        
    def get_initialization_phases(self, hir_message):
        return {'before_host_up': [{
                'name': self._phase_mongodb,
                'steps': [
                        self._step_accept_scalr_conf,
                        self._step_check_cfg_server,
                ]
        }]}


    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._service_name = BEHAVIOUR
        bus.on("init", self.on_init)
        bus.define_events(
                'before_%s_data_bundle' % BEHAVIOUR,
                
                '%s_data_bundle' % BEHAVIOUR,
                
                # @param host: New main hostname 
                'before_%s_change_main' % BEHAVIOUR,
                
                # @param host: New main hostname 
                '%s_change_main' % BEHAVIOUR,
                
                'before_subordinate_promote_to_main',
                
                'subordinate_promote_to_main'
        )       
        
        self._phase_mongodb = 'Configure MongoDB'
        self._phase_data_bundle = self._op_data_bundle = 'MongoDB data bundle'
        self._phase_backup = self._op_backup = 'MongoDB backup'
        self._step_upload_to_cloud_storage = 'Upload data to cloud storage'             
        self._step_create_snapshot = 'Create snapshot'
        self._step_stop_balancer = 'Stop balancer'
        self._step_fsync = 'Perform fsync'
        self._step_accept_scalr_conf = 'Accept Scalr configuration'
        self._step_check_cfg_server = 'Check that ConfigServer is running'
        self._step_change_hostname = 'Change hostname'
        self._step_init_main = 'Initialize Main'
        self._step_init_subordinate = 'Initialize Subordinate'
        self._step_start_arbiter = 'Start Arbiter'
        self._step_start_cfg_server = 'Start ConfigServer'
        self._step_start_router = 'Start Router'
        self._step_enter_rs0_barrier = 'Enter rs-0 barrier'
        self._step_wait_cfg_server = 'Wait for ConfigServer on mongo-0-0'
        self._step_auth_on_cfg_server_and_router = 'Authenticate on ConfigServer and Router'
        self._step_create_scalr_users = 'Create Scalr users'
        self._step_create_shard = 'Create Shard'
        
        self.api = mongodb_api.MongoDBAPI()
        
        self.on_reload()   
        self._status_trackers = dict()


    def on_init(self):
                    
        bus.on("host_init_response", self.on_host_init_response)
        bus.on("before_host_up", self.on_before_host_up)
        #if self._cnf.state in (ScalarizrState.BOOTSTRAPPING, ScalarizrState.IMPORTING):
        self._insert_iptables_rules()
        
        if 'ec2' == self._platform.name:
            __node__['ec2']['hostname_as_pubdns'] = '0'

        if self._cnf.state in (ScalarizrState.INITIALIZING, ScalarizrState.BOOTSTRAPPING):
            self.mongodb.stop_default_init_script()
    
        if self._cnf.state == ScalarizrState.RUNNING:
            storage_vol = __mongodb__['volume']
            storage_vol.ensure(mount=True)

            self.mongodb.password = self.scalr_password
            self.mongodb.start_shardsvr()
            self.mongodb.cli.auth(mongo_svc.SCALR_USER, self.scalr_password)
            
            if self.shard_index == 0 and self.rs_id == 0:
                self.mongodb.start_config_server()

            if self.rs_id in (0,1):
                self.mongodb.router_cli.auth(mongo_svc.SCALR_USER, self.scalr_password)
                self.mongodb.configsrv_cli.auth(mongo_svc.SCALR_USER, self.scalr_password)
                self.mongodb.start_router(1)


    def on_reload(self):
        self._queryenv = bus.queryenv_service
        self._platform = bus.platform
        self._cnf = bus.cnf
        self._role_name = __node__['role_name']
        self._storage_path = mongo_svc.STORAGE_PATH
        self._tmp_dir = os.path.join(self._storage_path, STORAGE_TMP_DIR)
        
        self.mongodb = mongo_svc.MongoDB()
        self.mongodb.disable_requiretty()
        key_path = self._cnf.key_path(BEHAVIOUR)
        self.mongodb.keyfile = mongo_svc.KeyFile(key_path)
        

    def on_host_init_response(self, message):
        """
        Check MongoDB data in host init response
        @type message: scalarizr.messaging.Message
        @param message: HostInitResponse
        """
        with bus.initialization_op as op:

            with op.phase(self._phase_mongodb):
                with op.step(self._step_accept_scalr_conf):
                    if not message.body.has_key(BEHAVIOUR):
                        raise HandlerError("HostInitResponse message for %s behaviour must have '%s' property " 
                                                        % (BEHAVIOUR, BEHAVIOUR))

                    mongodb_data = message.mongodb.copy()

                    if 'volume_config' in mongodb_data:
                        __mongodb__['volume'] = storage2.volume(mongodb_data.pop('volume_config'))

                    if 'snapshot_config' in mongodb_data:
                        __mongodb__['snapshot'] = storage2.snapshot(mongodb_data.pop('snapshot_config'))

                    mongodb_key = mongodb_data.pop('keyfile', None)
                    mongodb_key = mongodb_key or cryptotool.pwgen(22)
                    self._cnf.write_key(BEHAVIOUR, mongodb_key)
                    
                    mongodb_data['password'] = mongodb_data.get('password') or cryptotool.pwgen(10)

                    if 'mms' in mongodb_data and not mongodb_data['mms']:
                        del mongodb_data['mms']

                    self._logger.debug("Update %s config with %s", (BEHAVIOUR, mongodb_data))
                    __mongodb__.update(mongodb_data)


    def on_before_host_up(self, hostup_msg):
        """
        Check that replication is up in both main and subordinate cases
        @type hostup_msg: scalarizr.messaging.Message
        @param hostup_msg: HostUp message
        """
        with bus.initialization_op as op:
            with op.phase(self._phase_mongodb):
                with op.step(self._step_check_cfg_server):

                    first_in_rs = True
                    cfg_server_running = False
    
                    local_ip = self._platform.get_private_ip()
                    role_hosts = self._queryenv.list_roles(behaviour=BEHAVIOUR, with_init=True)[0].hosts
    
                    for host in role_hosts:
                        if host.internal_ip == local_ip:
                            continue
                        hostname = HOSTNAME_TPL % (host.shard_index, host.replica_set_index)
                        Hosts.set(host.internal_ip, hostname)
                        if host.shard_index == self.shard_index :
                            first_in_rs = False

                        if host.shard_index == 0 and host.replica_set_index == 0:
                            if host.status == "Running":
                                cfg_server_running = True

                with op.step(self._step_change_hostname):
                    """ Set hostname"""
                    self._logger.info('Setting new hostname: %s' % self.hostname)
                    Hosts.set(local_ip, self.hostname)
                    with open('/etc/hostname', 'w') as f:
                        f.write(self.hostname)
                    system2(('hostname', '-F', '/etc/hostname'))

                rs_name = RS_NAME_TPL % self.shard_index
                
                if first_in_rs:
                    with op.step(self._step_init_main):
                        self._init_main(hostup_msg, rs_name)
                else:
                    with op.step(self._step_init_subordinate):
                        self._init_subordinate(hostup_msg, rs_name)

                possible_self_arbiter = "%s:%s" % (self.hostname, mongo_svc.ARBITER_DEFAULT_PORT)
                if possible_self_arbiter in self.mongodb.arbiters:
                    with op.step(self._step_start_arbiter):
                        self.mongodb.start_arbiter()

                if self.shard_index == 0 and self.rs_id == 0:
                    with op.step(self._step_start_cfg_server):
                        self.mongodb.start_config_server()

                        # Enable MMS Agent
                        if 'mms' in __mongodb__:
                            self.api.enable_mms(__mongodb__['mms']['api_key'], __mongodb__['mms']['secret_key'])

                    hostup_msg.mongodb['config_server'] = 1
                else:
                    hostup_msg.mongodb['config_server'] = 0


        if self.rs_id in (0,1):
            if not cfg_server_running:

                class Status:
                    def __init__(self, is_ready=False, is_notified=False, ip_addr=None):
                        self.is_ready = is_ready
                        self.is_notified = is_notified
                        self.ip_addr = ip_addr

                wait_for_config_server = False

                if self.rs_id == 0:
                    with op.step(self._step_enter_rs0_barrier):
                        self.mongodb.mongod.restart(reason="Workaround, authentication bug in mongo"
                                                                                        "(see https://jira.mongodb.org/browse/SERVER-4238)")

                        wait_for_int_hostups = True
                        shards_total = int(__mongodb__['shards_total'])
                        """ Status table = {server_id : {is_ready, is_notified, ip_addr}, ...} """
                        status_table = {}

                        """ Fill status table """
                        for i in range(shards_total):
                            if i == self.shard_index:
                                continue
                            status_table[i] = Status(False, False, None)

                        for host in role_hosts:
                            """ Skip ourself """
                            if host.shard_index == self.shard_index and \
                                                            host.replica_set_index == self.rs_id:
                                continue

                            """ Check if it's really cluster initialization, or configserver just failed """
                            #if host.replica_set_index != 0 or host.status == "Running":
                            if host.status == "Running":
                                """ Already have replicas """
                                wait_for_int_hostups = False
                                if self.shard_index != 0:
                                    wait_for_config_server = True
                                break

                            if host.shard_index > (shards_total - 1):
                                """ WTF? Artifact server from unknown shard. Just skip it """
                                continue

                            status_table[host.shard_index] = Status(False, False, host.internal_ip)

                        if wait_for_int_hostups:
                            int_before_hostup_msg_body = dict(shard_index=self.shard_index,
                                    replica_set_index=self.rs_id)
                            msg_store = P2pMessageStore()
                            local_handled_msg_ids = []

                            while True:
                                if not status_table:
                                    break

                                """ Inform unnotified servers """
                                for host_status in filter(lambda h: not h.is_notified, status_table.values()):
                                    if host_status.ip_addr:
                                        try:
                                            self.send_int_message(host_status.ip_addr,
                                                    MongoDBMessages.INT_BEFORE_HOST_UP,
                                                    int_before_hostup_msg_body)

                                            host_status.is_notified = True
                                        except:
                                            self._logger.warning('%s' % sys.exc_info()[1])

                                """ Handle all HostInits and HostDowns """
                                msg_queue_pairs = msg_store.get_unhandled('http://0.0.0.0:8013')
                                messages = [pair[1] for pair in msg_queue_pairs]
                                for message in messages:

                                    if message.name not in (Messages.HOST_INIT, Messages.HOST_DOWN):
                                        continue

                                    if message.id in local_handled_msg_ids:
                                        continue

                                    node_shard_id = int(message.mongodb['shard_index'])
                                    node_rs_id = int(message.mongodb['replica_set_index'])

                                    if node_shard_id == self.shard_index:
                                        continue

                                    if message.name == Messages.HOST_INIT:
                                        """ Updating hostname in /etc/hosts """
                                        self.on_HostInit(message)
                                        if node_rs_id == 0:
                                            status_table[node_shard_id] = Status(False, False, message.local_ip)

                                    elif message.name == Messages.HOST_DOWN:
                                        if node_rs_id == 0:
                                            status_table[node_shard_id] = Status(False, False, None)

                                    local_handled_msg_ids.append(message.id)


                                """ Handle all IntBeforeHostUp messages """
                                msg_queue_pairs = msg_store.get_unhandled('http://0.0.0.0:8012')
                                messages = [pair[1] for pair in msg_queue_pairs]

                                for message in messages:
                                    if message.name == MongoDBMessages.INT_BEFORE_HOST_UP:
                                        try:
                                            node_shard_id = int(message.shard_index)
                                            node_status = status_table[node_shard_id]
                                            status_table[node_shard_id] = Status(True, True, node_status.ip_addr)

                                        finally:
                                            msg_store.mark_as_handled(message.id)

                                if all([status.is_ready for status in status_table.values()]):
                                    """ Everybody is ready """
                                    break

                                """ Sleep for a while """
                                time.sleep(10)

                else:
                    wait_for_config_server = True

                if wait_for_config_server:
                    self._logger.info('Waiting until mongo config server on mongo-0-0 becomes alive')
                    with op.step(self._step_wait_cfg_server):
                        while not cfg_server_running:
                            try:
                                time.sleep(20)
                                role_hosts = self._get_cluster_hosts()
                                for host in role_hosts:
                                    if host.shard_index == 0 and host.replica_set_index == 0:
                                        cfg_server_running = True
                                        break
                            except:
                                self._logger.debug('Caught exception', exc_info=sys.exc_info())


            with op.step(self._step_start_router):
                self.mongodb.start_router(1)
                hostup_msg.mongodb['router'] = 1

            if self.rs_id == 0 and self.shard_index == 0:
                with op.step(self._step_create_scalr_users):
                    try:
                        self.mongodb.router_cli.create_or_update_admin_user(mongo_svc.SCALR_USER, self.scalr_password)
                    except pymongo.errors.OperationFailure, err:
                        if 'unauthorized' in str(err) or 'not authorized' in str(err):
                            self._logger.warning(err)
                        else:
                            raise

                    except BaseException, e:
                        self._logger.error(e)

                        
            with op.step(self._step_auth_on_cfg_server_and_router):
                self.mongodb.router_cli.auth(mongo_svc.SCALR_USER, self.scalr_password)
                self.mongodb.configsrv_cli.auth(mongo_svc.SCALR_USER, self.scalr_password)
                
            with op.step(self._step_create_shard):
                self.create_shard()

        else:
            hostup_msg.mongodb['router'] = 0
    
        STATE[CLUSTER_STATE_KEY] = MongoDBClusterStates.RUNNING

        hostup_msg.mongodb['keyfile'] = self._cnf.read_key(BEHAVIOUR)
        hostup_msg.mongodb['password'] = self.scalr_password
        
        repl = 'primary' if first_in_rs else 'secondary'
        bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)


    def on_MongoDb_IntCreateBootstrapWatcher(self, message):
        self._stop_watcher(message.local_ip)

        if message.local_ip != self._platform.get_private_ip():

            shard_idx = int(message.mongodb['shard_index'])
            rs_idx = int(message.mongodb['replica_set_index'])

            hostname = HOSTNAME_TPL % (shard_idx, rs_idx)

            self._logger.debug('Adding %s as %s to hosts file', message.local_ip, hostname)
            Hosts.set(message.local_ip, hostname)

            wait_until(lambda: self.mongodb.primary_host, timeout=180,
                    start_text='Wait for primary node in replica set', logger=self._logger)

            is_main = self.mongodb.is_replication_main

            if is_main and self.shard_index == shard_idx:

                nodename = '%s:%s' % (hostname, mongo_svc.REPLICA_DEFAULT_PORT)
                if nodename not in self.mongodb.replicas:
                    self.mongodb.register_subordinate(hostname, mongo_svc.REPLICA_DEFAULT_PORT)
                else:
                    self._logger.warning('Host %s is already in replica set.' % nodename)

                watcher = StatusWatcher(hostname, self, message.local_ip)
                self._logger.info('Starting bootstrap watcher for node ip=%s', message.local_ip)
                watcher.start()
                self._status_trackers[message.local_ip] = watcher

                
    def create_shard(self):
        shard_index = self.shard_index
        shard_name = SHARD_NAME_TPL % shard_index
        shard_names = [s['_id'] for s in self.mongodb.router_cli.list_shards()]

        if shard_name in shard_names:
            self._logger.debug('Shard %s already exists.', shard_name)
            return

        self._logger.info('Initializing shard %s' % shard_name)
        rs_name = RS_NAME_TPL % shard_index
        self.mongodb.router_cli.add_shard(shard_name, rs_name, self.mongodb.replicas)


    def on_HostInit(self, message):
        if BuiltinBehaviours.MONGODB not in message.behaviour:
            return

        if message.local_ip != self._platform.get_private_ip():
        
            shard_idx = int(message.mongodb['shard_index'])
            rs_idx = int(message.mongodb['replica_set_index'])
            hostname = HOSTNAME_TPL % (shard_idx, rs_idx)
            
            self._logger.debug('Adding %s as %s to hosts file', message.local_ip, hostname)
            Hosts.set(message.local_ip, hostname)


    def _on_new_host_mms_configure(self, new_host_shard_id, new_host_rs_id):
        """
        Configure MMS on new host up

        :type new_host_shard_id: int
        :param new_host_shard_id: New server shard index

        :type new_host_rs_id: int
        :param new_host_rs_id: New server replica set index
        """

        if self.shard_index == 0 and self.rs_id == 0:
            up_node_host = HOSTNAME_TPL % (new_host_shard_id, new_host_rs_id)
            self._add_host_to_mms(up_node_host, mongo_svc.REPLICA_DEFAULT_PORT)
            if new_host_rs_id < 2:
                self._add_host_to_mms(up_node_host, mongo_svc.ROUTER_DEFAULT_PORT)
            if new_host_shard_id == 0 and new_host_rs_id == 0:
                self._add_host_to_mms(up_node_host, mongo_svc.CONFIG_SERVER_DEFAULT_PORT)
            if len(self._get_shard_hosts(new_host_shard_id)) % 2 == 0:
                self._add_host_to_mms(HOSTNAME_TPL % (new_host_shard_id, 0), mongo_svc.ARBITER_DEFAULT_PORT)
            else:
                self._delete_host_from_mms(HOSTNAME_TPL % (new_host_shard_id, 0), mongo_svc.ARBITER_DEFAULT_PORT)


    def on_HostUp(self, message):
        if BuiltinBehaviours.MONGODB not in message.behaviour:
            return

        new_host_shard_idx = int(message.mongodb['shard_index'])
        new_host_rs_id = int(message.mongodb['replica_set_index'])

        private_ip = self._platform.get_private_ip()
        if message.local_ip != private_ip:
            """ If mongos runs on this instance """

            if self.rs_id == 0 and self.shard_index == 0:
                self.update_shard()

            if self.rs_id in (0,1):
                """ Restart router if hostup sent from configserver node """
                if new_host_shard_idx == 0 and new_host_rs_id == 0:
                    self.mongodb.restart_router()
                self._logger.debug('Flushing router configuration')
                self.mongodb.router_cli.flush_router_cfg()
        
            if self.mongodb.is_replication_main and \
                                                                            self.shard_index == new_host_shard_idx:                    
                r = len(self.mongodb.replicas) 
                a = len(self.mongodb.arbiters)
                if r % 2 == 0 and not a:
                    self.mongodb.start_arbiter()
                    self.mongodb.register_arbiter(self.hostname)
                elif r % 2 != 0 and a:
                    for arbiter in self.mongodb.arbiters:
                        arb_host, arb_port = arbiter.split(':')
                        arb_port = int(arb_port)
                        self.mongodb.unregister_subordinate(arb_host, arb_port)
                    self.mongodb.stop_arbiter()
            else:
                if len(self.mongodb.replicas) % 2 != 0:
                    self.mongodb.stop_arbiter()

        if 'mms' in __mongodb__:
            self._on_new_host_mms_configure(new_host_shard_idx, new_host_rs_id)


    def update_shard(self):
        config = self.generate_cluster_config()
        self._logger.debug('Replacing sharding config with %s' % config)
        #self.mongodb.router_cli.connection.config.shards.remove()
        for shard in config:
            #self.mongodb.router_cli.connection.config.shards.update(shard)
            self.mongodb.router_cli.connection.config.shards.update(
                            {'_id' : shard['_id']}, {'host' :  shard['host']})


    def generate_cluster_config(self):
        shards = {}
        for host in self._get_cluster_hosts():
            hostname = HOSTNAME_TPL % (host.shard_index, host.replica_set_index)
            if host.shard_index in shards:
                shards[host.shard_index].append(hostname)
            else:
                shards[host.shard_index] = [hostname]

        info = []
        for shard_index, hostnames in shards.items():
            rset = RS_NAME_TPL % shard_index
            rset += '/'
            for hostname in hostnames:
                rset += '%s:%s,' % (hostname, mongo_svc.REPLICA_DEFAULT_PORT)
            if rset.endswith(','):
                rset = rset[:-1]
            info.append({'_id': SHARD_NAME_TPL % shard_index, 'host' : rset})

        return info


    def on_HostDown(self, message):
        if not BuiltinBehaviours.MONGODB in message.behaviour:
            return

        if message.local_ip in self._status_trackers:
            t = self._status_trackers[message.local_ip]
            t.stop()
            del self._status_trackers[message.local_ip]

        if STATE[CLUSTER_STATE_KEY] == MongoDBClusterStates.TERMINATING:
            return

        try:
            shard_idx = int(message.mongodb['shard_index'])
            rs_idx = int(message.mongodb['replica_set_index'])
        except:
            self._logger.debug('Received malformed HostDown message.')
            return

        down_node_host = HOSTNAME_TPL % (shard_idx, rs_idx)
        down_node_name = '%s:%s' % (down_node_host, mongo_svc.REPLICA_DEFAULT_PORT)
        
        if down_node_name not in self.mongodb.replicas:
            return
    
        replica_ip = Hosts.hosts().get(down_node_host)

        if not replica_ip or replica_ip != message.local_ip:
            self._logger.debug("Got %s from node %s but ip address doesn't match.", message.name, down_node_host)
            return
    
        is_main = self.mongodb.is_replication_main
        
        if not is_main and len(self.mongodb.replicas) == 2:
            local_ip = self._platform.get_private_ip()
            possible_self_arbiter = "%s:%s" % (local_ip, mongo_svc.ARBITER_DEFAULT_PORT)
            try:
                if possible_self_arbiter in self.mongodb.arbiters:
                    """ Start arbiter if it's not running """
                    self.mongodb.arbiter.start()
                    """ Wait until we become main """
                    wait_until(lambda: self.mongodb.is_replication_main, timeout=180)
                else:
                    raise Exception('Arbiter not found')
            except:
                """ Become primary and only member of rs """
                nodename = '%s:%s' % (self.hostname, mongo_svc.REPLICA_DEFAULT_PORT)
                rs_cfg = self.mongodb.cli.get_rs_config()
                rs_cfg['members'] = [m for m in rs_cfg['members'] if m['host'] == nodename]
                self.mongodb.cli.rs_reconfig(rs_cfg, force=True)
                try:
                    wait_until(lambda: self.mongodb.is_replication_main, timeout=30)
                except:
                    """ Looks like mongo stuck in secondary state (syncingTo dead node)
                            Restart should fix this
                    """
                    if "seconds reached" in str(sys.exc_info()[1]):
                        self.mongodb.mongod.restart(reason="Reconfiguring replica set")
                        wait_until(lambda: self.mongodb.is_replication_main, timeout=30)
                    else:
                        raise
        else:
            wait_until(lambda: self.mongodb.primary_host, timeout=180,
                             start_text='Wait for primary node in replica set', logger=self._logger)


            if self.mongodb.is_replication_main:
            
                """ Remove host from replica set"""
                self.mongodb.unregister_subordinate(down_node_host)
                
                """ If arbiter was running on the node - unregister it """
                possible_arbiter = "%s:%s" % (down_node_host, mongo_svc.ARBITER_DEFAULT_PORT)
                if possible_arbiter in self.mongodb.arbiters:
                    self.mongodb.unregister_subordinate(down_node_host, mongo_svc.ARBITER_DEFAULT_PORT)
                    
                """ Start arbiter if necessary """
                if len(self.mongodb.replicas) % 2 == 0:
                    self.mongodb.start_arbiter()
                    self.mongodb.register_arbiter(self.hostname)
                else:
                    for arbiter in self.mongodb.arbiters:
                        arb_host, arb_port = arbiter.split(':')
                        arb_port = int(arb_port)
                        self.mongodb.unregister_subordinate(arb_host, arb_port)
                    self.mongodb.stop_arbiter()
                    
            else:
                """ Get all replicas except down one, 
                        since we don't know if main already removed
                        node from replica set 
                """
                replicas = [r for r in self.mongodb.replicas if r != down_node_name]
                if len(replicas) % 2 != 0:
                    self.mongodb.stop_arbiter()

        if self.rs_id == 0 and self.shard_index == 0:
            self.update_shard()

                            
    def on_before_reboot_start(self, *args, **kwargs):
        self.mongodb.stop_arbiter()
        self.mongodb.stop_router()
        self.mongodb.stop_config_server()
        self.mongodb.mongod.stop('Rebooting instance')

                
    def _on_host_terminate_mms_configure(self, down_host_shard_id, down_host_rs_id):
        """
        Configure MMS on host terminate

        :type down_host_shard_id: int
        :param down_host_shard_id: Terminated server shard index

        :type down_host_rs_id: int
        :param down_host_rs_id: Terminated server replica set index
        """

        if self.shard_index == 0 and self.rs_id == 0:
            down_node_host = HOSTNAME_TPL % (down_host_shard_id, down_host_rs_id)
            self._delete_host_from_mms(down_node_host, mongo_svc.REPLICA_DEFAULT_PORT)
            if down_host_rs_id < 2:
                self._delete_host_from_mms(down_node_host, mongo_svc.ROUTER_DEFAULT_PORT)
            if down_host_shard_id == 0 and down_host_rs_id == 0:
                self._delete_host_from_mms(down_node_host, self.mongodb.arbiter.port)
            if STATE[CLUSTER_STATE_KEY] != MongoDBClusterStates.TERMINATING\
                    and len(self._get_shard_hosts(down_host_shard_id)) % 2 == 0:
                self._add_host_to_mms(HOSTNAME_TPL % (down_host_shard_id, 0), mongo_svc.ARBITER_DEFAULT_PORT)
            else:
                self._delete_host_from_mms(HOSTNAME_TPL % (down_host_shard_id, 0), mongo_svc.ARBITER_DEFAULT_PORT)

            
    def on_BeforeHostTerminate(self, message):
        if 'mms' in __mongodb__:
            down_host_shard_id = int(message.mongodb['shard_index'])
            down_host_rs_id = int(message.mongodb['replica_set_index'])
            self._on_host_terminate_mms_configure(down_host_shard_id, down_host_rs_id)

        if STATE[CLUSTER_STATE_KEY] == MongoDBClusterStates.TERMINATING:
            return

        if not BuiltinBehaviours.MONGODB in message.behaviour:
            return

        if message.local_ip == self._platform.get_private_ip():
            STATE[CLUSTER_STATE_KEY] = MongoDBClusterStates.TERMINATING
            storage_vol = __mongodb__['volume']

            if self.mongodb.is_replication_main:
                self.mongodb.cli.step_down(180, force=True)
            self.mongodb.stop_arbiter()
            self.mongodb.stop_config_server()
            self.mongodb.mongod.stop('Server will be terminated')   
            self._logger.info('Detaching %s storage' % BEHAVIOUR)
            storage_vol.detach()
            if STATE[REMOVE_VOLUME_KEY]:
                self._logger.info("Destroying storage")
                storage_vol.destroy()

            if self._cnf.state == ScalarizrState.INITIALIZING:
                if storage_vol.id and STATE[MONGO_VOLUME_CREATED] == storage_vol.id:
                    storage_vol.destroy(remove_disks=True)

        else:
            shard_idx = int(message.mongodb['shard_index'])
            rs_idx = int(message.mongodb['replica_set_index'])

            down_node_host = HOSTNAME_TPL % (shard_idx, rs_idx)
            down_node_name = '%s:%s' % (down_node_host, mongo_svc.REPLICA_DEFAULT_PORT)
            down_possible_arbiter = '%s:%s' % (down_node_host, mongo_svc.ARBITER_DEFAULT_PORT)

            if down_node_name not in self.mongodb.replicas:
                return

            replica_ip = Hosts.hosts().get(down_node_host)

            if not replica_ip or replica_ip != message.local_ip:
                self._logger.debug("Got %s from node %s but ip address doesn't match.", message.name, down_node_host)
                return

            def node_terminated_or_deleted(node_name):
                for node in self.mongodb.cli.get_rs_status()['members']:
                    if node['name'] == node_name:
                        if int(node['health']) == 0:
                            return True
                        else:
                            return False
                else:
                    return True

            self._logger.debug('Wait until node is down or removed from replica set')
            wait_until(node_terminated_or_deleted,  args=(down_node_name,), logger=self._logger, timeout=180)

            if down_possible_arbiter in self.mongodb.arbiters:
                self._logger.debug('Wait until arbiter is down or removed from replica set')
                wait_until(node_terminated_or_deleted, args=(down_possible_arbiter,), logger=self._logger, timeout=180)

            self.on_HostDown(message)

            
    def on_MongoDb_IntCreateDataBundle(self, message):
        try:
            msg_data = self._create_data_bundle()
        except:
            self._logger.exception('Failed to create data bundle')
            msg_data = {'status': 'error', 'last_error': str(sys.exc_info()[1])}
        self.send_int_message(message.local_ip, 
                                MongoDBMessages.INT_CREATE_DATA_BUNDLE_RESULT,
                                msg_data)
                

    def on_MongoDb_CreateDataBundle(self, message):
        try:
            self.send_message(
                            MongoDBMessages.CREATE_DATA_BUNDLE_RESULT, 
                            self._create_data_bundle())
        except:
            self.send_result_error_message(
                            MongoDBMessages.CREATE_DATA_BUNDLE_RESULT, 
                            'Failed to create data bundle')
                            
            

    def _create_data_bundle(self):
        if not self.mongodb.is_replication_main:
            self._logger.debug('Not a main. Skipping data bundle')
            return
    
        try:
            op = operation(name=self._op_data_bundle, phases=[{
                    'name': self._phase_data_bundle, 
                    'steps': [
                            self._step_stop_balancer, 
                            self._step_fsync, 
                            self._step_create_snapshot
                    ]
            }])
            op.define()
            
            with op.phase(self._phase_data_bundle):
                with op.step(self._step_stop_balancer):
                    bus.fire('before_%s_data_bundle' % BEHAVIOUR)
                    self.mongodb.router_cli.stop_balancer()
                    
                with op.step(self._step_fsync):
                    self.mongodb.cli.sync(lock=True)
                    
                with op.step(self._step_create_snapshot):
                    try:
                    
                        # Creating snapshot
                        snap = self._create_snapshot()
                        used_size = int(system2(('df', '-P', '--block-size=M', self._storage_path))[0].split('\n')[1].split()[2][:-1])
                        bus.fire('%s_data_bundle' % BEHAVIOUR, snapshot_id=snap.id)

                        # Notify scalr
                        msg_data = dict(
                                used_size       = '%.3f' % (float(used_size) / 1000,),
                                status          = 'ok'
                        )
                        msg_data[BEHAVIOUR] = self._compat_storage_data(snap=snap)
                        return msg_data
                    finally:
                        self.mongodb.cli.unlock()

        finally:
            self.mongodb.router_cli.start_balancer()
    
    """
    def on_MongoDb_CreateBackup(self, message):
            if not self.mongodb.is_replication_main:
                    self._logger.debug('Not a main. Skipping backup process')
                    return 
            
            tmpdir = backup_path = None
            try:
                    op = operation(name=self._op_backup, phases=[{
                            'name': self._phase_backup, 
                            'steps': [self._step_fsync] + 
                                            ["Backup '%s'" % db for db in self.mongodb.router_cli.list_database_names()] + 
                                            [self._step_upload_to_cloud_storage]
                    }])
                    op.define()                     
                    
                    with op.phase(self._phase_backup):
                    
                            #turn balancer off
                            self.mongodb.router_cli.stop_balancer()
                    
                            with op.step(self._step_fsync):
                                    #perform fsync
                                    self.mongodb.cli.sync()
                            
                            #create temporary dir for dumps
                            if not os.path.exists(self._tmp_dir):
                                    os.makedirs(self._tmp_dir)
                            tmpdir = tempfile.mkdtemp(self._tmp_dir)                
                            chown_r(tmpdir, mongo_svc.DEFAULT_USER)
    
                            #dump config db on router
                            r_dbs = self.mongodb.router_cli.list_database_names()
                            rdb_name = 'config'
                            if rdb_name  in r_dbs:
                                    with op.step("Backup '%s'" % rdb_name, warning=True):
                                            private_ip = self._platform.get_private_ip()
                                            router_port = mongo_svc.ROUTER_DEFAULT_PORT
                                            router_dump = mongo_svc.MongoDump(private_ip, router_port)
                                            router_dump_path = tmpdir + os.sep + 'router_' + rdb_name + '.bson'
                                            err = router_dump.create(rdb_name, router_dump_path)
                                            if err:
                                                    raise HandlerError('Error while dumping database %s: %s' % (rdb_name, err))
                            else:
                                    self._logger.warning('config db not found. Nothing to dump on router.')
                    
                            # Get databases list
                            dbs = self.mongodb.cli.list_database_names()
                            
                            # Defining archive name and path
                            rs_name = RS_NAME_TPL % self.shard_index
                            backup_filename = time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
                            backup_path = os.path.join(self._tmp_dir, backup_filename)
                            
                            # Creating archive 
                            backup = tarfile.open(backup_path, 'w:gz')
                            
                            # Dump all databases
                            self._logger.info("Dumping all databases")
                            md = mongo_svc.MongoDump()  
                            
                            for db_name in dbs:
                                    try:
                                            with ("Backup '%s'" % db_name):
                                                    dump_path = tmpdir + os.sep + db_name + '.bson'
                                                    err = md.create(db_name, dump_path)[1]
                                                    if err:
                                                            raise HandlerError('Error while dumping database %s: %s' % (db_name, err))
                                                    backup.add(dump_path, os.path.basename(dump_path))
                                    except:
                                            self._logger.exception('Cannot dump database %s', db_name)
                            backup.close()
                            
                            with op.step(self._step_upload_to_cloud_storage):
                                    # Creating list of full paths to archive chunks
                                    if os.path.getsize(backup_path) > BACKUP_CHUNK_SIZE:
                                            parts = [os.path.join(tmpdir, file) for file in split(backup_path, backup_filename, BACKUP_CHUNK_SIZE , tmpdir)]
                                    else:
                                            parts = [backup_path]
                                    sizes = [os.path.getsize(file) for file in parts]
                                                    
                                    cloud_storage_path = self._platform.scalrfs.backups(BEHAVIOUR)
                                    self._logger.info("Uploading backup to cloud storage (%s)", cloud_storage_path)
                                    trn = transfer.Transfer()
                                    cloud_files = trn.upload(parts, cloud_storage_path)
                                    self._logger.info("%s backup uploaded to cloud storage under %s/%s", 
                                                                    BEHAVIOUR, cloud_storage_path, backup_filename)
                    
                    result = list(dict(path=path, size=size) for path, size in zip(cloud_files, sizes))
                    op.ok(data=result)
                    
                    # Notify Scalr
                    self.send_message(MongoDBMessages.CREATE_BACKUP_RESULT, dict(
                            status = 'ok',
                            backup_parts = result
                    ))
                                            
            except:
                    self.send_result_error_message(MongoDBMessages.CREATE_BACKUP_RESULT, 'Failed to create backup')
                    
            finally:
                    if tmpdir:
                            shutil.rmtree(tmpdir, ignore_errors=True)
                    if backup_path and os.path.exists(backup_path):
                            os.remove(backup_path)
                    self.mongodb.router_cli.start_balancer()
                            
    """


    def _init_main(self, message, rs_name):
        """
        Initialize mongodb main
        @type message: scalarizr.messaging.Message 
        @param message: HostUp message
        """
        
        self._logger.info("Initializing %s primary" % BEHAVIOUR)
        self.plug_storage()
        self.mongodb.prepare(rs_name)

        #Fix for mongo 2.2 auth, see https://jira.mongodb.org/browse/SERVER-6591
        self._logger.info("Starting mongod instance without --auth to add the first superuser")
        self.mongodb.auth = False
        self.mongodb.start_shardsvr()

        """ Check if replset already exists """
        if not list(self.mongodb.cli.connection.local.system.replset.find()):
            self.mongodb.initiate_rs()
        else:
            self._logger.info("Previous replica set configuration found. Changing members list.")
            nodename = '%s:%s' % (self.hostname, mongo_svc.REPLICA_DEFAULT_PORT)
            
            rs_cfg = self.mongodb.cli.get_rs_config()
            rs_cfg['members'] = filter(lambda n: n['host'] == nodename, rs_cfg['members'])

            if not rs_cfg['members']:
                rs_cfg['members'] = [{'_id' : 0, 'host': nodename}]

            rs_cfg['version'] += 10
            self.mongodb.cli.rs_reconfig(rs_cfg, force=True)
            wait_until(lambda: self.mongodb.is_replication_main, timeout=180)

        self.mongodb.cli.create_or_update_admin_user(mongo_svc.SCALR_USER, self.scalr_password)
        self.mongodb.mongod.stop("Terminating mongod instance to run it with --auth option")
        self.mongodb.auth = True
        self.mongodb.start_shardsvr()
        self.mongodb.cli.auth(mongo_svc.SCALR_USER, self.scalr_password)
        wait_until(lambda: self.mongodb.is_replication_main, sleep=5, logger=self._logger,
                                                timeout=120, start_text='Wait until node becomes replication primary')
        # Create snapshot
        self.mongodb.cli.sync(lock=True)
        try:
            snap = self._create_snapshot()
        finally:
            self.mongodb.cli.unlock()

        __mongodb__['snapshot'] = snap

        # Update HostInitResponse message 
        msg_data = self._compat_storage_data(__mongodb__['volume'], snap)
        message.mongodb = msg_data.copy()


    def _get_shard_hosts(self, shard_index):
        hosts = self._get_cluster_hosts()
        return [host for host in hosts if host.shard_index == shard_index]


    def _get_cluster_hosts(self):
        return self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts

    @property
    def mongo_tags(self):
        return prepare_tags(BEHAVIOUR)


    def plug_storage(self):
        storage_snap = None
        try:
            storage_volume = __mongodb__['volume']
        except:
            storage_snap = __mongodb__['snapshot']
            storage_volume = storage2.volume(type=storage_snap.type, snap=storage_snap)

        storage_volume.mpoint = self._storage_path
        storage_volume.ensure(mount=True, mkfs=True)

        if storage_snap is not None:
            STATE[MONGO_VOLUME_CREATED] = storage_volume.id

        __mongodb__['volume'] = storage_volume


    def _init_subordinate(self, message, rs_name):
        """
        Initialize mongodb subordinate
        @type message: scalarizr.messaging.Message 
        @param message: HostUp message
        """
        
        msg_store = P2pMessageStore()
        
        def request_and_wait_replication_status():
                
            self._logger.info('Notifying primary node about joining replica set')

            msg_body = dict(mongodb=dict(shard_index=self.shard_index,
                                            replica_set_index=self.rs_id))
            for host in self._get_shard_hosts(self.shard_index):
                self.send_int_message(host.internal_ip,
                                                MongoDBMessages.INT_CREATE_BOOTSTRAP_WATCHER,
                                                msg_body, broadcast=True)
        
            self._logger.info('Waiting for status message from primary node')
            initialized = stale = False     

            my_nodename = '%s:%s' % (self.hostname, mongo_svc.REPLICA_DEFAULT_PORT)

            while not initialized and not stale:

                """ Trying to check replication status from this node
                                If we're already primary or secondary - no need
                                to wait watcher result from primary """

                try:
                    rs_status = self.mongodb.cli.get_rs_status()

                    for member in rs_status['members']:
                        if member['name'] == my_nodename:
                            status = member['state']
                            if status in (1,2):
                                initialized = True
                                break
                except:
                    pass

                """ Check bootstrap result messages """

                msg_queue_pairs = msg_store.get_unhandled('http://0.0.0.0:8012')
                messages = [pair[1] for pair in msg_queue_pairs]
                for msg in messages:
                        
                    if not msg.name == MongoDBMessages.INT_BOOTSTRAP_WATCHER_RESULT:
                        continue                                                                                
                    try:
                        if msg.status == ReplicationState.INITIALIZED:
                            initialized = True
                            break
                        elif msg.status == ReplicationState.STALE:
                            stale = True
                            break                                                   
                        else:
                            raise HandlerError('Unknown state for replication state: %s' % msg.status)                                                                                                      
                    finally:
                        msg_store.mark_as_handled(msg.id)

                time.sleep(1)

            if initialized:
                self._logger.info('Mongo successfully joined replica set')

            return stale
    
        self._logger.info("Initializing %s secondary" % BEHAVIOUR)

        self.plug_storage()
        storage_vol = __mongodb__['volume']

        self.mongodb.stop_default_init_script()
        self.mongodb.prepare(rs_name)
        self.mongodb.start_shardsvr()

        
        first_start = not self._storage_valid()
        self.mongodb.auth = True

        if not first_start:
            self.mongodb.cli.auth(mongo_svc.SCALR_USER, self.scalr_password)
            self.mongodb.remove_replset_info()
            self.mongodb.mongod.stop('Cleaning replica set configuration')
            self.mongodb.start_shardsvr()

        stale = request_and_wait_replication_status()

        if stale:                       
            new_volume = None

            try:
                if PlatformFeatures.VOLUMES not in self._platform.features:
                    raise HandlerError('Platform does not support pluggable volumes')

                self._logger.info('Mongodb too stale to synchronize. Trying to get snapshot from primary')
                for host in self._get_shard_hosts(self.shard_index):
                    self.send_int_message(host.internal_ip,
                                    MongoDBMessages.INT_CREATE_DATA_BUNDLE,
                                    include_pad=True, broadcast=True)

                cdb_result_received = False
                while not cdb_result_received:
                    msg_queue_pairs = msg_store.get_unhandled('http://0.0.0.0:8012')
                    messages = [pair[1] for pair in msg_queue_pairs]
                    for msg in messages:
                        if not msg.name == MongoDBMessages.INT_CREATE_DATA_BUNDLE_RESULT:
                            continue

                        cdb_result_received = True
                        try:
                            if msg.status == 'ok':
                                self._logger.info('Received data bundle from main node.')
                                self.mongodb.mongod.stop()
                                
                                storage_vol.detach()
                                
                                snap_cnf = msg.mongodb.snapshot_config.copy()
                                new_volume = storage2.volume(type=snap_cnf['type'], mpoint=self._storage_path,
                                                                        snap=snap_cnf)
                                new_volume.ensure(mount=True)

                                self.mongodb.start_shardsvr()
                                stale = request_and_wait_replication_status()
                                
                                if stale:
                                    raise HandlerError('Got stale even when standing from snapshot.')
                                else:
                                    __mongodb__['volume'] = new_volume
                                    self.mongodb.cli.auth(mongo_svc.SCALR_USER, self.scalr_password)
                            else:
                                raise HandlerError('Data bundle failed.')
                                
                        finally:
                            msg_store.mark_as_handled(msg.id)
                                                                                    
                    time.sleep(1)
            except:
                self._logger.info('%s. Trying to perform clean sync' % sys.exc_info()[1] )
                if new_volume:
                    new_volume.destroy(force=True, remove_disks=True)
                storage_vol.ensure(mount=True)

                self._init_clean_sync()
                stale = request_and_wait_replication_status()
                if stale:
                    # TODO: raise distinct exception
                    raise HandlerError("Replication status is stale")
                self.mongodb.cli.auth(mongo_svc.SCALR_USER, self.scalr_password)

            else:
                storage_vol.destroy(force=True, remove_disks=True)

        else:
            self.mongodb.cli.auth(mongo_svc.SCALR_USER, self.scalr_password)

        __mongodb__['volume'] = storage_vol
        message.mongodb = self._compat_storage_data(storage_vol)
        

    def on_MongoDb_ClusterTerminate(self, message):
        try:
            STATE[CLUSTER_STATE_KEY] = MongoDBClusterStates.TERMINATING
            role_hosts = self._get_cluster_hosts()
            cluster_terminate_watcher = ClusterTerminateWatcher(role_hosts, self, int(message.timeout))
            cluster_terminate_watcher.start()
        except:
            self.send_result_error_message(MongoDBMessages.CLUSTER_TERMINATE_RESULT, 'Cluster terminate failed')
    
        # Delete mongodb-0-0 from MMS on terminate cluster
        if 'mms' in __mongodb__ and self.shard_index == 0 and self.rs_id == 0:
            self._delete_host_from_mms(self.hostname, mongo_svc.REPLICA_DEFAULT_PORT)
            self._delete_host_from_mms(self.hostname, mongo_svc.ROUTER_DEFAULT_PORT)
            self._delete_host_from_mms(self.hostname, mongo_svc.CONFIG_SERVER_DEFAULT_PORT)
        

    def on_MongoDb_IntClusterTerminate(self, message):
        try:
            STATE[CLUSTER_STATE_KEY] = MongoDBClusterStates.TERMINATING
            if not self.mongodb.mongod.is_running:
                self.mongodb.start_shardsvr()
            is_replication_main = self.mongodb.is_replication_main
            self.mongodb.mongod.stop()
            self.mongodb.stop_config_server()
            
            msg_body = dict(status='ok',
                                            shard_index=self.shard_index,
                                            replica_set_index=self.rs_id,
                                            is_main=int(is_replication_main))
        except:
            msg_body = dict(status='error',
                                            last_error=str(sys.exc_info()[1]),
                                            shard_index=self.shard_index,
                                            replica_set_index=self.rs_id)
                    
        finally:
            self.send_int_message(message.local_ip,
                            MongoDBMessages.INT_CLUSTER_TERMINATE_RESULT, msg_body)


    def on_MongoDb_RemoveShard(self, message):
        try:
            if not self.rs_id in (0,1):
                raise Exception('No router running on host')

            cluster_dbs = self.mongodb.router_cli.list_cluster_databases()
            exclude_unsharded = ('test', 'admin')
            """ Get all unpartitioned db names where we are primary """
            unsharded = self.get_unpartitioned_dbs(shard_name=self.shard_name)
            """ Exclude 'admin' and 'test' databases """
            unsharded = [db for db in unsharded if db not in exclude_unsharded]

            exclude_local = exclude_unsharded + ('local',)
            local_db_list = self.mongodb.cli.list_database_names()
            local_db_list = filter(lambda db: db not in exclude_local, local_db_list)
            local_db_list = filter(lambda db: db not in cluster_dbs, local_db_list)

            if unsharded:
                """ Send Scalr sad message with unsharded database list """
                raise Exception('You have %s unsharded databases in %s shard (%s)' % \
                                        (len(unsharded), self.shard_index, ', '.join(unsharded)))
            elif local_db_list:
                raise Exception('You have %s local databases in %s shard (%s)' %\
                                                (len(local_db_list), self.shard_index, ', '.join(local_db_list)))
            else:
                """ Start draining """
                watcher = DrainingWatcher(self)
                watcher.start()
        except:
            err_msg = sys.exc_info()[1]
            msg_body = dict(status='error', last_error=err_msg, shard_index=self.shard_index)
            self.send_message(MongoDBMessages.REMOVE_SHARD_RESULT, msg_body)


    def get_unpartitioned_dbs(self, shard_name=None):
        dbs = self.mongodb.router_cli.list_cluster_databases()
        dbs = filter(lambda db: db['partitioned'] == False, dbs)
        if shard_name:
            dbs = filter(lambda db: db['primary'] == shard_name, dbs)
        return [db['_id'] for db in dbs]
                        

    def _get_keyfile(self):
        try:
            return __mongodb__['keyfile']
        except:
            return None


    def _create_snapshot(self):
        system2('sync', shell=True)

        snap = self._create_storage_snapshot()
                
        wait_until(lambda: snap.status() in (snap.COMPLETED, snap.FAILED))
        if snap.status() == snap.FAILED:
            raise HandlerError('%s storage snapshot creation failed. See log for more details' % BEHAVIOUR)

        return snap


    def _create_storage_snapshot(self):
        #TODO: check mongod journal option if service is running!
        self._logger.info("Creating mongodb's storage snapshot")
        try:
            return __mongodb__['volume'].snapshot(tags=self.mongo_tags, nowait=True)
        except storage2.StorageError, e:
            self._logger.error("Cannot create %s data snapshot. %s", (BEHAVIOUR, e))
            raise
    

    def _compat_storage_data(self, vol=None, snap=None):
        ret = dict()
        if vol:
            ret['volume_config'] = vol.config()
        if snap:
            ret['snapshot_config'] = snap.config()
        return ret      


    def _storage_valid(self):
        if os.path.isdir(mongo_svc.STORAGE_DATA_DIR):
            return True
        return False


    def _init_clean_sync(self):
        self._logger.info('Trying to perform clean resync from cluster members')
        """ Stop mongo, delete all mongodb datadir content and start mongo"""
        self.mongodb.mongod.stop()
        for root, dirs, files in os.walk(mongo_svc.STORAGE_DATA_DIR):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))
        self.mongodb.start_shardsvr()   
                        

    def _stop_watcher(self, ip):
        if ip in self._status_trackers:
            self._logger.debug('Stopping bootstrap watcher for ip %s', ip)
            t = self._status_trackers[ip]
            t.stop()
            del self._status_trackers[ip]
    
    def _insert_iptables_rules(self, *args, **kwargs):
        self._logger.debug('Adding iptables rules for scalarizr ports')

        if iptables.enabled():
            iptables.FIREWALL.ensure([
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(mongo_svc.ROUTER_DEFAULT_PORT)},
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(mongo_svc.ARBITER_DEFAULT_PORT)},
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(mongo_svc.REPLICA_DEFAULT_PORT)},
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(mongo_svc.CONFIG_SERVER_DEFAULT_PORT)},
            ])
    
            
    def _add_host_to_mms(self, host, port):
        """
        Function to add host to MMS
        MMS api call https://mms.10gen.com/host/v1/addHost/API_KEY?hostname=URL_ENCODED_HOSTNAME&port=PORT

        :type host: string
        :param host: hostname

        :type port: string or int
        :param port: MongoDB instance port
        """

        req = 'https://mms.10gen.com/host/v1/addHost/%s?%s'\
            % (__mongodb__['mms']['api_key'], urllib.urlencode({'hostname':host, 'port':port}))
        urllib2.urlopen(req)


    def _delete_host_from_mms(self, host, port):
        """
        Function to delete host from MMS
        MMS api call https://mms.10gen.com/host/v1/deleteHost/API_KEY?hostname=URL_ENCODED_HOSTNAME&port=PORT

        :type host: string
        :param host: hostname

        :type port: string or int
        :param port: MongoDB instance port
        """

        req = 'https://mms.10gen.com/host/v1/deleteHost/%s?%s'\
            % (__mongodb__['mms']['api_key'], urllib.urlencode({'hostname':host, 'port':port}))
        urllib2.urlopen(req)
        
            
    @property
    def shard_index(self):
        if not hasattr(self, "_shard_index"):
            self._shard_index = int(__mongodb__['shard_index'])
        return self._shard_index


    @property
    def rs_id(self):
        if not hasattr(self, "_rs_index"):
            self._rs_index = int(__mongodb__['replica_set_index'])
        return self._rs_index


    def has_config_server(self):
        return self.shard_index == 0 and self.rs_id == 0


    @property
    def shard_name(self):
        return SHARD_NAME_TPL % self.shard_index

                
    @property
    def scalr_password(self):
        return __mongodb__['password']

    @property
    def hostname(self):
        return HOSTNAME_TPL % (self.shard_index, self.rs_id)



class DrainingWatcher(threading.Thread):
        
    def __init__(self, handler):
        """
        @type handler: MongoDBHandler
        """
        super(DrainingWatcher, self).__init__()
        self.handler = handler
        self.shard_index = self.handler.shard_index
        self.shard_name = SHARD_NAME_TPL % (self.shard_index)
        self.router_cli = self.handler.mongodb.router_cli
        self._logger = self.handler._logger


    def is_draining_complete(self, ret):
        if ret['state'] == 'completed':
            self._logger.debug('Draining process completed.')
                    
            """ We can terminate shard instances now """

            return True
        return False


    def send_ok_result(self):
        msg_body=dict(status='ok', shard_index=self.shard_index)
        self.handler.send_message(MongoDBMessages.REMOVE_SHARD_RESULT, msg_body)


    def run(self):
        try:
            self.router_cli.start_balancer()

            ret = self.router_cli.remove_shard(self.shard_name)
            if ret['ok'] != 1:
                # TODO: find error message end send it to scalr
                raise Exception('Cannot remove shard %s' % self.shard_name)

            if self.is_draining_complete(ret):
                STATE[REMOVE_VOLUME_KEY] = 1
                self.send_ok_result()
                return

            self._logger.debug('Starting the process of removing shard %s' % self.shard_name)
    
            ret = self.router_cli.remove_shard(self.shard_name)                     
            if self.is_draining_complete(ret):
                STATE[REMOVE_VOLUME_KEY] = 1
                self.send_ok_result()
                return


            """ Get initial chunks count """
            init_chunks = ret['remaining']['chunks']
            last_notification_chunks_count = init_chunks
    
            self._logger.debug('Total chunks to move: %s' % init_chunks)
    
            # Calculating 5% 
            trigger_step = init_chunks / 20
    
            while True:
                ret = self.router_cli.remove_shard(self.shard_name)
        
                self._logger.debug('removeShard process returned state "%s"' % ret['state'])
        
                if self.is_draining_complete(ret):
                    STATE[REMOVE_VOLUME_KEY] = 1
                    self.send_ok_result()
                    return
            
                elif ret['state'] == 'ongoing':
                    chunks_left = ret['remaining']['chunks']
                    self._logger.debug('Chunks left: %s', chunks_left)
            
                    if chunks_left == 0:
                        unsharded = self.handler.get_unpartitioned_dbs(shard_name=self.shard_name)

                        """ Handle test db move """
                        if 'test' in unsharded:
                            """ Send it to shard-0 """
                            self.router_cli.move_primary('test', SHARD_NAME_TPL % 0)
                            unsharded.remove('test')

                        if unsharded:
                            raise Exception("You have %s unsharded databases on shard %s (%s)" % \
                                                                    len(unsharded), self.shard_index, ', '.join(unsharded))

                    progress = last_notification_chunks_count - chunks_left

                    if progress > trigger_step:
                        progress_in_pct = int((float(init_chunks - chunks_left) / init_chunks) * 100)

                        msg_body = dict(shard_index=self.shard_index, total_chunks=init_chunks,
                                                chunks_left=chunks_left, progress=progress_in_pct)
                        self.handler.send_message(MongoDBMessages.REMOVE_SHARD_STATUS, msg_body)                                        
                        last_notification_chunks_count = chunks_left    
                        
                time.sleep(15)

        except:
            msg_body = dict(shard_index=self.shard_index, status='error', last_error=sys.exc_info()[1])
            self.handler.send_message(MongoDBMessages.REMOVE_SHARD_RESULT,msg_body)



class StatusWatcher(threading.Thread):
        
    def __init__(self, hostname, handler, local_ip):
        """
        @type handler: MongoDBHandler
        """
        super(StatusWatcher, self).__init__()
        self.hostname = hostname
        self.handler=handler
        self.local_ip = local_ip
        self._stop = threading.Event()
        
    def stop(self):
        self._stop.set()
        
    def run(self):
        nodename = '%s:%s' % (self.hostname, mongo_svc.REPLICA_DEFAULT_PORT)
        initialized = stale = False
        while not (initialized or stale or self._stop.is_set()):
            rs_status = self.handler.mongodb.cli.get_rs_status()
            
            for member in rs_status['members']:
                if not member['name'] == nodename:
                    continue
            
                status = member['state']
                
                if status in (1,2):
                    msg = {'status' : ReplicationState.INITIALIZED}
                    self.handler.send_int_message(self.local_ip, MongoDBMessages.INT_BOOTSTRAP_WATCHER_RESULT, msg)
                    initialized = True
                    break
            
                if status == 3:
                    if 'errmsg' in member and 'RS102' in member['errmsg']:
                        msg = {'status' : ReplicationState.STALE}
                        self.handler.send_int_message(self.local_ip, MongoDBMessages.INT_BOOTSTRAP_WATCHER_RESULT, msg)
                        stale = True

            time.sleep(3)
                                    
        self.handler._status_trackers.pop(self.local_ip)
        


class ClusterTerminateWatcher(threading.Thread):
        
    def __init__(self, role_hosts, handler, timeout):
        """
        :type handler: MongoDBHandler
        """
        super(ClusterTerminateWatcher, self).__init__()
        self.role_hosts = role_hosts
        self.handler = handler
        self.full_status = {}
        now = datetime.datetime.utcnow()
        self.start_date = str(now)
        self.deadline = now + datetime.timedelta(timeout)
        self.next_heartbeat = None
        self.node_ips = {}
        self.total_nodes_count = len(self.role_hosts)
        self.logger = self.handler._logger
        
    def run(self):
        try:
            # Send cluster terminate notification to all role nodes
            self.logger.info("Notifying all nodes about cluster termination.")
            for host in self.role_hosts:

                shard_idx = host.shard_index
                rs_idx = host.replica_set_index

                if not shard_idx in self.full_status:
                    self.full_status[shard_idx] = {}

                if not shard_idx in self.node_ips:
                    self.node_ips[shard_idx] = {}

                self.node_ips[shard_idx][rs_idx] = host.internal_ip

                self.send_int_cluster_terminate_to_node(host.internal_ip,
                                                                                                        shard_idx, rs_idx)

            msg_store = P2pMessageStore()
            cluster_terminated = False
            self.next_heartbeat = datetime.datetime.utcnow() + datetime.timedelta(seconds=HEARTBEAT_INTERVAL)

            while not cluster_terminated:
                # If timeout reached
                if datetime.datetime.utcnow() > self.deadline:
                    raise Exception('Cluster termination timeout reached.')

                msg_queue_pairs = msg_store.get_unhandled('http://0.0.0.0:8012')
                messages = [pair[1] for pair in msg_queue_pairs]

                for msg in messages:
                    if msg.name != MongoDBMessages.INT_CLUSTER_TERMINATE_RESULT:
                        continue

                    try:
                        shard_id = int(msg.shard_index)
                        rs_id = int(msg.replica_set_index)

                        self.logger.info("Received termination status (%s) from mongo-%s-%s",
                                                        msg.status, shard_id, rs_id)

                        if msg.status == 'ok':
                            if 'last_error' in self.full_status[shard_id][rs_id]:
                                del self.full_status[shard_id][rs_id]['last_error']
                            self.full_status[shard_id][rs_id]['status'] = TerminationState.TERMINATED
                            self.full_status[shard_id][rs_id]['is_main'] = int(msg.is_main)
                        else:
                            self.full_status[shard_id][rs_id]['status'] = TerminationState.FAILED
                            self.full_status[shard_id][rs_id]['last_error'] = msg.last_error
                    finally:
                        msg_store.mark_as_handled(msg.id)

                if datetime.datetime.utcnow() > self.next_heartbeat:
                    self.logger.debug("Preparing ClusterTerminate status message")

                    # It's time to send message to scalr
                    msg_body = dict(nodes=[])

                    terminated_nodes_count = 0

                    for shard_id in self.full_status.keys():
                        for rs_id in self.full_status[shard_id].keys():
                            node_info = dict(shard_index=shard_id, replica_set_index=rs_id)
                            node_info.update(self.full_status[shard_id][rs_id])
                            msg_body['nodes'].append(node_info)
                            status = self.full_status[shard_id][rs_id]['status']

                            if status in (TerminationState.UNREACHABLE, TerminationState.FAILED):
                                ip = self.node_ips[shard_id][rs_id]
                                self.send_int_cluster_terminate_to_node(ip,     shard_id, rs_id)
                            elif status == TerminationState.TERMINATED:
                                terminated_nodes_count += 1

                    progress = int(float(terminated_nodes_count) * 100 / self.total_nodes_count)
                    msg_body['progress'] = progress
                    msg_body['start_date'] = self.start_date

                    self.logger.info("Sending cluster terminate status (progress: %s%%)", progress)
                    self.handler.send_message(MongoDBMessages.CLUSTER_TERMINATE_STATUS, msg_body)

                    if terminated_nodes_count == self.total_nodes_count:
                        break
                    else:
                        self.next_heartbeat += datetime.timedelta(seconds=HEARTBEAT_INTERVAL)

            self.logger.info("Mongodb cluster successfully terminated.")
            self.handler.send_message(MongoDBMessages.CLUSTER_TERMINATE_RESULT,
                                                                                                            dict(status='ok'))
        except:
            err_msg = sys.exc_info()[1]
            self.handler.send_message(MongoDBMessages.CLUSTER_TERMINATE_RESULT,
                    dict(status='error', last_error=err_msg))

    def send_int_cluster_terminate_to_node(self, ip, shard_idx, rs_idx):
        try:
            self.handler.send_int_message(ip,
                                                                    MongoDBMessages.INT_CLUSTER_TERMINATE,
                                                                    broadcast=True)
        except:
            self.full_status[shard_idx][rs_idx] = \
                                                            {'status' : TerminationState.UNREACHABLE}
        self.full_status[shard_idx][rs_idx] = \
                                                        {'status' : TerminationState.PENDING}                                   
