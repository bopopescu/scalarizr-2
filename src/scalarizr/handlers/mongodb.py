'''
Created on Sep 30, 2011

@author: Dmytro Korsakov
'''
import os
import sys
import time
import shutil
import logging
import tarfile
import tempfile
import datetime
import threading

from scalarizr import config
from scalarizr.bus import bus
from scalarizr.platform import PlatformFeatures
from scalarizr.messaging import Messages
from scalarizr.util import system2, wait_until, Hosts, cryptotool
from scalarizr.util.filetool import split, rchown
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import ServiceCtlHandler, HandlerError
from scalarizr.storage import Storage, Snapshot, StorageError, Volume, transfer
import scalarizr.services.mongodb as mongo_svc
from scalarizr.messaging.p2p import P2pMessageStore


BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.MONGODB

STORAGE_VOLUME_CNF		= 'mongodb.json'
STORAGE_SNAPSHOT_CNF	= 'mongodb-snap.json'
STORAGE_TMP_DIR 		= "tmp"

OPT_VOLUME_CNF			= 'volume_config'
OPT_SNAPSHOT_CNF		= 'snapshot_config'
OPT_KEYFILE				= "keyfile"
OPT_SHARD_INDEX			= "shard_index"
OPT_RS_ID				= "replica_set_index"
OPT_PASSWORD			= "password"

BACKUP_CHUNK_SIZE		= 200*1024*1024

HOSTNAME_TPL			= "mongo-%s-%s"
RS_NAME_TPL				= "rs-%s"

HEARTBEAT_INTERVAL		= 60


		
def get_handlers():
	return (MongoDBHandler(), )



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
		key_file				 A key file with at least 6 Base64 characters
		volume_config			Master storage configuration			(on master)
		snapshot_config			Master storage snapshot				 (both)
	)
	
	= HOST_UP =
	@ivar mysql=dict(
		root_password:			 'scalr' user password					  (on master)
		repl_password:			 'scalr_repl' user password				(on master)
		stat_password:			 'scalr_stat' user password				(on master)
		log_file:				 Binary log file							(on master) 
		log_pos:				 Binary log file position				(on master)
		volume_config:			Current storage configuration			(both)
		snapshot_config:		Master storage snapshot					(on master)		 
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
	
	
		

class ReplicationState:
	INITIALIZED = 'initialized'
	STALE		= 'stale'
	
	
class TerminationState:
	FAILED = 'failed'
	UNREACHABLE = 'unreachable'
	TERMINATED = 'terminated'
	PENDING = 'pending_terminate'
	

class MongoDBHandler(ServiceCtlHandler):
	_logger = None
		
	_queryenv = None
	""" @type _queryenv: scalarizr.queryenv.QueryEnvService	"""
	
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
				MongoDBMessages.INT_BOOTSTRAP_WATCHER_RESULT,
				MongoDBMessages.INT_CREATE_DATA_BUNDLE,
				MongoDBMessages.INT_CREATE_DATA_BUNDLE_RESULT,
				MongoDBMessages.CLUSTER_TERMINATE,
				MongoDBMessages.INT_CLUSTER_TERMINATE,				
				Messages.UPDATE_SERVICE_CONFIGURATION,
				Messages.BEFORE_HOST_TERMINATE,
				Messages.HOST_DOWN,
				Messages.HOST_INIT)
		
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		bus.on("init", self.on_init)
		bus.define_events(
			'before_%s_data_bundle' % BEHAVIOUR,
			
			'%s_data_bundle',
			
			# @param host: New master hostname 
			'before_%s_change_master',
			
			# @param host: New master hostname 
			'%s_change_master',
			
			'before_slave_promote_to_master',
			
			'slave_promote_to_master'
		)	
		self.on_reload()   
		self._status_trackers = dict()
	
	
	def on_init(self):
			
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("before_reboot_start", self.on_before_reboot_start)
		
		if 'ec2' == self._platform.name:
			updates = dict(hostname_as_pubdns = '0')
			self._cnf.update_ini('ec2', {'ec2': updates}, private=False)
		
		if self._cnf.state == ScalarizrState.RUNNING:
	
			storage_conf = Storage.restore_config(self._volume_config_path)
			self.storage_vol = Storage.create(storage_conf)
			if not self.storage_vol.mounted():
				self.storage_vol.mount()
				
			self.mongodb.authenticate(mongo_svc.SCALR_USER, self.scalr_password)
			self.mongodb.start_shardsvr()
			
			if self.shard_index == 0 and self.rs_id == 0:
				self.mongodb.start_config_server()

			if self.rs_id in (0,1):
				self.mongodb.start_router()
			
	
	def on_reload(self):
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
		self._storage_path = mongo_svc.STORAGE_PATH
		self._tmp_dir = os.path.join(self._storage_path, STORAGE_TMP_DIR)
		
		self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
		self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))
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
		if not message.body.has_key(BEHAVIOUR):
			raise HandlerError("HostInitResponse message for %s behaviour must have '%s' property " 
							% (BEHAVIOUR, BEHAVIOUR))
		
		path = os.path.dirname(self._volume_config_path)
		if not os.path.exists(path):
			os.makedirs(path)
		
		mongodb_data = message.mongodb.copy()
		self._logger.info('Got %s part of HostInitResponse: %s' % (BEHAVIOUR, mongodb_data))
		
		for key, fpath in ((OPT_VOLUME_CNF, self._volume_config_path), 
						(OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
			if os.path.exists(fpath):
				os.remove(fpath)
			
			if key in mongodb_data:
				if mongodb_data[key]:
					Storage.backup_config(mongodb_data[key], fpath)
				del mongodb_data[key]
				
		mongodb_key = mongodb_data[OPT_KEYFILE]
		del mongodb_data[OPT_KEYFILE]
		
		mongodb_key = mongodb_key or cryptotool.pwgen(22)
		self._cnf.write_key(BEHAVIOUR, mongodb_key)
		
		if mongodb_data.get('password'):
			mongodb_data['password'] = mongodb_data.get('password')
		else:
			mongodb_data['password'] =  cryptotool.pwgen(10)
		
		self._logger.debug("Update %s config with %s", (BEHAVIOUR, mongodb_data))
		self._update_config(mongodb_data)
		

	def on_before_host_up(self, hostup_msg):
		"""
		Check that replication is up in both master and slave cases
		@type message: scalarizr.messaging.Message		
		@param message: HostUp message
		"""

		first_in_rs = True	
		hosts = self._queryenv.list_roles(self._role_name)[0].hosts
		for host in hosts:
			hostname = HOSTNAME_TPL % (host.shard_index, host.replica_set_index)
			Hosts.set(host.internal_ip, hostname)
			if host.shard_index == self.shard_index:
				first_in_rs = False

		""" Set hostname"""
		self.hostname = HOSTNAME_TPL % (self.shard_index, self.rs_id)
		local_ip = self._platform.get_private_ip()
		Hosts.set(local_ip, self.hostname)
		with open('/etc/hostname', 'w') as f:
			f.write(self.hostname)
		system2(('hostname', '-F', '/etc/hostname'))
		
		rs_name = RS_NAME_TPL % self.shard_index
		
		make_shard = False
		
		if first_in_rs:
			make_shard = self._init_master(hostup_msg, rs_name)	
		else:
			self._init_slave(hostup_msg, rs_name)

		self._logger.debug('shard_index=%s, type(shard_index)=%s' % (self.shard_index, type(self.shard_index)))
		self._logger.debug('rs_id=%s, type(rs_id)=%s' % (self.rs_id, type(self.rs_id)))
		
		if self.shard_index == 0 and self.rs_id == 0:
			self.mongodb.start_config_server()
			hostup_msg.mongodb['config_server'] = 1
		else:
			hostup_msg.mongodb['config_server'] = 0

		if self.rs_id in (0,1):
			self.mongodb.start_router()
			hostup_msg.mongodb['router'] = 1
		
			if make_shard:
				self._logger.info('Initializing shard')
				self.create_shard()
		else:
			hostup_msg.mongodb['router'] = 0
			
		hostup_msg.mongodb['keyfile'] = self._cnf.read_key(BEHAVIOUR)
	
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
			
			is_master = self.mongodb.is_replication_master
			
			if is_master and self.shard_index == shard_idx:
				
				nodename = '%s:%s' % (hostname, mongo_svc.REPLICA_DEFAULT_PORT)
				if nodename not in self.mongodb.replicas:
					self.mongodb.register_slave(hostname, mongo_svc.REPLICA_DEFAULT_PORT)
				else:
					self._logger.warning('Host %s is already in replica set.' % nodename)
				
				
				watcher = StatusWatcher(hostname, self, message.local_ip)
				self._logger.info('Starting bootstrap watcher for node ip=%s', message.local_ip)
				watcher.start()
				self._status_trackers[message.local_ip] = watcher

				
	def create_shard(self):
		rs_name = RS_NAME_TPL % self.shard_index
		return self.mongodb.router_cli.add_shard(rs_name, self.mongodb.replicas)


	def on_HostInit(self, message):
		if message.local_ip != self._platform.get_private_ip():
		
			shard_idx = int(message.mongodb['shard_index'])
			rs_idx = int(message.mongodb['replica_set_index'])
			hostname = HOSTNAME_TPL % (shard_idx, rs_idx)
			
			self._logger.debug('Adding %s as %s to hosts file', message.local_ip, hostname)
			Hosts.set(message.local_ip, hostname)


	def on_HostUp(self, message):
		private_ip = self._platform.get_private_ip()
		if message.local_ip != private_ip:
			if self.mongodb.is_replication_master and \
											self.shard_index == message.shard_index:			   
				r = len(self.mongodb.replicas) 
				a = len(self.mongodb.arbiters)
				if r % 2 == 0 and not a:
					self.mongodb.start_arbiter()
					self.mongodb.register_arbiter(private_ip)
				elif r % 2 != 0 and a:
					for arbiter in self.mongodb.arbiters:
						self.mongodb.unregister_slave(arbiter)
					self.mongodb.stop_arbiter()
			else:
				if len(self.mongodb.replicas) % 2 != 0:
					self.mongodb.stop_arbiter()
					
					
	def on_HostDown(self, message):
		if message.local_ip in self._status_trackers:
			t = self._status_trackers[message.local_ip]
			t.stop()
			del self._status_trackers[message.local_ip]

		if self.mongodb.is_replication_master:
			if message.local_ip in self.mongodb.replicas:
				""" Remove host from replica set"""
				self.mongodb.unregister_slave(message.local_ip)
				""" If arbiter was running on the node - unregister it """
				possible_arbiter = "%s:%s" % (message.local_ip, mongo_svc.ARBITER_DEFAULT_PORT)
				if possible_arbiter in self.mongodb.arbiters:
					self.mongodb.unregister_slave(message.local_ip, mongo_svc.ARBITER_DEFAULT_PORT)
				""" Start arbiter if necessary """
				if len(self.mongodb.replicas) % 2 == 0:
					self.mongodb.start_arbiter()
				else:
					self.mongodb.stop_arbiter()
						
		elif len(self.mongodb.replicas) == 2:
			# Become primary and only member of rs
			hostname = HOSTNAME_TPL % (self.shard_index, self.rs_id)
			nodename = '%s:%s' % (hostname, mongo_svc.REPLICA_DEFAULT_PORT)
			
			rs_cfg = self.mongodb.cli.get_rs_config()
			rs_cfg['members'] = [m for m in rs_cfg['members'] if m['host'] == nodename]
			self.mongodb.cli.rs_reconfig(rs_cfg, force=True)
			wait_until(lambda: self.mongodb.is_replication_master, timeout=180)	
					
					
	def on_before_reboot_start(self, *args, **kwargs):
		self.mongodb.stop_arbiter()
		self.mongodb.stop_router()
		self.mongodb.stop_config_server()
		self.mongodb.mongod.stop('Rebooting instance')
		pass
	

	def on_BeforeHostTerminate(self, message):
		if message.local_ip == self._platform.get_private_ip():
			self.mongodb.stop_config_server()
			self.mongodb.mongod.stop('Server will be terminated')	
			
			self._logger.info('Detaching %s storage' % BEHAVIOUR)
			self.storage_vol.detach()
			
			
	def on_MongoDb_IntCreateDataBundle(self, message):
		msg_data = self._create_data_bundle()
		if msg_data:
			self.send_int_message(message.local_ip, 
								MongoDBMessages.INT_CREATE_DATA_BUNDLE_RESULT,
								msg_data)
			
	
	def on_MongoDb_CreateDataBundle(self, message):
		msg_data = self._create_data_bundle()
		if msg_data:
			self.send_message(MongoDBMessages.CREATE_DATA_BUNDLE_RESULT, msg_data)		
			
	
	def _create_data_bundle(self):
		if not self.mongodb.is_replication_master:
			self._logger.debug('Not a master. Skipping data bundle')
			return
		
		try:
			bus.fire('before_%s_data_bundle' % BEHAVIOUR)
			self.mongodb.router_cli.stop_balancer()
			self.mongodb.cli.sync()
			
			# Creating snapshot		
			snap = self._create_snapshot()
			used_size = int(system2(('df', '-P', '--block-size=M', self._storage_path))[0].split('\n')[1].split()[2][:-1])
			bus.fire('%s_data_bundle' % BEHAVIOUR, snapshot_id=snap.id)			
			
			# Notify scalr
			msg_data = dict(
				used_size	= '%.3f' % (float(used_size) / 1000,),
				status		= 'ok'
			)
			msg_data[BEHAVIOUR] = self._compat_storage_data(snap=snap)
			return msg_data
		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			msg_data = dict(status = 'error', last_error = str(e))
			return msg_data
		finally:
			self.mongodb.router_cli.start_balancer()
		
			
	
	def on_MongoDb_CreateBackup(self, message):
		if not self.mongodb.is_replication_master:
			self._logger.debug('Not a master. Skipping backup process')
			return 
		
		tmpdir = backup_path = None
		try:
			#turn balancer off
			self.mongodb.router_cli.stop_balancer()
			
			#perform fsync
			self.mongodb.cli.sync()
			
			#create temporary dir for dumps
			if not os.path.exists(self._tmp_dir):
				os.makedirs(self._tmp_dir)
			tmpdir = tempfile.mkdtemp(self._tmp_dir)		
			rchown(mongo_svc.DEFAULT_USER, tmpdir) 

			#dump config db on router
			r_dbs = self.mongodb.router_cli.list_databases()
			rdb_name = 'config'
			if rdb_name  in r_dbs:
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
			dbs = self.mongodb.cli.list_databases()
			
			# Defining archive name and path
			rs_name = RS_NAME_TPL % self.shard_index
			backup_filename = '%s-%s-backup-'%(BEHAVIOUR,rs_name) + time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
			backup_path = os.path.join(self._tmp_dir, backup_filename)
			
			# Creating archive 
			backup = tarfile.open(backup_path, 'w:gz')
			
			# Dump all databases
			self._logger.info("Dumping all databases")
			md = mongo_svc.MongoDump()  
			
			for db_name in dbs:
				dump_path = tmpdir + os.sep + db_name + '.bson'
				err = md.create(db_name, dump_path)[1]
				if err:
					raise HandlerError('Error while dumping database %s: %s' % (db_name, err))
				backup.add(dump_path, os.path.basename(dump_path))
			backup.close()
			
			# Creating list of full paths to archive chunks
			if os.path.getsize(backup_path) > BACKUP_CHUNK_SIZE:
				parts = [os.path.join(tmpdir, file) for file in split(backup_path, backup_filename, BACKUP_CHUNK_SIZE , tmpdir)]
			else:
				parts = [backup_path]
					
			self._logger.info("Uploading backup to cloud storage (%s)", self._platform.cloud_storage_path)
			trn = transfer.Transfer()
			result = trn.upload(parts, self._platform.cloud_storage_path)
			self._logger.info("%s backup uploaded to cloud storage under %s/%s", 
							BEHAVIOUR, self._platform.cloud_storage_path, backup_filename)
			
			# Notify Scalr
			self.send_message(MongoDBMessages.CREATE_BACKUP_RESULT, dict(
				status = 'ok',
				backup_parts = result
			))
						
		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(MongoDBMessages.CREATE_BACKUP_RESULT, dict(
				status = 'error',
				last_error = str(e)
			))
			
		finally:
			if tmpdir:
				shutil.rmtree(tmpdir, ignore_errors=True)
			if backup_path and os.path.exists(backup_path):
				os.remove(backup_path)
			self.mongodb.router_cli.start_balancer()
				
				
	def _init_master(self, message, rs_name):
		"""
		Initialize mongodb master
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		
		self._logger.info("Initializing %s master" % BEHAVIOUR)
		
		# Plug storage
		volume_cnf = Storage.restore_config(self._volume_config_path)
		try:
			snap_cnf = Storage.restore_config(self._snapshot_config_path)
			volume_cnf['snapshot'] = snap_cnf
		except IOError:
			pass
		
		self.storage_vol = self._plug_storage(mpoint=self._storage_path, vol=volume_cnf)
		Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
		
		init_start = not self._storage_valid()	
		
		self.mongodb.prepare(rs_name)
		self.mongodb.start_shardsvr()
				
		if init_start:
			self.mongodb.initiate_rs()			
		else:
			
			hostname = HOSTNAME_TPL % (self.shard_index, self.rs_id)
			nodename = '%s:%s' % (hostname, mongo_svc.REPLICA_DEFAULT_PORT)
			
			rs_cfg = self.mongodb.cli.get_rs_config()
			rs_cfg['members'] = [{'_id' : 0, 'host': nodename}]
			rs_cfg['version'] += 1
			self.mongodb.cli.rs_reconfig(rs_cfg, force=True)
			wait_until(lambda: self.mongodb.is_replication_master, timeout=180)
						
		password = self.scalr_password
		self.mongodb.cli.create_or_update_admin_user(mongo_svc.SCALR_USER, password)
		self.mongodb.authenticate(mongo_svc.SCALR_USER, password)
		
		msg_data = dict()
		msg_data['password'] = password

		# Create snapshot
		snap = self._create_snapshot()
		Storage.backup_config(snap.config(), self._snapshot_config_path)

		# Update HostInitResponse message 
		msg_data.update(self._compat_storage_data(self.storage_vol, snap))
					
		message.mongodb = msg_data.copy()
		try:
			del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
		except KeyError:
			pass
		self._update_config(msg_data)
		
		return init_start
	
	
	def _get_shard_hosts(self):
		hosts = self._queryenv.list_roles(self._role_name)[0].hosts
		shard_index = self.shard_index
		return [host for host in hosts if host.shard_index == shard_index]

	def _init_slave(self, message, rs_name):
		"""
		Initialize mongodb slave
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		
		msg_store = P2pMessageStore()
		
		def request_and_wait_replication_status():
			
			self._logger.info('Notify primary node we are joining replica set')

			msg_body = dict(mongodb=dict(shard_index=self.shard_index,
							replica_set_index=self.rs_id))
			for host in self._get_shard_hosts():
				self.send_int_message(host.internal_ip,
								MongoDBMessages.INT_CREATE_BOOTSTRAP_WATCHER,
								msg_body, broadcast=True)
			
			self._logger.info('Waiting for status message from primary node')
			initialized = stale = False	
			
			while not initialized and not stale:
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
			return stale
		
		self._logger.info("Initializing %s slave" % BEHAVIOUR)

		# Plug storage
		volume_cfg = Storage.restore_config(self._volume_config_path)
		volume = Storage.create(Storage.blank_config(volume_cfg))	
		self.storage_vol = self._plug_storage(self._storage_path, volume)
		Storage.backup_config(self.storage_vol.config(), self._volume_config_path)

		self.mongodb.stop_default_init_script()
		self.mongodb.prepare(rs_name)
		self.mongodb.start_shardsvr()
		self.mongodb.authenticate(mongo_svc.SCALR_USER, self.scalr_password)
		
		first_start = not self._storage_valid()
		if not first_start:
			self.mongodb.cli.connection.local.system.replset.remove()
			self.mongodb.mongod.stop('Removing previous replication set info')
			self.mongodb.start_shardsvr()

		stale = request_and_wait_replication_status()

		if stale:			
			new_volume = None

			try:
				if PlatformFeatures.VOLUMES not in self._platform.features:
					raise HandlerError('Platform does not support pluggable volumes')

				self._logger.info('Too stale to synchronize. Trying to get snapshot from primary')
				for host in self._get_shard_hosts():
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
								self._logger.info('Received data bundle from master node.')
								self.mongodb.mongod.stop()
								
								self.storage_vol.detach()
								
								snap_cnf = msg.mongodb.snapshot_config.copy()
								new_volume = self._plug_storage(self._storage_path,
																	 {'snapshot': snap_cnf})
								self.mongodb.start_shardsvr()
								stale = request_and_wait_replication_status()
								
								if stale:
									raise HandlerError('Got stale even when standing from snapshot.')
								else:
									self.storage_vol.destroy()
									self.storage_vol = new_volume
							else:
								raise HandlerError('Data bundle failed.')
								
						finally:
							msg_store.mark_as_handled(msg.id)
														
					time.sleep(1)
			except:
				self._logger.warning('%s. Trying to perform clean sync' % sys.exc_info()[1] )
				if new_volume:
					new_volume.destroy()
					
				# TODO: new storage
				self._init_clean_sync()
				stale = request_and_wait_replication_status()
				if stale:
					# TODO: raise distinct exception
					raise HandlerError("Replication status is stale")
		else:
			self._logger.info('Successfully joined replica set')

		message.mongodb = self._compat_storage_data(self.storage_vol)
		
		
	def on_MongoDb_ClusterTerminate(self, message):
		role_hosts = self._queryenv.list_roles(self._role_name)[0].hosts
		cluster_terminate_watcher = ClusterTerminateWatcher(role_hosts, self)
		cluster_terminate_watcher.start()

		
	def on_MongoDb_IntClusterTerminate(self, message):
		try:
			is_replication_master = self.mongodb.is_replication_master
			self.mongodb.mongod.stop()
			self.mongodb.stop_config_server()
			
			self._logger.info('Detaching %s storage' % BEHAVIOUR)
			self.storage_vol.detach()
			
			msg_body = dict(status='ok',
							shard_index=self.shard_index,
							replica_set_index=self.rs_id,
							is_master=int(is_replication_master))
		except:
			msg_body = dict(status='error',
							last_error=str(sys.exc_info()[1]),
							shard_index=self.shard_index,
							replica_set_index=self.rs_id)
				
		finally:
			self.send_int_message(message.local_ip,
					MongoDBMessages.INT_CLUSTER_TERMINATE_RESULT, msg_body)
		
		
		

	def _get_keyfile(self):
		password = None 
		if self._cnf.rawini.has_option(CNF_SECTION, OPT_KEYFILE):
			password = self._cnf.rawini.get(CNF_SECTION, OPT_KEYFILE)	
		return password


	def _update_config(self, data): 
		#ditching empty data
		updates = dict()
		for k,v in data.items():
			if v: 
				updates[k] = v
		
		self._cnf.update_ini(BEHAVIOUR, {CNF_SECTION: updates})


	def _plug_storage(self, mpoint, vol):
		if not isinstance(vol, Volume):
			vol = Storage.create(vol)

		try:
			if not os.path.exists(mpoint):
				os.makedirs(mpoint)
			if not vol.mounted():
				vol.mount(mpoint)
		except StorageError, e:
			if 'you must specify the filesystem type' in str(e):
				vol.mkfs()
				vol.mount(mpoint)
			else:
				raise
		return vol


	def _create_snapshot(self):
		
		system2('sync', shell=True)
		# Creating storage snapshot
		snap = self._create_storage_snapshot()
			
		wait_until(lambda: snap.state in (Snapshot.CREATED, Snapshot.COMPLETED, Snapshot.FAILED))
		if snap.state == Snapshot.FAILED:
			raise HandlerError('%s storage snapshot creation failed. See log for more details' % BEHAVIOUR)
		
		return snap


	def _create_storage_snapshot(self):
		#TODO: check mongod journal option if service is running!
		self._logger.info("Creating storage snapshot")
		try:
			return self.storage_vol.snapshot()
		except StorageError, e:
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
		
			
	@property
	def shard_index(self):
		return int(self._cnf.rawini.get(CNF_SECTION, OPT_SHARD_INDEX))

	
	@property
	def rs_id(self):
		return int(self._cnf.rawini.get(CNF_SECTION, OPT_RS_ID))

	
	@property
	def scalr_password(self):
		return self._cnf.rawini.get(CNF_SECTION, OPT_PASSWORD)
	
	
class StatusWatcher(threading.Thread):
	
	def __init__(self, hostname, handler, local_ip):
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

		super(StatusWatcher, self).__init__()
		self.role_hosts = role_hosts
		self.handler = handler
		self.full_status = {}
		now = datetime.datetime.utcnow()
		self.start_date = str(now)
		self.deadline = now + datetime.timedelta(timeout)
		self.next_heartbeat = None
		self.node_ips = {}
		self.total_nodes_count = len(self.role_hosts)
		
	def run(self):
		# Send cluster terminate notification to all role nodes
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
				self.handler.send_message(MongoDBMessages.CLUSTER_TERMINATE_RESULT,
										dict(status='error'))
				break
						
			msg_queue_pairs = msg_store.get_unhandled('http://0.0.0.0:8012')
			messages = [pair[1] for pair in msg_queue_pairs]
			
			for msg in messages:
				if not msg.name == MongoDBMessages.INT_CLUSTER_TERMINATE_RESULT:
					continue
				
				try:
					shard_id = int(msg.shard_index)
					rs_id = int(msg.replica_set_index)
					
					if msg.status == 'ok':
						if 'last_error' in self.full_status[shard_id][rs_id]:
							del self.full_status[shard_id][rs_id]['last_error']
						self.full_status[shard_id][rs_id]['status'] = TerminationState.TERMINATED
						self.full_status[shard_id][rs_id]['is_master'] = int(msg.is_master) 
					else:
						self.full_status[shard_id][rs_id]['status'] = TerminationState.FAILED
						self.full_status[shard_id][rs_id]['last_error'] = msg.last_error
				finally:
					msg_store.mark_as_handled(msg.id)

			if datetime.datetime.utcnow() > self.next_heartbeat:
				# It's time to send message to scalr
				msg_body = dict(nodes=[])
			
				terminated_nodes_count = 0
				
				for shard_id in range(len(self.full_status)):
					for rs_id in range(len(self.full_status[shard_id])):
						node_info = dict(shard_index=shard_id, replica_set_index=rs_id)
						node_info.update(self.full_status[shard_id][rs_id])
						msg_body['nodes'].append(node_info)
						status = self.full_status[shard_id][rs_id]['status']
						
						if status in (TerminationState.UNREACHABLE, TerminationState.FAILED):
							ip = self.node_ips[shard_id][rs_id]
							self.send_int_cluster_terminate_to_node(ip,	shard_idx, rs_idx)
						elif status == TerminationState.TERMINATED:
							terminated_nodes_count += 1
							
				msg_body['progress'] = terminated_nodes_count * 100 / self.total_nodes_count
				msg_body['start_date'] = self.start_date
				
				self.handler.send_message(MongoDBMessages.CLUSTER_TERMINATE_STATUS, msg_body)


				if terminated_nodes_count == self.total_nodes_count:
					cluster_terminated = True
					break
				else:
					self.next_heartbeat += datetime.timedelta(seconds=HEARTBEAT_INTERVAL)
				
		if cluster_terminated:
			self.handler.send_message(MongoDBMessages.CLUSTER_TERMINATE_RESULT,
															dict(status='ok'))

	def send_int_cluster_terminate_to_node(self, ip, shard_idx, rs_idx):
		try:
			self.handler.send_int_message(ip,
										MongoDBMessages.INT_CLUSTER_TERMINATE,
										broadcast=True)
			self.full_status[shard_idx][rs_idx] = \
									{'status' : TerminationState.PENDING}
		except:
			self.full_status[shard_idx][rs_idx] = \
									{'status' : TerminationState.UNREACHABLE}
					
						
			

												