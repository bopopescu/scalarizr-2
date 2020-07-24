from __future__ import with_statement

'''
Created on April 18th, 2011

@author: Dmytro Korsakov
'''
from __future__ import with_statement

import os
import time
import shutil
import tarfile
import logging
import tempfile

from scalarizr import config
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.config import ScalarizrState, BuiltinBehaviours
from scalarizr.handlers import ServiceCtlHandler, HandlerError, DbMsrMessages
from scalarizr.linux.coreutils import chown_r
from scalarizr.linux.coreutils import split
from scalarizr.util import system2, disttool, software, cryptotool, initdv2
from scalarizr.storage import transfer
from scalarizr.linux import iptables
from scalarizr.handlers import operation, prepare_tags
from scalarizr.services import make_backup_steps
from scalarizr.api import service as preset_service
from scalarizr.services.postgresql import PostgreSql, PSQL, ROOT_USER, PG_DUMP, \
    PgUser, SU_EXEC, PgSQLPresetProvider, __postgresql__
from scalarizr.services import postgresql as postgresql_svc
from scalarizr.node import __node__
from scalarizr import storage2
from scalarizr.services import backup


BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.POSTGRESQL
LOG = logging.getLogger(__name__)

PG_SOCKET_DIR = '/var/run/postgresql/'
STORAGE_PATH = "/mnt/pgstorage"
STORAGE_VOLUME_CNF = 'postgresql.json'
STORAGE_SNAPSHOT_CNF = 'postgresql-snap.json'

OPT_VOLUME_CNF = 'volume_config'
OPT_SNAPSHOT_CNF = 'snapshot_config'
OPT_ROOT_USER = 'root_user'
OPT_ROOT_PASSWORD = "root_password"
OPT_ROOT_SSH_PUBLIC_KEY = "root_ssh_public_key"
OPT_ROOT_SSH_PRIVATE_KEY = "root_ssh_private_key"
OPT_CURRENT_XLOG_LOCATION = 'current_xlog_location'
OPT_REPLICATION_MASTER = postgresql_svc.OPT_REPLICATION_MASTER


__postgresql__.update({
    'port': 5432,
    'storage_dir': '/mnt/pgstorage',
    'root_user': 'scalr',
    'pgdump_chunk_size': 200 * 1024 * 1024,
})


def get_handlers():
    return PostgreSqlHander(),


SSH_KEYGEN_SELINUX_MODULE = """
module local 1.0;

require {
    type initrc_tmp_t;
    type ssh_keygen_t;
    type initrc_t;
    type etc_runtime_t;
    class tcp_socket { read write };
    class file { read write getattr };
}

#============= ssh_keygen_t ==============
allow ssh_keygen_t etc_runtime_t:file { read write getattr };
allow ssh_keygen_t initrc_t:tcp_socket { read write };
allow ssh_keygen_t initrc_tmp_t:file { read write };
"""


class PostgreSqlHander(ServiceCtlHandler):
    _logger = None

    _queryenv = None
    """ @type _queryenv: scalarizr.queryenv.QueryEnvService """

    _platform = None
    """ @type _platform: scalarizr.platform.Ec2Platform """

    _cnf = None
    ''' @type _cnf: scalarizr.config.ScalarizrCnf '''

    preset_provider = None

    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return BEHAVIOUR in behaviour and (
            message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
            or message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
            or message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
            or message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
            or message.name == Messages.UPDATE_SERVICE_CONFIGURATION
            or message.name == Messages.HOST_INIT
            or message.name == Messages.BEFORE_HOST_TERMINATE
            or message.name == Messages.HOST_UP
            or message.name == Messages.HOST_DOWN)

    
    def get_initialization_phases(self, hir_message):
        if BEHAVIOUR in hir_message.body:
            steps = [self._step_accept_scalr_conf, self._step_create_storage]
            if hir_message.body[BEHAVIOUR]['replication_main'] == '1':
                steps += [self._step_init_main, self._step_create_data_bundle]
            else:
                steps += [self._step_init_subordinate]
            steps += [self._step_collect_host_up_data]
            
            return {'before_host_up': [{
                'name': self._phase_postgresql, 
                'steps': steps
            }]}
    
    
    def __init__(self):
        self._service_name = SERVICE_NAME
        ServiceCtlHandler.__init__(self, SERVICE_NAME, initdv2.lookup(SERVICE_NAME))
        bus.on("init", self.on_init)
        bus.define_events(
            'before_postgresql_data_bundle',
            
            'postgresql_data_bundle',
            
            # @param host: New main hostname 
            'before_postgresql_change_main',
            
            # @param host: New main hostname 
            'postgresql_change_main',
            
            'before_subordinate_promote_to_main',
            
            'subordinate_promote_to_main'
        )   
        
        self._phase_postgresql = 'Configure PostgreSQL'
        self._phase_data_bundle = self._op_data_bundle = 'PostgreSQL data bundle'
        self._phase_backup = self._op_backup = 'PostgreSQL backup'
        self._step_upload_to_cloud_storage = 'Upload data to cloud storage'
        self._step_accept_scalr_conf = 'Accept Scalr configuration'
        self._step_patch_conf = 'Patch configuration files'
        self._step_create_storage = 'Create storage'
        self._step_init_main = 'Initialize Main'
        self._step_init_subordinate = 'Initialize Subordinate'
        self._step_create_data_bundle = 'Create data bundle'
        self._step_change_replication_main = 'Change replication Main'
        self._step_collect_host_up_data = 'Collect HostUp data'
        
        self.on_reload()        


    def on_init(self):      
        #temporary fix for starting-after-rebundle issue
        if not os.path.exists(PG_SOCKET_DIR):
            os.makedirs(PG_SOCKET_DIR)
            chown_r(PG_SOCKET_DIR, 'postgres')
            
        bus.on("host_init_response", self.on_host_init_response)
        bus.on("before_host_up", self.on_before_host_up)
        bus.on("before_reboot_start", self.on_before_reboot_start)

        self._insert_iptables_rules()       

        if __node__['state'] == ScalarizrState.BOOTSTRAPPING:
            
            if disttool.is_redhat_based():      
                    
                checkmodule_paths = software.whereis('checkmodule')
                semodule_package_paths = software.whereis('semodule_package')
                semodule_paths = software.whereis('semodule')
            
                if all((checkmodule_paths, semodule_package_paths, semodule_paths)):
                    
                    with open('/tmp/sshkeygen.te', 'w') as fp:
                        fp.write(SSH_KEYGEN_SELINUX_MODULE)
                    
                    self._logger.debug('Compiling SELinux policy for ssh-keygen')
                    system2((checkmodule_paths[0], '-M', '-m', '-o',
                             '/tmp/sshkeygen.mod', '/tmp/sshkeygen.te'), logger=self._logger)
                    
                    self._logger.debug('Building SELinux package for ssh-keygen')
                    system2((semodule_package_paths[0], '-o', '/tmp/sshkeygen.pp',
                             '-m', '/tmp/sshkeygen.mod'), logger=self._logger)
                    
                    self._logger.debug('Loading ssh-keygen SELinux package')                    
                    system2((semodule_paths[0], '-i', '/tmp/sshkeygen.pp'), logger=self._logger)


        if __node__['state'] == 'running':

            vol = storage2.volume(__postgresql__['volume'])
            if not vol.tags:
                vol.tags = self.resource_tags()
            vol.ensure(mount=True)
            
            self.postgresql.service.start()
            self.accept_all_clients()
            
            self._logger.debug("Checking presence of Scalr's PostgreSQL root user.")
            root_password = self.root_password
            
            if not self.postgresql.root_user.exists():
                self._logger.debug("Scalr's PostgreSQL root user does not exist. Recreating")
                self.postgresql.root_user = self.postgresql.create_linux_user(ROOT_USER, root_password)
            else:
                try:
                    self.postgresql.root_user.check_system_password(root_password)
                    self._logger.debug("Scalr's root PgSQL user is present. Password is correct.")              
                except ValueError:
                    self._logger.warning("Scalr's root PgSQL user was changed. Recreating.")
                    self.postgresql.root_user.change_system_password(root_password)
                    
            if self.is_replication_main:  
                #ALTER ROLE cannot be executed in a read-only transaction
                self._logger.debug("Checking password for pg_role scalr.")      
                if not self.postgresql.root_user.check_role_password(root_password):
                    LOG.warning("Scalr's root PgSQL role was changed. Recreating.")
                    self.postgresql.root_user.change_role_password(root_password)
            

    def on_reload(self):
        self._queryenv = bus.queryenv_service
        self._platform = bus.platform
        self.postgresql = PostgreSql()
        self.preset_provider = PgSQLPresetProvider(self.postgresql.postgresql_conf)
        preset_service.services[BEHAVIOUR] = self.preset_provider

    
    def on_HostInit(self, message):
        if message.local_ip != self._platform.get_private_ip() and message.local_ip in self.pg_hosts:
            LOG.debug('Got new subordinate IP: %s. Registering in pg_hba.conf' % message.local_ip)
            self.postgresql.register_subordinate(message.local_ip)
            

    def on_HostUp(self, message):
        if message.local_ip == self._platform.get_private_ip():
            self.accept_all_clients()
        elif message.local_ip in self.farm_hosts:
            self.postgresql.register_client(message.local_ip)
        
    
    
    def on_HostDown(self, message):
        if  message.local_ip != self._platform.get_private_ip():
            self.postgresql.unregister_client(message.local_ip)
            if self.is_replication_main and self.farmrole_id == message.farm_role_id:
                self.postgresql.unregister_subordinate(message.local_ip)
    
    @property           
    def farm_hosts(self):
        list_roles = self._queryenv.list_roles()
        servers = []
        for serv in list_roles:
            for host in serv.hosts :
                servers.append(host.internal_ip or host.external_ip)
        LOG.debug("QueryEnv returned list of servers within farm: %s" % servers)
        return servers              
        
        
    @property
    def pg_hosts(self):
        '''
        All pg instances including those in Initializing state
        '''
        list_roles = self._queryenv.list_roles(behaviour=BEHAVIOUR, with_init=True)
        servers = []
        for pg_serv in list_roles:
            for pg_host in pg_serv.hosts:
                servers.append(pg_host.internal_ip or pg_host.external_ip)
        LOG.debug("QueryEnv returned list of %s servers: %s" % (BEHAVIOUR, servers))
        return servers
    
    
    def accept_all_clients(self):
        farm_hosts = self.farm_hosts
        for ip in farm_hosts:
                self.postgresql.register_client(ip, force=False)
        if farm_hosts:
            self.postgresql.service.reload('Granting access to all servers within farm.', force=True)
                

    @property
    def root_password(self):
        return __postgresql__['%s_password' % ROOT_USER]


    @property   
    def farmrole_id(self):
        return __node__[config.OPT_FARMROLE_ID]
    
            
    def store_password(self, name, password):
        __postgresql__['%s_password' % name] = password


    @property
    def _tmp_path(self):
        return os.path.join(__postgresql__['storage_dir'], 'tmp')


    @property
    def is_replication_main(self):
        return True if int(__postgresql__[OPT_REPLICATION_MASTER]) else False


    def resource_tags(self):
        return prepare_tags(BEHAVIOUR, db_replication_role=self.is_replication_main)


    def on_host_init_response(self, message):
        """
        Check postgresql data in host init response
        @type message: scalarizr.messaging.Message
        @param message: HostInitResponse
        """
        
        with bus.initialization_op as op:
            with op.phase(self._phase_postgresql):
                with op.step(self._step_accept_scalr_conf):
        
                    if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
                        raise HandlerError("HostInitResponse message for PostgreSQL behaviour must have 'postgresql' property and db_type 'postgresql'")

                    postgresql_data = message.postgresql.copy()

                    #Extracting service configuration preset from message
                    if 'preset' in postgresql_data:
                        self.initial_preset = postgresql_data['preset']
                        LOG.debug('Scalr sent current preset: %s' % self.initial_preset)
                        del postgresql_data['preset']

                    #Extracting or generating postgresql root password
                    postgresql_data['%s_password' % ROOT_USER] = postgresql_data.get(OPT_ROOT_PASSWORD) or cryptotool.pwgen(10)
                    del postgresql_data[OPT_ROOT_PASSWORD]

                    #Extracting replication ssh keys from message
                    root = PgUser(ROOT_USER, self.postgresql.pg_keys_dir)
                    root.store_keys(postgresql_data[OPT_ROOT_SSH_PUBLIC_KEY], postgresql_data[OPT_ROOT_SSH_PRIVATE_KEY])
                    del postgresql_data[OPT_ROOT_SSH_PUBLIC_KEY]
                    del postgresql_data[OPT_ROOT_SSH_PRIVATE_KEY]


                    if postgresql_data.get('volume'):
                        # New format
                        postgresql_data['compat_prior_backup_restore'] = False
                        postgresql_data['volume'] = storage2.volume(postgresql_data['volume'])
                        LOG.debug("message.pg['volume']:", postgresql_data['volume'])
                        if 'backup' in postgresql_data:
                            postgresql_data['backup'] = backup.backup(postgresql_data['backup'])
                            LOG.debug("message.pg['backup']:", postgresql_data['backup'])
                        if 'restore' in postgresql_data:
                            postgresql_data['restore'] = backup.restore(postgresql_data['restore'])
                            LOG.debug("message.pg['restore']:", postgresql_data['restore'])
                    else:

                        # Compatibility transformation
                        # - volume_config -> volume
                        # - main n'th start, type=ebs - del snapshot_config
                        # - snapshot_config -> restore
                        # - create backup object on main 1'st start

                        postgresql_data['compat_prior_backup_restore'] = True
                        if postgresql_data.get(OPT_VOLUME_CNF):
                            postgresql_data['volume'] = storage2.volume(
                                postgresql_data.pop(OPT_VOLUME_CNF))

                        elif postgresql_data.get(OPT_SNAPSHOT_CNF):
                            postgresql_data['volume'] = storage2.volume(
                                type=postgresql_data[OPT_SNAPSHOT_CNF]['type'])

                        else:
                            raise HandlerError('No volume config or snapshot config provided')

                        if postgresql_data['volume'].device and \
                                        postgresql_data['volume'].type in ('ebs', 'csvol', 'cinder', 'raid'):
                            LOG.debug("Main n'th start detected. Removing snapshot config from message")
                            postgresql_data.pop(OPT_SNAPSHOT_CNF, None)

                        if postgresql_data.get(OPT_SNAPSHOT_CNF):
                            postgresql_data['restore'] = backup.restore(
                                type='snap_postgresql',
                                snapshot=postgresql_data.pop(OPT_SNAPSHOT_CNF),
                                volume=postgresql_data['volume'])

                        if int(postgresql_data['replication_main']):
                            postgresql_data['backup'] = backup.backup(
                                type='snap_postgresql',
                                volume=postgresql_data['volume'])

                    LOG.debug("Update postgresql config with %s", postgresql_data)
                    __postgresql__.update(postgresql_data)
                    __postgresql__['volume'].mpoint = __postgresql__['storage_dir']
                    __postgresql__['volume'].tags = self.resource_tags()
                    if 'backup' in __postgresql__:
                        __postgresql__['backup'].tags = self.resource_tags()


    def on_before_host_up(self, message):
        """
        Configure PostgreSQL behaviour
        @type message: scalarizr.messaging.Message      
        @param message: HostUp message
        """

        repl = 'main' if self.is_replication_main else 'subordinate'
        #bus.fire('before_postgresql_configure', replication=repl)
        
        if self.is_replication_main:
            self._init_main(message)                                    
        else:
            self._init_subordinate(message)
        # Force to resave volume settings
        __postgresql__['volume'] = storage2.volume(__postgresql__['volume'])
        bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl, preset=self.initial_preset)
                    
                
    def on_before_reboot_start(self, *args, **kwargs):
        """
        Stop PostgreSQL and unplug storage
        """
        self.postgresql.service.stop('rebooting')


    def on_BeforeHostTerminate(self, message):
        LOG.info('Handling BeforeHostTerminate message from %s' % message.local_ip)
        if message.local_ip == self._platform.get_private_ip():
            LOG.info('Stopping %s service' % BEHAVIOUR)
            self.postgresql.service.stop('Server will be terminated')
            if not self.is_replication_main:
                LOG.info('Destroying volume %s' % __postgresql__['volume'].id)
                __postgresql__['volume'].destroy(remove_disks=True)
                LOG.info('Volume %s has been destroyed.' % __postgresql__['volume'].id)
            else:
                __postgresql__['volume'].umount()


    def on_DbMsr_CreateDataBundle(self, message):
        
        try:
            op = operation(name=self._op_data_bundle, phases=[{
                'name': self._phase_data_bundle, 
                'steps': [self._step_create_data_bundle]
            }])
            op.define()
            
            with op.phase(self._phase_data_bundle):
                with op.step(self._step_create_data_bundle):
                    
                    bus.fire('before_postgresql_data_bundle')
                    snap = self._create_snapshot()
                    used_size = int(system2(('df', '-P', '--block-size=M', STORAGE_PATH))[0].split('\n')[1].split()[2][:-1])
                    bus.fire('postgresql_data_bundle', snapshot_id=snap.id)

                    # Notify scalr
                    msg_data = {
                    'db_type': BEHAVIOUR,
                    'status': 'ok',
                    'used_size' : '%.3f' % (float(used_size) / 1000,),
                    BEHAVIOUR: {OPT_SNAPSHOT_CNF: dict(snap)}
                    }
                    self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, msg_data)

            op.ok()

        except (Exception, BaseException), e:
            LOG.exception(e)
            
            # Notify Scalr about error
            self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, dict(
                db_type     = BEHAVIOUR,
                status      ='error',
                last_error  = str(e)
            ))
            

    def on_DbMsr_PromoteToMain(self, message):
        """
        Promote subordinate to main
        @type message: scalarizr.messaging.Message
        @param message: postgresql_PromoteToMain
        """
        LOG.debug("on_DbMsr_PromoteToMain")
        postgresql = message.body[BEHAVIOUR]

        if int(__postgresql__['replication_main']):
            LOG.warning('Cannot promote to main. Already main')
            return

        LOG.info('Starting Subordinate -> Main promotion')
        bus.fire('before_subordinate_promote_to_main')

        msg_data = {
            'db_type' : BEHAVIOUR,
            'status' : 'ok',
            BEHAVIOUR : {}
        }

        tx_complete = False

        new_vol = None
        if postgresql.get('volume_config'):
            new_vol = storage2.volume(postgresql.get('volume_config'))

        try:
            self.postgresql.stop_replication()

            if new_vol and new_vol.type not in ('eph', 'lvm'):
                self.postgresql.service.stop('Unplugging subordinate storage and then plugging main one')

                old_vol = storage2.volume(__postgresql__['volume'])
                old_vol.detach(force=True)

                new_vol.mpoint = __postgresql__['storage_dir']
                new_vol.ensure(mount=True)

                if not self.postgresql.cluster_dir.is_initialized(STORAGE_PATH):
                    raise HandlerError("%s is not a valid postgresql storage" % STORAGE_PATH)

                __postgresql__['volume'] = new_vol
                msg_data[BEHAVIOUR] = {'volume_config': dict(new_vol)}

            subordinates = [host.internal_ip for host in self._get_subordinate_hosts()]
            self.postgresql.init_main(STORAGE_PATH, self.root_password, subordinates)
            __postgresql__[OPT_REPLICATION_MASTER] = 1

            if not new_vol or new_vol.type in ('eph', 'lvm'):
                snap = self._create_snapshot()
                __postgresql__['snapshot'] = snap
                msg_data[BEHAVIOUR].update({OPT_SNAPSHOT_CNF : dict(snap)})

            msg_data[OPT_CURRENT_XLOG_LOCATION] = None # useless but required by Scalr

            self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)

            tx_complete = True
            bus.fire('subordinate_promote_to_main')

        except (Exception, BaseException), e:
            LOG.exception(e)

            self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, dict(
                db_type=BEHAVIOUR,
                status="error",
                last_error=str(e)
            ))

            self.postgresql.service.stop('Unplugging broken storage and then plugging the old one')
            if new_vol:
                new_vol.detach()
            # Get back subordinate storage
            if old_vol:
                old_vol.ensure(mount=True)
            self.postgresql.service.start()
        
        if tx_complete and new_vol and new_vol.type not in ('eph', 'lvm'):
            # Delete subordinate EBS
            old_vol.destroy(remove_disks=True)

    
    def on_DbMsr_NewMainUp(self, message):
        """
        Switch replication to a new main server
        @type message: scalarizr.messaging.Message
        @param message:  DbMsr_NewMainUp
        """
        try:
            assert message.body.has_key("db_type")
            assert message.body.has_key("local_ip")
            assert message.body.has_key("remote_ip")
            assert message.body.has_key(BEHAVIOUR)

            postgresql_data = message.body[BEHAVIOUR]

            if int(__postgresql__['replication_main']):
                LOG.debug('Skip NewMainUp. My replication role is main')
                return

            host = message.local_ip or message.remote_ip
            LOG.info("Switching replication to a new PostgreSQL main %s", host)
            bus.fire('before_postgresql_change_main', host=host)

            LOG.debug("__postgresql__['volume']: %s", __postgresql__['volume'])

            if __postgresql__['volume'].type in ('eph', 'lvm'):
                if 'restore' in postgresql_data:
                    restore = backup.restore(**postgresql_data['restore'])
                else:
                    restore = backup.restore(
                        type='snap_postgresql',
                        volume=__postgresql__['volume'],
                        snapshot=postgresql_data[OPT_SNAPSHOT_CNF])

                if __postgresql__['volume'].type == 'eph':
                    self.postgresql.service.stop('Swapping storages to reinitialize subordinate')

                    LOG.info('Reinitializing Subordinate from the new snapshot %s',
                        restore.snapshot['id'])
                    new_vol = restore.run()

                #self.postgresql.service.start()

            self.postgresql.init_subordinate(STORAGE_PATH, host, __postgresql__['port'], self.root_password)
            LOG.debug("Replication switched")
            bus.fire('postgresql_change_main', host=host)

            msg_data = dict(
                db_type = BEHAVIOUR,
                status = 'ok'
            )
            self.send_message(DbMsrMessages.DBMSR_NEW_MASTER_UP_RESULT, msg_data)

        except (Exception, BaseException), e:
            LOG.exception(e)

            msg_data = dict(
                db_type = BEHAVIOUR,
                status="error",
                last_error=str(e))
            self.send_message(DbMsrMessages.DBMSR_NEW_MASTER_UP_RESULT, msg_data)


    def on_DbMsr_CreateBackup(self, message):
        #TODO: Think how to move the most part of it into Postgresql class 
        # Retrieve password for scalr pg user
        tmpdir = backup_path = None
        try:
            # Get databases list
            psql = PSQL(user=self.postgresql.root_user.name)
            databases = psql.list_pg_databases()
            if 'template0' in databases:
                databases.remove('template0')
            
            
            op = operation(name=self._op_backup, phases=[{
                'name': self._phase_backup
            }])
            op.define()         
            
            with op.phase(self._phase_backup):
            
                if not os.path.exists(self._tmp_path):
                    os.makedirs(self._tmp_path)
                    
                # Defining archive name and path
                backup_filename = time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
                backup_path = os.path.join(self._tmp_path, backup_filename)
                
                # Creating archive 
                backup = tarfile.open(backup_path, 'w:gz')
    
                # Dump all databases
                LOG.info("Dumping all databases")
                tmpdir = tempfile.mkdtemp(dir=self._tmp_path)       
                chown_r(tmpdir, self.postgresql.root_user.name)

                def _single_backup(db_name):
                    dump_path = tmpdir + os.sep + db_name + '.sql'
                    pg_args = '%s %s --no-privileges -f %s' % (PG_DUMP, db_name, dump_path)
                    su_args = [SU_EXEC, '-', self.postgresql.root_user.name, '-c', pg_args]
                    err = system2(su_args)[1]
                    if err:
                        raise HandlerError('Error while dumping database %s: %s' % (db_name, err))
                    backup.add(dump_path, os.path.basename(dump_path))  

                make_backup_steps(databases, op, _single_backup)                        

                backup.close()
                
                with op.step(self._step_upload_to_cloud_storage):
                    # Creating list of full paths to archive chunks
                    if os.path.getsize(backup_path) > __postgresql__['pgdump_chunk_size']:
                        parts = [os.path.join(tmpdir, file) for file in split(backup_path, backup_filename, __postgresql__['pgdump_chunk_size'], tmpdir)]
                    else:
                        parts = [backup_path]
                    sizes = [os.path.getsize(file) for file in parts]
                        
                    cloud_storage_path = self._platform.scalrfs.backups(BEHAVIOUR)
                    LOG.info("Uploading backup to cloud storage (%s)", cloud_storage_path)
                    trn = transfer.Transfer()
                    cloud_files = trn.upload(parts, cloud_storage_path)
                    LOG.info("Postgresql backup uploaded to cloud storage under %s/%s",
                                    cloud_storage_path, backup_filename)
            
            result = list(dict(path=path, size=size) for path, size in zip(cloud_files, sizes))
            op.ok(data=result)
                
            # Notify Scalr
            self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
                db_type = BEHAVIOUR,
                status = 'ok',
                backup_parts = result
            ))
                        
        except (Exception, BaseException), e:
            LOG.exception(e)
            
            # Notify Scalr about error
            self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
                db_type = BEHAVIOUR,
                status = 'error',
                last_error = str(e)
            ))
            
        finally:
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)
            if backup_path and os.path.exists(backup_path):
                os.remove(backup_path)              
        
                                
    def _init_main(self, message):
        """
        Initialize postgresql main
        @type message: scalarizr.messaging.Message 
        @param message: HostUp message
        """
        
        LOG.info("Initializing PostgreSQL main")
        
        with bus.initialization_op as op:
            with op.step(self._step_create_storage):

                # Plug storage
                if 'restore' in __postgresql__ and\
                   __postgresql__['restore'].type == 'snap_postgresql':
                    __postgresql__['restore'].run()
                else:
                    if __node__['platform'] == 'idcf':
                        if __postgresql__['volume'].id:
                            LOG.info('Cloning volume to workaround reattachment limitations of IDCF')
                            __postgresql__['volume'].snap = __postgresql__['volume'].snapshot()

                    __postgresql__['volume'].ensure(mount=True, mkfs=True)
                    LOG.debug('Postgres volume config after ensure: %s', dict(__postgresql__['volume']))
                
            with op.step(self._step_init_main):
                self.postgresql.init_main(mpoint=STORAGE_PATH, password=self.root_password)
                

            with op.step(self._step_create_data_bundle):
                if 'backup' in __postgresql__:
                    __postgresql__['restore'] = __postgresql__['backup'].run()
            
            with op.step(self._step_collect_host_up_data):
                # Update HostUp message 
                msg_data = dict({OPT_REPLICATION_MASTER         :   str(int(self.is_replication_main)),
                                OPT_ROOT_USER               :   self.postgresql.root_user.name,
                                OPT_ROOT_PASSWORD           :   self.root_password,
                                OPT_ROOT_SSH_PRIVATE_KEY    :   self.postgresql.root_user.private_key,
                                OPT_ROOT_SSH_PUBLIC_KEY     :   self.postgresql.root_user.public_key,
                                OPT_CURRENT_XLOG_LOCATION   :   None})

                if __postgresql__['compat_prior_backup_restore']:
                    if 'restore' in __postgresql__:
                        msg_data.update(dict(
                            snapshot_config=dict(__postgresql__['restore'].snapshot)))
                    msg_data.update(dict(
                        volume_config=dict(__postgresql__['volume'])))
                else:
                    msg_data.update(dict(
                        volume=dict(__postgresql__['volume'])
                    ))
                    for key in ('backup', 'restore'):
                        if key in __postgresql__:
                            msg_data[key] = dict(__postgresql__[key])

                message.db_type = BEHAVIOUR
                message.postgresql = msg_data.copy()

                try:
                    del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
                except KeyError:
                    pass

                __postgresql__.update(msg_data)


    def _get_main_host(self):
        main_host = None
        LOG.info("Requesting main server")
        while not main_host:
            try:
                main_host = list(host 
                    for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts 
                    if host.replication_main)[0]
            except IndexError:
                LOG.debug("QueryEnv respond with no postgresql main. " +
                        "Waiting %d seconds before the next attempt", 5)
                time.sleep(5)
        return main_host
    
    def _get_subordinate_hosts(self):
        LOG.info("Requesting standby servers")
        return list(host for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts 
                if not host.replication_main)
                
    def _init_subordinate(self, message):
        """
        Initialize postgresql subordinate
        @type message: scalarizr.messaging.Message 
        @param message: HostUp message
        """
        LOG.info("Initializing postgresql subordinate")
        
        with bus.initialization_op as op:
            with op.step(self._step_create_storage):
                LOG.debug("Initialize subordinate storage")
                if 'restore' in __postgresql__ and\
                   __postgresql__['restore'].type == 'snap_postgresql':
                    __postgresql__['restore'].run()
                else:
                    __postgresql__['volume'].ensure(mount=True, mkfs=True)
            
            with op.step(self._step_init_subordinate):
                # Change replication main 
                main_host = self._get_main_host()
                        
                LOG.debug("Main server obtained (local_ip: %s, public_ip: %s)",
                        main_host.internal_ip, main_host.external_ip)
                
                host = main_host.internal_ip or main_host.external_ip
                self.postgresql.init_subordinate(STORAGE_PATH, host, __postgresql__['port'], self.root_password)
            
            with op.step(self._step_collect_host_up_data):
                # Update HostUp message
                message.db_type = BEHAVIOUR


    def _create_snapshot(self):
        LOG.info("Creating PostgreSQL data bundle")
        backup_obj = backup.backup(type='snap_postgresql',
            volume=__postgresql__['volume'],
            tags=self.resource_tags())
        restore = backup_obj.run()
        return restore.snapshot


    def _insert_iptables_rules(self):
        if iptables.enabled():
            iptables.FIREWALL.ensure([
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(__postgresql__['port'])},
            ])
