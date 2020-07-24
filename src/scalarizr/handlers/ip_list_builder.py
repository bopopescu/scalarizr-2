from __future__ import with_statement
'''
Created on Dec 11, 2009

@author: Dmytro Korsakov
@author: marat
'''

from __future__ import with_statement

# Core
from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.messaging import Messages
from scalarizr.config import BuiltinBehaviours, ScalarizrState

# Stdlibs
import logging, os
import shutil


# TODO: Configurator
# TODO: handle IPAddressChanged


def get_handlers ():
    return [IpListBuilder()]

class IpListBuilder(Handler):
    name = "ip_list_builder"
    _logger = None
    _base_path = None

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        bus.on(init=self.on_init, reload=self.on_reload)
        self.on_reload()

    def on_init(self, *args, **kwargs):
        bus.on("start", self.on_start)
        bus.on("before_host_up", self.on_before_host_up)

    def on_reload(self):
        self._queryenv = bus.queryenv_service
        cnf = bus.cnf; ini = cnf.rawini
        self._base_path = ini.get(self.name, "base_path")
        self._base_path = self._base_path.replace('$etc_path', bus.etc_path)
        self._base_path = os.path.normpath(self._base_path)

    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return message.name == Messages.HOST_UP \
                or message.name == Messages.HOST_DOWN \
                or message.name == Messages.REBOOT_START \
                or message.name == Messages.REBOOT_FINISH \
                or message.name == 'Mysql_NewMainUp'

    def on_start(self, *args):
        cnf = bus.cnf
        if cnf.state == ScalarizrState.RUNNING:
            self._rebuild()

    def on_before_host_up(self, *args):
        self._rebuild()

    def _rebuild(self):
        """
        Build current hosts structure on farm
        """
        self._logger.debug('Rebuild farm hosts structure')
        if os.path.exists(self._base_path):
            shutil.rmtree(self._base_path)

        for role in self._queryenv.list_roles():
            for host in role.hosts:
                self._modify_tree(
                        role.name,
                        role.behaviour,
                        host.internal_ip or host.external_ip,
                        modfn=self._create_file,
                        replication_main=host.replication_main
                )

    def on_HostUp(self, message):
        behaviour = message.behaviour
        ip = message.local_ip or message.remote_ip
        rolename = message.role_name

        if ip and rolename and behaviour:
            self._logger.debug("Add host (role_name: %s, behaviour: %s, ip: %s)",
                            rolename, behaviour, ip)
            self._modify_tree(rolename, behaviour, ip,
                            modfn=self._create_file,
                            replication_main='mysql' in behaviour and self._host_is_replication_main(ip, 'mysql'))

    def on_HostDown(self, message):
        behaviour = message.behaviour
        ip = message.local_ip or message.remote_ip
        rolename = message.role_name

        if ip and rolename and behaviour:
            self._logger.debug("Remove host (role_name: %s, behaviour: %s, ip: %s)",
                                            rolename, behaviour, ip)
            self._modify_tree(rolename, behaviour, ip,
                            modfn=self._remove_file,
                            replication_main='mysql' in behaviour and self._host_is_replication_main(ip, 'mysql'))

    def on_Mysql_NewMainUp(self, message):
        ip = message.local_ip or message.remote_ip
        if ip:
            self._remove_file(os.path.join(self._base_path, 'mysql-subordinate', ip))

            main_path = os.path.join(self._base_path, 'mysql-main')
            if os.path.exists(main_path):
                shutil.rmtree(main_path)
            self._create_dir(main_path)
            self._create_file(os.path.join(main_path, ip))

    on_RebootStart = on_HostDown

    on_RebootFinish = on_HostUp

    def _modify_tree(self, rolename, behaviours, ip, modfn=None, replication_main=None):
        # Touch/Unlink %role_name%/xx.xx.xx.xx
        modfn(os.path.join(self._base_path, rolename, ip))

        for behaviour in behaviours:
            if behaviour == BuiltinBehaviours.MYSQL:
                suffix = "main" if replication_main else "subordinate"
                # Touch/Unlink mysql-(main|subordinate)/xx.xx.xx.xx
                mysql_path = os.path.join(self._base_path, "mysql-" + suffix)
                modfn(os.path.join(mysql_path, ip))
            else:
                # Touch/Unlink %behaviour%/xx.xx.xx.xx
                modfn(os.path.join(self._base_path, behaviour, ip))

    def _create_dir(self, d):
        if not os.path.exists(d):
            try:
                self._logger.debug("Create dir %s", d)
                os.makedirs(d, 0755)
            except OSError, x:
                self._logger.exception(x)

    def _create_file(self, f):
        self._create_dir(os.path.dirname(f))
        try:
            self._logger.debug("Touch file %s", f)
            open(f, 'w').close()
            os.chmod(f, 0644)
        except OSError, x:
            self._logger.error(x)

    def _remove_dir(self, d):
        if os.path.exists(d) and not os.listdir(d):
            try:
                self._logger.debug("Remove dir %s", d)
                os.rmdir(d)
            except OSError, x:
                self._logger.error(x)

    def _remove_file(self, f):
        if os.path.exists(f):
            try:
                self._logger.debug("Remove file %s", f)
                os.remove(f)
            except OSError, x:
                self._logger.error(x)
        self._remove_dir(os.path.dirname(f))

    def _host_is_replication_main(self, ip, behaviour):
        try:
            received_roles = self._queryenv.list_roles(behaviour=behaviour)
        except:
            self._logger.error('Can`t retrieve list of roles from Scalr.')
            raise

        for role in received_roles:
            for host in role.hosts:
                if ip == host.internal_ip:
                    return host.replication_main
        return False
