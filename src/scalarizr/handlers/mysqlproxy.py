from __future__ import with_statement
'''
Created on Sep 20, 2011

@author: Spike
'''

from __future__ import with_statement

import os
import time
import logging

from scalarizr import config
from scalarizr.bus import bus
from scalarizr.libs.metaconf import Configuration, NoPathError
from scalarizr.util import initdv2, software, system2
from scalarizr.messaging import Messages
from scalarizr.handlers import ServiceCtlHandler, DbMsrMessages
from scalarizr.config import BuiltinBehaviours

BEHAVIOUR = SERVICE_NAME = 'mysql_proxy'
CONFIG_FILE_PATH = '/etc/mysql_proxy.conf'
PID_FILE = '/var/run/mysql-proxy.pid'
NEW_MASTER_UP = "Mysql_NewMainUp"
LOG_FILE = '/var/log/mysql-proxy.log'

LOG = logging.getLogger(__name__)


def get_handlers():
    return (MysqlProxyHandler(),)

class MysqlProxyInitScript(initdv2.ParametrizedInitScript):


    def __init__(self):
        res = software.whereis('mysql-proxy')
        if not res:
            raise initdv2.InitdError("Mysql-proxy binary not found. Check your installation")
        self.bin_path = res[0]
        version_str = system2((self.bin_path, '-V'))[0].splitlines()[0]
        self.version = tuple(map(int, version_str.split()[1].split('.')))
        self.sock = initdv2.SockParam(4040)


    def status(self):
        if not os.path.exists(PID_FILE):
            return initdv2.Status.NOT_RUNNING

        with open(PID_FILE) as f:
            pid = int(f.read())

        try:
            os.kill(pid, 0)
        except OSError:
            try:
                os.remove(PID_FILE)
            except OSError:
                pass
            return initdv2.Status.NOT_RUNNING
        else:
            return initdv2.Status.RUNNING


    def start(self):
        if not self.running:
            LOG.debug('Starting mysql-proxy')
            pid = os.fork()
            if pid == 0:
                os.setsid()
                pid = os.fork()
                if pid != 0:
                    os._exit(0)

                os.chdir('/')
                os.umask(0)

                import resource     # Resource usage information.
                maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
                if (maxfd == resource.RLIM_INFINITY):
                    maxfd = 1024

                for fd in range(0, maxfd):
                    try:
                        os.close(fd)
                    except OSError:
                        pass

                os.open('/dev/null', os.O_RDWR)

                os.dup2(0, 1)
                os.dup2(0, 2)

                try:
                    os.execl(self.bin_path, 'mysql-proxy', '--defaults-file=' + CONFIG_FILE_PATH)
                except Exception:
                    os._exit(255)
            initdv2.wait_sock(self.sock)


    def stop(self):
        if self.running:
            LOG.debug('Stopping mysql-proxy')
            with open(PID_FILE) as f:
                pid = int(f.read())

            os.kill(pid, 15)

            # Check pid is dead
            for i in range(5):
                try:
                    os.kill(pid, 0)
                except OSError:
                    break
                else:
                    time.sleep(1)
            else:
                os.kill(pid, 9)


    def restart(self):
        self.stop()
        self.start()


    reload = restart


initdv2.explore(BEHAVIOUR, MysqlProxyInitScript)

def is_mysql_role(behaviours):
    return bool(set((BuiltinBehaviours.MYSQL,
               BuiltinBehaviours.MYSQL2,
               BuiltinBehaviours.PERCONA)).intersection(set(behaviours)))


class MysqlProxyHandler(ServiceCtlHandler):


    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.service = initdv2.lookup(BEHAVIOUR)
        self._service_name = BEHAVIOUR
        bus.on(init=self.on_init)


    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return message.behaviour and is_mysql_role(message.behaviour) and message.name in (
                    Messages.HOST_UP,
                    Messages.HOST_DOWN,
                    NEW_MASTER_UP,
                    DbMsrMessages.DBMSR_NEW_MASTER_UP
        )


    def on_init(self):
        bus.on(
            start=self.on_start,
            before_host_up=self.on_before_host_up,
            reload=self.on_reload
        )


    def on_reload(self):
        self._reload_backends()


    def on_start(self):
        cnf = bus.cnf
        if cnf.state == config.ScalarizrState.RUNNING:
            self._reload_backends()


    def on_before_host_up(self, msg):
        self._reload_backends()


    def _reload_backends(self):
        self._logger.info('Updating mysql-proxy backends list')
        self.config = Configuration('mysql')
        if os.path.exists(CONFIG_FILE_PATH):
            self.config.read(CONFIG_FILE_PATH)
            self.config.remove('./mysql-proxy/proxy-backend-addresses')
            self.config.remove('./mysql-proxy/proxy-read-only-backend-addresses')

        try:
            self.config.get('./mysql-proxy')
        except NoPathError:
            self.config.add('./mysql-proxy')

        queryenv = bus.queryenv_service
        roles = queryenv.list_roles()
        main = None
        subordinates = []

        for role in roles:
            if not is_mysql_role(role.behaviour):
                continue

            for host in role.hosts:
                ip = host.internal_ip or host.external_ip
                if host.replication_main:
                    main = ip
                else:
                    subordinates.append(ip)

        if main:
            self._logger.debug('Adding mysql main %s to  mysql-proxy defaults file', main)
            self.config.add('./mysql-proxy/proxy-backend-addresses', '%s:3306' % main)
        if subordinates:
            self._logger.debug('Adding mysql subordinates to  mysql-proxy defaults file: %s', ', '.join(subordinates))
            for subordinate in subordinates:
                self.config.add('./mysql-proxy/proxy-read-only-backend-addresses', '%s:3306' % subordinate)

        self.config.set('./mysql-proxy/pid-file', PID_FILE, force=True)
        self.config.set('./mysql-proxy/daemon', 'true', force=True)
        self.config.set('./mysql-proxy/log-file', LOG_FILE, force=True)
        if self.service.version > (0,8,0):
            self.config.set('./mysql-proxy/plugins', 'proxy', force=True)

        self._logger.debug('Saving new mysql-proxy defaults file')
        self.config.write(CONFIG_FILE_PATH)
        os.chmod(CONFIG_FILE_PATH, 0660)

        self.service.restart()


    def on_HostUp(self, message):
        self._reload_backends()

    on_DbMsr_NewMainUp = on_Mysql_NewMainUp = on_HostDown = on_HostUp
