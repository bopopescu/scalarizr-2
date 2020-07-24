'''
Created on Aug 1, 2012

@author: dmitry
'''

from __future__ import with_statement

import os
import sys
import time
import logging
import threading
from scalarizr import config
from scalarizr.bus import bus
from scalarizr import handlers, rpc
from scalarizr.linux import iptables
from scalarizr.util import system2, PopenError
from scalarizr.services import redis as redis_service
from scalarizr.handlers import redis as redis_handler
from scalarizr.services.redis import __redis__
from scalarizr.util.cryptotool import pwgen


BEHAVIOUR = CNF_SECTION = redis_handler.CNF_SECTION
OPT_REPLICATION_MASTER = redis_handler.OPT_REPLICATION_MASTER
OPT_PERSISTENCE_TYPE = redis_handler.OPT_PERSISTENCE_TYPE
STORAGE_PATH = redis_handler.STORAGE_PATH
DEFAULT_PORT = redis_service.DEFAULT_PORT
BIN_PATH = redis_service.BIN_PATH
DEFAULT_CONF_PATH = redis_service.DEFAULT_CONF_PATH


LOG = logging.getLogger(__name__)


class RedisAPI(object):

    _cnf = None
    _queryenv = None

    def __init__(self):
        self._cnf = bus.cnf
        self._queryenv = bus.queryenv_service
        ini = self._cnf.rawini
        self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)



    @rpc.service_method
    def launch_processes(self, num=None, ports=None, passwords=None, async=False):
        if ports and passwords and len(ports) != len(passwords):
            raise AssertionError('Number of ports must be equal to number of passwords')
        if num and ports and num != len(ports):
            raise AssertionError('When ports range is passed its length must be equal to num parameter')
        if not self.is_replication_main:
            if not passwords or not ports:
                raise AssertionError('ports and passwords are required to launch processes on redis subordinate')
        available_ports = self.available_ports
        if num > len(available_ports):
            raise AssertionError('Cannot launch %s new processes: Ports available: %s' % (num, str(available_ports)))

        if ports:
            for port in ports:
                if port not in available_ports:
                    raise AssertionError('Cannot launch Redis process on port %s: Already running' % port)
        else:
            ports = available_ports[:num]

        if async:
            txt = 'Launch Redis processes'
            op = handlers.operation(name=txt)
            def block():
                op.define()
                with op.phase(txt):
                    with op.step(txt):
                        result = self._launch(ports, passwords, op)
                op.ok(data=dict(ports=result[0], passwords=result[1]))
            threading.Thread(target=block).start()
            return op.id

        else:
            result = self._launch(ports, passwords)
            return dict(ports=result[0], passwords=result[1])


    @rpc.service_method
    def shutdown_processes(self, ports, remove_data=False, async=False):
        if async:
            txt = 'Shutdown Redis processes'
            op = handlers.operation(name=txt)
            def block():
                op.define()
                with op.phase(txt):
                    with op.step(txt):
                        self._shutdown(ports, remove_data, op)
                op.ok()
            threading.Thread(target=block).start()
            return op.id
        else:
            return self._shutdown(ports, remove_data)


    @rpc.service_method
    def list_processes(self):
        return self.get_running_processes()


    def _launch(self, ports=[], passwords=[], op=None):
        LOG.debug('Launching redis processes on ports %s with passwords %s' % (ports, passwords))
        is_replication_main = self.is_replication_main

        primary_ip = self.get_primary_ip()
        assert primary_ip is not None

        new_passwords = []
        new_ports = []



        for port,password in zip(ports, passwords or [None for port in ports]):
            if op:
                op.step('Launch Redis %s on port %s' % ('Main' if is_replication_main else 'Subordinate', port))
            try:
                if op:
                    op.__enter__()

                if iptables.enabled():
                    iptables.FIREWALL.ensure({
                            "jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": port
                    })


                redis_service.create_redis_conf_copy(port)
                redis_process = redis_service.Redis(is_replication_main, self.persistence_type, port, password)

                if not redis_process.service.running:
                    LOG.debug('Launch Redis %s on port %s' % ('Main' if is_replication_main else 'Subordinate', port))
                    if is_replication_main:
                        current_password = redis_process.init_main(STORAGE_PATH)
                    else:
                        current_password = redis_process.init_subordinate(STORAGE_PATH, primary_ip, port)
                    new_passwords.append(current_password)
                    new_ports.append(port)
                    LOG.debug('Redis process has been launched on port %s with password %s' % (port, current_password))

                else:
                    raise BaseException('Cannot launch redis on port %s: the process is already running' % port)

            except:
                if op:
                    op.__exit__(sys.exc_info())
                raise
            finally:
                if op:
                    op.__exit__(None)
        return (new_ports, new_passwords)


    def _shutdown(self, ports, remove_data=False, op=None):
        is_replication_main = self.is_replication_main
        freed_ports = []
        for port in ports:
            if op:
                msg = 'Shutdown Redis %s on port %s' % ('Main' if is_replication_main else 'Subordinate', port)
                op.step(msg)
            try:
                if op:
                    op.__enter__()
                LOG.debug('Shutting down redis instance on port %s' % (port))
                instance = redis_service.Redis(port=port)
                if instance.service.running:
                    password = instance.redis_conf.requirepass
                    instance.password = password
                    LOG.debug('Dumping redis data on disk using password %s from config file %s' % (password, instance.redis_conf.path))
                    instance.redis_cli.save()
                    LOG.debug('Stopping the process')
                    instance.service.stop()
                    freed_ports.append(port)
                if remove_data and os.path.exists(instance.db_path):
                    os.remove(instance.db_path)
            except:
                if op:
                    op.__exit__(sys.exc_info())
                raise
            finally:
                if op:
                    op.__exit__(None)
        return dict(ports=freed_ports)


    @property
    def busy_ports(self):
        redis_service.get_busy_ports()


    @property
    def available_ports(self):
        redis_service.get_available_ports()


    def get_running_processes(self):
        processes = {}
        ports = []
        passwords = []
        for port in self.busy_ports:
            conf_path = redis_service.get_redis_conf_path(port)

            if port == redis_service.DEFAULT_PORT:
                args = ('ps', '-G', 'redis', '-o', 'command', '--no-headers')
                out = system2(args, silent=True)[0].split('\n')
                try:
                    p = [x for x in out if x and BIN_PATH in x and redis_service.DEFAULT_CONF_PATH in x]
                except PopenError:
                    p = []
                if p:
                    conf_path = redis_service.DEFAULT_CONF_PATH

            LOG.debug('Got config path %s for port %s' % (conf_path, port))
            redis_conf = redis_service.RedisConf(conf_path)
            password = redis_conf.requirepass
            processes[port] = password
            ports.append(port)
            passwords.append(password)
            LOG.debug('Redis config %s has password %s' % (conf_path, password))
        return dict(ports=ports, passwords=passwords)


    @property
    def is_replication_main(self):
        value = 0
        if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_REPLICATION_MASTER):
            value = self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)
        res = True if int(value) else False
        LOG.debug('is_replication_main: %s' % res)
        return res


    @property
    def persistence_type(self):
        value = 'snapshotting'
        if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_PERSISTENCE_TYPE):
            value = self._cnf.rawini.get(CNF_SECTION, OPT_PERSISTENCE_TYPE)
        LOG.debug('persistence_type: %s' % value)
        return value


    def get_primary_ip(self):
        main_host = None
        LOG.info("Requesting main server")
        while not main_host:
            try:
                main_host = list(host
                        for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts
                        if host.replication_main)[0]
            except IndexError:
                LOG.debug("QueryEnv respond with no %s main. " % BEHAVIOUR +
                                "Waiting %d seconds before the next attempt" % 5)
                time.sleep(5)
        host = main_host.internal_ip or main_host.external_ip
        LOG.debug('primary IP: %s' % host)
        return host


    @rpc.service_method
    def reset_password(self, port=DEFAULT_PORT, new_password=None):
        """ Reset auth for Redis process on port `port`. Return new password """
        if not new_password:
            new_password = pwgen(20)

        redis_conf = redis_service.RedisConf.find(port=port)
        redis_conf.requirepass = new_password

        if redis_conf.subordinateof:
            redis_conf.mainauth = new_password

        redis_wrapper = redis_service.Redis(port=port)
        redis_wrapper.service.reload()

        if int(port) == DEFAULT_PORT:
            __redis__['main_password'] = new_password

        return new_password


    @rpc.service_method
    def replication_status(self):
        ri = redis_service.RedisInstances()

        if ri.main:
            mains = {}
            for port in ri.ports:
                mains[port] = {'status':'up'}
            return {'mains': mains}

        subordinates = {}
        for redis_process in ri.instances:
            repl_data = {}
            for key, val in redis_process.redis_cli.info.items():
                if key.startswith('main'):
                    repl_data[key] = val
            repl_data['status'] = repl_data['main_link_status']
            subordinates[redis_process.port] = repl_data

        return {'subordinates': subordinates}
