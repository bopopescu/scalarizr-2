from __future__ import with_statement

import os
import sys
import re
import logging
import subprocess
import threading
import time
import signal
import errno

from scalarizr import linux, storage2
from scalarizr.storage2 import cloudfs
from scalarizr.linux import coreutils, pkgmgr
from scalarizr.node import __node__
from scalarizr.services import mysql as mysql_svc
from scalarizr.services import backup
from scalarizr.libs import bases
from scalarizr.handlers import transfer_result_to_backup_result


LOG = logging.getLogger(__name__)


class Error(Exception):
    pass


__behavior__ = 'percona' \
            if 'percona' in __node__['behavior'] \
            else 'mysql2'


__mysql__ = __node__[__behavior__]
__mysql__.update({
    'behavior': __behavior__,
    'port': 3306,
    'storage_dir': '/mnt/dbstorage',
    'data_dir': '/mnt/dbstorage/mysql-data',
    'binlog_dir': '/mnt/dbstorage/mysql-misc',
    'tmp_dir': '/mnt/dbstorage/tmp',
    'root_user': 'scalr',
    'repl_user': 'scalr_repl',
    'stat_user': 'scalr_stat',
    'pma_user': 'pma',
    'debian.cnf': '/etc/mysql/debian.cnf',
    'my.cnf': '/etc/my.cnf' if linux.os['family'] in ('RedHat', 'Oracle') else '/etc/mysql/my.cnf',
    'mysqldump_chunk_size': 200,
    'stop_subordinate_timeout': 180,
    'change_main_timeout': 60,
    'defaults': {
        'datadir': '/var/lib/mysql',
        'log_bin': 'mysql_bin'
    }
})


# TODO: move
def eradicate(process):
    """
    Kill process tree.

    :param process: pid (int) or subprocess.Popen instance

    """

    class Victim(object):

        def __init__(self, process):
            self._obj = process

        @property
        def pid(self):
            return self._obj if isinstance(self._obj, int) else \
                   self._obj.pid

        def get_children(self):
            try:
                pgrep = linux.system(linux.build_cmd_args(
                    executable="pgrep",
                    short=["-P"],
                    params=[str(self.pid)]))
            except linux.LinuxError:
                children = []
            else:
                children = map(int, pgrep[0].splitlines())
            return children

        def die(self, grace=2):
            if isinstance(self._obj, subprocess.Popen):
                try:
                    self._obj.terminate()
                    time.sleep(grace)
                    self._obj.kill()
                except OSError, e:
                    if e.errno == errno.ESRCH:
                        pass  # no such process
                    else:
                        LOG.debug("Failed to stop pid %s" % self.pid)
                time.sleep(0.1)
                self._obj.poll()  # avoid leaving defunct processes
            else:
                try:
                    os.kill(self.pid, signal.SIGTERM)
                    time.sleep(grace)
                    os.kill(self.pid, signal.SIGKILL)
                except OSError, e:
                    if e.errno == errno.ESRCH:
                        pass  # no such process
                    else:
                        LOG.debug("Failed to stop pid %s" % self.pid)

    victim = Victim(process)
    children = victim.get_children()
    victim.die()
    map(eradicate, children)


class MySQLSnapBackup(backup.SnapBackup):
    def __init__(self, **kwds):
        super(MySQLSnapBackup, self).__init__(**kwds)
        self.on(
            freeze=self.freeze,
            unfreeze=self.unfreeze
        )
        self._mysql_init = mysql_svc.MysqlInitScript()

    def _client(self):
        return mysql_svc.MySQLClient(
                    __mysql__['root_user'],
                    __mysql__['root_password'])

    def freeze(self, volume, state):
        self._mysql_init.start()
        client = self._client()
        client.lock_tables()
        coreutils.sync()
        if int(__mysql__['replication_main']):
            (log_file, log_pos) = client.main_status()
        else:
            subordinate_status = client.subordinate_status()
            log_pos = subordinate_status['Exec_Main_Log_Pos']
            log_file = subordinate_status['Main_Log_File']

        upd = {'log_file': log_file, 'log_pos': log_pos}
        state.update(upd)
        self.tags.update(upd)

    def unfreeze(self, *args):
        client = self._client()
        client.unlock_tables()


class MySQLSnapRestore(backup.SnapRestore):
    def __init__(self, **kwds):
        super(MySQLSnapRestore, self).__init__(**kwds)
        self.on(complete=self.complete)

    def complete(self, volume):
        vol = storage2.volume(volume)
        vol.mpoint = __mysql__['storage_dir']
        vol.mount()


backup.backup_types['snap_mysql'] = MySQLSnapBackup
backup.restore_types['snap_mysql'] = MySQLSnapRestore


class XtrabackupMixin(object):
    def __init__(self):
        self.error_messages.update({
            'invalid_backup_type': "Invalid backup type: %s. "
                                "Supported values are 'full' | 'incremental'"
        })

    def _check_backup_type(self):
        msg = self.error_messages['invalid_backup_type'] % self.backup_type
        assert self.backup_type in ('full', 'incremental', 'auto'), msg

    def _client(self):
        return mysql_svc.MySQLClient(
                    __mysql__['root_user'],
                    __mysql__['root_password'])


class XtrabackupStreamBackup(XtrabackupMixin, backup.Backup):
    def __init__(self,
                backup_type='full',
                no_lock=False,
                from_lsn=None,
                compressor=None,
                prev_cloudfs_source=None,
                cloudfs_target=None,
                **kwds):
        backup.Backup.__init__(self,
                backup_type=backup_type,
                no_lock=no_lock,
                from_lsn=int(from_lsn or 0),
                compressor=compressor,
                prev_cloudfs_source=prev_cloudfs_source,
                cloudfs_target=cloudfs_target,
                **kwds)
        XtrabackupMixin.__init__(self)
        self._re_lsn = re.compile(r"xtrabackup: The latest check point " \
                            "\(for incremental\): '(\d+)'")
        self._re_lsn_51 = re.compile(r"xtrabackup: The latest check point "
                            "\(for incremental\): '(\d+:\d+)'")
        self._re_binlog = re.compile(r"innobackupex: MySQL binlog position: " \
                            "filename '([^']+)', position (\d+)")
        self._re_subordinate_binlog = re.compile(r"innobackupex: MySQL subordinate binlog position: " \
                            "main host '[^']+', filename '([^']+)', position (\d+)")
        self._re_lsn_innodb_stat = re.compile(r"Log sequence number \d+ (\d+)")

        self._killed = False
        self._xbak = None
        self._transfer = None
        self._xbak_init_lock = threading.Lock()

    def _run(self):
        self._check_backup_type()

        kwds = {
            'stream': 'xbstream',
            # Compression is broken
            #'compress': True,
            #'compress_threads': os.sysconf('SC_NPROCESSORS_ONLN'),
            'user': __mysql__['root_user'],
            'password': __mysql__['root_password']
        }
        if self.no_lock:
            kwds['no_lock'] = True
        if not int(__mysql__['replication_main']):
            kwds['safe_subordinate_backup'] = True
            kwds['subordinate_info'] = True

        current_lsn = None
        if self.backup_type == 'auto':
            client = self._client()
            innodb_stat = client.fetchone('SHOW INNODB STATUS')[0]
            for line in innodb_stat.splitlines():
                m = self._re_lsn_innodb_stat.search(line)
                if m:
                    current_lsn = int(m.group(1))
                    break

        if self.backup_type in ('incremental', 'auto'):
            if self.prev_cloudfs_source:
                # Download manifest and get it's to_lsn
                mnf = cloudfs.Manifest(cloudfs_path=self.prev_cloudfs_source)
                self.from_lsn = mnf.meta['to_lsn']
            else:
                self._check_attr('from_lsn')
            if self.backup_type == 'incremental' or \
                (self.backup_type == 'auto' and current_lsn and current_lsn >= self.from_lsn):
                kwds.update({
                    'incremental': True,
                    'incremental_lsn': self.from_lsn
                })
        LOG.debug('self._config: %s', self._config)
        LOG.debug('kwds: %s', kwds)

        if self.backup_type == 'incremental':
            LOG.info('Creating incremental xtrabackup (from LSN: %s)', self.from_lsn)
        else:
            LOG.info('Creating full xtrabackup')

        with self._xbak_init_lock:
            if self._killed:
                raise Error("Canceled")
            self._xbak = innobackupex.args(__mysql__['tmp_dir'], **kwds).popen()
            LOG.debug('Creating LargeTransfer, src=%s dst=%s', self._xbak.stdout,
                self.cloudfs_target)
            self._transfer = cloudfs.LargeTransfer(
                        [self._xbak.stdout],
                        self.cloudfs_target,
                        compressor=self.compressor)
        manifesto = self._transfer.run()
        if self._killed:
            raise Error("Canceled")
        stderr = self._xbak.communicate()[1]
        if self._xbak.returncode:
            raise Error(stderr)

        with self._xbak_init_lock:
            self._xbak = None
            self._transfer = None

        log_file = log_pos = to_lsn = None
        re_binlog = self._re_binlog \
                    if int(__mysql__['replication_main']) else \
                    self._re_subordinate_binlog
        for line in stderr.splitlines():
            m = self._re_lsn.search(line) or self._re_lsn_51.search(line)
            if m:
                to_lsn = m.group(1)
                continue
            m = re_binlog.search(line)
            if m:
                log_file = m.group(1)
                log_pos = int(m.group(2))
                continue
            if log_file and log_pos and to_lsn:
                break

        rst = backup.restore(type='xtrabackup',
                backup_type=self.backup_type,
                from_lsn=self.from_lsn,
                to_lsn=to_lsn,
                cloudfs_source=manifesto.cloudfs_path,
                prev_cloudfs_source=self.prev_cloudfs_source,
                log_file=log_file,
                log_pos=log_pos)

        # Update manifest
        LOG.debug('rst: %s', dict(rst))
        manifesto.meta = dict(rst)
        manifesto.save()

        LOG.info('Created %s xtrabackup. (LSN: %s..%s, log_file: %s, log_pos: %s)',
                rst.backup_type, rst.from_lsn, rst.to_lsn, rst.log_file, rst.log_pos)

        return rst

    def _kill(self):
        LOG.debug("Killing XtrabackupStreamBackup")
        self._killed = True

        with self._xbak_init_lock:
            if self._transfer:
                self._transfer.kill()
            if self._xbak:
                self._xbak_kill()

    def _xbak_kill(self):
        """ Popen.kill() is not enough when one of the children gets hung """

        LOG.debug("Killing process tree of pid %s" % self._xbak.pid)
        eradicate(self._xbak)

        # sql-subordinate not running? run
        if not int(__mysql__['replication_main']):
            try:
                self._client().start_subordinate_io_thread()
            except:
                LOG.warning('Cannot start subordinate io thread', exc_info=sys.exc_info())


class XtrabackupStreamRestore(XtrabackupMixin, backup.Restore):
    def __init__(self,
                cloudfs_source=None,
                prev_cloudfs_source=None,
                **kwds):
        backup.Restore.__init__(self,
                cloudfs_source=cloudfs_source,
                prev_cloudfs_source=prev_cloudfs_source,
                **kwds)
        XtrabackupMixin.__init__(self)
        self._mysql_init = mysql_svc.MysqlInitScript()

    def _run(self):
        # Apply resource's meta
        mnf = cloudfs.Manifest(cloudfs_path=self.cloudfs_source)
        bak = backup.restore(**mnf.meta)

        incrementals = []
        if bak.backup_type == 'incremental':
            incrementals = [bak]
            while bak.prev_cloudfs_source:
                tmpmnf = cloudfs.Manifest(cloudfs_path=bak.prev_cloudfs_source)
                bak = backup.restore(**tmpmnf.meta)
                if bak.backup_type == 'incremental':
                    incrementals.insert(0, bak)
        self.incrementals = incrementals
        if self.incrementals:
            self.log_file = self.incrementals[-1].log_file
            self.log_pos = self.incrementals[-1].log_pos
        else:
            self.log_file = bak.log_file
            self.log_pos = bak.log_pos

        coreutils.clean_dir(__mysql__['data_dir'])

        LOG.info('Downloading the base backup (LSN: 0..%s)', bak.to_lsn)
        trn = cloudfs.LargeTransfer(
                bak.cloudfs_source,
                __mysql__['data_dir'],
                streamer=xbstream.args(
                        extract=True,
                        directory=__mysql__['data_dir']))
        trn.run()

        LOG.info('Preparing the base backup')
        innobackupex(__mysql__['data_dir'],
                apply_log=True,
                redo_only=True,
                user=__mysql__['root_user'],
                password=__mysql__['root_password'])

        if incrementals:
            inc_dir = os.path.join(__mysql__['tmp_dir'], 'xtrabackup-restore-inc')
            i = 0
            for inc in incrementals:
                try:
                    os.makedirs(inc_dir)
                    inc = backup.restore(inc)
                    LOG.info('Downloading incremental backup #%d (LSN: %s..%s)', i,
                            inc.from_lsn, inc.to_lsn)
                    trn = cloudfs.LargeTransfer(
                            inc.cloudfs_source,
                            inc_dir,
                            streamer=xbstream.args(
                                    extract=True,
                                    directory=inc_dir))

                    trn.run()  # todo: Largetransfer should support custom decompressor proc
                    LOG.info('Preparing incremental backup #%d', i)
                    innobackupex(__mysql__['data_dir'],
                            apply_log=True,
                            redo_only=True,
                            incremental_dir=inc_dir,
                            user=__mysql__['root_user'],
                            password=__mysql__['root_password'])
                    i += 1
                finally:
                    coreutils.remove(inc_dir)

        LOG.info('Preparing the full backup')
        innobackupex(__mysql__['data_dir'],
                apply_log=True,
                user=__mysql__['root_user'],
                password=__mysql__['root_password'])
        coreutils.chown_r(__mysql__['data_dir'], 'mysql', 'mysql')

        self._mysql_init.start()
        if int(__mysql__['replication_main']):
            LOG.info("Main will reset it's binary logs, "
                    "so updating binary log position in backup manifest")
            log_file, log_pos = self._client().main_status()
            meta = mnf.meta
            meta.update({'log_file': log_file, 'log_pos': log_pos})
            mnf.meta = meta
            mnf.save()


backup.backup_types['xtrabackup'] = XtrabackupStreamBackup
backup.restore_types['xtrabackup'] = XtrabackupStreamRestore


class MySQLDumpBackup(backup.Backup):
    '''
    Example:
        bak = backup.backup(
                type='mysqldump',
                cloudfs_dir='s3://scalr-1a8f341e/backups/mysql/1265/')
        bak.run()
    '''

    def __init__(self,
                cloudfs_dir=None,
                file_per_database=True,
                chunk_size=None,
                **kwds):
        super(MySQLDumpBackup, self).__init__(cloudfs_dir=cloudfs_dir,
                file_per_database=file_per_database,
                chunk_size=chunk_size or __mysql__['mysqldump_chunk_size'],
                **kwds)
        self.features.update({
            'start_subordinate': False
        })
        self.transfer = None
        self._popens = []
        self._killed = False
        self._run_lock = threading.Lock()
        self._popen_creation_lock = threading.Lock()

    def _run(self):
        LOG.debug("Running MySQLDumpBackup")
        client = mysql_svc.MySQLClient(
                    __mysql__['root_user'],
                    __mysql__['root_password'])
        self._databases = client.list_databases()

        with self._run_lock:
            if self._killed:
                raise Error("Canceled")
            self.transfer = cloudfs.LargeTransfer(self._gen_src, self._dst,
                                    streamer=None, chunk_size=self.chunk_size)
        result = self.transfer.run()
        if not result:
            raise Error("Error while transfering to cloud storage")

        def log_stderr(popen):
            LOG.debug("mysqldump log_stderr communicate")
            out, err = popen.communicate()
            LOG.debug("mysqldump log_stderr communicate done")
            if err:
                LOG.debug("mysqldump stderr: %s", err)
        map(log_stderr, self._popens)

        if self._killed:
            raise Error("Canceled")

        result = transfer_result_to_backup_result(result)
        return result

    def _gen_src(self):
        if self.file_per_database:
            for db_name in self._databases:
                self._current_db = db_name
                params = __mysql__['mysqldump_options'].split() + [db_name]
                _mysqldump.args(*params)
                with self._popen_creation_lock:
                    if self._killed:
                        return
                    popen = _mysqldump.popen(stdin=None, bufsize=-1)
                    self._popens.append(popen)
                stream = popen.stdout
                yield cloudfs.NamedStream(stream, db_name)
        else:
            params = __mysql__['mysqldump_options'].split() + ['--all-databases']
            _mysqldump.args(*params)
            with self._popen_creation_lock:
                if self._killed:
                    return
                popen = _mysqldump.popen(stdin=None, bufsize=-1)
                self._popens.append(popen)
            yield popen.stdout

    @property
    def _dst(self):
        return os.path.join(self.cloudfs_dir, 'mysql')

    def _kill(self):
        LOG.debug("Killing MySQLDumpBackup")
        with self._popen_creation_lock:
            self._killed = True

        with self._run_lock:
            if self.transfer:
                self.transfer.kill()
            map(eradicate, self._popens)
        LOG.debug("...killed MySQLDumpBackup")


backup.backup_types['mysqldump'] = MySQLDumpBackup


class User(bases.ConfigDriven):
    def __init__(self, user=None, password=None, privileges='*'):
        pass

    def ensure(self):
        pass

    def exists(self):
        pass

    def delete(self):
        pass


class Exec(object):

    executable = None
    package = None

    def __init__(self, executable, package=None):
        assert isinstance(executable, basestring)
        self.executable = executable
        self.package = package
        self.cmd = None
        LOG.debug('Exec[%s] package=%s', self.executable, self.package)

    def check(self):
        if not os.access(self.executable, os.X_OK):
            if self.package:
                pkgmgr.installed(self.package)
            else:
                msg = 'Executable %s is not found, you should either ' \
                    'specify `package` attribute or install the software ' \
                    'manually' % (self.executable)
                raise linux.LinuxError(msg)

    def args(self, *params, **long_kwds):
        self.cmd = linux.build_cmd_args(
            executable=self.executable,
            long=long_kwds,
            params=params)
        LOG.debug('cmd: %s', self.cmd)
        return self

    def popen(self, **kwds):
        self.check()
        kwds['close_fds'] = True
        if not 'stdin' in kwds:
            kwds['stdin'] = subprocess.PIPE
        if not 'stdout' in kwds:
            kwds['stdout'] = subprocess.PIPE
        if not 'stderr' in kwds:
            kwds['stderr'] = subprocess.PIPE
        return subprocess.Popen(self.cmd, **kwds)

    def __call__(self, *params, **long_kwds):
        self.args(*params, **long_kwds)
        self.check()
        return linux.system(self.cmd)


_mysqldump = Exec("/usr/bin/mysqldump")


class PerconaExec(Exec):

    def check(self):
        if linux.os['family'] in ('RedHat', 'Oracle'):
            # Avoid "Can't locate Time/HiRes.pm in @INC"
            # with InnoDB Backup Utility v1.5.1-xtrabackup
            pkgmgr.installed('perl-Time-HiRes')         

        mgr = pkgmgr.package_mgr()
        if not 'percona' in mgr.repos():
            if linux.os['family'] in ('RedHat', 'Oracle'):
                url = 'http://www.percona.com/downloads/percona-release/percona-release-0.0-1.%s.rpm' % linux.os['arch']
                pkgmgr.RpmPackageMgr().install(url)
            else:
                codename = linux.ubuntu_release_to_codename[linux.os['lsb_release']]
                pkgmgr.apt_source(
                        'percona.list', 
                        ['deb http://repo.percona.com/apt %s main' % codename],
                        gpg_keyserver='hkp://keys.gnupg.net',
                        gpg_keyid='CD2EFD2A')
            mgr.updatedb()


        return super(PerconaExec, self).check()


innobackupex = PerconaExec('/usr/bin/innobackupex',
                package='percona-xtrabackup')

xbstream = PerconaExec('/usr/bin/xbstream',
                package='percona-xtrabackup')


def my_print_defaults(*option_groups):
    out = linux.system(linux.build_cmd_args(
            executable='/usr/bin/my_print_defaults',
            params=option_groups))[0]
    ret = {}
    for line in out.splitlines():
        cols = line.split('=')
        ret[cols[0][2:]] = cols[1] if len(cols) > 1 else True
    for key in __mysql__['defaults']:
        if key not in ret:
            ret[key] = __mysql__['defaults'][key]
    return ret


def mysqldump(*databases, **long_kwds):
    output = long_kwds.pop('output', None)
    cmd = linux.build_cmd_args(
            executable='/usr/bin/mysqldump',
            long=long_kwds,
            params=databases)
    kwds = {}
    if output:
        kwds['stdout'] = open(output, 'w+')
    return linux.system(cmd, **kwds)


def mysqlbinlog(log_file, **log_kwds):
    return linux.system(linux.build_cmd_args(
            executable='/usr/bin/mysqlbinlog',
            long=log_kwds,
            params=[log_file]))


def mysqlbinlog_head():
    '''
    Returns the first binary log file position
    Example:
        >> binlog_head()
        >> ('binlog.000001', 107)
    '''
    my_defaults = my_print_defaults('mysqld')
    binlog_dir = os.path.dirname(my_defaults['log_bin']) \
                if my_defaults['log_bin'][0] == '/' \
                else my_defaults['datadir']
    binlog_index = os.path.join(binlog_dir,
                    os.path.basename(my_defaults['log_bin'])) + '.index'
    with open(binlog_index) as fp:
        binlog_1 = fp.readline().strip()
        binlog_1 = os.path.join(binlog_dir, binlog_1)
    # FORMAT_DESCRIPTION_EVENT minimum length
    # @see http://dev.mysql.com/doc/internals/en/binary-log-versions.html
    stop_position = 91
    out = mysqlbinlog(binlog_1, verbose=True,
                    stop_position=stop_position)[0]
    end_log_pos_re = re.compile(r'end_log_pos\s+(\d+)')
    for line in out.splitlines():
        m = end_log_pos_re.search(line)  # must be search?
        if m:
            return (os.path.basename(binlog_1), m.group(1))

    msg = 'Failed to read FORMAT_DESCRIPTION_EVENT ' \
            'at the top of the %s' % binlog_1
    raise Error(msg)
