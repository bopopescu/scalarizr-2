from __future__ import with_statement
'''
Created on Nov 25, 2011

@author: marat

Pluggable API to get system information similar to SNMP, Facter(puppet), Ohai(chef)
'''

from __future__ import with_statement

import os
import logging
import sys
import glob
import time
import signal
import subprocess as subps

from multiprocessing import pool

from scalarizr import rpc, linux
from scalarizr.bus import bus
from scalarizr.util import system2, dns, disttool
from scalarizr.linux import mount
from scalarizr.util import kill_childs
from scalarizr.queryenv import ScalingMetric

LOG = logging.getLogger(__name__)


class _ScalingMetricStrategy(object):
    """Strategy class for custom scaling metric"""

    @staticmethod
    def _get_execute(metric):
        if not os.access(metric.path, os.X_OK):
            raise BaseException("File is not executable: '%s'" % metric.path)
  
        exec_timeout = 3
  
        proc = subps.Popen(metric.path, stdout=subps.PIPE, stderr=subps.PIPE, close_fds=True)
 
        timeout_time = time.time() + exec_timeout
        while time.time() < timeout_time:
            if proc.poll() is None:
                time.sleep(0.2)
            else:
                break
        else:
            kill_childs(proc.pid)
            if hasattr(proc, 'terminate'):
                # python >= 2.6
                proc.terminate()
            else:
                os.kill(proc.pid, signal.SIGTERM)
            raise BaseException('Timeouted')
                                
        stdout, stderr = proc.communicate()
        
        if proc.returncode > 0:
            raise BaseException(stderr if stderr else 'exitcode: %d' % proc.returncode)
        
        return stdout
  
  
    @staticmethod
    def _get_read(metric):
        try:
            with open(metric.path, 'r') as fp:
                value = fp.readline()
        except IOError:
            raise BaseException("File is not readable: '%s'" % metric.path)
  
        return value


    @staticmethod
    def get(metric):
        error = ''
        try:
            if metric.retrieve_method == ScalingMetric.RetriveMethod.EXECUTE:
                value = float(_ScalingMetricStrategy._get_execute(metric))
            elif metric.retrieve_method == ScalingMetric.RetriveMethod.READ:
                value = float(_ScalingMetricStrategy._get_read(metric))
            else:
                raise BaseException('Unknown retrieve method %s' % metric.retrieve_method)
        except (BaseException, Exception), e:
            value = 0.0
            error = str(e)[0:255]

        return {'id':metric.id, 'name':metric.name, 'value':value, 'error':error}


class SystemAPI(object):

    _HOSTNAME = '/etc/hostname'
    _DISKSTATS = '/proc/diskstats'
    _PATH = ['/usr/bin/', '/usr/local/bin/']
    _CPUINFO = '/proc/cpuinfo'
    _NETSTATS = '/proc/net/dev'

    def _readlines(self, path):
        with open(path, "r") as fp:
            return fp.readlines()


    def add_extension(self, extension):
        '''
        @param extension: Object with some callables to extend SysInfo public interface
        @type extension: object
        @note: Duplicates resolves by overriding old function with a new one
        '''

        for name in dir(extension):
            attr = getattr(extension, name)
            if not name.startswith('_') and callable(attr):
                if hasattr(self, name):
                    LOG.warn('Duplicate attribute %s. Overriding %s with %s', 
                            name, getattr(self, name), attr)
                setattr(self, name, attr)


    @rpc.service_method
    def call_auth_shutdown_hook(self):
        script_path = '/usr/local/scalarizr/hooks/auth-shutdown'
        LOG.debug("Executing %s" % script_path)
        if os.access(script_path, os.X_OK):
            return subps.Popen(script_path, stdout=subps.PIPE,
                stderr=subps.PIPE, close_fds=True).communicate()[0].strip()
        else:
            raise Exception('File not exists: %s' % script_path)


    @rpc.service_method
    def fqdn(self, fqdn=None):
        '''
        Get/Update host FQDN
        @param fqdn: Fully Qualified Domain Name to set for this host
        @rtype: str: Current FQDN
        '''

        if fqdn:
            # changing permanent hostname
            try:
                with open(self._HOSTNAME, 'r') as fp:
                    old_hn = fp.readline().strip()
                with open(self._HOSTNAME, 'w+') as fp:
                    fp.write('%s\n' % fqdn)
            except:
                raise Exception, 'Can`t write file `%s`.' % \
                    self._HOSTNAME, sys.exc_info()[2]
            # changing runtime hostname
            try:
                system2(('hostname', fqdn))
            except:
                with open(self._HOSTNAME, 'w+') as fp:
                    fp.write('%s\n' % old_hn)
                raise Exception('Failed to change the hostname to `%s`' % fqdn)
            # changing hostname in hosts file
            if old_hn:
                hosts = dns.HostsFile()
                hosts._reload()
                if hosts._hosts:
                    for index in range(0, len(hosts._hosts)):
                        if isinstance(hosts._hosts[index], dict) and \
                                        hosts._hosts[index]['hostname'] == old_hn:
                            hosts._hosts[index]['hostname'] = fqdn
                    hosts._flush()
            return fqdn
        else:
            return system2(('hostname', ))[0].strip()


    @rpc.service_method
    def block_devices(self):
        '''
        Block devices list
        @return: List of block devices including ramX and loopX
        @rtype: list 
        '''

        lines = self._readlines(self._DISKSTATS)
        devicelist = []
        for value in lines:
            devicelist.append(value.split()[2])
        return devicelist


    @rpc.service_method
    def uname(self):
        '''
        Return system information
        @rtype: dict
        
        Sample:
        {'kernel_name': 'Linux',
        'kernel_release': '2.6.41.10-3.fc15.x86_64',
        'kernel_version': '#1 SMP Mon Jan 23 15:46:37 UTC 2012',
        'nodename': 'marat.office.webta',           
        'machine': 'x86_64',
        'processor': 'x86_64',
        'hardware_platform': 'x86_64'}
        '''

        uname = disttool.uname()
        return {
            'kernel_name': uname[0],
            'nodename': uname[1],
            'kernel_release': uname[2],
            'kernel_version': uname[3],
            'machine': uname[4],
            'processor': uname[5],
            'hardware_platform': disttool.arch()
        }


    @rpc.service_method
    def dist(self):
        '''
        Return Linux distribution information 
        @rtype: dict

        Sample:
        {'distributor': 'ubuntu',
        'release': '12.04',
        'codename': 'precise'}
        '''
        return {
            'distributor': linux.os['name'].lower(),
            'release': str(linux.os['release']),
            'codename': linux.os['codename']
        }


    @rpc.service_method
    def pythons(self):
        '''
        Return installed Python versions
        @rtype: list

        Sample:
        ['2.7.2+', '3.2.2']
        '''

        res = []
        for path in self._PATH:
            pythons = glob.glob(os.path.join(path, 'python[0-9].[0-9]'))
            for el in pythons:
                res.append(el)
        #check full correct version
        LOG.debug('variants of python bin paths: `%s`. They`ll be checking now.', list(set(res)))
        result = []
        for pypath in list(set(res)):
            (out, err, rc) = system2((pypath, '-V'), raise_exc=False)
            if rc == 0:
                result.append((out or err).strip())
            else:
                LOG.debug('Can`t execute `%s -V`, details: %s',\
                        pypath, err or out)
        return map(lambda x: x.lower().replace('python', '').strip(), sorted(list(set(result))))


    @rpc.service_method
    def cpu_info(self):
        '''
        Return CPU info from /proc/cpuinfo
        @rtype: list
        '''

        lines = self._readlines(self._CPUINFO)
        res = []
        index = 0
        while index < len(lines):
            core = {}
            while index < len(lines):
                if ':' in lines[index]:
                    tmp = map(lambda x: x.strip(), lines[index].split(':'))
                    (key, value) = list(tmp) if len(list(tmp)) == 2 else (tmp, None)
                    if key not in core.keys():
                        core.update({key:value})
                    else:
                        break
                index += 1
            res.append(core)
        return res


    @rpc.service_method
    def cpu_stat(self):
        '''
        Return CPU stat from /proc/stat
        @rtype: dict
        
        Sample: {
            'user': 8416,
            'nice': 0,
            'system': 6754,
            'idle': 147309
        }
        '''
        cpu = open('/proc/stat').readline().strip().split()
        return {
            'user': int(cpu[1]),
            'nice': int(cpu[2]),
            'system': int(cpu[3]),
            'idle': int(cpu[4])
        }


    @rpc.service_method
    def mem_info(self):
        '''
        Return Memory information from /proc/meminfo
        @rtype: dict
        
        Sample: {
            'total_swap': 0,
            'avail_swap': 0,
            'total_real': 604364,
            'total_free': 165108,
            'shared': 168,
            'buffer': 17832,
            'cached': 316756
        }
        '''
        info = {}
        for line in open('/proc/meminfo'):
            pairs = line.split(':', 1)
            info[pairs[0]] = int(pairs[1].strip().split(' ')[0])
        return {
            'total_swap': info['SwapTotal'],
            'avail_swap': info['SwapFree'],
            'total_real': info['MemTotal'],
            'total_free': info['MemFree'],
            'shared': info['Shmem'],
            'buffer': info['Buffers'],
            'cached': info['Cached']
        }


    @rpc.service_method
    def load_average(self):
        '''
        Return Load average (1, 5, 15) in 3 items list  
        '''

        return os.getloadavg()


    @rpc.service_method
    def disk_stats(self):
        '''
        Disks I/O statistics
        @rtype: {
            <device>: {
                <read>: {
                    <num>: total number of reads completed successfully
                    <sectors>: total number of sectors read successfully
                    <bytes>: total number of bytes read successfully
                }
                <write>: {
                    <num>: total number of writes completed successfully
                    <sectors>: total number of sectors written successfully
                    <bytes>: total number of bytes written successfully
                },
            ...
        }
        '''
        #http://www.kernel.org/doc/Documentation/iostats.txt

        lines = self._readlines(self._DISKSTATS)
        devicelist = {}
        for value in lines:
            params = value.split()[2:]
            device = params[0]
            for i in range(1, len(params)-1):
                params[i] = int(params[i])
            if len(params) == 12:
                read = {'num': params[1], 'sectors': params[3], 'bytes': params[3]*512}
                write = {'num': params[5], 'sectors': params[7], 'bytes': params[7]*512}
            elif len(params) == 5:
                read = {'num': params[1], 'sectors': params[2], 'bytes': params[2]*512}
                write = {'num': params[3], 'sectors': params[4], 'bytes': params[4]*512}
            else:
                raise Exception, 'number of column in %s is unexpected. Count of column =\
                     %s' % (self._DISKSTATS, len(params)+2)
            devicelist[device] = {'write': write, 'read': read}
        return devicelist


    @rpc.service_method
    def net_stats(self):
        '''
        Network I/O statistics
        @rtype: {
            <iface>: {
                <receive>: {
                    <bytes>: total received bytes
                    <packets>: total received packets
                    <errors>: total receive errors
                }
                <transmit>: {
                    <bytes>: total transmitted bytes
                    <packets>: total transmitted packets
                    <errors>: total transmit errors
                }
            },
            ...
        }
        '''

        lines = self._readlines(self._NETSTATS)
        res = {}
        for row in lines:
            if ':' not in row:
                continue
            row = row.split(':')
            iface = row.pop(0).strip()
            columns = map(lambda x: x.strip(), row[0].split())
            res[iface] = {
                'receive': {'bytes': columns[0], 'packets': columns[1], 'errors': columns[2]},
                'transmit': {'bytes': columns[8], 'packets': columns[9], 'errors': columns[10]},
            }

        return res


    @rpc.service_method
    def statvfs(self, mpoints=None):
        if not isinstance(mpoints, list):
            raise Exception('Argument "mpoints" should be a list of strings, '
                        'not %s' % type(mpoints))

        res = dict()
        mounts = mount.mounts()
        for mpoint in mpoints:
            try:
                assert mpoint in mounts
                mpoint_stat = os.statvfs(mpoint)
                res[mpoint] = dict()
                res[mpoint]['total'] = (mpoint_stat.f_bsize * mpoint_stat.f_blocks) / 1024
                res[mpoint]['free'] = (mpoint_stat.f_bsize * mpoint_stat.f_bavail) / 1024
            except:
                res[mpoint] = None

        return res


    @rpc.service_method
    def scaling_metrics(self):
        '''
        @return list of scaling metrics
        @rtype: list
        
        Sample: [{
            'id': 101011, 
            'name': 'jmx.scaling', 
            'value': 1, 
            'error': None
        }, {
            'id': 202020,
            'name': 'app.poller',
            'value': None,
            'error': 'Couldnt connect to host'
        }]
        '''

        # Obtain scaling metrics from Scalr.
        scaling_metrics = bus.queryenv_service.get_scaling_metrics()
        
        max_threads = 10
        wrk_pool = pool.ThreadPool(processes=max_threads)

        try:
            return wrk_pool.map_async(_ScalingMetricStrategy.get, scaling_metrics).get()
        finally:
            wrk_pool.close()
            wrk_pool.join()
