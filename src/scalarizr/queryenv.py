from __future__ import with_statement
'''
Created on Dec 23, 2009

@author: Dmytro Korsakov
'''
import binascii
import logging
import os
import sys
import urllib
import urllib2
import time

from scalarizr.util import cryptotool
from scalarizr.util import urltool

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET


class QueryEnvError(Exception):
    pass

class QueryEnvService(object):
    _logger = None

    url = None
    api_version = None
    key_path = None
    server_id = None

    def __init__(self, url, server_id=None, key_path=None, api_version='2012-04-17'):
        self._logger = logging.getLogger(__name__)
        self.url = url if url[-1] != "/" else url[0:-1]
        self.server_id = server_id
        self.key_path = key_path
        self.api_version = api_version

    def fetch(self, command, **params):
        """
        @return object
        """
        # Perform HTTP request
        url = "%s/%s/%s" % (self.url, self.api_version, command)
        request_body = {}
        request_body["operation"] = command
        request_body["version"] = self.api_version
        if {} != params :
            for key, value in params.items():
                request_body[key] = value

        file = open(self.key_path)
        key = binascii.a2b_base64(file.read())
        file.close()

        signature, timestamp = cryptotool.sign_http_request(request_body, key)

        try:
            # Work over [Errno -3] Temporary failure in name resolution
            # http://bugs.centos.org/view.php?id=4814
            os.chmod('/etc/resolv.conf', 0755)
        except OSError:
            self._logger.debug('Cant chmod /etc/resolv.conf: %s', sys.exc_info()[1])

        post_data = urllib.urlencode(request_body)
        headers = {
                "Date": timestamp,
                "X-Signature": signature,
                "X-Server-Id": self.server_id
        }
        response = None
        wait_seconds = 30
        msg_wait = 'Waiting %d seconds before the next try' % wait_seconds
        while True:
            try:
                self._logger.debug("QueryEnv request: %s", post_data)
                opener = urllib2.build_opener(urltool.HTTPRedirectHandler)
                req = urllib2.Request(url, post_data, headers)
                response = opener.open(req)
                break
            except:
            	e = sys.exc_info()[1]
                if isinstance(e, urllib2.HTTPError):
                    resp_body = e.read() if e.fp is not None else ""
                    self._logger.warn('QueryEnv failed. HTTP %s. %s. %s', e.code, resp_body or e.msg, msg_wait)
                else:
                    self._logger.warn('QueryEnv failed. %s. %s', e, msg_wait)
                self._logger.warn('Sleep %s seconds before next attempt...', wait_seconds)
                time.sleep(wait_seconds)

        resp_body = response.read()
        self._logger.debug("QueryEnv response: %s", resp_body)
        return resp_body


    def list_roles(self, role_name=None, behaviour=None, with_init=None):
        """
        @return Role[]
        """
        parameters = {}
        if None != role_name:
            parameters["role"] = role_name
        if None != behaviour:
            parameters["behaviour"] = behaviour
        if None != with_init:
            parameters["showInitServers"] = "1"

        return self._request("list-roles", parameters, self._read_list_roles_response)

    def list_role_params(self, name=None):
        """
        @return dict
        """
        parameters = {}
        if name:
            parameters["name"] = name
        return {'params':self._request("list-role-params", parameters, self._read_list_role_params_response)}


    def list_farm_role_params(self, farm_role_id=None):
        """
        @return dict
        """
        parameters = {}
        if farm_role_id:
            parameters["farm-role-id"] = farm_role_id
        return {'params':self._request("list-farm-role-params", parameters, self._read_list_farm_role_params_response) or {}}


    def get_server_user_data(self):
        """
        @return: dict
        """
        return self._request('get-server-user-data', {}, self._read_get_server_user_data_response)

    def list_scripts (self, event=None, event_id=None, asynchronous=None, name=None,
            target_ip=None, local_ip=None):
        """
        @return Script[]
        """
        parameters = {}
        if None != event:
            parameters["event"] = event
        if None != event_id:
            parameters["event_id"] = event_id
        if None != asynchronous:
            parameters["asynchronous"] = asynchronous
        if None != name :
            parameters["name"] = name
        if None != target_ip:
            parameters['target_ip'] = target_ip
        if None != local_ip:
            parameters['local_ip'] = local_ip
        return self._request("list-scripts",parameters, self._read_list_scripts_response)

    def list_virtual_hosts(self, name=None, https=None):
        """
        @return VirtualHost[]
        """
        parameters = {}
        if None != name :
            parameters["name"] = name
        if None != https:
            parameters["https"] = https
        return self._request("list-virtualhosts",parameters, self._read_list_virtualhosts_response)

    def get_https_certificate(self):
        """
        @return (cert, pkey, cacert)
        """
        return self._request("get-https-certificate",{}, self._read_get_https_certificate_response)

    def list_ebs_mountpoints(self):
        """
        @return Mountpoint[]
        """
        return self._request("list-ebs-mountpoints",{}, self._read_list_ebs_mountpoints_response)

    def get_latest_version(self):
        """
        @return string
        """
        return self._request("get-latest-version",{}, self._read_get_latest_version_response)

    def get_service_configuration(self, behaviour):
        """
        @return dict
        """
        return self._request("get-service-configuration",{},
                        self._read_get_service_configuration_response, (behaviour,))

    def get_scaling_metrics(self):
        '''
        @return: list of ScalingMetric
        '''
        return self._request('get-scaling-metrics', {}, self._read_get_scaling_metrics_response)

    def get_global_config(self):
        """
        @return dict
        """
        return {'params':self._request("get-global-config", {}, self._read_get_global_config_response)}

    def _request (self, command, params={}, response_reader=None, response_reader_args=None):
        xml = self.fetch(command, **params)
        response_reader_args = response_reader_args or ()
        return response_reader(xml, *response_reader_args)

    def _read_get_global_config_response(self, xml):
        """
        @return dict
        """
        ret = xml2dict(ET.XML(xml))
        if ret:
            data = ret[0]
        return data['values'] if 'values' in data else {}


    def _read_list_roles_response(self, xml):
        ret = []
        data = xml2dict(ET.XML(xml))
        roles = data['roles'] or []
        for rdict in roles:
            behaviours = rdict['behaviour'].split(',')
            if behaviours == ('base',) or behaviours == ('',):
                behaviours = ()
            name = rdict['name']
            hosts = []
            if 'hosts' in rdict and rdict['hosts']:
                hosts = [RoleHost.from_dict(d) for d in rdict['hosts']]
            farm_role_id  = rdict['id'] if 'id' in rdict else None
            role = Role(behaviours, name, hosts, farm_role_id)
            ret.append(role)
        return ret


    def _read_list_ebs_mountpoints_response(self, xml):
        ret = []
        data = xml2dict(ET.XML(xml))
        mpoints = data['mountpoints'] or []
        for mp in mpoints:
            create_fs = bool(int(mp["createfs"]))
            is_array = bool(int(mp["isarray"]))
            volumes = [Volume(vol_data["volume-id"], vol_data["device"]) for vol_data in mp["volumes"]]
            ret.append(Mountpoint(mp["name"], mp["dir"], create_fs, is_array, volumes))
        return ret


    def _read_list_scripts_response(self, xml):
        ret = []
        data = xml2dict(ET.XML(xml))
        scripts = data['scripts'] or []
        for raw_script in scripts:
            asynchronous = bool(int(raw_script["asynchronous"]))
            exec_timeout = int(raw_script["exec-timeout"])
            script = Script(asynchronous, exec_timeout, raw_script["name"], raw_script["body"])
            ret.append(script)
        return ret

    def _read_list_role_params_response(self, xml):
        ret = {}
        data = xml2dict(ET.XML(xml))
        role_params = data['params'] or []
        for raw_param in role_params:
            key = raw_param['name']
            value = raw_param['value']
            ret[key]=value
        return ret

    def _read_get_server_user_data_response(self, xml):
        data = xml2dict(ET.XML(xml))
        user_data = data['user-data']
        return user_data['values'] if 'values' in user_data else {}

    def _read_list_farm_role_params_response(self, xml):
        return xml2dict(ET.XML(xml))

    def _read_get_latest_version_response(self, xml):
        result = xml2dict(ET.XML(xml))
        return result['version'] if 'version' in result else None

    def _read_get_https_certificate_response(self, xml):
        result = xml2dict(ET.XML(xml))
        if 'virtualhost' in result:
            data = result['virtualhost']
            cert = data['cert'] if 'cert' in data else None
            pkey = data['pkey'] if 'pkey' in data else None
            ca = data['ca_cert'] if 'ca_cert' in data else None
            return (cert, pkey, ca)
        return (None, None, None)


    def _read_list_virtualhosts_response(self, xml):
        ret = []

        result = xml2dict(ET.XML(xml))
        raw_vhosts = result['vhosts'] or []
        for raw_vhost in raw_vhosts:
            if raw_vhost:
                hostname = raw_vhost['hostname']
                v_type = raw_vhost['type']
                raw_data = raw_vhost['raw']
                https = bool(int(raw_vhost['https'])) if 'https' in raw_vhost else False
                vhost = VirtualHost(hostname,v_type,raw_data,https)
                ret.append(vhost)
        return ret


    def _read_get_service_configuration_response(self, xml, behaviour):
        data = xml2dict(ET.XML(xml))
        preset = Preset()
        if 'newPresetsUsed' in data and data['newPresetsUsed']:
            preset.new_engine = True
        else:
            for raw_preset in data:
                if behaviour != raw_preset['behaviour']:
                    continue
                preset.name = raw_preset['preset-name']
                preset.restart_service = raw_preset['restart-service']
                preset.settings = raw_preset['values']
                preset.new_engine = False
        return preset


    def _read_get_scaling_metrics_response(self, xml):
        ret = []
        data = xml2dict(ET.XML(xml))
        raw_metrics = data['metrics'] or []
        for metric_el in raw_metrics:
            m = ScalingMetric()
            m.id = metric_el['id']
            m.name = metric_el['name']
            m.path = metric_el['path']
            m.retrieve_method = metric_el['retrieve-method'].strip()
            ret.append(m)
        return ret


class Preset(object):
    settings = None
    name = None
    restart_service = None
    new_engine = None

    def __init__(self, name = None, settings = None, restart_service = None):
        self.settings = {} if not settings else settings
        self.name = None if not name else name
        self.restart_service = None if not restart_service else restart_service

    def __repr__(self):
        return 'name = ' + str(self.name)\
               + "; restart_service = " + str(self.restart_service)\
               + "; settings = " + str(self.settings)\
               + "; new_engine = " + str(self.new_engine)


class Mountpoint(object):
    name = None
    dir = None
    create_fs = False
    is_array = False
    volumes  = None

    def __init__(self, name=None, dir=None, create_fs=False, is_array=False, volumes=None):
        self.volumes = volumes or []
        self.name = name
        self.dir = dir
        self.create_fs = create_fs
        self.is_array = is_array

    def __str__(self):
        opts = (self.name, self.dir, self.create_fs, len(self.volumes))
        return "qe:Mountpoint(name: %s, dir: %s, create_fs: %s, num_volumes: %d)" % opts

    def __repr__(self):
        return "name = " + str(self.name) \
+ "; dir = " + str(self.dir) \
+ "; create_fs = " + str(self.create_fs) \
+ "; is_array = " + str(self.is_array) \
+ "; volumes = " + str(self.volumes)

class Volume(object):
    volume_id  = None
    device = None

    def __init__(self, volume_id=None, device=None):
        self.volume_id = volume_id
        self.device = device

    def __str__(self):
        return "qe:Volume(volume_id: %s, device: %s)" % (self.volume_id, self.device)

    def __repr__(self):
        return 'volume_id = ' + str(self.volume_id) \
+ "; device = " + str(self.device)

class Role(object):
    behaviour = None
    name = None
    hosts = None
    farm_role_id = None

    def __init__(self, behaviour=None, name=None, hosts=None, farm_role_id=None):
        self.behaviour = behaviour
        self.name = name
        self.hosts = hosts or []
        self.farm_role_id = farm_role_id

    def __str__(self):
        opts = (self.name, self.behaviour, len(self.hosts), self.farm_role_id)
        return "qe:Role(name: %s, behaviour: %s, num_hosts: %s, farm_role_id: %s)" % opts

    def __repr__(self):
        return 'behaviour = ' + str(self.behaviour) \
+ "; name = " + str(self.name) \
+ "; hosts = " + str(self.hosts) \
+ "; farm_role_id = " + str(self.farm_role_id) + ";"


class QueryEnvResult(object):
    @classmethod
    def from_dict(cls,dict_data):
        kwargs = {}
        for k,v in dict_data.items():
            member = k.replace('-','_')
            if hasattr(cls, member):
                kwargs[member] = v
        obj = cls(**kwargs)
        return obj

class RoleHost(QueryEnvResult):
    index = None
    replication_main = False
    internal_ip = None
    external_ip     = None
    shard_index = None
    replica_set_index = None
    status = None

    def __init__(self,index=None, replication_main=False, internal_ip=None, external_ip=None,
                            shard_index=None, replica_set_index=None, status=None):
        self.internal_ip = internal_ip
        self.external_ip = external_ip
        self.status = status
        if index:
            self.index = int(index)
        if replication_main:
            self.replication_main = bool(int(replication_main))
        if shard_index:
            self.shard_index = int(shard_index)
        if replica_set_index:
            self.replica_set_index = int(replica_set_index)


    def __repr__(self):
        return "index = " + str(self.index) \
+ "; replication_main = " + str(self.replication_main) \
+ "; internal_ip = " + str(self.internal_ip) \
+ "; external_ip = " + str(self.external_ip) \
+ "; shard_index = " + str(self.shard_index) \
+ "; replica_set_index = " + str(self.replica_set_index)


class Script(object):
    asynchronous = False
    exec_timeout = None
    name = None
    body = None

    def __init__(self, asynchronous=False, exec_timeout=None, name=None, body=None):
        self.asynchronous = asynchronous
        self.exec_timeout = exec_timeout
        self.name = name
        self.body = body

    def __repr__(self):
        return "asynchronous = " + str(self.asynchronous) \
+ "; exec_timeout = " + str(self.exec_timeout) \
+ "; name = " + str(self.name) \
+ "; body = " + str(self.body)

class VirtualHost(object):
    hostname = None
    type = None
    raw = None
    https = False

    def __init__(self, hostname=None, type=None, raw=None, https=False):
        self.hostname = hostname
        self.type = type
        self.raw = raw
        self.https = https


    def __repr__(self):
        return "hostname = " + str(self.hostname) \
+ "; type = " + str(self.type) \
+ "; raw = " + str(self.raw) \
+ "; https = " + str(self.https)

class ScalingMetric(object):
    class RetriveMethod:
        EXECUTE = 'execute'
        READ = 'read'

    id = None
    name = None
    path = None

    _retrieve_method = None
    def _get_retrieve_method(self):
        return self._retrieve_method
    def _set_retrieve_method(self, v):
        if v in (self.RetriveMethod.EXECUTE, self.RetriveMethod.READ):
            self._retrieve_method = v
        else:
            raise ValueError("Invalid value '%s' for ScalingMetric.retrieve_method")

    retrieve_method = property(_get_retrieve_method, _set_retrieve_method)

    def __str__(self):
        return 'qe:ScalingMetric(%s, id: %s, path: %s:%s)' % (self.name, self.id, self.path, self.retrieve_method)


def xml2dict(el):
    if el.attrib:
        ret = el.attrib
        if el.tag == 'settings':
            ret['values'] = {}
            for ch in el:
                ret['values'][ch.attrib['key']] = ch.text
        else:
            for ch in el:
                ret[ch.tag] = xml2dict(ch)
        return ret
    if len(el):
        if el.tag == 'settings':
            return {'values':dict((ch.attrib['key'], ch.text) for ch in el)}

        if el.tag == 'user-data':
            ret = {}
            ret['values'] = {}
            for ch in el:
                key = ch.attrib['name']
                value = ch[0].text
                ret['values'][key] = value
            return ret


        tag = el[0].tag
        list_tags = ('item', 'role', 'host', 'settings', 'volume', 'mountpoint', 'script', 'param', 'vhost', 'metric')
        if tag in list_tags and  all(ch.tag == tag for ch in el):
            return list(xml2dict(ch) for ch in el)
        else:
            return dict((ch.tag, xml2dict(ch)) for ch in el)
    else:
        return el.text
