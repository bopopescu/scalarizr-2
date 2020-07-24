'''
Created on 19.01.2010

@author: Dmytro Korsakov
'''
import unittest
import os
import string

from scalarizr.bus                              import bus
from scalarizr.handlers                 import nginx
from szr_unittest                               import RESOURCE_PATH
from scalarizr.config                   import ScalarizrCnf
from szr_unittest_libs.mock     import QueryEnvService
from scalarizr.queryenv                 import Role, RoleHost, VirtualHost
import shutil


class Message:
    local_ip = '8.8.8.8'


class TestNginx(unittest.TestCase):


    def setUp(self):

        bus.etc_path = os.path.join(RESOURCE_PATH, 'etc')
        cnf = ScalarizrCnf(bus.etc_path)
        cnf.load_ini('www')
        bus.cnf = cnf
        self._cnf = bus.cnf

        bus.base_path = os.path.realpath(RESOURCE_PATH + "/../../..")
        bus.share_path = os.path.join(bus.base_path, 'share')

        bus.queryenv_service = _EmptyQueryEnv()
        bus.define_events("before_host_down", "init")


    def _test_on_VhostReconfigure(self):
        https_include_path = "/etc/nginx/https.include"
        #_queryenv = bus.queryenv_service = _QueryEnv()
        _queryenv = bus.queryenv_service = qe
        https_include = None
        with open(https_include_path, 'r') as fp:
            https_include = fp.read()

        cert_path =  self._cnf.key_path("https.crt")
        pk_path = self._cnf.key_path("https.key")

        cert = None
        with open(cert_path, 'r') as fp:
            cert = fp.read()
        pk = None
        with open(pk_path, 'r') as fp:
            pk = fp.read()

        print 'Cleaning..'
        for file in (https_include_path, cert_path, pk_path):
            if os.path.exists(file):
                os.remove(file)
                print 'File %s deleted.' % file

        n = nginx.NginxHandler()
        n.on_VhostReconfigure(None)

        self.assertTrue(os.path.isfile(https_include_path))
        self.assertTrue(os.path.isfile(cert_path))
        self.assertTrue(os.path.isfile(pk_path))

        self.assertEquals(_queryenv.list_virtual_hosts()[0].raw + '\n', https_include)

        #temporary
        self.assertTrue(cert.startswith(_queryenv.get_https_certificate()[0]))

        self.assertEquals(_queryenv.get_https_certificate()[1], pk)


    def _test_creating_upstream_list(self):
        config = bus.config
        sect_name = nginx.CNF_SECTION
        nginx_incl = "/etc/nginx/app-servers.include"
        config.set(sect_name, "app_include_path",nginx_incl)
        if os.path.exists(nginx_incl):
            os.remove(nginx_incl)

        n = nginx.NginxHandler()
        n._reload_upstream()

        self.assertTrue(os.path.exists(nginx_incl))

    def _test_changing_upstream_list(self):
        bus.queryenv_service = qe
        config = bus.config
        sect_name = nginx.CNF_SECTION
        nginx_incl = "/etc/nginx/app-servers.include"
        config.set(sect_name, "app_include_path",nginx_incl)

        custom_include = 'upstream backend {\n\n        server  8.8.8.8:80\tweight=5;\n\n       server  7.7.7.7:80\tdebug;\n}'
        print custom_include
        with open(nginx_incl, 'w') as fp:
            fp.write(custom_include)

        n = nginx.NginxHandler()
        n._reload_upstream()
        n._reload_upstream()

        new_incl = None
        with open(nginx_incl, 'r') as fp:
            new_incl = fp.read()
        print new_incl

        #queryenv has only 8.8.8.8 in list_roles, so 7.7.7.7 supposed not to exist
        self.assertRaises(ValueError, string.index,*(new_incl, '7.7.7.7;'))
        #ip_hash wasn`t in original file, so after reconfigure it supposed not to exist either
        self.assertRaises(ValueError, string.index,*(new_incl, 'ip_hash;'))
        #8.8.8.8 had 'weight' option, so it not supposed to be vanished
        self.assertNotEquals(string.find(new_incl, 'weight=5;'), -1)
        #check that there is only one include
        include_str = 'include  /etc/nginx/https.include;'
        self.assertNotEquals(string.find(new_incl, include_str), '-1')
        self.assertEquals(string.find(new_incl, include_str), string.rfind(new_incl, include_str))

    def test_main_config(self):
        bus.queryenv_service = qe
        config = bus.config
        sect_name = nginx.CNF_SECTION
        nginx_incl = "/etc/nginx/app-servers.include"
        config.set(sect_name, "app_include_path",nginx_incl)

        #moving nginx_incl
        if os.path.exists(nginx_incl):
            shutil.move(nginx_incl, nginx_incl+'.temp')
            pass

        n = nginx.NginxHandler()
        #n._reload_upstream()
        n._update_main_config()

        main_cfg = None
        with open('/etc/nginx/nginx.conf', 'r') as fp:
            main_cfg = fp.read()

        include = 'include      /etc/nginx/app-servers.include;'
        self.assertRaises(ValueError, string.index,*(main_cfg, include))
        #moving back
        if os.path.exists(nginx_incl + '.temp'):
            shutil.move(nginx_incl + '.temp', nginx_incl)


    def _test_on_BeforeHostTerminate(self):
        config = bus.config
        include_path = "/etc/nginx/app-servers.include"
        config.set('www','app_include_path',include_path)
        data = """\nupstream backend {\n\tip_hash;\n\n\t\tserver 8.8.8.8:80;\n\n}"""
        with open(include_path, 'w') as fp:
            fp.write(data)
        n = nginx.NginxHandler()
        n.on_BeforeHostTerminate(Message)

        new_data = None
        with open(include_path, 'r') as fp:
            new_data = fp.read()
        self.assertEquals(new_data,"""\nupstream backend {\n\tip_hash;\n\n\tserver\t127.0.0.1:80;\n}\n""")



#-----mock-----

role_host = RoleHost(
                                                index='1',
                                                replication_main="1",
                                                internal_ip="8.8.8.8",
                                                external_ip="192.168.1.93")
vhost = VirtualHost(
                        hostname = "test1.net",type = "nginx",raw= """server {
          listen       443;
        server_name  test.org www.test.org www2.test.org;

        ssl                  on;
        ssl_certificate      /home/shaitanich/workspace/scalarizr-trunk/test/unit/resources/etc/private.d/keys/https.crt;
        ssl_certificate_key  /home/shaitanich/workspace/scalarizr-trunk/test/unit/resources/etc/private.d/keys/https.key;

        ssl_session_timeout  10m;
        ssl_session_cache    shared:SSL:10m;

        ssl_protocols  SSLv2 SSLv3 TLSv1;
        ssl_ciphers  ALL:!ADH:!EXPORT56:RC4+RSA:+HIGH:+MEDIUM:+LOW:+SSLv2:+EXP;
        ssl_prefer_server_ciphers   on;

        location / {
            proxy_pass         http://backend;
            proxy_set_header   Host             $host;
            proxy_set_header   X-Real-IP        $remote_addr;
            proxy_set_header   X-Forwarded-For  $proxy_add_x_forwarded_for;

            client_max_body_size       10m;
            client_body_buffer_size    128k;


            proxy_buffering on;
            proxy_connect_timeout 15;
            proxy_intercept_errors on;
        }
    } """,
                        https = True)

cert_dir = os.path.join(RESOURCE_PATH, '../../integ/resources/apache-cert/')
print os.path.exists(cert_dir)

server_crt = None
server_key = None
ca_crt = None
with open(os.path.join(cert_dir, 'server.crt'), 'r') as fp:
    server_crt = fp.read()
with open(os.path.join(cert_dir, 'server.key'), 'r') as fp:
    server_key = fp.read()
with open(os.path.join(cert_dir, 'ca.crt'), 'r') as fp:
    ca_crt = fp.read()

qe = QueryEnvService(list_roles=[Role(["www"], "nginx", [role_host])],
                                         list_virtual_hosts=[vhost],
                                         get_https_certificate=(server_crt, server_key, ca_crt))

bus.queryenv_service = qe
print qe
print qe.list_roles()
#print qe.list_virtual_hosts()


class _EmptyQueryEnv:
    def list_roles(self,behaviour, role_name):
        return []

if __name__ == "__main__":
    unittest.main()
