from __future__ import with_statement
'''
Created on Nov 26, 2010

@author: Dmytro Korsakov
'''
from __future__ import with_statement

from scalarizr.messaging import Queues
from scalarizr.config import ScalarizrCnf
from scalarizr.queryenv import QueryEnvService
from scalarizr.bus import bus
from scalarizr.util.software import system_info, whereis
from scalarizr import init_script
from scalarizr.util import system2
import smtplib
from email.Utils import formatdate
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email import Encoders

from optparse import OptionParser, _, HelpFormatter, OptionGroup

import ConfigParser
import tarfile
import sys
import os
import logging
from scalarizr.messaging.p2p import P2pMessageStore
try:
    import json
except ImportError:
    import simplejson as json
from xml.dom import minidom

#23.09.11----------------------------------------------------------------------------------------------
try:
    from prettytable import PrettyTable as PTable
except:
    print('Error: prettytable modul not found!')


from yaml import dump
from yaml.representer import Representer
from yaml.emitter import Emitter
from yaml.serializer import Serializer
from yaml.resolver import Resolver

from scalarizr.messaging import Message


LOG = logging.getLogger('szradm')
QUERYENV_API_VERSION = None

def encode(a, encoding='ascii'):
    'used for recursive ecnode in class MarkAsUnhandledCommand'
    if isinstance(a, dict):
        ret = {}
        for key, value in a.items():
            if not isinstance(value, list):
                ret[key.encode(encoding)] = encode(value, encoding) \
                        if isinstance(value, dict) else value.encode(encoding)\
                        if isinstance(value, basestring) else value

            elif isinstance(value, list):
                temp_list=[]
                for item in value:
                    temp_list.append(encode(item, encoding))
                ret[key.encode(encoding)]=temp_list

        return ret
    elif isinstance(a, list):
        temp_list=[]
        for item in value:
            temp_list.append(encode(item, encoding))
            ret[key.encode(encoding)]=temp_list
        return ret
    elif isinstance(a, str):
        return a.encode(encoding)
    else:
        try:
            return a.encode(encoding)
        except:
            raise LookupError('Not suspectived input param type in encode method.'
                    ' Type of input param: %s'%type(a))


class SzradmRepresenter(Representer):

    def represent_str(self, data):
        tag = None
        style = None
        try:
            data = unicode(data, 'ascii')
            tag = u'tag:yaml.org,2002:str'
            if '\n' in data or len(data)>=128:#long string or multiline
                style="|"
        except UnicodeDecodeError:
            try:
                data = unicode(data, 'utf-8')
                tag = u'tag:yaml.org,2002:python/str'
                if '\n' in data or len(data)>=128: #long string or multiline
                    style="|"
            except UnicodeDecodeError:
                data = data.encode('base64')
                tag = u'tag:yaml.org,2002:binary'
                style = '|'
        return self.represent_scalar(tag, data, style=style)

SzradmRepresenter.add_representer(str, SzradmRepresenter.represent_str)


class SzradmDumper(Emitter, Serializer, SzradmRepresenter, Resolver):

    def __init__(self, stream,
                    default_style=None, default_flow_style=None,
                    canonical=None, indent=None, width=None,
                    allow_unicode=None, line_break=None,
                    encoding=None, explicit_start=None, explicit_end=None,
                    version=None, tags=None):

        Emitter.__init__(self, stream, canonical=canonical,
                        indent=indent, width=width,
                        allow_unicode=allow_unicode, line_break=line_break)
        Serializer.__init__(self, encoding=encoding,
                        explicit_start=explicit_start, explicit_end=explicit_end,
                        version=version, tags=tags)
        SzradmRepresenter.__init__(self, default_style=default_style,
                        default_flow_style=default_flow_style)
        Resolver.__init__(self)


class ScalrError(BaseException):
    pass


class IndHelpFormatter(HelpFormatter):
    """Format help with indented section bodies.
"""
    def __init__(self,
             indent_increment=0,
             max_help_position=24,
             width=None,
             short_first=1):
        HelpFormatter.__init__(
    self, indent_increment, max_help_position, width, short_first)

    def format_usage(self, usage):
        return _("    %s") % usage

    def format_heading(self, heading):
        return "%*s%s:\n" % (self.current_indent, "", heading)

    def format_description(self, description):
        if description:
            return "\n\t%s" % self._format_text(description)
        else:
            return ""

    def format_epilog(self, epilog):
        if epilog:
            return "\n" + self._format_text(epilog) + ""
        else:
            return ""


class Command(object):
    name = None
    method = None
    parser = None
    fields = None
    group = None
    kwds = {}

    @property
    def usage(self):
        return self.parser.get_usage()

    @classmethod
    def queryenv(cls):
        if not hasattr(cls, '_queryenv'):
            cls._queryenv = new_queryenv()
        return cls._queryenv

    def __init__(self, argv=None):
        if argv:
            self.kwds = self.parser.parse_args(argv)[0].__dict__
        else:
            self.kwds=None

    def run(self):
        if self.kwds:
            result = getattr(self.queryenv(), self.method)(**self.kwds)
        else:
            result = getattr(self.queryenv(), self.method)()
        self.output(result)

    def output(self, result):
        out = None
        if self.fields:
            out=PTable(self.fields)
        for row in self.iter_result(result):
            if not out and self.fields:
                out=PTable(self.fields)
            elif out:
                out.add_row(row)

        if len(self.fields)==5:
            alignment=len(self.fields)*'l'
            #alignment=(len(self.fields)-1)*'c'
            #alignment+='r'
            out.aligns=alignment

        print (out)

    def get_db_conn(self):
        return bus.db


class GetlatestVersionCommand(Command):
    name="get-latest-version"
    method="get_latest_version"
    group = "QueryEnv"
    fields =['version']
    parser = OptionParser(usage='get-latest-version ',
            description='Display latest versioin', formatter= IndHelpFormatter())

    def iter_result(self, result):
        '''return:
        {version: string}'''
        yield [result]


class ListEbsMountpointsCommand(Command):
    name = "list-ebs-mountpoints"
    method = "list_ebs_mountpoints"
    group = "QueryEnv"
    fields =['name', 'dir', 'createfs', 'isarray', 'volume-id', 'device']
    parser = OptionParser(usage='list-ebs-mountpoints ',
            description='Display ebs mountpoints', formatter=IndHelpFormatter())

    def iter_result(self, result):
        #Mountpoint[]
        for d in result:
            vols=[]
            devs=[]
            for _vol in d.volumes:
                vols.append(_vol.volume_id)
                devs.append(_vol.device)
            vols=', '.join(vols)
            devs=', '.join(devs)
            yield [d.name, d.dir, d.create_fs, d.is_array, vols, devs]


class ListRolesCommand(Command):
    name = "list-roles"
    method = "list_roles"
    group = "QueryEnv"
    fields = ['behaviour','name', 'farm-role-id', 'index', 'internal-ip',
            'external-ip', 'replication-main']
    parser = OptionParser(usage='list-roles [-b --behaviour] '
            '[-r --role] [--with-initializing]', description='Display roles list',
             formatter= IndHelpFormatter())
    parser.add_option('-b', '--behaviour', dest='behaviour', help='Role behaviour')
    parser.add_option('-r', '--role-name', dest='role_name', help='Role name')
    parser.add_option('--with-initializing', dest='with_init',
                                    action='store_true', default=None, help='Show initializing servers')

    def iter_result(self, result):
        '''Return array of result'''
        for d in result:
            behaviour=', '.join(d.behaviour)
            for host in d.hosts:
                yield [behaviour, d.name, d.farm_role_id, str(host.index),
                        host.internal_ip, host.external_ip,
                        str(host.replication_main)]


class GetHttpsCertificateCommand(Command):
    name = "get-https-certificate"
    method = "get_https_certificate"
    fields = ['cert', 'pkey', 'cacert']
    group = "QueryEnv"
    parser = OptionParser(usage='get-https-certificate ',
            description='Display cert, pkey https certificate\n',
            formatter=IndHelpFormatter())

    def iter_result(self, result):
        '''return: (cert, pkey, cacert)'''
        (cert, pkey, cacert)=result
        yield [cert, pkey, cacert]


class ListRoleParamsCommand(Command):
    name = "list-role-params"
    method = "list_role_params"
    fields = ['Keys', 'Values']
    group = "QueryEnv"
    parser = OptionParser(usage='list-role-params',
            description='Display list role params', formatter=IndHelpFormatter())

    def iter_result(self, result):
        '''dictionary'''
        for key in result.keys():
            yield [key, result[key]]

    def run(self):
        if self.kwds:
            result = getattr(self.queryenv(), self.method)(**self.kwds)
        else:
            result = getattr(self.queryenv(), self.method)()
        #LOG.debug('\n before encode: %s\n'%result)
        result=encode(result)
        #LOG.debug('\n after encode: %s\n'%result)
        yaml=dump(result, Dumper=SzradmDumper, default_flow_style=False)
        print yaml


class ListVirtualhostsCommand(Command):
    name = "list-virtualhosts"
    method = "list_virtual_hosts"
    fields = ['hostname', 'https', 'type', 'raw']
    group = "QueryEnv"
    parser = OptionParser(usage='list-virtualhosts'
            ' [-n --name] [-s --https] ',
            description='Display list of virtual hosts', formatter=IndHelpFormatter())
    parser.add_option('-n', '--name', dest='name', help='Show virtual host by name')
    parser.add_option('-s', '--https', dest='https', help='Show virtual hosts by https')

    def iter_result(self, result):
        '''return: [hostname=string,type=string,raw=string, https=0|1]'''
        for d in result:
            yield [d.hostname, d.https, d.type, d.raw]


class ListScriptsCommand(Command):
    name = "list-scripts"
    method = "list_scripts"
    fields = ['asynchronous', 'exec-timeout', 'name', 'body']
    group = "QueryEnv"
    parser = OptionParser(usage='list-scripts [-e --event]'
            ' [-a --asynchronous] [-n --name]',
            description='Display list of scripts', formatter=IndHelpFormatter())
    parser.add_option('-e', '--event', dest='event', help='Show scripts host on event')
    parser.add_option('-a', '--asynchronous', dest='asynchronous',
            help='Show scripts host by asynchronous')
    parser.add_option('-n', '--name', dest='name', help='Show script(s) with name')

    def iter_result(self, result):
        '''return:      [asynchronous=1|0, exec_timeout=string, name=string,body=string]'''
        for d in result:
            yield [d.asynchronous, d.exec_timeout, d.name, d.body]


class ListMessagesCommand(Command):
    name = "list-messages"
    method = "list_messages"
    group = "Messages"
    fields = ['id', 'name', 'date', 'direction', 'handled?']
    parser = OptionParser(usage='list-messages [-n --name]',
            description='Display list of messages', formatter=IndHelpFormatter())

    parser.add_option('-n', '--name', dest='name', help='Show message(s) with name')

    def iter_result(self, result):
        '''return:      [asynchronous=1|0, exec_timeout=string, name=string,body=string]'''
        for d in result:
            yield [d[0], d[1], d[2], d[3], d[4]]

    def run(self):
        try:
            conn=self.get_db_conn()
            cur = conn.cursor()
            if self.kwds and self.kwds['name']:
                cur.execute("SELECT `message_id`,`message_name`,\
                        `out_last_attempt_time`,`is_ingoing`,`in_is_handled` FROM\
                        p2p_message WHERE `message_name`='%s'"% self.kwds['name'])
            else:
                cur.execute("SELECT `message_id`,`message_name`,\
                        `out_last_attempt_time`,`is_ingoing`,`in_is_handled`\
                        FROM p2p_message")
            res=[]

            for row in cur.fetchall():
                res.append([row[0],row[1], row[2],'in' if row[3] else 'out',
                        'yes' if row[4] else 'no'])
            self.output(res)
        except Exception,e:
            LOG.warn('Error connecting to db or not correct request look '
                    'at in sradm>ListMessagesCommand>method `run`. Details: %s'% e)
        finally:
            cur.close()


class MessageDetailsCommand(Command):
    name = "message-details"
    method = "message_details"
    group = "Messages"
    fields=['message']
    parser = OptionParser(usage='message-details [-j|--json] MESSAGE_ID',
            description='Display messages with message id', formatter=IndHelpFormatter())

    parser.add_option('-j', '--json', dest='json', action="store_true",
                                                                                    help='Print result in json format')

    def __init__(self,argv=None):
        if argv:
            if isinstance(argv, list):
                if '-h'in argv or '--help'in argv:
                    self.kwds = self.parser.parse_args(argv)[0].__dict__
                else:
                    self.kwds = dict()
                    if '--json' in argv:
                        argv.remove('--json')
                        self.kwds['json'] = True
                    self.kwds['message_id'] = argv[0]

            else:
                if argv != '-h' or argv != '--help':
                    self.kwds={'message_id':argv}
                else: self.kwds = self.parser.parse_args(argv)[0].__dict__

    def iter_result(self, result):
        return [result]

    def run(self):
        try:
            conn=self.get_db_conn()
            cur = conn.cursor()

            assert self.kwds['message_id'], 'message_id must be defined'
            query="SELECT `message`,`format` FROM p2p_message WHERE `message_id`='%s'"\
                    % self.kwds['message_id']
            cur.execute(query)
            res = cur.fetchone()
            if res:
                msg=Message()
                msg_format = res[1]
                msg.fromjson(res[0]) if 'json' == msg_format else msg.fromxml(res[0])
                try:
                    mdict=encode({u'id':msg.id, u'name':msg.name,
                            u'meta':msg.meta, u'body':msg.body})
                    if self.kwds and self.kwds.get('json'):
                        # print json
                        out = json.dumps(mdict, indent=4, sort_keys=True)
                    else:
                        # Print yaml
                        out = dump(mdict, Dumper=SzradmDumper, default_flow_style=False)
                    print out
                except Exception, e:
                    raise LookupError('Error in recursive encode '
                            '(szradm->MessageDetailsCommand: l442) Details: %s'%e)
            else:
                print('not found with that name')
            #self.output(res)
        except Exception, e:
            #LOG.debug('id=%s, name=%s, meta=%s, body=%s\n'%(msg.id, msg.name, msg.meta,
            #       msg.body))
            LOG.warn('Exception in szradm>MessageDetailsCommand>method '
                    'run. Details: %s'% e)
        finally:
            cur.close()


class MarkAsUnhandledCommand(Command):
    name = "mark-as-unhandled"
    method = "mark-as-unhandled"
    group = "Messages"
    parser = OptionParser(usage='mark-as-unhandled MESSAGE_ID',
            description='mark as unhandled message_id', formatter=IndHelpFormatter())
    fields = ['id', 'name', 'date', 'direction', 'handled?']

    def __init__(self,argv=None):
        if argv:
            if isinstance(argv, list):
                if '-h'in argv or '--help'in argv:
                    self.kwds = self.parser.parse_args(argv)[0].__dict__
                else:
                    self.kwds={'message_id':argv[0]}
            else:
                if argv != '-h' or argv != '--help':
                    self.kwds={'message_id':argv}
                else: self.kwds = self.parser.parse_args(argv)[0].__dict__

    def iter_result(self, result):
        for d in result:
            yield [d[0], d[1], d[2], d[3], d[4]]

    def run(self):
        try:
            conn=self.get_db_conn()
            cur = conn.cursor()
            LOG.debug('mark-us-unhandled message with id: %s'%self.kwds['message_id'])
            assert self.kwds['message_id'], 'message_id must be defined'
            cur.execute("UPDATE p2p_message SET in_is_handled = ? WHERE message_id = '%s'"
                            %self.kwds['message_id'], (0,))
            conn.commit()
            cur.close()
            cur = conn.cursor()
            cur.execute("""SELECT `message_id`,`message_name`,\
                            `out_last_attempt_time`,`is_ingoing`,`in_is_handled` FROM\
                            p2p_message WHERE `message_id`='%s'"""% self.kwds['message_id'])
            res=[]
            for row in cur:
                res.append([row[0],row[1], row[2],'in' if row[3] else 'out', 'yes' if row[4] else 'no'])
            self.output(res)
        except Exception, e:
            LOG.warn('Exceptioin in szradm -> MarkAsUnhandledCommand -> method run.'
                     ' Details: %s'%e)
        finally:
            cur.close()
            pass


class Help(Command):
    name='help'
    com_dict=None
    print_groups=['QueryEnv', 'Messages']
    parser = None
    group='help'

    def __init__(self,com_d=None, groups=None):
        if com_d:
            self.com_dict=com_d
            if groups:
                self.print_groups=list(groups)

    def run(self, com_d=None, parser_misc=None):
        if com_d:
            self.com_dict=com_d
        if self.com_dict:
            str='Scalarizr administration utility'
            if not parser_misc:
                parser_misc=help_misc()
            print '%s\n\n%s' % (str, parser_misc.format_help())

            for gr in self.print_groups:
                st= '\n'+gr if gr != 'QueryEnv' else gr
                print('\n%s  commands:\n'%st)
                for com_name in self.com_dict.keys():
                    com_obj=self.com_dict.get(com_name)()
                    if not isinstance(com_obj, Help) and gr==com_obj.group:
                        print('%s'%com_obj.usage)


#default options list:
def help_misc():
    parser = OptionParser(usage="Usage: %prog [options] key=value key2=value2 ...")
    parser.add_option("-q", "--queryenv", dest="queryenv", action="store_true",
            default=False, help="QueryEnv CLI")
    parser.add_option('--api-version', dest='api_version', help='QueryEnv API version')
    parser.add_option("-m", "--msgsnd", dest="msgsnd", action="store_true",
            default=False, help="Message sender CLI")
    parser.add_option("-r", "--repair", dest="repair", action="store_true",
            default=False,  help="Repair database")
    parser.add_option('--reinit', dest='reinit', action='store_true', default=False,
            help='Reinitialize Scalarizr')
    parser.add_option("-n", "--name", dest="name", default=None, help="Name")
    parser.add_option("-f", "--msgfile", dest="msgfile", default=None, help="File")
    parser.add_option("-e", "--endpoint", dest="endpoint", default=None, help="Endpoint")
    parser.add_option("-o", "--queue", dest="queue", default=None, help="Queue")
    parser.add_option("-s", "--qa-report", dest="report", action="store_true",
            default=None, help="Build report with logs and system info")
    parser.add_option("--fire-event", help='Fire custom event in Scalr. Parameters are passed in a key=value form')
    return parser
#-------------------------------------------------------------------------------------------------

def get_mx_records(email):
    out = system2('%s -t mx %s' % (whereis('host')[0], email.split('@')[-1]), shell=True)[0]
    mxs = [mx.split()[-1][:-1] if mx.endswith('.') else mx for mx in out.split('\n')]
    if '' in mxs: mxs.remove('')
    #from sets import Set
    #return list(Set(mxs))
    temp = {}
    for x in mxs: temp[x] = None
    return list(temp.keys())

ini = None
def init_cnf():
    cnf = ScalarizrCnf(bus.etc_path)
    cnf.bootstrap()
    globals()['ini'] = cnf.rawini

def new_queryenv():
    init_cnf()
    key_path = os.path.join(bus.etc_path, ini.get('general', 'crypto_key_path'))
    server_id = ini.get('general', 'server_id')
    url = ini.get('general','queryenv_url')
    api_version = QUERYENV_API_VERSION
    if not api_version:
        cnf = bus.cnf
        if not bus.scalr_version:
            version_file = cnf.private_path('.scalr-version')
            if os.path.exists(version_file):
                with open(version_file, 'r') as fp:
                    bus.scalr_version = tuple(fp.read().strip().split('.'))

        if bus.scalr_version:
            if bus.scalr_version >= (3, 5, 3):
                api_version = '2012-07-01'
            elif bus.scalr_version >= (3, 1, 0):
                api_version = '2012-04-17'
            else:
                api_version = '2010-09-23'
        else:
            api_version = '2012-07-01'
    return QueryEnvService(url, server_id, key_path, api_version)


def main():
    global ini
    init_script()

#23.09.11-------------------------------------------------------------------------------------------------
    com_dict={'list-roles':ListRolesCommand,
                    'get-latest-version':GetlatestVersionCommand,
                    'list-ebs-mountpoints':ListEbsMountpointsCommand,
                    'get-https-certificate':GetHttpsCertificateCommand,
                    'list-role-params':ListRoleParamsCommand,
                    'list-virtualhosts':ListVirtualhostsCommand,
                    'list-scripts':ListScriptsCommand,
                    'list-messages':ListMessagesCommand,
                    'message-details':MessageDetailsCommand,
                    'mark-as-unhandled':MarkAsUnhandledCommand,

                    'help':Help,
                    '--help':Help,
                    '-h':Help
            }
    str=None
    com=None
    com_find=None

    if len(sys.argv)>1:
        com_find=1
    if com_find and com_dict.has_key(sys.argv[1]):
        if sys.argv[2:]:
            str=sys.argv[2:]
        #select command class in list and create object of this class with param
        try:
            com=com_dict.get(sys.argv[1])(str)
            if com:
                if isinstance(com, Help):
                    com.run(com_dict)
                else:
                    com.run()
        except Exception, e:
            com=Help(com_dict)
            com.run()
            raise LookupError("Cant execute command or option. Error: %s" % (e))
    else:
        parser=help_misc()
#------------------------------------------------------------------------------------------------
        (options, raw_args) = parser.parse_args()
        if options.api_version:
            globals()['QUERYENV_API_VERSION'] = options.api_version

        if not options.queryenv and not options.msgsnd and not options.repair \
                and not options.report and not options.reinit and not options.fire_event:
            #printing full help
            com=Help(com_dict)
            com.run()
            sys.exit()

        args = []
        kv = {}
        for pair in raw_args:
            raw = pair.split("=")
            if len(raw)>=2:
                k = raw[0]
                v = raw[1]
                kv[k] = v
            elif len(raw)==1:
                args.append(pair)

        if options.queryenv:

            if not args:
                #printing full help
                com=Help(com_dict)
                com.run()
                sys.exit()

            qe = new_queryenv()
            xml = qe.fetch(*args, **kv)
            print minidom.parseString(xml).toprettyxml()

        if options.msgsnd:

            if not options.queue or (not options.msgfile and not options.name):
                com=Help(com_dict)
                com.run()
                sys.exit()

            msg_service = bus.messaging_service
            producer = msg_service.get_producer()

            init_cnf()

            producer.endpoint = options.endpoint or ini.get('messaging_p2p', 'producer_url')
            msg = msg_service.new_message()

            if options.msgfile:
                str = None
                with open(options.msgfile, 'r') as fp:
                    str = fp.read()
                if str:
                    try:
                        msg.fromxml(str)
                    except:
                        try:
                            msg.fromjson(str)
                        except:
                            print 'Unknown message format'
                            sys.exit(1)
            else:
                msg.body = kv

            if options.name:
                msg.name = options.name

            producer.send(options.queue, msg)

            print "Done"

        if options.reinit:
            print 'Call scalarizr to reinitialize role (see /var/log/scalarizr.log for results)'

            init_script()

            conn = bus.db
            cur = conn.cursor()
            try:
                with open('/etc/scalr/private.d/.state', 'w') as fp:
                    fp.write('bootstrapping')

                msg_service = bus.messaging_service
                msg = msg_service.new_message()

                cur.execute('SELECT message, format '
                                'FROM p2p_message '
                                'WHERE message_name = ? '
                                'ORDER BY id DESC '
                                'LIMIT 1', ('HostInitResponse', )
                )
                raw_msg, format = cur.fetchone()

                if 'xml' == format:
                    msg.fromxml(raw_msg)
                elif 'json' == format:
                    msg.fromjson(raw_msg)

                producer = msg_service.get_producer()
                producer.send(Queues.CONTROL, msg)

            finally:
                cur.close()


        if options.report:                      #collecting
            hostname = system2(whereis('hostname'), shell=True)[0]
            tar_file = os.path.join(os.getcwd(), 'report-%s.tar.gz' % hostname)
            json_file = os.path.join(os.getcwd(), 'sysinfo-%s.json' % hostname)

            cnf = bus.cnf
            cnf.bootstrap()
            ini = cnf.rawini
            try:
                log_params = ini.get('handler_file', 'args')
                try:
                    log_file = log_params(0)
                except (IndexError, TypeError):
                    raise
            except Exception, BaseException:
                log_file = '/var/log/scalarizr.log'

            file = open(json_file, 'w')
            json.dump(system_info(), file, sort_keys=True, indent=4)
            file.close()

            tar = tarfile.open(tar_file, "w:gz")
            tar.add(json_file)
            if os.path.exists(log_file):
                tar.add(log_file)
            tar.close()

            #cleaning
            if os.path.exists(json_file):
                os.remove(json_file)

            #sending
            fromaddr='root@%s' % hostname
            try:
                email = ini.get('general', 'report_email')
            except ConfigParser.NoOptionError:
                print "Unable to send email: section 'report_mail' not found in config file."
                print "Although you can send %s to support manually." % tar_file
                sys.exit(1)

            role_name = ini.get('general', 'role_name')
            server_id = ini.get('general', 'server_id')

            toaddrs=[email]
            subject = 'scalarizr report from hostname %s (role: %s , serverid: %s)' % (hostname, role_name, server_id)

            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = email
            msg['Date'] = formatdate(localtime=True)
            msg['Subject'] = subject

            text_msg = MIMEText(subject)
            msg.attach(text_msg)

            part = MIMEBase('application', "octet-stream")
            part.set_payload( open(tar_file,"rb").read())
            Encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(tar_file))
            msg.attach(part)

            for server in get_mx_records(email):
                try:
                    print 'Sending message to %s through %s' % (email, server)
                    smtp = smtplib.SMTP(server)
                    smtp.sendmail(fromaddr, toaddrs, msg.as_string())
                    break
                except (Exception, BaseException), e:
                    print e, '\nTrying next mx entry'
                finally:
                    smtp.close()

            print "Done."

        if options.fire_event:

            msg_service = bus.messaging_service
            producer = msg_service.get_producer()

            init_cnf()

            producer.endpoint = ini.get('messaging_p2p', 'producer_url')
            msg = msg_service.new_message('FireEvent',
                            body={'event_name': options.fire_event, 'params': kv})
            print 'Sending %s' % options.fire_event
            producer.send('control', msg)

            print "Done"


if __name__ == '__main__':
    main()

#look in /test/unit/testcases/szradm_test.py
