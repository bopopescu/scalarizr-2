from __future__ import with_statement
'''
Created on Jun 29, 2010

A cute library to read and write configurations in a various formats
using single interface.
Primary goal: support Ini, Xml, Yaml, ProtocolBuffers, Nginx, Apache2

@author: marat
@author: spike
'''

import sys
import re
import os

from utils import quote, unquote, indent, strip_quotes

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET

try:
    from  cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

default_format = "ini"

class MetaconfError(Exception):
    pass

class ParseError(BaseException):
    """
    Throw it in providers read method
    """
    def __init__(self, errors):
        self._err = "File contains parsing errors: "
        for error in errors:
            self._err += '\n\tNo: %d,\tLine: %s' % error

    def __str__(self):
        return self._err

class NoPathError(MetaconfError):
    pass


class Configuration:
    etree = None
    """
    @ivar xml.etree.ElementTree.ElementTree etree:
    """

    _root_path = None
    """
    @ivar str _root_path:
    """

    _format = None
    """
    @ivar str _format:
    """

    _provider = None

    def __init__(self, format=default_format, root_path="", etree=None, filename=None):

        if etree and not isinstance(etree, ET.ElementTree):
            raise MetaconfError("etree param must be instance of ElementTree. %s passed" % (etree,))

        try:
            pvd_name = '%sFormatProvider' % format.capitalize()
            pvd_module = __import__('providers.%s_pvd' % format, globals(), locals(), [pvd_name], -1)
            self._provider = getattr(pvd_module, pvd_name)()
        except:
            raise MetaconfError('Unknown or broken format provider: %s' % format)
        #if not format_providers.has_key(format):
        #       raise MetaconfError("Unknown format: %s" % format)

        self._root_path = quote(root_path)
        self._format = format
        self.etree = etree
        self._config_count = 0
        if filename:
            self._read0(filename)

    def __eq__(self, other):
        if not isinstance(other, Configuration):
            return False
        if not type(self._provider) == type(other._provider):
            return False
        try:
            mine = StringIO()
            others = StringIO()
            self.write_fp(mine, close=False)
            other.write_fp(others, close=False)
            return mine.getvalue() == others.getvalue()
        except:
            return False

    def _init(self):
        #if not self._provider or self._config_count > 0:
        #       self._provider = format_providers[self._format]()
        if not self.etree:
            root = ET.Element("mc_conf/")
            self.etree = ET.ElementTree(root)
        self._sections = []
        self._cursect = '.'

    def read(self, filenames):
        if isinstance(filenames, basestring):
            self._read0(filenames)
        else:
            for file in filenames:
                self._read0(file)
        if self.etree:
            indent(self.etree.getroot())


    def _read0(self, file):
        fp = open(file)
        self.readfp(fp)



    def readfp(self, fp):
        try:
            self._init()
            nodes = self._provider.read(fp)
            if self._config_count > 0:
                for node in nodes:
                    self._extend(node)
            else:
                root = self.etree.getroot()
                for node in nodes:
                    root.append(node)
            self._config_count += 1
        finally:
            fp.close()
        """
        self._init()
        for child in :
                self.etree.getroot().append(child)
        """

    def write_fp(self, fp, close = True):
        """
        Writes configuration to fp with provider's method 'write'.
        If 'close' parameter passed with 'False' value, fp won't be closed.
        """
        if not self.etree or self.etree.getroot() == None:
            raise MetaconfError("Nothing to write! Create the tree first (readfp or read)")
        self._provider.write(fp, self.etree, close)

    def write(self, file_path):
        if not self.etree or self.etree.getroot() == None:
            raise MetaconfError("Nothing to write! Create the tree first (readfp or read)")

        tmp_str = StringIO()
        try:
            self._provider.write(tmp_str, self.etree, close = False)
            fp = open(file_path, 'w')
            fp.write(tmp_str.getvalue())
            fp.close()
        except:
            raise

    def extend(self, conf):
        """
        Extend self with options from another config
        Comments and blank lines from importing config will not be added
        """
        self._init()
        for node in conf.etree.getroot():
            self._extend(node)

    def comment(self, path):
        """
        Comment part of the configuration (one option or subtree)
        """
        path = quote(path)
        parent_els      = self._find_all(os.path.join(path,'..'))
        if not parent_els:
            return

        el_to_comment_path = os.path.basename(path)

        for parent_el in parent_els:
            nodes_to_cmt = parent_el.findall(el_to_comment_path)
            for node_to_cmt in nodes_to_cmt:
                comment_value = StringIO()
                temp_root       = ET.Element('mc_conf')
                temp_tree       = ET.ElementTree(temp_root)
                temp_root.append(node_to_cmt)
                new_conf        = Configuration(format=self._format, etree=temp_tree)
                new_conf._init()
                new_conf.write_fp(comment_value, close = False)
                index = list(parent_el).index(node_to_cmt)
                comment         = ET.Comment(comment_value.getvalue().strip())
                parent_el.insert(index, comment)
                parent_el.remove(node_to_cmt)

    def uncomment(self, path):
        """
        Try to find appropriate configuration piece in comments on path's level,
        And then uncomment it
        """
        path = quote(path)
        parent_path = os.path.dirname(path)
        el_name = os.path.basename(path)
        temp_nodes = self._find_all(parent_path)

        if not temp_nodes:
            raise MetaconfError("Path %s doesn't exist" % unquote(path))
        for temp_node in temp_nodes:
            for child in temp_node:

                if not callable(child.tag):
                    continue
                temp_conf = Configuration(self._format)

                try:
                    temp_conf.readfp(StringIO(child.text.strip()))
                except:
                    continue

                comment_node = temp_conf.etree.find(el_name)

                if comment_node == None:
                    continue

                temp_node.insert(list(temp_node).index(child), comment_node)
                temp_node.remove(child)
                del(temp_conf)

    def _extend(self, node):
        if not callable(node.tag) and node.tag != '':
            cursect = self._cursect + '/' + node.tag
            exist_list = self.etree.findall(cursect)
            if exist_list:
                if len(exist_list) == 1 and exist_list[0].attrib == node.attrib:
                    if len(exist_list[0]):
                        self._sections.append(self._cursect)
                        self._cursect  = cursect
                        for child in node:
                            self._extend(child)
                        self._cursect  = self._sections.pop()
                    else:
                        if exist_list[0].attrib == node.attrib:
                            self._add_element(cursect, self._cursect, node)
                            self.etree.find(self._cursect).remove(exist_list[0])

                        #if node.text != exist_list[0].text or (bool(exist_list[0].attrib or node.attrib) ^ (exist_list[0].attrib != node.attrib)):
                        #       self._add_element(cursect, self._cursect, node)
                else:
                    equal = 0
                    for exist_node in exist_list:
                        equal += 0 if not self._compare_tree(exist_node, node) else 1
                    if not equal:
                        self._add_element(cursect, self._cursect, node)
            else:
                self.etree.find(self._cursect).append(node)

    def _compare_tree(self, first, second):

        if first.text and second.text and first.text.strip() != second.text.strip():
            return False

        if first.attrib != second.attrib:
            return False

        first_childs = list(first)
        second_childs = list(second)

        if first_childs and second_childs:
            if len(first_childs) != len(second_childs):
                return False

            comparison = 0
            for f_child in first_childs:
                for s_child in second_childs:
                    comparison += 0 if not self._compare_tree(f_child, s_child) else 1
            if comparison != len(first_childs):
                return False

        return True

    def _add_element(self, after, parent, node):
        after_element  = self.etree.findall(after)[-1]
        parent_element = self.etree.find(parent)
        parent_element.insert(list(parent_element).index(after_element), node)

    def __iter__(self):
        """
        Returns keys iterator
        """
        return self.etree.findall(self._root_path + "*")
        #return ElementPath13.findall(self.etree, self._root_path + "*")

    def _find_all(self, path):
        if not self.etree:
            self._init()
        if not path:
            path = '.'
        ret = self.etree.findall(self._root_path + path)
        indexes = []
        for node in ret:
            if callable(node.tag):
                indexes.append(ret.index(node))

        indexes.reverse()

        for i in indexes:
            del ret[i]

        return ret
        """
        ret = []
        try:
                it = ElementPath13.findall(self.etree, self._root_path + quote(path))
                while 1:
                        ret.append(it.next())
        except StopIteration:
                return ret
        """

    def _find(self, path):
        if not self.etree:
            self._init()
        el = self.etree.find(self._root_path + path)
        # el = ElementPath13.find(self.etree, self._root_path + quote(path))
        if el != None:
            return el
        else:
            raise NoPathError(quote(path))

    def get(self, path):
        if not self.etree:
            self._init()

        """
        @see http://effbot.org/zone/element-xpath.htm
        v = conf.get("general/server_id")
        v = "3233-322"
        """

        return self._find(path).text

    def get_float(self, path):
        return float(self.get(path))

    def get_int(self, path):
        return int(self.get(path))

    def get_boolean(self, path):
        return self.get(path).lower() in ["1", "yes", "true", "on"]

    def get_list(self, path):
        return list(el.text for el in self._find_all(path) if el.tag)

    def get_dict(self, path):
        return [x.attrib for x in self._find_all(path) if x.attrib]

    def _normalize_path(self, path):
        if path[-1] != '/':
            path = path + '/'
        return path

    def _is_element(self, node):
        return node.tag and not callable(node.tag)

    def items(self, path):
        '''
        Returns a list of (name, value) pairs
        '''
        return tuple(
                (node.tag, node.text)
                for node in self._find_all(self._normalize_path(path))
                if self._is_element(node)
        )

    def children(self, path):
        '''
        Returns a list of child names (options and sections)
        '''
        ret_list = self._find_all(self._normalize_path(path))
        return tuple(node.tag for node in ret_list if self._is_element(node))

    def sections(self, path):
        '''
        Returns a list of child sections
        '''
        nodes = self._find_all(self._normalize_path(path))
        return tuple(node.tag for node in nodes
                        if self._is_element(node) and (len(node) or node.attrib.get('mc_type') == 'section'))

    def options(self, path):
        '''
        Returns a list of child options
        '''
        nodes = self._find_all(self._normalize_path(path))
        return tuple(node.tag for node in nodes
                        if self._is_element(node) and not (len(node) or node.attrib.get('mc_type') == 'section'))

    def set(self, path, value, force=False):
        if not self.etree:
            self._init()
        el = self.etree.find(self._root_path + path)
        if el != None:
            self._set(el, value)
        elif force:
            self.add(path, value)
        else:
            raise NoPathError("Path %s doesn't exist" % path)

    def _set(self, el, value):
        if isinstance(value, dict):
            for key in value:
                el.attrib.update({key: value[key]})
        else:
            if value and value != 'None':
                el.text = value

    def add(self, path, value=None, before_path=None, force=False):
        """
        Add value at path <path>
        if before_path specified, new option will be added right after it.
        """
        value = str(value)

        if not self.etree:
            self._init()

        after_element = None
        before_element = None

        if path.endswith('/'):
            path = path[:-1]

        if before_path:
            if '/' in path and '/' in before_path:
                raise Exception('Use absolute path in path or before_path arguments')
            if '/' in before_path:
                # Search by before_path
                try:
                    before_element = self._find(before_path)
                    parent = self._find(os.path.join(before_path, '..'))
                except:
                    raise MetaconfError('Cannot find %s' % before_path)
            else:
                parent_path     = os.path.dirname(path) or '.'
                parent  = self._find(parent_path)
                path_list = self._find_all(parent_path +'/'+ before_path)
                if len(path_list):
                    before_element = path_list[0]
        else:
            parent_path = os.path.dirname(path) or '.'
            try:
                parent  = self._find(parent_path)
            except:
                if force:
                    if re.search('\*|\.\.|\[|\]|//', parent_path):
                        raise MetaconfError("Can't use predicates with force argument")
                    self.add(parent_path, force=True)
                    parent  = self._find(parent_path)
                else:
                    raise
            path_list = self._find_all(path)
            if len(path_list):
                after_element = path_list[-1]

        el = self._provider.create_element(self.etree, os.path.join(self._root_path, path), value)

        if after_element != None:
            parent.insert(list(parent).index(after_element) + 1, el)
        elif before_element != None:
            parent.insert(list(parent).index(before_element), el)
        else:
            parent.append(el)
            self._set(el, value)
        self._set(el, value)

        """
        1.
        [general]
        server_id = Piska

        conf.add("general/behaviour", "cassandra", "general/server_id")

        [general]
        behaviour = cassandra
        server_id = Piska

        2.
        [general]
        behaviour = app

        conf.add("general/behaviour", "cassandra")
root_path=path+"/"
        [general]
        behaviour = app
        behaviour = cassandra
        """

        """
        Create elements, call _set
        """


    def remove(self, path, value=None):
        """
        Remove path. If value is passed path is treatead as list key,
        # and config removes specified value from it.
        """
        """
        conf.remove("Seeds/Seed")
        empty Seeds
        conf.remove("Seeds/Seed", "143.66.21.76")
        remove "143.66.21.76" from list
        """
        try:
            opt_list = self._find_all(path)
            parent = self.subset(path)._find('..')
            if value:
                for opt in opt_list:
                    if opt.text.strip() == value:
                        parent.remove(opt)
            else:
                for opt in opt_list:
                    parent.remove(opt)
        except NoPathError:
            pass

    def subset(self, path):
        """
        Return wrapper for configuration subset under specified path
        """

        """
        find el at path

        subconf = conf["Seeds/Seed[1]"]

        """
        self._find(path)
        return Configuration(format=self._format, etree=self.etree, root_path=path+"/")


    @property
    def empty(self):
        return not bool(list(self.etree.getroot()))

"""
class PyConfigParserAdapter:
        def __init__(self, conf):
                pass

        def sections(self):
                pass

        def add_section(self, section, before=None):
                pass

        def has_section(self, section):
                pass

        def options(self, section):
                pass
"""


"""
class YamlFormatProvider:

        def __init__(self):
                if not YamlFormatProvider._yaml:
                        try:
                                YamlFormatProvider._yaml = __import__("yaml")
                        except ImportError:
                                raise MetaconfError("`'yaml' module is not defined. Install PyYAML package")

        def read(self, fp, filename):
                try:
                        self._root = ET.Element('configuration')
                        self._cursect = self._root
                        dict = YamlFormatProvider._yaml.load(fp.read(), Loader = YamlFormatProvider._yaml.BaseLoader)
                        self._parse(dict)
                        indent(self._root)
                        return list(self._root)
                except (BaseException, Exception), e:
                        raise ParseError((e,))

        def _parse(self, iterable):
                if isinstance(iterable, dict):
                        cursect = []
                        for key in iterable:
                                new_opt = ET.SubElement(self._cursect, str(key))
                                cursect.append(self._cursect)
                                self._cursect = new_opt
                                self._parse(iterable[key])
                                self._cursect = cursect.pop()
                elif isinstance(iterable, list):
                        for value in iterable:
                                self._parse(value)
                else:
                        self._cursect.text = str(iterable)

format_providers["yaml"] = YamlFormatProvider
"""

"""
[general]
; Server behaviour (app|www|mysql)
behaviour = app
behaviour = www
behaviour = mysql

; Role name ex = mysqllvm64
role_name

; Platform on which scalarizr is deployed
;   ec2     - Amazon EC2,
;   rs      - RackSpace cloud servers
;   vps     - Standalone VPS server
platform = vps
"""

'''
# use for xml.etree.ElementTree for hierarchy

root = ET.Element("configuration")
gen = ET.SubElement(root, "general")
gen.append(ET.Comment("Server behaviour (app|www|mysql)"))
bh = ET.SubElement(gen, "behaviour")
bh.text = "app"
bh = ET.SubElement(gen, "behaviour")
bh.text = "www"
bh = ET.SubElement(gen, "behaviour")
bh.text = "mysql"
# ...


conf = Configuration("ini")
bhs = conf.get_list("general/behaviour")
platform = conf.get("general/platform")
conf.set("handler_mysql/replication_main", 1, bool)

# Access sections
sect = conf.subset("handler_mysql")  # 1 way
sect = conf["handler_mysql"]                    # 2 shorter way
sect.set("replication_main", 1, bool)


class XmlFormatProvider:
        pass
format_providers["xml"] = XmlFormatProvider


"""
<Storage>
  <!--======================================================================-->
  <!-- Basic Configuration                                                  -->
  <!--======================================================================-->

  <!--
   ~ The name of this cluster.  This is mainly used to prevent machines in
   ~ one logical cluster from joining another.
  -->
  <ClusterName>Test Cluster</ClusterName>
<AutoBootstrap>false</AutoBootstrap>

  <!--
   ~ See http://wiki.apache.org/cassandra/HintedHandoff
  -->
  <HintedHandoffEnabled>true</HintedHandoffEnabled>

  <!--
   ~ Keyspaces and ColumnFamilies:
   ~ A ColumnFamily is the Cassandra concept closest to a relational
   ~ table.  Keyspaces are separate groups of ColumnFamilies.  Except in
   ~ very unusual circumstances you will have one Keyspace per application.

   ~ There is an implicit keyspace named 'system' for Cassandra internals.
  -->
  <Keyspaces>
    <Keyspace Name="Keyspace1">
        <ColumnFamily Name="Standard2"
                    CompareWith="UTF8Type"
                    KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>


  </Keyspaces>


  <Seeds>
      <Seed>127.0.0.1</Seed>
      <Seed>10.196.18.36</Seed>
  </Seeds>

</Storage> """
'''



"""
1.
####First:

<Storage>

  <Keyspaces>
    <Keyspace Name="Keyspace">
        <ColumnFamily Name="Standard2" CompareWith="UTF8Type" KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
  </Keyspaces>

   <Keyspaces>
    <Keyspace Name="Keyspace">
        <ColumnFamily Name="Standard3" CompareWith="UTF8Type" KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
  </Keyspaces>

  <Seeds>
      <Seed>127.0.0.1</Seed>
      <Seed>10.196.18.36</Seed>
  </Seeds>
</Storage>

####Second:

<Storage>
  <Keyspaces>
    <Keyspace Name="Keyspace">
        <ColumnFamily Name="Standard2" CompareWith="UTF8Type" KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
  </Keyspaces>

  <Keyspaces>
    <Keyspace Name="Keyspace">
        <ColumnFamily Name="Standard4" CompareWith="UTF8Type" KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
  </Keyspaces>

  <Seeds>
      <Seed>127.0.0.1</Seed>
      <Seed>10.196.18.36</Seed>
  </Seeds>
</Storage>

2. #### First

<Keyspaces>
  <Keyspace Name="Keyspace">
        <ColumnFamily Name="Standard" CompareWith="UTF8Type" KeysCached="100%"/>
     <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
  </Keyspace>
</Keyspaces>

##### Second

<Keyspaces>
  <Keyspace Name="Keyspace3">
        <ColumnFamily Name="Standard" CompareWith="UTF8Type" KeysCached="100%"/>
     <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
  </Keyspace>
</Keyspaces2>

3. ####### First


"""
