import importlib
import re
from types import ModuleType
import numpy as np
from enum import Enum
from .core import DataJointError, from_camel_case
from .relational import _Relational
from .heading import Heading
import logging


# table names have prefixes that designate their roles in the processing chain
logger = logging.getLogger(__name__)

Role = Enum('Role','manual lookup imported computed job')
rolePrefix = {
    Role.manual   : '',
    Role.lookup   : '#',
    Role.imported : '_',
    Role.computed : '__',
    Role.job      : '~'
    }
prefixRole = dict(zip(rolePrefix.values(),rolePrefix.keys()))

mysql_constants = ['CURRENT_TIMESTAMP']


class Base(_Relational):
    """
    datajoint.Base integrates all data manipulation and data declaration functions.
    An instance of the class provides an interface to a single table in the database.

    An instance of the class can be produced in two ways:
        1. direct instantiation  (used mostly for debugging and odd jobs)
        2. instantiation from a derived class (regular recommended use)

    With direct instantiation, instance parameters must be explicitly specified.
    With a derived class, all the instance parameters are taken from the module
    of the deriving class. The module must declare the connection object conn.
    The name of the deriving class is used as the table's className.

    The table associated with an instance of Base is identified by the className
    property, which is a string in CamelCase. The actual table name is obtained 
    by converting className from CamelCase to underscore_separated_words and 
    prefixing according to the table's role.
    
    The table declaration can be specified in the doc string of the inheriting 
    class, in the DataJoint table declaration syntax. 
    
    Base also implements the methods insert and delete to insert and delete tuples
    from the table. It can also be an argument in relational operators: restrict, 
    join, pro, and aggr.  See class _Relational.
    
    Base instances return their table's heading by looking it up in the connection
    object. This ensures that Base instances contain the current table definition
    even after tables are modified after the instance is created.
    """

    def __init__(self, conn=None, dbname=None, class_name=None, declaration=None):
        if self.__class__ is Base:
            # instantiate without subclassing
            if not (conn and dbname and class_name):
                raise DataJointError('Missing argument: please specify conn, dbname, and class name.')
            self.class_name = class_name
            self.conn = conn
            self.dbname = dbname
            self.declaration = declaration
            if dbname not in self.conn.modules:    # register with a fake module, enclosed in backquotes
                self.conn.bind('`{0}`'.format(dbname), dbname)
        else:
            # instantiate a derived class
            if conn or dbname or class_name or declaration:
                raise DataJointError('With derived classes, constructor arguments are ignored')
            self.class_name = self.__class__.__name__
            module = self.__module__
            mod_obj = importlib.import_module(module)
            try:
                self.conn = mod_obj.conn
            except AttributeError:
                raise DataJointError("Please define object 'conn' in '{}'.".format(self.__module__))
            try:
                self.dbname = self.conn.dbnames[module]
            except KeyError:
                raise DataJointError('Module {} is not bound to a database. See datajoint.connection.bind'.format(self.__module__))
            # take table declaration from the deriving class' doc string
            self.declaration = self.__doc__


    def insert(self, tup, ignoreErrors=False, replace=False):
        """
        insert one tuple.  tup can be an iterable in matching order, a dict with named fields, or an np.void.

        EXAMPLE:
        b = djtest.Subject()
        b.insert(dict(subject_id=7,species="mouse",real_id=1007,date_of_birth="2014-09-01"))
        """
        if issubclass(type(tup), tuple) or issubclass(type(tup), list):
           valueList = ','.join([repr(q) for q in tup])
           fieldList = '`'+'`,`'.join(self.heading.names[0:len(tup)])+'`'
        elif issubclass(type(tup), dict):
            valueList = ','.join([repr(tup[q]) for q in self.heading.names if q in tup])
            fieldList = '`'+'`,`'.join([q for q in self.heading.names if q in tup])+'`'
        elif issubclass(type(tup), np.void):
            valueList = ','.join([repr(tup[q]) for q in self.heading.names if q in tup])
            fieldList = '`'+'`,`'.join(tup.dtype.fields)+'`'
        else:
            raise DataJointError('Datatype %s cannot be inserted' % type(tup))
        if replace:
            sql = 'REPLACE'
        elif ignoreErrors:
            sql = 'INSERT IGNORE'
        else:
            sql = 'INSERT'
        sql += " INTO %s (%s) VALUES (%s)" % (self.full_table_name, fieldList, valueList)
        logger.info(sql)
        self.conn.query(sql)

    def drop(self):
        """
        drop table
        """
        # TODO make cascading
        self.conn.query('DROP TABLE %s' % self.full_table_name)
        self.conn.clear_dependencies(dbname=self.dbname)
        self.conn.load_headings(dbname=self.dbname, force=True)


    @property    
    def sql(self):
        return self.full_table_name + self._whereClause
                
    @property
    def heading(self):
        self.declare()
        return self.conn.headings[self.dbname][self.table_name]

    @property
    def is_declared(self):
        "True if table is found in the database"
        self.conn.load_headings(self.dbname)
        return self.class_name in self.conn.table_names[self.dbname]

    @property
    def table_name(self):
        self.declare()
        return self.conn.table_names[self.dbname][self.class_name]


    @property
    def full_table_name(self):
        return '`%s`.`%s`' % (self.dbname, self.table_name)

    @property
    def full_class_name(self):
        return '{}.{}'.format(self.__module__, self.class_name)

    @property
    def primary_key(self):
        return self.heading.primary_key

    def declare(self):
        """
        Declare the table in database if it doesn't already exist.
        """
        if not self.is_declared:
            self._declare()
            if not self.is_declared:
                raise DataJointError('Table could not be declared for %s' % self.class_name)

    """
    Data Definition Functionalities
    """
    def set_table_comment(self, newComment):
        """
        Update the table comment in the table declaration.
        """
        # TODO: add verification procedure
        self.alter('COMMENT="%s"' % newComment)

    def add_attribute(self, definition, first=False,  after=''):
        """
        Add a new attribute to the table. A full line from the
        table definition is passed in as "definition".

        The definition can specify where to place the new attribute.
        Make after="First" to add the attribute as the first attribute
        or "AFTER `attribute`" to place it after an existing attribute.
        """
        position = ' FIRST' if first else (' AFTER %s' % after if after else '')
        sql = self._field_to_SQL(self._parse_attr_def(definition))
        self._alter('ADD COLUMN %s%s' % (sql[:-2], position))

    def drop_attribute(self, attrName):
        """
        Drop the attribute attrName from this table
        """
        self._alter('DROP COLUMN `%s`' % attrName)

    def alter_attribute(self, attrName, newDefinition):
        """
        Alter the definition of the field attrName in
        this table using the newDefinition.
        """
        sql = self._field_to_SQL(self._parse_attr_def(newDefinition))
        self._alter('CHANGE COLUMN `%s` %s' % (attrName, sql[:-2]))

    def erd(self, subset=None, prog='dot'):
        """
        plot the schema's entity relationship diagram (ERD).
        The layout programs can be 'dot' (default), 'neato', 'fdp', 'sfdp', 'circo', 'twopi'
        """
        if not subset:
            g = self.graph
        else:
            g = self.graph.copy()
        """
         g = self.graph
         else:
         g = self.graph.copy()
         for i in g.nodes():
         if i not in subset:
         g.remove_node(i)
        def tablelist(tier):
        return [i for i in g if self.tables[i].tier==tier]

        pos=nx.graphviz_layout(g,prog=prog,args='')
        plt.figure(figsize=(8,8))
        nx.draw_networkx_edges(g, pos, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tablelist('manual'),
        node_color='g', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tablelist('computed'),
        node_color='r', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tablelist('imported'),
        node_color='b', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tablelist('lookup'),
        node_color='gray', node_size=120, alpha=0.3)
        nx.draw_networkx_labels(g, pos, nodelist = subset, font_weight='bold', font_size=9)
        nx.draw(g,pos,alpha=0,with_labels=false)
        plt.show()
        """

    @classmethod
    def get_module(cls, module_name):
        mod_obj = importlib.import_module(cls.__module__)
        attr = getattr(mod_obj, module_name, None)
        if isinstance(attr, ModuleType):
            return attr
        if mod_obj.__package__:
            try:
                return importlib.import_module('.'+module_name, mod_obj.__package__)
            except ImportError:
                pass
        try:
            return importlib.import_module(module_name)
        except ImportError:
            return None

    def get_base(self, module_name, class_name):
        """
        load base relation from module.  If the base relation is not defined in
        the module, then construct it using Base constructor.
        """
        mod_obj = self.get_module(module_name)
        try:
            ret = getattr(mod_obj, class_name)()
        except KeyError:
            ret = self.__class__(conn=self.conn,
                                 dbname=self.conn.schemas[module_name],
                                 class_name=class_name)
        return ret



    #////////////////////////////////////////////////////////////
    # Private Methods
    #////////////////////////////////////////////////////////////

    def _field_to_SQL(self, field):
        """
        Converts an attribute definition tuple into SQL code
        """
        if field.isNullable:
            default = 'DEFAULT NULL'
        else:
            default = 'NOT NULL'
            # if some default specified
            if field.default:
                # enclose value in quotes (even numeric), except special SQL values
                # or values already enclosed by the user
                if field.default.upper() in mysql_constants or field.default[:1] in ["'", '"']:
                    default = '%s DEFAULT %s' % (default, field.default)
                else:
                    default = '%s DEFAULT "%s"' % (default, field.default)

        # TODO: escape instead! - same goes for Matlab side implementation
        assert not any((c in r'\"' for c in field.comment)), \
            'Illegal characters in attribute comment "%s"' % field.comment

        return '`{name}` {type} {default} COMMENT "{comment}",\n'.format(\
            name=field.name, type=field.type, default=default, comment=field.comment)

    def _alter(self, alterStatement):
        """
        Execute ALTER TABLE statment for this table. The schema
        will be reloaded within the connection object.
        """
        sql = 'ALTER TABLE %s %s' % (self.full_table_name, alterStatement)
        self.conn.query(sql)
        self.conn.load_headings(self.dbname, force=True)
        # TODO: place table definition sync mechanism

    def _declare(self):
        """
        _declare is called when no table in the database matches this object
        """
        table_info, parents, referenced, fieldDefs, indexDefs = self._parse_declaration()
        defined_name = table_info['module'] + '.' + table_info['className']
        expected_name = self.__module__.split('.')[-1] + '.' + self.class_name
        if not defined_name == expected_name:
            raise DataJointError('Table name {} does not match the declared'
                                 'name {}'.format(expected_name, defined_name))

        # compile the CREATE TABLE statement
        # TODO: support prefix
        table_name = rolePrefix[table_info['tier']] + from_camel_case(self.class_name)
        sql = 'CREATE TABLE `%s`.`%s` (\n' % (self.dbname, table_name)

        # add inherited primary key fields
        primary_key_fields = set()
        non_key_fields = set()
        for p in parents:
            for key in p.primary_key:
                field = p.heading[key]
                if field.name not in primary_key_fields:
                    primary_key_fields.add(field.name)
                    sql += self._field_to_SQL(field)
                else:
                    logger.debug('Field definition of {} in {} ignored'.format(
                        field.name, p.full_class_name))

        # add newly defined primary key fields
        for field in (f for f in fieldDefs if f.isKey):
            if field.isNullable:
                raise DataJointError('Primary key {} cannot be nullable'.format(
                    field.name))
            if field.name in primary_key_fields:
                raise DataJointError('Duplicate declaration of the primary key '\
                                     '{key}. Check to make sure that the key '\
                                     'is not declared already in referenced '\
                                     'tables'.format(key=field.name))
            primary_key_fields.add(field.name)
            sql += self._field_to_SQL(field)

        # add secondary foreign key attributes
        for r in referenced:
            keys = (x for x in r.heading.attrs.values() if x.isKey)
            for field in keys:
                if field.name not in primary_key_fields | non_key_fields:
                    non_key_fields.add(field.name)
                    sql += self._field_to_SQL(field)

        # add dependent attributes
        for field in (f for f in fieldDefs if not f.isKey):
            non_key_fields.add(field.name)
            sql += self._field_to_SQL(field)

        # add primary key declaration
        assert len(primary_key_fields)>0, 'table must have a primary key'
        keys = ', '.join(primary_key_fields)
        sql += 'PRIMARY KEY (%s),\n' % keys

        # add foreign key declarations
        for ref in parents+referenced:
            keys = ', '.join(ref.primary_key)
            sql += 'FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE CASCADE ON DELETE RESTRICT,\n' % \
                    (keys, ref.full_table_name, keys)


        # add secondary index declarations
        # gather implicit indexes due to foreign keys first
        implicit_indices = []
        for fk_source in parents+referenced:
            implicit_indices.append(fk_source.primary_key)

        #for index in indexDefs:
        #TODO: finish this up...

        # close the declaration
        sql = '%s\n) ENGINE = InnoDB, COMMENT "%s"' % (sql[:-2], table_info['comment'])

        # make sure that the table does not alredy exist
        self.conn.load_headings(self.dbname, force=True)
        if not self.is_declared:
            # execute declaration
            logger.debug('\n<SQL>\n' + sql + '</SQL>\n\n')
            self.conn.query(sql)
            self.conn.load_headings(self.dbname, force=True)

    def _parse_declaration(self):
        """
        Parse declaration and create new SQL table accordingly
        """
        parents = []
        referenced = []
        index_defs = []
        field_defs = []
        declaration = re.split(r'\s*\n\s*', self.declaration.strip())

        # remove comment lines
        declaration = [x for x in declaration if not x.startswith('#')]
        ptrn = """
        ^(?P<module>\w+)\.(?P<className>\w+)\s*     #  module.className
        \(\s*(?P<tier>\w+)\s*\)\s*                  #  (tier)
        \#\s*(?P<comment>.*)$                       #  comment
        """
        p = re.compile(ptrn, re.X)
        table_info = p.match(declaration[0]).groupdict()
        if table_info['tier'] not in Role.__members__:
            raise DataJointError('InvalidTableTier: Invalid tier {tier} for table\
                                 {module}.{cls}'.format(tier=table_info['tier'],
                                                        module=table_info['module'],
                                                        cls=table_info['className']))
        table_info['tier'] = Role[table_info['tier']] # convert into enum

        in_key = True # parse primary keys
        field_ptrn = """
        ^[a-z][a-z\d_]*\s*          # name
        (=\s*\S+(\s+\S+)*\s*)?      # optional defaults
        :\s*\w.*$                   # type, comment
        """
        fieldP = re.compile(field_ptrn, re.I + re.X) # ignore case and verbose

        for line in declaration[1:]:
            if line.startswith('---'):
                in_key = False # start parsing non-PK fields
            elif line.startswith('->'):
                # foreign key
                module, className = line[2:].strip().split('.')
                rel = self.get_base(self.conn, module, className)
                (parents if in_key else referenced).append(rel)
            elif re.match(r'^(unique\s+)?index[^:]*$', line):
                index_defs.append(self._parse_index_def(line))
            elif fieldP.match(line):
                field_defs.append(self._parse_attr_def(line, in_key))
            else:
                raise DataJointError('Invalid table declaration line "%s"' % line)

        return table_info, parents, referenced, field_defs, index_defs

    def _parse_attr_def(self, line, inKey=False):
        """
        Parse attribute definition line in the declaration and returns
        an attribute tuple.
        """
        line = line.strip()
        attr_ptrn = """
        ^(?P<name>[a-z][a-z\d_]*)\s*             # field name
        (=\s*(?P<default>\S+(\s+\S+)*?)\s*)?      # default value
        :\s*(?P<type>\w[^\#]*[^\#\s])\s*         # datatype
        (\#\s*(?P<comment>\S*(\s+\S+)*)\s*)?$    # comment
        """

        attrP = re.compile(attr_ptrn, re.I + re.X)
        m = attrP.match(line)
        assert m, 'Invalid field declaration "%s"' % line
        attr_info = m.groupdict()
        if not attr_info['comment']:
            attr_info['comment'] = ''
        if not attr_info['default']:
            attr_info['default'] = ''
        attr_info['isNullable'] = attr_info['default'].lower() == 'null'
        assert (not re.match(r'^bigint', attr_info['type'], re.I) or not attr_info['isNullable']), \
            'BIGINT attributes cannot be nullable in "%s"' % line

        attr_info['isKey'] = inKey;
        attr_info['isAutoincrement'] = None
        attr_info['isNumeric'] = None
        attr_info['isString'] = None
        attr_info['isBlob'] = None
        attr_info['computation'] = None
        attr_info['dtype'] = None

        return Heading.AttrTuple(**attr_info)

    def _parse_index_def(self, line):
        line = line.strip()
        index_ptrn = """
        ^(?P<unique>UNIQUE)?\s*INDEX\s*      # [UNIQUE] INDEX
        \((?P<attributes>[^\)]+)\)$          # (attr1, attr2)
        """
        indexP = re.compile(index_ptrn, re.I + re.X)
        m = indexP.match(line)
        assert m, 'Invalid index declaration "%s"' % line
        index_info = m.groupdict()
        attributes = re.split(r'\s*,\s*', index_info['attributes'].strip())
        index_info['attributes'] = attributes
        assert len(attributes) == len(set(attributes)), \
        'Duplicate attributes in index declaration "%s"' % line
        return index_info