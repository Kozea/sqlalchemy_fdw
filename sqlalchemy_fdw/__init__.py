"""Contains Schema element and compilers for foreign table and fdw.
"""

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement, Table
from sqlalchemy.sql.base import _bind_or_error
from sqlalchemy import sql
from sqlalchemy import types
from .util import sql_options


class ForeignTable(Table):
    """Defines a Foreign Table

    A Foreign Table is a postgresql table located on a remote server.
    To create remote servers, look at :class:`ForeignDataWrapper`.

    This functionality has been tagged stable in postgresql 9.1

    Assuming you already created a server 'myserver', a foreign table can be
    defined on it like this::

        mytable = ForeignTable("mytable", metadata,
                               Column('id', Integer),
                               Column('name', Unicode),
                               pgfdw_server='myserver)

    You can then use it like any table, except:
        - only select statements are supported
        - constraints are not supported

    Constructor arguments are the same as :class:`Table`, plus:

    :param: pgfdw_server: the name of the server this table belongs to.

    :param: pgfdw_options: a dictionary containing the table options.

        These options are passed directly to the foreign table as an OPTIONS
        clause.

        e.g::

            mytable = ForeignTable("mytable", metadata,
                                   Column('id', Integer),
                                   Column('name', Unicode),
                                   pgfdw_server='myserver',
                                   pgfdw_options={'option1': 'test'})

        Results in the following sql::

        CREATE FOREIGN TABLE mytable (
            id integer,
            name character varying
        ) server myserver options (
            "option1" 'test'
        );


    For more information on the available foreign data wrappers,
    see `http://pgxn.org/tag/foreign%20data%20wrapper/`.

    """

    def __new__(cls, *args, **kwargs):
        if not args:
            # python3k pickle seems to call this
            return object.__new__(cls)

        table = super(ForeignTable, cls).__new__(cls, *args, **kwargs)
        metadata = args[1]
        table.pgfdw_server = kwargs.pop('pgfdw_server', None)
        table.pgfdw_options = kwargs.pop('pgfdw_options', None) or {}
        if 'FOREIGN' not in table._prefixes:
            table._prefixes.append('FOREIGN')

        if not hasattr(metadata, '_foreign_tables'):
            metadata._foreign_tables = {}

        metadata._foreign_tables[table.key] = table

        autoload = kwargs.get('autoload', False)
        autoload_with = kwargs.get('autoload_with', None)
        if autoload:
            if autoload_with:
                autoload_with.run_callable(
                    autoload_with.dialect.get_foreign_table_options,
                    table)
            else:
                bind = _bind_or_error(
                    table.metadata,
                    msg="No engine is bound to this ForeignTable's MetaData. "
                    "Pass an engine to the Table via "
                    "autoload_with=<someengine>, "
                    "or associate the MetaData with an engine via "
                    "metadata.bind=<someengine>")
                bind.run_callable(
                    bind.dialect.get_foreign_table_options, table)
        return table


class ForeignDataWrapper(DDLElement):
    """Defines a foreign data wrapper server

    A foreign data wrapper server must be defined to access a foreign data
    wrapper installed as an extension.

    Basic usage::

        myfdw = ForeignDataWrapper('myfdw', 'mysql_fdw', metadata)
        myfdw.create(checkfirst=True)
        # Create some :class:`ForeignTable`
        myfdw.drop()

    Constructor accepts the following arguments:

    :param: name: the server name
    :param: extension_name: the foreign data wrapper extension to be used
    :param: metadata: (optional) the :class:`MetaData` object to bind with
    :param: bind: (optional) the :class:`Engine` object to bind with

    """

    def __init__(self, name, extension_name, metadata=None, bind=None,
                 options=None):
        self.name = name
        self.options = options or {}
        self.extension_name = extension_name
        self.metadata = metadata
        self._bind = bind

    @property
    def bind(self):
        """Returns the current bind"""
        return self._bind or self.metadata.bind

    def check_existence(self, bind=None):
        """Checks if a server with the same name already exists.

        :param: bind: (optional) if not bind is supplied, the current binding
                      (from the metatadata) will be used.

        """
        if bind is None:
            bind = _bind_or_error(self)
        bindparams = [
            sql.bindparam('name', str(self.name), type_=types.Unicode)]
        cursor = bind.execute(
            sql.text(
                "select srvname from pg_foreign_server where srvname = :name"
            ).bindparams(*bindparams)
        )
        return bool(cursor.first())

    def create(self, bind=None, checkfirst=False):
        """Create the server.

        :param: bind: (optional) The bind to use instead of the instance one
        :param: checkfirst: Check if the server exists before creating it.

        """

        if bind is None:
            bind = _bind_or_error(self)
        if not checkfirst or not self.check_existence(bind):
            CreateForeignDataWrapper(
                self.name, self.extension_name, bind=bind, options=self.options
            ).execute()

    def drop(self, bind=None, checkfirst=False, cascade=False):
        """Drop the server

        :param: bind: (optional) The bind to use instead of the instance one
        :param: checkfirst: Check if the server exists before dropping it.
        :param: cascade: appends the CASCADE keyword to the drop statement.

        """
        if bind is None:
            bind = _bind_or_error(self)
        if not checkfirst or self.check_existence(bind):
            DropForeignDataWrapper(
                self.name, self.extension_name, bind=bind, cascade=cascade
            ).execute()


class CreateForeignDataWrapper(ForeignDataWrapper):
    """The concrete create statement"""
    pass


class DropForeignDataWrapper(ForeignDataWrapper):
    """The concrete drop statement"""

    def __init__(self, *args, **kwargs):
        self.cascade = kwargs.pop('cascade', False)
        super(DropForeignDataWrapper, self).__init__(*args, **kwargs)


@compiles(CreateForeignDataWrapper)
def visit_create_fdw(create, compiler, **kw):
    """Compiler for the create server statement"""
    preparer = compiler.dialect.identifier_preparer
    statement = (
        "CREATE server %s foreign data wrapper %s " % (
            preparer.quote_identifier(create.name),
            preparer.quote_identifier(create.extension_name)))
    statement += sql_options(create.options, preparer)
    return statement


@compiles(DropForeignDataWrapper)
def visit_drop_fdw(drop, compiler, **kw):
    """Compiler for drop server statement"""
    preparer = compiler.dialect.identifier_preparer
    statement = "DROP server %s " % (preparer.quote_identifier(drop.name))
    if drop.cascade:
        statement += " CASCADE"
    return statement
