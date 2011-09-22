"""A custom dialect for handling foreign tables on postgresql"""

from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from .util import sql_options


class PGDDLCompilerFdw(PGDDLCompiler):
    """A DDL compiler for the pgfdw dialect, for managing foreign tables"""

    def post_create_table(self, table):
        if hasattr(table, 'fdw_server'):
            preparer = self.dialect.identifier_preparer
            post = ' server %s ' % table.fdw_server
            post += sql_options(table.fdw_options, preparer)
            return post
        else:
            return super(PGDDLCompilerFdw, self).post_create_table(table)

    def visit_drop_table(self, drop):
        prefix = ""
        if hasattr(drop.element, 'fdw_server'):
            prefix = "FOREIGN"
        return "DROP %s TABLE %s" % (prefix,
                self.preparer.format_table(drop.element))


class PGDialectFdw(PGDialect_psycopg2):
    """An sqldialect based on psyopg2 for managing foreign tables

    To use it, simply use pgfdw in the connection string::

        create_engine('pgfdw://user:password@localhost:5432/dbname')

    """

    ddl_compiler = PGDDLCompilerFdw

dialect = PGDialectFdw
