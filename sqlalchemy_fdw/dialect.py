"""A custom dialect for handling foreign tables on postgresql"""

from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.engine import reflection
from sqlalchemy.schema import Table, ForeignKeyConstraint
from sqlalchemy import sql, types as sqltypes, exc
from .util import sql_options


def is_foreign(t):
    return t.key in getattr(t.metadata, '_foreign_tables', {})


class PGDDLCompilerFdw(PGDDLCompiler):
    """A DDL compiler for the pgfdw dialect, for managing foreign tables"""

    def post_create_table(self, table):
        if is_foreign(table):
            preparer = self.dialect.identifier_preparer
            post = ' server %s ' % table.pgfdw_server
            post += sql_options(table.pgfdw_options, preparer)
            return post
        else:
            return super(PGDDLCompilerFdw, self).post_create_table(table)

    def visit_drop_table(self, drop):
        prefix = ""
        if is_foreign(drop.element):
            prefix = "FOREIGN"
        return "DROP %s TABLE %s" % (
            prefix, self.preparer.format_table(drop.element))

    def create_table_constraints(self, table,
                                 _include_foreign_key_constraints=None):
        # No constraint in foreign tables
        if is_foreign(table):
            return ''
        else:
            constraints = []
            if table.primary_key:
                constraints.append(table.primary_key)

            constraints.extend([c for c in table._sorted_constraints
                                if c is not table.primary_key])

            def foreign_foreign_key(constraint):
                """Return whether this is a foreign key
                   referencing a foreign table"""
                return isinstance(
                    constraint, ForeignKeyConstraint
                ) and is_foreign(constraint._referred_table)

            return ", \n\t".join(p for p in (
                self.process(constraint)
                for constraint in constraints
                if (
                    constraint._create_rule is None or
                    constraint._create_rule(self))
                and (
                    not self.dialect.supports_alter or
                    not getattr(constraint, 'use_alter', False))
                and not foreign_foreign_key(constraint)
                ) if p is not None)


class PGDialectFdw(PGDialect_psycopg2):
    """An sqldialect based on psyopg2 for managing foreign tables

    To use it, simply use pgfdw in the connection string::

        create_engine('pgfdw://user:password@localhost:5432/dbname')

    """

    ddl_compiler = PGDDLCompilerFdw

    construct_arguments = [
        (Table, {
            "server": None,
            "options": None
        })
    ]

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        if schema is not None:
            current_schema = schema
        else:
            current_schema = self.default_schema_name
        PK_SQL = """
            SELECT cu.column_name
            FROM information_schema.table_constraints tc
            INNER JOIN information_schema.key_column_usage cu
                on cu.constraint_name = tc.constraint_name and
                    cu.table_name = tc.table_name and
                    cu.table_schema = tc.table_schema
            WHERE cu.table_name = :table_name and
                    constraint_type = 'PRIMARY KEY'
                    and cu.table_schema = :schema;
        """
        t = sql.text(PK_SQL).columns(attname=sqltypes.Unicode)
        c = connection.execute(t, table_name=table_name, schema=current_schema)
        primary_keys = [r[0] for r in c.fetchall()]
        return primary_keys

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is not None:
            current_schema = schema
        else:
            current_schema = self.default_schema_name

        result = connection.execute(
            sql.text(
                "SELECT relname FROM pg_class c "
                "WHERE relkind in ('r', 'f') "
                f"AND '{current_schema}' = (select nspname from pg_namespace n "
                "where n.oid = c.relnamespace) "
            ).columns(relname=sqltypes.Unicode)
        )
        return [row[0] for row in result]

    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        """Fetch the oid for schema.table_name.

        Several reflection methods require the table oid.  The idea for using
        this method is that it can be fetched one time and cached for
        subsequent calls.

        """
        table_oid = None
        if schema is not None:
            schema_where_clause = "n.nspname = :schema"
        else:
            schema_where_clause = "pg_catalog.pg_table_is_visible(c.oid)"
        query = """
            SELECT c.oid
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE (%s)
            AND c.relname = :table_name AND c.relkind in ('r', 'v', 'f')
        """ % schema_where_clause
        # Since we're binding to unicode, table_name and schema_name must be
        # unicode.
        table_name = str(table_name)
        bindparams = [sql.bindparam('table_name', type_=sqltypes.Unicode)]
        if schema is not None:
            schema = str(schema)
            bindparams.append(sql.bindparam('schema', type_=sqltypes.Unicode))
        s = sql.text(query).bindparams(*bindparams).columns(oid=sqltypes.Integer)
        c = connection.execute(s, table_name=table_name, schema=schema)
        table_oid = c.scalar()
        if table_oid is None:
            raise exc.NoSuchTableError(table_name)
        return table_oid

    @reflection.cache
    def get_foreign_table_options(self, connection, pgfdw_table):
        oid = self.get_table_oid(connection, pgfdw_table.name,
                                 pgfdw_table.schema)
        query = """
        SELECT ftoptions, srvname
        FROM pg_foreign_table t inner join pg_foreign_server s
        ON t.ftserver = s.oid
        WHERE t.ftrelid = :oid
        """
        s = sql.text(query).bindparams(
            sql.bindparam('oid', type_=sqltypes.Integer)
        ).columns(ftoptions=ARRAY(sqltypes.Unicode), srvname=sqltypes.Unicode)
        c = connection.execute(s, oid=oid)
        options, srv_name = c.fetchone()
        pgfdw_table.pgfdw_server = srv_name
        pgfdw_table.pgfdw_options = dict([
            option.split('=', 1) for option in options
        ]) if options is not None else {}

dialect = PGDialectFdw
