"""A custom dialect for handling foreign tables on postgresql"""

from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler, ARRAY
from sqlalchemy.engine import reflection
from sqlalchemy import sql, types as sqltypes, exc
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
        t = sql.text(PK_SQL, typemap={'attname':sqltypes.Unicode})
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
            sql.text("SELECT relname FROM pg_class c "
                "WHERE relkind in ('r', 'f') "
                "AND '%s' = (select nspname from pg_namespace n "
                "where n.oid = c.relnamespace) " %
                current_schema,
                typemap = {'relname':sqltypes.Unicode}
            )
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
            AND c.relname = :table_name AND c.relkind in ('r','v', 'f')
        """ % schema_where_clause
        # Since we're binding to unicode, table_name and schema_name must be
        # unicode.
        table_name = str(table_name)
        if schema is not None:
            schema = str(schema)
        s = sql.text(query, bindparams=[
            sql.bindparam('table_name', type_=sqltypes.Unicode),
            sql.bindparam('schema', type_=sqltypes.Unicode)
            ],
            typemap={'oid':sqltypes.Integer}
        )
        c = connection.execute(s, table_name=table_name, schema=schema)
        table_oid = c.scalar()
        if table_oid is None:
            raise exc.NoSuchTableError(table_name)
        return table_oid

    @reflection.cache
    def get_foreign_table_options(self, connection, fdw_table):
        oid = self.get_table_oid(connection, fdw_table.name, fdw_table.schema)
        query = """
        SELECT ftoptions, srvname
        FROM pg_foreign_table t inner join pg_foreign_server s
        ON t.ftserver = s.oid
        WHERE t.ftrelid = :oid
        """
        s = sql.text(query, bindparams=[
                sql.bindparam('oid', type_=sqltypes.Integer)],
                typemap={'ftoptions': ARRAY(sqltypes.Unicode),
                    'srvname': sqltypes.Unicode})
        c = connection.execute(s, oid=oid)
        options, srv_name = c.fetchone()
        fdw_table.fdw_server = srv_name
        fdw_table.fdw_options = dict([option.split('=', 1)
            for option in options]) if options is not None else {}

dialect = PGDialectFdw
