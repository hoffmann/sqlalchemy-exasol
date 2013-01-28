#!/usr/bin/python
# -*- coding: utf-8 -*-
# exasol_sa/base.py
# Copyright (C) 2012 Dennis Joest, Peter Hoffmann
#

from sqlalchemy import types as sqltypes

from sqlalchemy.dialects.oracle import base as oracle
from sqlalchemy.dialects.oracle.base import NUMBER, DOUBLE_PRECISION
from sqlalchemy.sql import compiler, visitors, expression
from sqlalchemy.engine import reflection
from sqlalchemy import util, sql, log
from sqlalchemy import types as sqltypes

from sqlalchemy.types import VARCHAR, NVARCHAR, CHAR, DATE, DATETIME, \
    BLOB, CLOB, TIMESTAMP, FLOAT, DECIMAL, INTEGER

import re


class ExasolCompiler(oracle.OracleCompiler):

    def visit_select(self, select, **kwargs):
        """LIMIT is supported native in EXASOL
        """

        offset = select._offset

        # Offset/Limit Query by ORA Driver

        if offset is not None:
            return oracle.OracleCompiler.visit_select(self, select,
                    **kwargs)

        # Limit only with order by: use EXA Algorithms

        if not getattr(select, '_oracle_visit', None):
            if not self.dialect.use_ansi:
                if self.stack and 'from' in self.stack[-1]:
                    existingfroms = self.stack[-1]['from']
                else:
                    existingfroms = None

                froms = select._get_display_froms(existingfroms)
                whereclause = self._get_nonansi_join_whereclause(froms)
                if whereclause is not None:
                    select = select.where(whereclause)
                    select._oracle_visit = True

        kwargs['iswrapper'] = getattr(select, '_is_wrapper', False)
        return compiler.SQLCompiler.visit_select(self, select, **kwargs)

    def limit_clause(self, select):
        (limit, offset) = (select._limit, select._offset)

        if (limit, offset) == (None, None):
            return ''
        if offset is not None:
            return ''  # offset not supported by EXA use ORA fallback

        # No offset provided, so just use the limit

        return ' \n LIMIT %d' % (int(limit), )


class ExasolDialect(oracle.OracleDialect):

    statement_compiler = ExasolCompiler
    max_identifier_length = 128
    supports_unicode_statements = False
    supports_unicode_binds = False

    # change use_binds_for_limits to false for exasol compatability

    def __init__(
        self,
        use_ansi=True,
        optimize_limits=False,
        use_binds_for_limits=False,
        **kwargs
        ):

        # print "XX",self.supports_unicode_statements

        oracle.OracleDialect.__init__(self, **kwargs)
        self.use_ansi = use_ansi
        self.optimize_limits = optimize_limits
        self.use_binds_for_limits = use_binds_for_limits

        # print "XXX",self.supports_unicode_statements

        self.supports_unicode_statements = False

        # print self.driver.supports_unicode_statements

    @reflection.cache
    def get_table_names(
        self,
        connection,
        schema=None,
        **kw
        ):
        schema = self.denormalize_name(schema
                or self.default_schema_name)

        # note that table_names() isnt loading DBLINKed or synonym'ed tables

        if schema is None:
            schema = self.default_schema_name
        s = \
            sql.text("SELECT table_name FROM exa_all_tables WHERE table_schema = 'BUCHDACH'"
                     )

        # cursor = connection.execute(s, owner=schema)

        cursor = connection.execute(s)
        return [self.normalize_name(row[0]) for row in cursor]

    def has_table(
        self,
        connection,
        table_name,
        schema=None,
        ):
        if not schema:
            schema = self.default_schema_name
        cursor = \
            connection.execute(sql.text('SELECT table_name FROM exa_all_tables WHERE table_name = :name AND table_owner = :schema_name'
                               ),
                               name=self.denormalize_name(table_name),
                               schema_name=self.denormalize_name(schema))
        return cursor.first() is not None

    def has_sequence(
        self,
        connection,
        sequence_name,
        schema=None,
        ):
        raise NotImplementedError('Exasol has no table all_sequences')

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        s = 'SELECT user_name FROM all_users ORDER BY user_name'
        cursor = connection.execute(s)
        return [self.normalize_name(row[0]) for row in cursor]

    @reflection.cache
    def get_columns(
        self,
        connection,
        table_name,
        schema=None,
        **kw
        ):
        """

        kw arguments can be:

            oracle_resolve_synonyms

            dblink

        """

        resolve_synonyms = kw.get('oracle_resolve_synonyms', False)
        dblink = kw.get('dblink', '')
        info_cache = kw.get('info_cache')

        (table_name, schema, dblink, synonym) = \
            self._prepare_reflection_args(
            connection,
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
            )
        columns = []

        # if self._supports_char_length:
        #    char_length_col = 'char_length'
        # else:
        #    char_length_col = 'data_length'

        # SELECT column_name, column_type, column_maxsize, column_num_prec, column_num_scale, column_is_nullable, column_default FROM EXA_ALL_COLUMNS ;
        #                "SELECT column_name, column_type, column_maxsize, NVL(column_num_prec, 0), NVL(column_num_scale, 0), "

        c = \
            connection.execute(sql.text('SELECT column_name, column_type, column_maxsize, column_maxsize, NVL(column_num_scale, 0), column_is_nullable, column_default FROM EXA_ALL_COLUMNS WHERE column_table = :table_name ORDER BY column_object_id'
                               ), table_name=table_name)

        # c = connection.execute(sql.text(
        #        "SELECT column_name, data_type, %(char_length_col)s, data_precision, data_scale, "
        #        "nullable, data_default FROM ALL_TAB_COLUMNS%(dblink)s "
        #        "WHERE table_name = :table_name AND owner = :owner "
        #        "ORDER BY column_id" % {'dblink': dblink, 'char_length_col':char_length_col}),
        #                      table_name=table_name, owner=schema)

        for row in c:
            (
                colname,
                orig_colname,
                coltype,
                length,
                precision,
                scale,
                nullable,
                default,
                ) = (
                self.normalize_name(row[0]),
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5] == 'Y',
                row[6],
                )

            if coltype.startswith('DECIMAL'):
                coltype = DECIMAL(precision, scale)
            elif coltype.startswith('VARCHAR'):
                coltype = VARCHAR(precision, scale)
            elif coltype == 'DOUBLE':
                coltype = DOUBLE_PRECISION(precision, scale)
            elif coltype == 'NUMBER':
                coltype = NUMBER(precision, scale)
            elif coltype in ('VARCHAR2', 'NVARCHAR2', 'CHAR'):
                coltype = self.ischema_names.get(coltype)(length)
            elif 'WITH TIME ZONE' in coltype:
                coltype = TIMESTAMP(timezone=True)
            else:
                coltype = re.sub(r'\(\d+\)', '', coltype)
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'"
                               % (coltype, colname))
                    coltype = sqltypes.NULLTYPE

            cdict = {
                'name': colname,
                'type': coltype,
                'nullable': nullable,
                'default': default,
                'autoincrement': default is None,
                }
            if orig_colname.lower() == orig_colname:
                cdict['quote'] = True

            columns.append(cdict)
        return columns

    @reflection.cache
    def _get_constraint_data(
        self,
        connection,
        table_name,
        schema=None,
        dblink='',
        **kw
        ):

        # TODO implement me

        return []

    @reflection.cache
    def get_indexes(
        self,
        connection,
        table_name,
        schema=None,
        resolve_synonyms=False,
        dblink='',
        **kw
        ):

        # TODO implement me

        return []

    @reflection.cache
    def get_view_names(
        self,
        connection,
        schema=None,
        **kw
        ):
        schema = self.denormalize_name(schema
                or self.default_schema_name)

        # s = sql.text("SELECT view_name FROM exa_all_views WHERE owner = :owner")

        s = sql.text('SELECT view_name FROM exa_all_views')
        cursor = connection.execute(s,
                                    owner=self.denormalize_name(schema))
        return [self.normalize_name(row[0]) for row in cursor]

    def _get_default_schema_name(self, connection):

        # print 'joo', self.supports_unicode_statements

        return self.normalize_name(connection.execute('SELECT USER FROM DUAL'
                                   ).scalar())


