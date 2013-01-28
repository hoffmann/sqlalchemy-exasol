#!/usr/bin/python
# -*- coding: utf-8 -*-
from exasol_sa.base import ExasolDialect
from sqlalchemy.connectors.pyodbc import PyODBCConnector


class ExasolDialect_pyodbc(PyODBCConnector, ExasolDialect):

    supports_unicode_statements = False

    def __init__(self, supports_unicode_binds=None, **kw):
        super(ExasolDialect_pyodbc, self).__init__(**kw)
        self.supports_unicode_binds = False

    def initialize(self, connection):
        super(PyODBCConnector, self).initialize(connection)
        self.supports_unicode_binds = False


dialect = ExasolDialect_pyodbc
