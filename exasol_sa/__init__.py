#!/usr/bin/python
# -*- coding: utf-8 -*-
from exasol_sa import base, pyodbc

base.dialect = pyodbc.dialect
