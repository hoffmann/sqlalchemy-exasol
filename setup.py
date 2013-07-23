#!/usr/bin/python
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name='Exasol-SQLalchemy',
    version='0.1',
    description='Exasol Driver for SQLalchemy',
    author='Peter Hoffmann',
    author_email='ph@peter-hoffmann.com',
    url='http://github.com/hoffmann/sqlalchemy-exasol',
    packages=['exasol_sa'],
    entry_points="""
        [sqlalchemy.dialects]
        exasol = exasol_sa:base.dialect
      """,
    )
