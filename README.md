sqlalchemy-exasol
=================

Exasol Driver for SQLalchemy

Please consider this driver a proof of concept and an alpha release.

Installation
============

    virtualenv test
    source test/bin/activate

    export LIBRARY_PATH="/usr/local/unixODBC/lib:/usr/local/unixODBC/lib64/"
    export CPATH="/usr/local/unixODBC/include/"


    pip install pyodbc
    pip install sqlalchemy

    git clone https://github.com/hoffmann/sqlalchemy-exasol.git
    cd sqlalchemy-exasol
    python setup.py install


Usage
=====

    from sqlalchemy import create_engine

    engine =create_engine('exasol:///?odbc_connect=DSN', echo=True)
    result = engine.execute('SELECT * FROM foo')

    print result.fetchall()

