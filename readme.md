OxCOVID19
===========

The **OxCOVID19 Database** is available at <https://covid19.oii.ox.ac.uk/>. 
This package, `oxcovid19db` is designed to interact with and use the database 
and analyse the results with `pandas`.

It provides a class `Connect`. A `Connect` object provides a connection
to the database and a method 
`Connect.execute(query, data)` which can be used to query the database in the 
same way as `Cursor.execute(query, data)` from the package `psycopg2`.  

The package also provides a method `merge` which merges two dataframes extracted
from the database tables using their geographical information. Where the
geographical information does not permit a standard merge, some geographical
areas may be combined.

Depending on the table, different rules are used (sum, mean, etc).
This functionality is not complete and the user should proceed with caution.

Use
------------

Install the package with `pip install oxcovid19db`.

The github repository https://github.com/johnharveymath/oxcovid19db includes
an example script at `bin/example.py` which demonstrates the use of the package.

In brief, a connection can be opened with 

`db_conn = oxcovid19db.Connect()`

Then `db_conn.execute(query, data)` will query the database 
using the `Cursor.execute(query, data)` method from `psycopg2`
and return a DataFrame.

Two such DataFrames, `df1` and `df2` can be merged using

`oxcovid19db.merge(df1, df2)`

which will return a DataFrame merged along geographical and date columns.

Contributors
------------

While this package was written by John Harvey, it owes much to the code used to
build the OxCOVID19 Database, and thanks are due to all who contributed to that project.
