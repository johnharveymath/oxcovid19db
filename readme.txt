===========
OxCOVID19
===========

The **OxCOVID19 Database** is available at `<https://covid19.eng.ox.ac.uk/>`
This package, ``oxcovid19db`` is designed to interact with and use the database.

It provides a class ``Connect``. A ``Connect`` object provides a connection
to the database with methods for querying the database.

It also provides a method ``merge`` which merges two dataframes extracted
from the database tables using their geographical information. Where the
geographical information does not permit a standard merge, some geographical
areas may be combined.

Depending on the table, different rules are used (sum, mean, etc).
This functionality is not complete and the user should proceed with caution.

Contributors
=========

While this package was written by John Harvey, it owes much to the code used to
build the OxCOVID19 Database, and thanks are due to all who contributed.
