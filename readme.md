# Package to interact with Oxford COVID-19 (OxCOVID19) Database

The **OxCOVID19 Database** is available at [https://covid19.eng.ox.ac.uk/](https://covid19.eng.ox.ac.uk/). 
This repository houses an **extremely** preliminary version of a package to interact with and use the database.

---

See example.py for a fairly complete set of use cases. The connect object provides a connection to the database.
It is based heavily on code used in the OxCOVID19DB itself.
The merge method permits a merge of two dataframes extracted from the database tables.
Where the geographical information does not permit a standard merge, some geographical areas may be combined.
Depending on the table, different rules are used (sum, mean, etc).
This functionality is not complete and the user should proceed with caution.