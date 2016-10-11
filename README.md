sqlalchemy_fdw is a postgresql dialect for sqlalchemy adding support for foreign
data wrappers.

Installation
------------
```bash

pip install sqlalchemy-fdw
```

Usage
-----

```python
from sqlalchemy import create_engine, Table, Column, MetaData
from sqlalchemy import Integer, Unicode
from sqlalchemy_fdw import ForeignTable, ForeignDataWrapper


engine = create_engine('pgfdw://user:password@host:port/dbname')
metadata = MetaData()
metadata.bind = engine

fdw = ForeignDataWrapper("myfdwserver", "myfdwextension", metadata=metadata,
                            options={'option1': 'test'})
fdw.create()

table = ForeignTable("myforeigntable", metadata,
            Column('col1', Integer),
            Column('col2', Unicode),
            pgfdw_server='myfdwserver',
            pgfdw_options={
                'tableoption': 'optionvalue'
            }
        )
table.create(checkfirst=True)
table.drop()
fdw.drop(cascade=True)
```
