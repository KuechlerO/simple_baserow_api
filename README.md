# simple_baserow_api

A [Baserow API](https://baserow.io/docs/apis/rest-api) wrapper for Python:
*simple_baserow_api* created by KuechlerO

For a detailed documentation, please visit the [simple_baserow_api documentation](https://kuechlero.github.io/simple_baserow_api/).

## Install it from PyPI

```bash
pip install simple_baserow_api
```

## Usage

### Initialize the API
```py
from simple_baserow_api import BaserowApi

# Initialize the API
api = BaserowApi(database_url="https://your-baserow-instance.com", token="your-token")
```

### Retrieve data from a table

#### Get all field / column specifications for a table
```py
# Get fields (i.e. columns) for a table
fields = api.get_fields(table_id=1)
print(fields)
```

#### Output
```py
[
    {
        "id": 1,
        "table_id": 1,
        "name": "Name",
        "order": 0,
        "type": "text",
        "primary": True,
        "read_only": False,
        "description": None,
        "text_default": ""
    },
    ...
]
```

#### Get all writable fields for a table
Some fields are read-only and cannot be written to (e.g. primary key fields and formula fields).
Thus, it is important to know which fields are writable.
This is useful when you want to add a new row to a table.

```py
# Get writable fields (i.e. columns) for a table
fields = api.get_writable_fields(table_id=1)
print(fields)
```

#### Output
```py
[
    {
        "id": 1,
        "table_id": 1,
        "name": "Name",
        "order": 0,
        "type": "text",
        "primary": True,
        "read_only": False,
        "description": None,
        "text_default": ""
    },
    ...
]
```

#### Get all data from a table
Get all data from a table. 
This is useful when you want to retrieve all data from a table.
```py
# Get data from a table
data = api.get_data(table_id=1, writable_only=True)
print(data)
```

#### Output
```py
[
    {
        "id": 1,
        "field_name": "value",
        ...
    },
    ...
]
```

#### Get a single row from a table
Get a single row from a table.
This is useful when you want to retrieve a specific row from a table.
```py
# Get a single row from a table
row = api.get_entry(table_id=1, row_id=1)
print(row)
```

#### Output
```py
{
    "id": 1,
    "field_name": "value",
    ...
}
```


#### Add / change data in a table
Add a new row to a table.

```py
# Add a new row to a table
row_id = api.add_data(table_id=1, data={"field_name": "value"})
# Change a row in a table
row_id = api.add_data(table_id=1, row_id=1, data={"field_name": "value"})
```

#### Add / change multiple rows in a table
Add multiple new rows to a table.

```py
# Add multiple new rows to a table
row_ids, errors = api.add_data_batch(table_id=1, entries=[{"field_name": "value"}, {"field_name": "value"}], fail_on_error=True)
# Change multiple rows in a table (by providing row_id)
row_ids, errors = api.add_data_batch(table_id=1, entries=[{"id": 1, "field_name": "value"}, {"id": 2, "field_name": "value"}], fail_on_error=True)
```


## Development
Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.

---
Thank you for using simple_baserow_api! 
If you encounter any issues or have any questions, please open an issue on our GitHub repository.
