# simple_baserow_api

A lightweight Python wrapper for the [Baserow REST API](https://baserow.io/docs/apis/rest-api), developed by [Oliver Kuechler](https://github.com/KuechlerO), forked from [xiamaz' python-baserow-simple](https://github.com/xiamaz/python-baserow-simple).

📚 Full documentation available at: [simple_baserow_api Documentation](https://kuechlero.github.io/simple_baserow_api/)

## 🚀 Installation

```bash
pip install simple_baserow_api
```

## 🔧 Features

- Intuitive access to Baserow tables, fields, and rows
- Reading and writing table data (single and batch)
- Field metadata, writable-field detection, and read-only handling
- Optional linked-row resolution and field include/exclude
- Lookup rows by column value (`find_entries`)
- Synchronize / upsert a row from one table into another (`synchronize_data`)
- Optional field-compatibility checks when writing
- JWT login helpers and schema operations (create/delete database & tables)

## 🛠️ Basic Usage

### 1. Initialize the API

#### Database token (row data)

```python
from simple_baserow_api import BaserowApi

api = BaserowApi(
    database_url="https://api.baserow.io",  # or your self-hosted URL
    token="your-database-token",
)
```

#### JWT from email / password (needed for schema changes)

```python
api = BaserowApi.from_credentials(
    database_url="https://api.baserow.io",
    email="you@example.com",
    password="secret",
)
```

### 2. Retrieve Table Metadata

#### Get All Field Definitions
```python
fields = api.get_fields(table_id=1)
```

#### Get Writable Fields (excluding read-only)
Some fields are read-only and cannot be written to (e.g. formula fields).
This is useful when you want to add a new row to a table.

```python
writable_fields = api.get_writable_fields(table_id=1)
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


### 3. Read Table Data

#### Get All Rows
```python
rows = api.get_data(table_id=1, writable_only=True)
# -> {row_id: {"Name": "...", ...}, ...}
```

#### Get a Single Row by ID
```python
row = api.get_entry(table_id=1, row_id=1)
```

#### Find Rows by Column Value
```python
matches = api.find_entries(table_id=1, column="Individuum ID", value="ABC-123")
# -> {row_id: {...}, ...}
```

### 4. Write Data

#### Add a New Row
```python
row_id = api.add_data(table_id=1, data={"Name": "value"})
```

#### Update an Existing Row
```python
row_id = api.add_data(table_id=1, row_id=1, data={"Name": "new_value"})
```

#### Field Compatibility Check
Validate that payload keys exist on the table before writing. On mismatch:
warn (default) or raise when `fail_on_error=True`.

```python
row_id = api.add_data(
    table_id=1,
    data={"Name": "value", "UnknownField": "x"},
    check_field_compatibility=True,
    fail_on_error=False,  # warn and continue
)
```

The same options exist on `add_data_batch`.

#### Add or Update Multiple Rows
```python
# No ID: new row is created; with ID: existing row is updated
entries = [
    {"Name": "value1"},
    {"id": 2, "Name": "updated_value2"},
]

row_ids, errors = api.add_data_batch(table_id=1, entries=entries, fail_on_error=True)
```

### 5. Synchronize a Row Between Tables

Copy one source row into a target table. Matching uses an identifier column
(update if found, otherwise create). Compatible fields are transferred;
untransferable values produce warnings (or errors with `fail_on_error=True`).
Link-row values are rematched in the target linked table.

```python
target_row_id, messages = api.synchronize_data(
    source_table_id=10,
    source_row_id=5,
    target_table_id=20,
    identifier_column="Individuum ID",
    field_mapping={"Kommentar": "Kommentar Einschluss"},  # optional renames
    exclude_fields=["existierender Index"],  # optional skips
    fail_on_error=False,
)

# Preview without writing
target_row_id, messages = api.synchronize_data(
    source_table_id=10,
    source_row_id=5,
    target_table_id=20,
    identifier_column="Individuum ID",
    dry_run=True,
)
# messages include "dry_run: would create/update ..." and payload field names
```

### 6. Advanced Options

#### Include or Exclude Specific Fields
```python
filtered_data = api.get_data(table_id=1, include=["Name", "Status"])
filtered_data = api.get_data(table_id=1, exclude=["InternalNotes"])
```

#### Resolve Linked Fields Automatically
```python
row = api.get_entry(table_id=1, row_id=1, linked=True)
```

### 7. Schema Helpers (JWT)

Creating or deleting databases/tables requires a JWT-authenticated client
(`BaserowApi.from_credentials` or `jwt_token=True`).

```python
workspaces = api.list_workspaces()
database = api.create_database(workspace_id=workspaces[0]["id"], name="My DB")
table = api.create_table(
    database_id=database["id"],
    name="Samples",
    primary_field_name="Sample-ID",
    fields=[
        {"name": "Status", "type": "single_select",
         "select_options": [{"value": "open", "color": "blue"}]},
    ],
)
api.delete_table(table["id"])
api.delete_database(database["id"])
```


## 💻 Development

Want to contribute? See our [CONTRIBUTING.md](CONTRIBUTING.md) guide.

Integration tests create ephemeral tables against Baserow.io; see
`tests/conftest.py` for credentials / env overrides (`BASEROW_URL`,
`BASEROW_EMAIL`, `BASEROW_PASSWORD`).

---

Thank you for using **simple_baserow_api**.  
Please report bugs or request features by opening an issue on the [GitHub repository](https://github.com/KuechlerO/simple_baserow_api/issues).
