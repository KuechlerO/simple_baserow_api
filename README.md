# simple_baserow_api

A lightweight Python wrapper for the [Baserow REST API](https://baserow.io/docs/apis/rest-api), developed by [Oliver Kuechler](https://github.com/KuechlerO), forked from [xiamaz' python-baserow-simple](https://github.com/xiamaz/python-baserow-simple).

📚 Full documentation available at: [simple_baserow_api Documentation](https://kuechlero.github.io/simple_baserow_api/)

## 🚀 Installation

```bash
pip install simple_baserow_api
```

## 🔧 Features

- ✅ Intuitive access to Baserow tables, fields, and rows
- 🧹 Supports reading and writing table data
- 📄 Handles field metadata, writable fields, and batch operations
- 🔁 Built-in logic for updating existing rows and handling read-only fields
- 🔍 Optional linked data resolution and selective field inclusion/exclusion


## 🛠️ Basic Usage

### 1. Initialize the API
```python
from simple_baserow_api import BaserowApi

api = BaserowApi(
    database_url="https://your-baserow-instance.com",
    token="your-api-token"
)
```

### 2. Retrieve Table Metadata

#### Get All Field Definitions
```python
fields = api.get_fields(table_id=1)
```

#### Get Writable Fields (excluding read-only)
Some fields are read-only and cannot be written to (e.g. primary key fields and formula fields).
Thus, it is important to know which fields are writable.
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
```

#### Get a Single Row by ID
```python
row = api.get_entry(table_id=1, row_id=1)
```

#### Output
```py
[
    {
        "id": 1,
        "field_name1": "value",
        "field_name2": "value",
        ...
    },
    ...
]
```

### 4. Write Data

#### Add a New Row
```python
row_id = api.add_data(table_id=1, data={"field_name": "value"})
```

#### Update an Existing Row
```python
row_id = api.add_data(table_id=1, row_id=1, data={"field_name": "new_value"})
```

#### Add or Update Multiple Rows
```python
# No ID: new row is created; With ID: existing row is changed
entries = [
    {"field_name": "value1"},
    {"id": 2, "field_name": "updated_value2"}
]

row_ids, errors = api.add_data_batch(table_id=1, entries=entries, fail_on_error=True)
```

### 5. Advanced Options

#### Include or Exclude Specific Fields
```python
# Include only selected fields
filtered_data = api.get_data(table_id=1, include=["Name", "Status"])

# Exclude specific fields
filtered_data = api.get_data(table_id=1, exclude=["InternalNotes"])
```


#### Resolve Linked Fields Automatically
```python
row = api.get_entry(table_id=1, row_id=1, linked=True)
```


## 💻 Development

Want to contribute? See our [CONTRIBUTING.md](CONTRIBUTING.md) guide.

---

🙌 Thank you for using **simple_baserow_api**!  
Please report bugs or request features by opening an issue on the [GitHub repository](https://github.com/KuechlerO/simple_baserow_api/issues).

