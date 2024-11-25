# simple_baserow_api

[![codecov](https://codecov.io/gh/KuechlerO/simple_baserow_api/branch/main/graph/badge.svg?token=simple_baserow_api_token_here)](https://codecov.io/gh/KuechlerO/simple_baserow_api)
[![CI](https://github.com/KuechlerO/simple_baserow_api/actions/workflows/main.yml/badge.svg)](https://github.com/KuechlerO/simple_baserow_api/actions/workflows/main.yml)

Awesome simple_baserow_api created by KuechlerO

## Install it from PyPI

```bash
pip install simple_baserow_api
```

## Usage

```py
from simple_baserow_api import BaserowApi

# Initialize the API
api = BaserowApi(database_url="https://your-baserow-instance.com", token="your-token")

# Get fields for a table
fields = api.get_fields(table_id=1)
print(fields)

# Add a new row to a table
new_row_id = api.add_data(table_id=1, data={"field_name": "value"})
print(f"New row ID: {new_row_id}")

# Retrieve data from a table
data = api.get_data(table_id=1)
print(data)
```

## Development

Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.

---
Thank you for using simple_baserow_api! 
If you encounter any issues or have any questions, please open an issue on our GitHub repository.
