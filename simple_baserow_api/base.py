"""
simple_baserow_api base module.

This is the principal module of the simple_baserow_api project.
here you put your main classes and objects.

Be creative! do whatever you want!

If you want to replace this with a Flask application run:

    $ make init

and then choose `flask` as template.
"""

import warnings

# example constant variable
NAME = "simple_baserow_api"


from copy import deepcopy
from typing import Any, Dict

import requests


def load_token(token_path) -> str:
    """
    Load a token from a file.
    """
    with open(token_path) as tokenfile:
        token = tokenfile.readline().strip()
    return token


def format_value(raw_value, field_info):
    """
    Extract the value/id from a single_select, multiple_select or link_row field.

    Example:
    raw_value = {"value": "active"}
    field_info = {"type": "single_select"}
    formatted_value = format_value(raw_value, field_info)
    # formatted_value would be "active"
    """
    if field_info["type"] == "single_select":
        if isinstance(raw_value, dict):
            return raw_value["value"]
        elif raw_value is None:
            return raw_value
        raise RuntimeError(f"malformed single_select {raw_value}")
    elif field_info["type"] == "multiple_select":
        if isinstance(raw_value, list):
            return [v["value"] for v in raw_value]
        raise RuntimeError(f"malformed multiple_select {raw_value}")
    elif field_info["type"] == "link_row":
        if isinstance(raw_value, list):
            return [v["id"] for v in raw_value]
        raise RuntimeError(f"malformed link_row {raw_value}")
    else:
        return raw_value


class BaserowApi:
    """
    Baserow API class.
    """

    table_path = "api/database/rows/table"
    fields_path = "api/database/fields/table"

    def __init__(self, database_url: str, token=None, token_path=None):
        self._database_url = database_url
        if token_path:
            self._token = load_token(token_path)
        elif token:
            self._token = token
        self._fields: Dict[int, Any] = {}

    def _get_fields(self, table_id):
        """
        Get fields for a table.
        return: json-encoded list of fields
        """
        get_fields_url = f"{self._database_url}/{self.fields_path}/{table_id}/"
        resp = requests.get(
            get_fields_url,
            headers={"Authorization": f"Token {self._token}"},
        )

        resp.raise_for_status()
        data = resp.json()
        return data

    # TODO: Test if https://phenotips.charite.de/api/database/rows/table/581/?page=2" as next includes field names
    def _get_rows_data(
        self,
        url=None,
        table_id=None,
        entry_id=None,
        user_field_names=False,
        paginated=False,
    ):
        """
        Get rows for a table.
        - url: URL to get the data from.
        - table_id: ID of the table to get the data from.
        - entry_id: ID of the (row) entry to get.
        - user_field_names: Use user-friendly field names.
        - paginated: Get all pages of data.
        """
        if (not table_id and not url) or (table_id and url):
            raise RuntimeError("Either table_id or url must be provided, but not both.")
        if entry_id and not table_id:
            raise RuntimeError("entry_id can only be provided with table_id.")
        if entry_id and paginated:
            warnings.warn("entry_id is not paginated.")
            paginated = False

        if url:
            get_rows_url = url
        elif table_id:
            get_rows_url = f"{self._database_url}/{self.table_path}/{table_id}/"
            if entry_id:
                get_rows_url += f"{entry_id}/"
            if user_field_names:
                get_rows_url += "?user_field_names=true"
        else:
            raise RuntimeError("Either table_id or url must be provided.")

        resp = requests.get(
            get_rows_url,
            headers={"Authorization": f"Token {self._token}"},
        )

        resp.raise_for_status()
        data = resp.json()

        if not entry_id and paginated:
            if "results" not in data:
                raise RuntimeError(f"Could not get data from {get_rows_url}")

            if data["next"]:
                return data["results"] + self._get_rows_data(
                    url=data["next"], paginated=paginated
                )
            return data["results"]
        else:
            return data

    def _create_row(self, table_id, data, user_field_names=False):
        create_row_url = f"{self._database_url}/{self.table_path}/{table_id}/"
        if user_field_names:
            create_row_url += "?user_field_names=true"
        resp = requests.post(
            create_row_url,
            headers={
                "Authorization": f"Token {self._token}",
                "Content-Type": "application/json",
            },
            json=data,
        )
        resp.raise_for_status()
        resp_data = resp.json()
        if "id" in resp_data:
            return resp_data["id"]
        else:
            raise RuntimeError(f"Malformed response {resp_data}")

    def _create_rows(self, table_id, datas, user_field_names=False):
        create_rows_url = f"{self._database_url}/{self.table_path}/{table_id}/batch/"
        if user_field_names:
            create_rows_url += "?user_field_names=true"
        resp = requests.post(
            create_rows_url,
            headers={
                "Authorization": f"Token {self._token}",
                "Content-Type": "application/json",
            },
            json={"items": datas},
        )
        resp.raise_for_status()
        data = resp.json()
        ids = [e["id"] for e in data["items"]]
        return ids

    def _update_row(self, table_id, row_id, data, user_field_names=False):
        update_row_url = f"{self._database_url}/{self.table_path}/{table_id}/{row_id}/"
        if user_field_names:
            update_row_url += "?user_field_names=true"
        resp = requests.patch(
            update_row_url,
            headers={
                "Authorization": f"Token {self._token}",
                "Content-Type": "application/json",
            },
            json=data,
        )
        resp.raise_for_status()
        resp_data = resp.json()
        if "id" in resp_data:
            return resp_data["id"]
        else:
            raise RuntimeError(f"Malformed response {resp_data}")

    def _update_rows(self, table_id, datas, user_field_names=False):
        update_rows_url = f"{self._database_url}/{self.table_path}/{table_id}/batch/"
        if user_field_names:
            update_rows_url += "?user_field_names=true"
        resp = requests.patch(
            update_rows_url,
            headers={
                "Authorization": f"Token {self._token}",
                "Content-Type": "application/json",
            },
            json={"items": datas},
        )
        resp.raise_for_status()
        data = resp.json()
        ids = [e["id"] for e in data["items"]]
        return ids

    def _delete_row(self, table_id, row_id):
        delete_row_url = f"{self._database_url}/{self.table_path}/{table_id}/{row_id}/"
        resp = requests.delete(
            delete_row_url,
            headers={"Authorization": f"Token {self._token}"},
        )
        resp.raise_for_status()

    def _convert_selects(self, data, fields):
        """
        Convert the values in a dataset to their corresponding IDs
        based on field definitions.

        Example:
        data = {"status": "active", "tags": ["urgent", "important"]}
        fields = [
            {"name": "status", "type": "single_select", "select_options":
                [{"value": "active", "id": 1}, {"value": "inactive", "id": 2}], "read_only": False},
            {"name": "tags", "type": "multiple_select", "select_options":
                [{"value": "urgent", "id": 1}, {"value": "important", "id": 2}], "read_only": False}
        ]
        converted_data = self._convert_selects(data, fields)
        # converted_data would be {"status": 1, "tags": [1, 2]}
        """
        data_conv = deepcopy(data)

        def convert_option(v, opts):
            """
            Return the id of the option with value v.
            """
            if isinstance(v, int):
                return v

            for opt in opts:
                if opt["value"] == v:
                    return opt["id"]
            raise RuntimeError(f"Could not convert {v} to any of {opts}")

        for field in fields:
            if not field["read_only"] and field["name"] in data_conv:
                cur_value = data_conv[field["name"]]

                if cur_value is None or cur_value == []:
                    continue

                if field["type"] == "single_select":
                    data_conv[field["name"]] = convert_option(
                        cur_value, field["select_options"]
                    )

                elif field["type"] == "multiple_select":
                    new_value = []
                    for single_value in cur_value:
                        conv_value = convert_option(
                            single_value, field["select_options"]
                        )
                        new_value.append(conv_value)
                    data_conv[field["name"]] = new_value
        return data_conv

    def get_fields(self, table_id):
        """
        Get fields for a table.
        Fields are cached in the _fields attribute.
        TODO: Implement cache invalidation.
        """
        if table_id not in self._fields:
            self._fields[table_id] = self._get_fields(table_id)
        return self._fields[table_id]

    def get_writable_fields(self, table_id):
        """
        Get fields which can be written to.
        """
        fields = self.get_fields(table_id)
        writable_fields = [field for field in fields if not field["read_only"]]
        return writable_fields

    def get_data(self, table_id, writable_only=True, user_field_names=True):
        """Get all data in a table.

        writable_only - Only return fields which can be written to. This
        excludes all formula and computed fields.
        """
        if writable_only:
            fields = self.get_writable_fields(table_id)
        else:
            fields = self.get_fields(table_id)

        if user_field_names:
            names = {f["name"]: f for f in fields}
        else:
            names = {f'field_{f["id"]}': f for f in fields}

        data = self._get_rows_data(
            table_id=table_id, user_field_names=user_field_names, paginated=True
        )

        # Collect rows with their field names and values
        writable_data = {
            d["id"]: {k: format_value(v, names[k]) for k, v in d.items() if k in names}
            for d in data
        }

        return writable_data

    def get_entry(
        self, table_id, entry_id, linked=False, seen_tables=None, user_field_names=True
    ):
        """
        Get a single entry from a table.
        Attention: ID is not included.
        """
        data = self._get_rows_data(
            table_id=table_id,
            entry_id=entry_id,
            paginated=False,
            user_field_names=user_field_names,
        )
        fields = self.get_fields(table_id)
        names = {f["name"]: f for f in fields}
        names = names | {f'field_{f["id"]}': f for f in fields}
        formatted_data = {
            k: format_value(v, names[k]) for k, v in data.items() if k in names
        }

        seen_tables_next = seen_tables or []
        seen_tables_next.append(table_id)

        # fully hydrate with linked data
        # --> recursively get data from linked tables
        if linked:
            link_fields = [f for f in fields if f["type"] == "link_row"]
            for field in link_fields:
                linked_table_id = field["link_row_table_id"]
                if not seen_tables or linked_table_id not in seen_tables:
                    if ids := data.get(field["name"]):
                        formatted_data[field["name"]] = [
                            self.get_entry(
                                linked_table_id,
                                e_id["id"],
                                linked=False,
                                seen_tables=seen_tables_next,
                                user_field_names=user_field_names,
                            )
                            for e_id in ids
                        ]

        return formatted_data

    def add_data(self, table_id, data, row_id=None, user_field_names=True) -> int:
        """
        Add/Change data (single row) to a table.
        """
        fields = self.get_fields(table_id)
        data_conv = self._convert_selects(data, fields)
        if row_id:
            self._update_row(
                table_id, row_id, data_conv, user_field_names=user_field_names
            )
        else:
            row_id = self._create_row(
                table_id, data_conv, user_field_names=user_field_names
            )

        return row_id

    def add_data_batch(
        self, table_id, entries, user_field_names=True
    ) -> tuple[list, list]:
        """
        Add multiple entries.
        """
        entries_update = []
        entries_new = []
        for entry in entries:
            if entry.get("id") is not None:
                entries_update.append(entry)
            else:
                entries_new.append(entry)

        errors = []
        touched_ids = []
        if entries_new:
            try:
                touched_ids += self._create_rows(
                    table_id, entries_new, user_field_names=user_field_names
                )
            except requests.HTTPError as err:
                errors.append(f"Create rows ({len(entries_new)}): " + err.response.text)
        if entries_update:
            try:
                touched_ids += self._update_rows(
                    table_id, entries_update, user_field_names=user_field_names
                )
            except requests.HTTPError as err:
                errors.append(
                    f"Update rows ({len(entries_update)}): " + err.response.text
                )

        return touched_ids, errors
