from copy import deepcopy
from typing import Any, Optional

import requests
import warnings
import time

"""
simple_baserow_api base module.

This is the principal module of the simple_baserow_api project.
"""

# example constant variable
NAME = "simple_baserow_api"


def _format_value(raw_value: dict, field_info: dict) -> Any:
    """
    Extract the value/id from a single_select, multiple_select or
    link_row field.

    Example:
    raw_value = {"value": "active"}
    field_info = {"type": "single_select"}
    formatted_value = _format_value(raw_value, field_info)
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
    """BaserowAPI class: A wrapper around the Baserow API."""

    table_path = "api/database/rows/table"
    fields_path = "api/database/fields/table"

    def __init__(
        self,
        database_url: str,
        token: Optional[str] = None,
        token_path: Optional[str] = None,
        jwt_token: bool = False,
    ):
        """Initialize the BaserowApi class.
        This class is a wrapper around the Baserow API.

        Args:
            database_url (str): URL of the Baserow database.
            token (Optional[str], optional): Token-String for Baserow access.
                Defaults to None.
            token_path (Optional[str], optional): Path to file containing the
                Token-String. Defaults to None.
            jwt_token (bool, optional): Whether JWT-Token is used instead of
                Token-String. Defaults to False.
        """
        self._database_url = database_url
        if token_path:
            with open(token_path) as tokenfile:
                self._token = tokenfile.readline().strip()
        elif token:
            self._token = token

        self._token_mode = "JWT" if jwt_token else "Token"

    def get_fields(self, table_id: int) -> list[dict]:
        """Get all fields / column specifications for a table.

        Args:
            table_id (int): ID of the table of interest.

        Returns:
            list[dict]: List of column specifications (dict of fields)
        """
        get_fields_url = f"{self._database_url}/{self.fields_path}/{table_id}/"
        resp = requests.get(
            get_fields_url,
            headers={"Authorization": f"{self._token_mode} {self._token}"},
        )

        resp.raise_for_status()
        data = resp.json()
        return data

    def _get_rows_data(
        self,
        url: Optional[str] = None,
        table_id: Optional[int] = None,
        row_id: Optional[int] = None,
        user_field_names: bool = False,
        paginated: bool = False,
    ) -> dict:
        """Get rows data from a table.

        Args:
            url (str, optional): URL to lookup data. Defaults to None.
            table_id (int, optional): ID of table of interest. Defaults to
                None.
            row_id (int, optional): ID of entry of interest. Defaults to None.
                If provided, only the entry is returned. Else, all entries are
                returned.
            user_field_names (bool, optional): Whether to use field names or
                field IDs. Defaults to False.
            paginated (bool, optional): Whether to load multiple pages of data.
                Defaults to False.

        Returns:
            str: JSON encoded data.
        """
        if (not table_id and not url) or (table_id and url):
            raise RuntimeError(
                "Either table_id or url must be provided, " "but not both."
            )
        if row_id and not table_id:
            raise RuntimeError("row_id can only be provided with table_id.")
        if row_id and paginated:
            warnings.warn("row_id is not paginated.")
            paginated = False

        if url:
            get_rows_url = url
        elif table_id:
            get_rows_url = f"{self._database_url}/{self.table_path}/{table_id}/"
            if row_id:
                get_rows_url += f"{row_id}/"
            if user_field_names:
                get_rows_url += "?user_field_names=true"
        else:
            raise RuntimeError("Either table_id or url must be provided.")

        resp = requests.get(
            get_rows_url,
            headers={"Authorization": f"{self._token_mode} {self._token}"},
        )

        resp.raise_for_status()
        data = resp.json()

        # A: specific entry
        if row_id:  # Specific entry
            return data
        # B: all entries
        else:
            if "results" not in data:
                raise RuntimeError(
                    f"Could not get query result data from {get_rows_url}"
                )

            results = data["results"]  # All results (first page)
            if paginated:  # Get all remaining pages
                if data["next"]:
                    return results + self._get_rows_data(
                        url=data["next"], paginated=paginated
                    )

            # If no pagination, return all results
            return results

    def _create_row(
        self, table_id: int, data: dict, user_field_names: bool = False
    ) -> int:
        """Create a row in a table.

        Args:
            table_id (int): ID of the table of interest.
            data (dict): Data to add to the table.
            user_field_names (bool, optional): Whether to use field names of
            field IDs. Defaults to False.

        Returns:
            int: Row ID.
        """
        create_row_url = f"{self._database_url}/{self.table_path}/{table_id}/"
        if user_field_names:
            create_row_url += "?user_field_names=true"
        resp = requests.post(
            create_row_url,
            headers={
                "Authorization": f"{self._token_mode} {self._token}",
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

    def _create_rows(
        self, table_id: int, datas: list[dict], user_field_names: bool = False
    ):
        create_rows_url = f"{self._database_url}/{self.table_path}/{table_id}/batch/"
        if user_field_names:
            create_rows_url += "?user_field_names=true"
        resp = requests.post(
            create_rows_url,
            headers={
                "Authorization": f"{self._token_mode} {self._token}",
                "Content-Type": "application/json",
            },
            json={"items": datas},
        )
        resp.raise_for_status()
        data = resp.json()
        ids = [e["id"] for e in data["items"]]
        return ids

    def _update_row(
        self, table_id: int, data: dict, user_field_names: bool = False
    ) -> None:
        """Update a row in a table.

        Args:
            table_id (int): ID of the table of interest.
            data (dict): Data to update (has to include the row ID as id).
            user_field_names (bool, optional): Whether to use field names or field IDs
                for the data keys. Defaults to False.

        Raises:
            RuntimeError: If the response is malformed.
        """
        row_id = data.pop("id")
        update_row_url = f"{self._database_url}/{self.table_path}/{table_id}/{row_id}/"
        if user_field_names:
            update_row_url += "?user_field_names=true"
        resp = requests.patch(
            update_row_url,
            headers={
                "Authorization": f"{self._token_mode} {self._token}",
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

    def _update_rows(
        self, table_id: int, datas: list[dict], user_field_names: bool = False
    ):
        update_rows_url = f"{self._database_url}/{self.table_path}/{table_id}/batch/"
        if user_field_names:
            update_rows_url += "?user_field_names=true"
        resp = requests.patch(
            update_rows_url,
            headers={
                "Authorization": f"{self._token_mode} {self._token}",
                "Content-Type": "application/json",
            },
            json={"items": datas},
        )
        resp.raise_for_status()
        data = resp.json()
        ids = [e["id"] for e in data["items"]]
        return ids

    def _delete_row(self, table_id: int, row_id: int):
        delete_row_url = f"{self._database_url}/{self.table_path}/{table_id}/{row_id}/"
        resp = requests.delete(
            delete_row_url,
            headers={"Authorization": f"{self._token_mode} {self._token}"},
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
                [{"value": "active", "id": 1}, {"value": "inactive", "id": 2}],
                  "read_only": False},
            {"name": "tags", "type": "multiple_select", "select_options":
                [{"value": "urgent", "id": 1}, {"value": "important", "id": 2}],
                  "read_only": False}
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

    def get_writable_fields(self, table_id: int) -> list[dict]:
        """Get all writable fields in a table.

        Args:
            table_id (int): ID of the table of interest.

        Returns:
            list[dict]: List of writable fields.
        """
        fields = self.get_fields(table_id)
        writable_fields = [field for field in fields if not field["read_only"]]
        return writable_fields

    def get_data(
        self, table_id: int, writable_only: bool = True, user_field_names: bool = True
    ) -> dict[int, dict[str, Any]]:
        """Get all data from a table.

        Args:
            table_id (int): ID of the table of interest.
            writable_only (bool, optional): Only return fields which can be written to.
                This excludes all formula and computed fields. Defaults to True (only
                writable fields).
            user_field_names (bool, optional): Whether to reference columns by name
                or ID. Defaults to True (use names).

        Returns:
            dict[int, dict[str, Any]]: dictionary of data in the table.
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
            d["id"]: {k: _format_value(v, names[k]) for k, v in d.items() if k in names}
            for d in data
        }

        return writable_data

    def get_entry(
        self,
        table_id: int,
        row_id: int,
        linked: bool = False,
        seen_tables: Optional[list] = None,
        user_field_names: bool = True,
    ) -> dict:
        """Get a single entry from a table.

        Args:
            table_id (int): ID of the table of interest.
            row_id (int): Entry ID for the entry of interest.
            linked (bool, optional): Whether to fully hydrate the output with
                linked tables. Defaults to False (no data of linked tables is loaded).
            seen_tables (list, optional): List of already linked tables.
                These are not loaded again. Defaults to None.
            user_field_names (bool, optional): Whether to reference columns by name
                or ID. Defaults to True (use names).

        Returns:
            dict: Entry data.
        """
        data = self._get_rows_data(
            table_id=table_id,
            row_id=row_id,
            paginated=False,
            user_field_names=user_field_names,
        )
        fields = self.get_fields(table_id)
        names = {f["name"]: f for f in fields}
        names = names | {f'field_{f["id"]}': f for f in fields}
        formatted_data = {
            k: _format_value(v, names[k]) for k, v in data.items() if k in names
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

    def add_data(
        self,
        table_id: int,
        data: dict,
        row_id: Optional[int] = None,
        user_field_names: bool = True,
    ) -> int:
        """Add/Change data to a table.

        Args:
            table_id (int): Table ID.
            data (dict): Data to add/change.
            row_id (int, optional): Row ID where to enter the data. Defaults to None.
            user_field_names (bool, optional): Whether to reference columns by name or
              ID. Defaults to True.

        Returns:
            int: Row ID.
        """
        fields = self.get_fields(table_id)
        data_conv = self._convert_selects(data, fields)
        if row_id:
            data_conv["id"] = row_id
            self._update_row(table_id, data_conv, user_field_names=user_field_names)
        else:
            row_id = self._create_row(
                table_id, data_conv, user_field_names=user_field_names
            )

        return row_id

    def add_data_batch(
        self,
        table_id: int,
        entries: list[dict],
        user_field_names: bool = True,
        fail_on_error: bool = False,
    ) -> tuple[list, list]:
        """Add/Change data (multiple rows) to a table.

        Args:
            table_id (int): ID of the table of interest.
            entries (list[dict]): List of entries to add/change.
            user_field_names (bool, optional): Whether to use field names or field IDs.
                Defaults to True.
            fail_on_error (bool, optional): Whether to fail if error appears.
                Defaults to False.

        Returns:
            tuple[list, list]: List of touched IDs and list of errors.
        """

        def process_entries(input_entries, batch_operation, single_operation):
            """Helper function to process entries for create or update."""
            processed_ids = []
            try:
                processed_ids += batch_operation(
                    table_id, input_entries, user_field_names=user_field_names
                )
            except requests.HTTPError as err:
                if err.response.status_code == 504:  # Handle Gateway Timeout
                    # Sleep for 60 seconds and retry
                    warnings.warn(
                        f"Gateway Timeout: {err.response.text}. Retrying after 60 "
                        f"seconds with single operations."
                    )
                    time.sleep(60)
                    # Retry the batch operation
                    for entry in input_entries:  # Process each entry individually
                        processed_ids.append(
                            single_operation(
                                table_id,
                                entry,
                                user_field_names=user_field_names,
                            )
                        )
                else:
                    raise err
            return processed_ids

        # Split entries into new and update
        entries_update, entries_new, errors, touched_ids = [], [], [], []

        for entry in entries:
            if entry.get("id") is not None:
                entries_update.append(entry)
            else:
                entries_new.append(entry)

        if entries_new:
            try:
                touched_ids += process_entries(
                    entries_new, self._create_rows, self._create_row
                )
            except requests.HTTPError as err:
                errors.append(
                    f"{self._create_rows.__name__} rows ({len(entries_new)}):\
                          {err.response.text}"
                )
        if entries_update:
            try:
                touched_ids += process_entries(
                    entries_update, self._update_rows, self._update_row
                )
            except requests.HTTPError as err:
                errors.append(
                    f"{self._update_rows.__name__} rows ({len(entries_update)}):\
                      {err.response.text}"
                )

        if errors and fail_on_error:
            raise RuntimeError(errors)
        else:
            return touched_ids, errors
