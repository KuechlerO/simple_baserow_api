import time
import warnings
from copy import deepcopy
from typing import Any, Optional, Union

import requests

"""
simple_baserow_api base module.

This is the principal module of the simple_baserow_api project.
"""

# example constant variable
NAME = "simple_baserow_api"


def _format_value(raw_value: Any, field_info: dict, use_link_ids: bool = True) -> Any:
    """
    Normalize a raw Baserow cell value for a select or link field.

    For other field types the value is returned unchanged.

    Args:
        raw_value: Raw API cell value (dict/list/None/scalar depending on type).
        field_info: Field metadata (must include ``type``).
        use_link_ids: For ``link_row``, return linked row IDs when True,
            otherwise primary display values.

    Returns:
        A simplified Python value suitable for further processing.

    Raises:
        RuntimeError: If the raw value shape does not match the field type.
    """
    field_type = field_info["type"]

    if field_type == "single_select":
        if raw_value is None:
            return None
        if isinstance(raw_value, dict):
            return raw_value["value"]
        raise RuntimeError(f"malformed single_select {raw_value!r}")

    if field_type == "multiple_select":
        if raw_value is None:
            return None
        if isinstance(raw_value, list):
            return [v["value"] for v in raw_value]
        raise RuntimeError(f"malformed multiple_select {raw_value!r}")

    if field_type == "link_row":
        if raw_value is None:
            return None
        if not isinstance(raw_value, list):
            raise RuntimeError(f"malformed link_row {raw_value!r}")
        if use_link_ids:
            return [v["id"] for v in raw_value]
        return [v["value"] for v in raw_value]

    return raw_value


def _has_usable_type(field: dict) -> bool:
    """Return True if *field* has a non-None ``type`` value."""
    return field.get("type") is not None


def _check_for_fields_mismatch(
    input_fields1: list[dict], input_fields2: list[dict]
) -> None:
    """
    Check that every field in *input_fields1* exists in *input_fields2*.

    When both sides provide usable ``type`` values, also verify type
    compatibility (formula fields in *input_fields1* are skipped).

    :param input_fields1: Field dicts that must be present (e.g. payload keys).
    :param input_fields2: Field dicts to check against (e.g. table schema).
    :raises RuntimeError: If fields are missing or types are incompatible.
    """
    fields2_by_name = {field["name"]: field for field in input_fields2}

    # Type checks only when every input field provides a usable type.
    # Omitting ``type`` entirely means name-only validation (no warning).
    input_declares_types = any("type" in field for field in input_fields1)
    check_types = bool(input_fields1) and all(
        _has_usable_type(field) for field in input_fields1
    ) and all(_has_usable_type(field) for field in input_fields2)
    if input_declares_types and not check_types:
        warnings.warn(
            "Type information is missing for some fields. "
            "Type compatibility will not be checked."
        )

    missing_fields = [
        field["name"]
        for field in input_fields1
        if field["name"] not in fields2_by_name
    ]

    mismatching_fields = []
    if check_types:
        for field1 in input_fields1:
            field_name = field1["name"]
            if field_name not in fields2_by_name:
                continue
            field1_type = field1["type"]
            field2_type = fields2_by_name[field_name]["type"]
            if field1_type != "formula" and field1_type != field2_type:
                mismatching_fields.append((field_name, field1_type, field2_type))

    error_parts = []
    if missing_fields:
        error_parts.append(f"Missing fields in table: {missing_fields}")
    if mismatching_fields:
        mismatch_descriptions = [
            f"'{name}' (type '{type1}' vs '{type2}')"
            for name, type1, type2 in mismatching_fields
        ]
        error_parts.append(
            f"Type mismatches for fields: {', '.join(mismatch_descriptions)}"
        )
    if error_parts:
        raise RuntimeError("; ".join(error_parts))


class BaserowApi:
    """BaserowAPI class: A wrapper around the Baserow API."""

    table_path = "api/database/rows/table"
    fields_path = "api/database/fields/table"
    tables_path = "api/database/tables"
    applications_path = "api/applications"
    workspaces_path = "api/workspaces"
    token_auth_path = "api/user/token-auth"

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
            database_url (str): URL of the Baserow instance
                (e.g. ``https://api.baserow.io``).
            token (Optional[str], optional): Token-String for Baserow access.
                Defaults to None.
            token_path (Optional[str], optional): Path to file containing the
                Token-String. Defaults to None.
            jwt_token (bool, optional): Whether JWT-Token is used instead of
                Token-String. Defaults to False.
                Schema operations (create/delete database or table) require JWT.
        """
        self._database_url = database_url.rstrip("/")
        if token_path:
            with open(token_path) as tokenfile:
                self._token = tokenfile.readline().strip()
        elif token:
            self._token = token
        else:
            self._token = None

        if not self._token:
            raise ValueError(
                "A Baserow token is required: pass token=... or token_path=..."
            )

        self._token_mode = "JWT" if jwt_token else "Token"

    # ------------------------------------------------------------------
    # Auth / HTTP helpers
    # ------------------------------------------------------------------

    def _auth_headers(self, content_type: Optional[str] = None) -> dict:
        """Build request headers including Authorization."""
        headers = {"Authorization": f"{self._token_mode} {self._token}"}
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    @staticmethod
    def login(database_url: str, email: str, password: str) -> dict:
        """Obtain a JWT access token via email/password.

        Args:
            database_url: Baserow API base URL.
            email: Account email.
            password: Account password.

        Returns:
            Full token-auth response (includes ``access_token`` and
            ``refresh_token``).

        Raises:
            requests.HTTPError: If authentication fails.
        """
        url = f"{database_url.rstrip('/')}/{BaserowApi.token_auth_path}/"
        resp = requests.post(url, json={"email": email, "password": password})
        resp.raise_for_status()
        return resp.json()

    @classmethod
    def from_credentials(
        cls, database_url: str, email: str, password: str
    ) -> "BaserowApi":
        """Create a JWT-authenticated client from email and password.

        Args:
            database_url: Baserow API base URL.
            email: Account email.
            password: Account password.

        Returns:
            Authenticated ``BaserowApi`` instance (JWT mode).
        """
        token_data = cls.login(database_url, email, password)
        return cls(
            database_url=database_url,
            token=token_data["access_token"],
            jwt_token=True,
        )

    # ------------------------------------------------------------------
    # Schema helpers (JWT required)
    # ------------------------------------------------------------------

    def list_workspaces(self) -> list[dict]:
        """List workspaces visible to the authenticated user.

        Returns:
            List of workspace dicts.
        """
        url = f"{self._database_url}/{self.workspaces_path}/"
        resp = requests.get(url, headers=self._auth_headers())
        resp.raise_for_status()
        return resp.json()

    def create_database(
        self, workspace_id: int, name: str, init_with_data: bool = False
    ) -> dict:
        """Create a database application in a workspace.

        Requires JWT authentication.

        Args:
            workspace_id: Target workspace ID.
            name: Database name.
            init_with_data: Whether Baserow should seed example data.

        Returns:
            Created application dict (includes ``id``).
        """
        url = (
            f"{self._database_url}/{self.applications_path}/"
            f"workspace/{workspace_id}/"
        )
        resp = requests.post(
            url,
            headers=self._auth_headers("application/json"),
            json={"name": name, "type": "database", "init_with_data": init_with_data},
        )
        resp.raise_for_status()
        return resp.json()

    def delete_database(self, database_id: int) -> None:
        """Delete a database application.

        Requires JWT authentication.

        Args:
            database_id: Application ID of the database to delete.
        """
        url = f"{self._database_url}/{self.applications_path}/{database_id}/"
        resp = requests.delete(url, headers=self._auth_headers())
        resp.raise_for_status()

    def create_table(
        self,
        database_id: int,
        name: str,
        primary_field_name: str = "Name",
        fields: Optional[list[dict]] = None,
    ) -> dict:
        """Create a table and optionally additional fields.

        Requires JWT authentication. The table is created with a single
        primary text field named *primary_field_name*. Any *fields* are
        then created via :meth:`create_field`.

        Args:
            database_id: Parent database application ID.
            name: Table name.
            primary_field_name: Name of the primary text field.
            fields: Optional list of field-create payloads
                (each must include at least ``name`` and ``type``).

        Returns:
            Dict with ``id``, ``name``, and ``fields`` (full field list).
        """
        url = (
            f"{self._database_url}/{self.tables_path}/database/{database_id}/"
        )
        resp = requests.post(
            url,
            headers=self._auth_headers("application/json"),
            json={
                "name": name,
                "data": [[primary_field_name], [""]],
                "first_row_header": True,
            },
        )
        resp.raise_for_status()
        table = resp.json()
        table_id = table["id"]

        if fields:
            for field_spec in fields:
                self.create_field(table_id, field_spec)

        return {
            "id": table_id,
            "name": name,
            "fields": self.get_fields(table_id),
        }

    def delete_table(self, table_id: int) -> None:
        """Delete a table.

        Requires JWT authentication.

        Args:
            table_id: Table ID to delete.
        """
        url = f"{self._database_url}/{self.tables_path}/{table_id}/"
        resp = requests.delete(url, headers=self._auth_headers())
        resp.raise_for_status()

    def create_field(self, table_id: int, field_spec: dict) -> dict:
        """Create a field on a table.

        Requires JWT authentication.

        Args:
            table_id: Target table ID.
            field_spec: Field payload (must include ``name`` and ``type``;
                type-specific keys such as ``select_options`` or
                ``link_row_table_id`` as needed).

        Returns:
            Created field dict from the API.
        """
        url = f"{self._database_url}/{self.fields_path}/{table_id}/"
        resp = requests.post(
            url,
            headers=self._auth_headers("application/json"),
            json=field_spec,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Field / row access
    # ------------------------------------------------------------------

    def get_fields(self, table_id: int) -> list[dict]:
        """Get all fields / column specifications for a table.

        Args:
            table_id (int): ID of the table of interest.

        Returns:
            list[dict]: List of column specifications (dict of fields)
        """
        get_fields_url = f"{self._database_url}/{self.fields_path}/{table_id}/"
        resp = requests.get(get_fields_url, headers=self._auth_headers())
        resp.raise_for_status()
        return resp.json()

    def _payload_keys_as_fields(
        self, data: Union[dict, list[dict]], table_fields: list[dict]
    ) -> list[dict]:
        """Convert payload keys to field descriptors for compatibility checks.
        Example: {"field_42": "x", "id": 7} with table field id 42 named becomes
        "Medgen ID" → [{"name": "Medgen ID"}].

        Keys named ``field_<id>`` are resolved to the corresponding field name.
        The special key ``id`` (row id) is ignored.
        """
        fields_by_id = {field["id"]: field for field in table_fields}
        keys: set[str] = set()
        entries = [data] if isinstance(data, dict) else data
        for entry in entries:
            for key in entry:
                if key == "id":
                    continue
                if key.startswith("field_"):
                    try:
                        field_id = int(key.split("_", 1)[1])
                    except ValueError:
                        keys.add(key)
                        continue
                    if field_id in fields_by_id:
                        keys.add(fields_by_id[field_id]["name"])
                    else:
                        keys.add(key)
                else:
                    keys.add(key)
        # Name-only descriptors: skip type checks against the table schema.
        return [{"name": name} for name in sorted(keys)]

    def _validate_field_compatibility(
        self,
        table_id: int,
        data: Union[dict, list[dict]],
        fields: Optional[list[dict]] = None,
        fail_on_error: bool = False,
    ) -> list[dict]:
        """Validate that payload field names exist on the table.

        Args:
            table_id: ID of the table of interest.
            data: Single row dict or list of row dicts.
            fields: Optional pre-fetched table fields (avoids a second request).
            fail_on_error: If True, raise on incompatibility; otherwise warn.

        Returns:
            The table field list used for validation (fetched if needed).

        Raises:
            RuntimeError: When fields are incompatible and *fail_on_error* is True.
        """
        if fields is None:
            fields = self.get_fields(table_id)

        input_fields = self._payload_keys_as_fields(data, fields)
        try:
            _check_for_fields_mismatch(input_fields, fields)
        except RuntimeError as err:
            if fail_on_error:
                raise
            warnings.warn(str(err), stacklevel=2)
        return fields

    def _get_rows_data(
        self,
        url: Optional[str] = None,
        table_id: Optional[int] = None,
        row_id: Optional[int] = None,
        user_field_names: bool = False,
        paginated: bool = False,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ) -> Union[dict, list]:
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
            include (list[str], optional): List of fields to include in the
                response. Defaults to None (all fields).
            exclude (list[str], optional): List of fields to exclude from the
                response. Defaults to None (no fields excluded).
        Returns:
            A single row dict when *row_id* is set, otherwise a list of row
            dicts.
        """
        query_params = []

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
                query_params += ["user_field_names=true"]
        else:
            raise RuntimeError("Either table_id or url must be provided.")

        if include:
            query_params += [f"include={','.join(include)}"]
        if exclude:
            query_params += [f"exclude={','.join(exclude)}"]

        if query_params:
            separator = "&" if "?" in get_rows_url else "?"
            get_rows_url = get_rows_url + separator + "&".join(query_params)
        resp = requests.get(get_rows_url, headers=self._auth_headers())

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

    def _resolve_field(
        self, table_id: int, column: str, fields: Optional[list[dict]] = None
    ) -> dict:
        """Resolve a field name or ``field_<id>`` key to a field dict.

        Raises:
            RuntimeError: If the column cannot be found on the table.
        """
        if fields is None:
            fields = self.get_fields(table_id)
        fields_by_name = {f["name"]: f for f in fields}
        fields_by_id = {f["id"]: f for f in fields}

        if column in fields_by_name:
            return fields_by_name[column]
        if column.startswith("field_"):
            try:
                field_id = int(column.split("_", 1)[1])
            except ValueError as err:
                raise RuntimeError(f"Unknown column '{column}'") from err
            if field_id in fields_by_id:
                return fields_by_id[field_id]
        raise RuntimeError(f"Unknown column '{column}' in table {table_id}")

    def _primary_field(self, table_id: int, fields: Optional[list[dict]] = None) -> dict:
        """Return the primary field definition for a table."""
        if fields is None:
            fields = self.get_fields(table_id)
        for field in fields:
            if field.get("primary"):
                return field
        raise RuntimeError(f"No primary field found for table {table_id}")

    def find_entries(
        self,
        table_id: int,
        column: str,
        value: Any,
        *,
        user_field_names: bool = True,
        writable_only: bool = False,
        use_linked_row_ids: bool = True,
        paginated: bool = True,
    ) -> dict[int, dict[str, Any]]:
        """Find rows in a table where *column* equals *value*.

        Uses Baserow's ``filter__field_<id>__equal`` query filter.

        Args:
            table_id: Table to search.
            column: Field name or ``field_<id>`` key to match on.
            value: Value that must equal the column contents.
            user_field_names: Return/use human field names when True.
            writable_only: If True, omit read-only fields from results.
            use_linked_row_ids: Format link_row values as IDs (True) or
                primary display values (False).
            paginated: Load all result pages when True.

        Returns:
            Dict mapping row ID → formatted row data (same shape as
            :meth:`get_data`).
        """
        fields = self.get_fields(table_id)
        field = self._resolve_field(table_id, column, fields=fields)

        params = {f"filter__field_{field['id']}__equal": value}
        if user_field_names:
            params["user_field_names"] = "true"

        get_rows_url = f"{self._database_url}/{self.table_path}/{table_id}/"
        resp = requests.get(
            get_rows_url, headers=self._auth_headers(), params=params
        )
        resp.raise_for_status()
        data = resp.json()
        if "results" not in data:
            raise RuntimeError(f"Could not get query result data from {get_rows_url}")

        results = data["results"]
        if paginated and data.get("next"):
            results = results + self._get_rows_data(
                url=data["next"], paginated=True
            )

        if writable_only:
            names = {f["name"]: f for f in fields if not f["read_only"]}
        else:
            names = {f["name"]: f for f in fields}
        if not user_field_names:
            names = {
                f'field_{f["id"]}': f
                for f in fields
                if (not writable_only) or (not f["read_only"])
            }

        return {
            row["id"]: {
                k: _format_value(v, names[k], use_linked_row_ids)
                for k, v in row.items()
                if k in names
            }
            for row in results
        }

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
            headers=self._auth_headers("application/json"),
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
            headers=self._auth_headers("application/json"),
            json={"items": datas},
        )
        resp.raise_for_status()
        data = resp.json()
        ids = [e["id"] for e in data["items"]]
        return ids

    def _update_row(
        self, table_id: int, data: dict, user_field_names: bool = False
    ) -> int:
        """Update a row in a table.

        Args:
            table_id (int): ID of the table of interest.
            data (dict): Data to update (must include the row ID as ``id``).
                The caller's dict is not mutated.
            user_field_names (bool, optional): Whether to use field names or field IDs
                for the data keys. Defaults to False.

        Returns:
            int: Updated row ID.

        Raises:
            RuntimeError: If the response is malformed or ``id`` is missing.
        """
        payload = deepcopy(data)
        if "id" not in payload:
            raise RuntimeError("Update payload must include an 'id' key")
        row_id = payload.pop("id")
        update_row_url = f"{self._database_url}/{self.table_path}/{table_id}/{row_id}/"
        if user_field_names:
            update_row_url += "?user_field_names=true"
        resp = requests.patch(
            update_row_url,
            headers=self._auth_headers("application/json"),
            json=payload,
        )
        resp.raise_for_status()
        resp_data = resp.json()
        if "id" in resp_data:
            return resp_data["id"]
        raise RuntimeError(f"Malformed response {resp_data}")

    def _update_rows(
        self, table_id: int, datas: list[dict], user_field_names: bool = False
    ):
        update_rows_url = f"{self._database_url}/{self.table_path}/{table_id}/batch/"
        if user_field_names:
            update_rows_url += "?user_field_names=true"
        resp = requests.patch(
            update_rows_url,
            headers=self._auth_headers("application/json"),
            json={"items": datas},
        )
        resp.raise_for_status()
        data = resp.json()
        ids = [e["id"] for e in data["items"]]
        return ids

    def _delete_row(self, table_id: int, row_id: int):
        delete_row_url = f"{self._database_url}/{self.table_path}/{table_id}/{row_id}/"
        resp = requests.delete(delete_row_url, headers=self._auth_headers())
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
            # Support both human names and field_<id> keys in the payload.
            field_keys = [field["name"]]
            if "id" in field:
                field_keys.append(f"field_{field['id']}")
            active_key = next((k for k in field_keys if k in data_conv), None)
            if field.get("read_only") or active_key is None:
                continue

            cur_value = data_conv[active_key]

            if cur_value is None or cur_value == []:
                continue

            if field["type"] == "single_select":
                data_conv[active_key] = convert_option(
                    cur_value, field["select_options"]
                )

            elif field["type"] == "multiple_select":
                if not isinstance(cur_value, list):
                    raise RuntimeError(
                        f"multiple_select value for '{active_key}' must be a "
                        f"list, got {type(cur_value).__name__}"
                    )
                data_conv[active_key] = [
                    convert_option(single_value, field["select_options"])
                    for single_value in cur_value
                ]
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
        self,
        table_id: int,
        writable_only: bool = True,
        user_field_names: bool = True,
        paginated: bool = True,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
        use_linked_row_ids: bool = True,
    ) -> dict[int, dict[str, Any]]:
        """Get all data from a table.

        Args:
            table_id (int): ID of the table of interest.
            writable_only (bool, optional): Only return fields which can be written to.
                This excludes all formula and computed fields. Defaults to True (only
                writable fields).
            user_field_names (bool, optional): Whether to reference columns by name
                or ID. Defaults to True (use names).
            paginated (bool, optional): Whether to load multiple pages of data. Defaults to True.
            include (list[str], optional): List of fields to include in the
                response. Defaults to None (all fields).
            exclude (list[str], optional): List of fields to exclude from the
                response. Defaults to None (no fields excluded).
            use_linked_row_ids (bool, optional): Return IDs for linked rows, with False return values
                instead. Defaults to True (return IDs).

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
            table_id=table_id,
            user_field_names=user_field_names,
            paginated=paginated,
            include=include,
            exclude=exclude,
        )

        # Collect rows with their field names and values,
        writable_data = {
            d["id"]: {
                k: _format_value(v, names[k], use_linked_row_ids)
                for k, v in d.items()
                if k in names
            }
            for d in data
        }

        return writable_data

    def get_entry(
        self,
        table_id: int,
        row_id: int,
        linked: bool = False,
        use_linked_row_ids: bool = True,
        seen_tables: Optional[list] = None,
        user_field_names: bool = True,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ) -> dict:
        """Get a single entry from a table.

        Args:
            table_id (int): ID of the table of interest.
            row_id (int): Entry ID for the entry of interest.
            linked (bool, optional): Whether to fully hydrate the output with
                linked tables. Defaults to False (no data of linked tables is loaded).
            use_linked_row_ids (bool, optional): Return IDs for linked rows, with False return values
                instead. Ignored if linked is True. Defaults to True (return IDs).
            seen_tables (list, optional): List of already linked tables.
                These are not loaded again. Defaults to None.
            user_field_names (bool, optional): Whether to reference columns by name
                or ID. Defaults to True (use names).
            include (list[str], optional): List of fields to include in the
                response. Defaults to None (all fields).
            exclude (list[str], optional): List of fields to exclude from the
                response. Defaults to None (no fields excluded).

        Returns:
            dict: Entry data.
        """
        if linked and not use_linked_row_ids:
            warnings.warn(
                "Raw value output from 'use_linked_row_ids=False' is ignored because "
                "full linked information is used instead (linked=True)"
            )

        data = self._get_rows_data(
            table_id=table_id,
            row_id=row_id,
            paginated=False,
            user_field_names=user_field_names,
            include=include,
            exclude=exclude,
        )
        fields = self.get_fields(table_id)
        # If include or exclude are provided, filter the fields
        if include:
            fields = [f for f in fields if f["name"] in include]
        if exclude:
            fields = [f for f in fields if f["name"] not in exclude]

        names = {f["name"]: f for f in fields}
        names = names | {f'field_{f["id"]}': f for f in fields}

        formatted_data = {
            k: _format_value(v, names[k], use_linked_row_ids)
            for k, v in data.items()
            if k in names
        }

        # Copy to avoid mutating the caller's list across recursive calls.
        seen_tables_next = list(seen_tables) if seen_tables else []
        seen_tables_next.append(table_id)

        # fully hydrate with linked data
        # --> recursively get data from linked tables
        if linked:
            link_fields = [f for f in fields if f["type"] == "link_row"]
            for field in link_fields:
                linked_table_id = field["link_row_table_id"]
                if linked_table_id in seen_tables_next[:-1]:
                    continue
                # Raw API key depends on user_field_names.
                raw_key = (
                    field["name"]
                    if user_field_names
                    else f"field_{field['id']}"
                )
                linked_refs = data.get(raw_key) or []
                if not linked_refs:
                    continue
                formatted_data[field["name"] if user_field_names else raw_key] = [
                    self.get_entry(
                        linked_table_id,
                        e_id["id"],
                        linked=False,
                        use_linked_row_ids=use_linked_row_ids,
                        seen_tables=seen_tables_next,
                        user_field_names=user_field_names,
                        include=include,
                        exclude=exclude,
                    )
                    for e_id in linked_refs
                ]

        return formatted_data

    def add_data(
        self,
        table_id: int,
        data: dict,
        row_id: Optional[int] = None,
        user_field_names: bool = True,
        fail_on_error: bool = False,
        check_field_compatibility: bool = False,
    ) -> int:
        """Add/Change data to a table.

        Args:
            table_id (int): Table ID.
            data (dict): Data to add/change.
            row_id (int, optional): Row ID where to enter the data. Defaults to None.
            user_field_names (bool, optional): Whether to reference columns by name or
              ID. Defaults to True.
            fail_on_error (bool, optional): When *check_field_compatibility* is True,
                raise ``RuntimeError`` on incompatible fields instead of warning.
                Defaults to False.
            check_field_compatibility (bool, optional): Validate that payload keys
                exist as fields on the table before writing. Defaults to False.

        Returns:
            int: Row ID.

        Raises:
            RuntimeError: If field compatibility fails and *fail_on_error* is True.
        """
        fields = self.get_fields(table_id)
        if check_field_compatibility:
            fields = self._validate_field_compatibility(
                table_id,
                data,
                fields=fields,
                fail_on_error=fail_on_error,
            )

        data_conv = self._convert_selects(data, fields)
        if row_id:
            data_conv["id"] = row_id
            self._update_row(table_id, data_conv, user_field_names=user_field_names)
        else:
            row_id = self._create_row(
                table_id, data_conv, user_field_names=user_field_names
            )

        return row_id

    def synchronize_data(
        self,
        source_table_id: int,
        source_row_id: int,
        target_table_id: int,
        identifier_column: str,
        *,
        field_mapping: Optional[dict[str, str]] = None,
        link_match_column: Optional[Union[str, dict[str, str]]] = None,
        exclude_fields: Optional[list[str]] = None,
        include_fields: Optional[list[str]] = None,
        fail_on_error: bool = False,
        dry_run: bool = False,
    ) -> tuple[Optional[int], list[str]]:
        """Copy one source row into a target table (create or update).

        Matching on the target uses *identifier_column*: if a target row
        already has the same identifier value as the source, that row is
        updated; otherwise a new row is created.

        Compatible columns (same name by default, or via *field_mapping*)
        are copied. Read-only target fields, missing columns, and type
        mismatches are skipped with a warning. Link-row values are
        remapped by looking up matching entries in the target's linked
        table (by primary field, or by *link_match_column*).

        Args:
            source_table_id: Table containing the source row.
            source_row_id: Row ID to copy from.
            target_table_id: Table to write into.
            identifier_column: Column name used to find an existing target
                row (must exist on both tables unless mapped).
            field_mapping: Optional map of source field name → target field
                name. Unmapped fields with identical names are still copied.
            link_match_column: Column used to match linked rows in the
                target linked table. Either a single column name applied to
                all link fields, or a dict of target link-field name →
                match column. Defaults to each linked table's primary field.
            exclude_fields: Optional source field names to skip entirely
                (e.g. links handled separately by the caller).
            include_fields: If set, only these source field names are
                considered for transfer (in addition to the identifier
                column, which is always written). ``exclude_fields`` is
                applied after this filter.
            fail_on_error: If True, raise ``RuntimeError`` when any value
                cannot be transferred (hard failures such as missing fields,
                type mismatches, or unmatched links). Informational notices
                (e.g. ambiguous matches where the first row is used) do not
                trigger failure. If False (default), warn and continue.
            dry_run: If True, build the transfer payload and report what
                would happen, but do not write to the target table.
                Returns ``(existing_row_id_or_None, messages)``.

        Returns:
            Tuple of ``(target_row_id, messages)`` where *messages* contains
            both hard skip reasons and soft notices. In *dry_run* mode,
            ``target_row_id`` is the existing match ID if updating, else
            ``None`` (would create); no writes are performed.

        Raises:
            RuntimeError: If the identifier is missing/empty on the source,
                or if *fail_on_error* is True and hard transfer failures
                occurred.
        """
        field_mapping = field_mapping or {}
        exclude_set = set(exclude_fields or [])
        include_set = set(include_fields) if include_fields is not None else None
        # Identifier must always be eligible for transfer.
        if include_set is not None:
            include_set.add(identifier_column)
        source_fields = {f["name"]: f for f in self.get_fields(source_table_id)}
        target_fields = {f["name"]: f for f in self.get_fields(target_table_id)}

        # Display values for links (primary labels) so we can rematch them.
        source_entry = self.get_entry(
            source_table_id,
            source_row_id,
            use_linked_row_ids=False,
        )

        id_source_name = identifier_column
        # Allow mapping the identifier itself.
        id_target_name = field_mapping.get(identifier_column, identifier_column)
        if id_source_name not in source_entry:
            raise RuntimeError(
                f"Identifier column '{id_source_name}' missing on source row "
                f"{source_row_id}"
            )
        identifier_value = source_entry[id_source_name]
        if identifier_value in (None, ""):
            raise RuntimeError(
                f"Identifier column '{id_source_name}' is empty on source row "
                f"{source_row_id}"
            )

        payload: dict[str, Any] = {}
        hard_skips: list[str] = []
        notices: list[str] = []

        def _allowed(source_name: str) -> bool:
            if source_name in exclude_set:
                return False
            if include_set is not None and source_name not in include_set:
                return False
            return True

        # Build the set of source→target field pairs to consider.
        pairs: list[tuple[str, str]] = []
        mapped_sources = set(field_mapping.keys())
        for source_name, target_name in field_mapping.items():
            if not _allowed(source_name):
                continue
            pairs.append((source_name, target_name))
        for source_name in source_entry:
            if source_name in mapped_sources or not _allowed(source_name):
                continue
            if source_name in target_fields:
                pairs.append((source_name, source_name))

        for source_name, target_name in pairs:
            if source_name not in source_entry:
                hard_skips.append(
                    f"Source field '{source_name}' not present on source row"
                )
                continue
            if target_name not in target_fields:
                hard_skips.append(
                    f"Cannot transfer '{source_name}': target has no field "
                    f"'{target_name}'"
                )
                continue

            source_field = source_fields.get(source_name)
            target_field = target_fields[target_name]

            if target_field.get("read_only"):
                hard_skips.append(
                    f"Cannot transfer '{source_name}': target field is read-only"
                )
                continue

            if (
                source_field
                and source_field.get("type")
                and target_field.get("type")
                and source_field["type"] != "formula"
                and source_field["type"] != target_field["type"]
            ):
                hard_skips.append(
                    f"Cannot transfer '{source_name}': type mismatch "
                    f"({source_field['type']} → {target_field['type']})"
                )
                continue

            value = source_entry[source_name]

            if target_field["type"] == "link_row":
                resolved, link_hard, link_soft = self._resolve_link_values_for_sync(
                    value=value,
                    target_field=target_field,
                    target_field_name=target_name,
                    link_match_column=link_match_column,
                )
                hard_skips.extend(link_hard)
                notices.extend(link_soft)
                if resolved is not None:
                    payload[target_name] = resolved
                continue

            payload[target_name] = value

        skipped = hard_skips + notices
        for msg in skipped:
            warnings.warn(msg, stacklevel=2)
        if hard_skips and fail_on_error:
            raise RuntimeError(
                "synchronize_data could not transfer all values: "
                + "; ".join(hard_skips)
            )

        # Ensure identifier is always written so upsert can work.
        if id_target_name not in payload:
            if id_target_name in target_fields and not target_fields[id_target_name].get(
                "read_only"
            ):
                payload[id_target_name] = identifier_value
            else:
                raise RuntimeError(
                    f"Identifier column '{id_target_name}' is not writable on target"
                )

        # Always report which target fields are in the write payload.
        notices.append(f"transferred fields: {sorted(payload.keys())}")

        existing = self.find_entries(
            target_table_id, id_target_name, identifier_value
        )
        if len(existing) > 1:
            notice = (
                f"Multiple target rows match identifier '{identifier_value}' "
                f"on '{id_target_name}'; updating the first match"
            )
            notices.append(notice)
            warnings.warn(notice, stacklevel=2)

        skipped = hard_skips + notices

        if dry_run:
            if existing:
                target_row_id = next(iter(existing))
                notices.append(
                    f"dry_run: would update row {target_row_id} "
                    f"(identifier '{identifier_value}')"
                )
            else:
                target_row_id = None
                notices.append(
                    f"dry_run: would create new row "
                    f"(identifier '{identifier_value}')"
                )
            skipped = hard_skips + notices
            for msg in notices:
                if msg.startswith("dry_run:") or msg.startswith("transferred fields:"):
                    warnings.warn(msg, stacklevel=2)
            return target_row_id, skipped

        if existing:
            target_row_id = next(iter(existing))
            return (
                self.add_data(
                    target_table_id,
                    payload,
                    row_id=target_row_id,
                    user_field_names=True,
                ),
                skipped,
            )

        return (
            self.add_data(target_table_id, payload, user_field_names=True),
            skipped,
        )

    def _resolve_link_values_for_sync(
        self,
        value: Any,
        target_field: dict,
        target_field_name: str,
        link_match_column: Optional[Union[str, dict[str, str]]],
    ) -> tuple[Optional[list[int]], list[str], list[str]]:
        """Map source link display values to row IDs in the target linked table.

        Returns:
            ``(resolved_ids_or_None, hard_skips, soft_notices)``.
            ``None`` for resolved means the field is omitted from the payload.
            Soft notices (e.g. ambiguous matches) do not block ``fail_on_error``.
        """
        hard_skips: list[str] = []
        soft_notices: list[str] = []
        if value in (None, []):
            return [], hard_skips, soft_notices
        if not isinstance(value, list):
            hard_skips.append(
                f"Cannot transfer '{target_field_name}': expected list of "
                f"link values, got {type(value).__name__}"
            )
            return None, hard_skips, soft_notices

        linked_table_id = target_field.get("link_row_table_id")
        if linked_table_id is None:
            hard_skips.append(
                f"Cannot transfer '{target_field_name}': target link field "
                f"has no linked table"
            )
            return None, hard_skips, soft_notices

        if isinstance(link_match_column, dict):
            match_col = link_match_column.get(target_field_name)
        else:
            match_col = link_match_column
        if not match_col:
            match_col = self._primary_field(linked_table_id)["name"]

        resolved_ids: list[int] = []
        for link_value in value:
            matches = self.find_entries(linked_table_id, match_col, link_value)
            if not matches:
                hard_skips.append(
                    f"Cannot transfer link '{target_field_name}': no match "
                    f"for '{link_value}' in linked table {linked_table_id} "
                    f"(column '{match_col}')"
                )
                continue
            if len(matches) > 1:
                soft_notices.append(
                    f"Multiple matches for link '{target_field_name}' value "
                    f"'{link_value}' in table {linked_table_id}; using first"
                )
            resolved_ids.append(next(iter(matches)))

        if not resolved_ids and value:
            # All link values failed to resolve.
            return None, hard_skips, soft_notices
        return resolved_ids, hard_skips, soft_notices

    def add_data_batch(
        self,
        table_id: int,
        entries: list[dict],
        user_field_names: bool = True,
        fail_on_error: bool = False,
        check_field_compatibility: bool = False,
    ) -> tuple[list, list]:
        """Add/Change data (multiple rows) to a table.

        Args:
            table_id (int): ID of the table of interest.
            entries (list[dict]): List of entries to add/change.
            user_field_names (bool, optional): Whether to use field names or field IDs.
                Defaults to True.
            fail_on_error (bool, optional): Whether to fail if an error appears
                (HTTP errors after batch attempts, or field-compatibility issues
                when *check_field_compatibility* is True). Defaults to False.
            check_field_compatibility (bool, optional): Validate that payload keys
                exist as fields on the table before writing. Defaults to False.

        Returns:
            tuple[list, list]: List of touched IDs and list of errors.

        Raises:
            RuntimeError: If errors occur and *fail_on_error* is True, or if
                field compatibility fails with *fail_on_error* True.
        """
        if not entries:
            return [], []

        def process_entries(input_entries, batch_operation, single_operation):
            """Helper function to process entries for create or update."""
            processed_ids = []
            try:
                processed_ids += batch_operation(
                    table_id, input_entries, user_field_names=user_field_names
                )
            except requests.HTTPError as err:
                if err.response is not None and err.response.status_code == 504:
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

        if check_field_compatibility:
            self._validate_field_compatibility(
                table_id,
                entries,
                fail_on_error=fail_on_error,
            )

        # Split entries into new and update. Copy each entry so later
        # mutations (e.g. inside update helpers) cannot affect the caller.
        entries_update, entries_new, errors, touched_ids = [], [], [], []

        for entry in entries:
            entry_copy = deepcopy(entry)
            if entry_copy.get("id") is not None:
                entries_update.append(entry_copy)
            else:
                entries_new.append(entry_copy)

        if entries_new:
            try:
                touched_ids += process_entries(
                    entries_new, self._create_rows, self._create_row
                )
            except requests.HTTPError as err:
                err_text = err.response.text if err.response is not None else str(err)
                errors.append(
                    f"{self._create_rows.__name__} rows ({len(entries_new)}): "
                    f"{err_text}"
                )
        if entries_update:
            try:
                touched_ids += process_entries(
                    entries_update, self._update_rows, self._update_row
                )
            except requests.HTTPError as err:
                err_text = err.response.text if err.response is not None else str(err)
                errors.append(
                    f"{self._update_rows.__name__} rows ({len(entries_update)}): "
                    f"{err_text}"
                )

        if errors and fail_on_error:
            raise RuntimeError(errors)
        return touched_ids, errors
