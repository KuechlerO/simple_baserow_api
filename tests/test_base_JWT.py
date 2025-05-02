from simple_baserow_api import BaserowApi

import pytest
from requests.exceptions import HTTPError
import requests
import json


@pytest.fixture
def baserow_api():
    # ATTENTION: This database_url needs to be changed to the correct URL
    DB_URL = "https://phenotips.charite.de"
    TOKEN_AUTH_URL = "https://phenotips.charite.de/api/user/token-auth/"

    def get_user_tokens(email: str, password: str) -> str:
        """GET JWT token from Baserow API using email and password."""

        response = requests.post(
            TOKEN_AUTH_URL, json={"email": email, "password": password}
        )
        return response.json()

    # load email and password from file
    with open(
        "/Users/oliverkuchler/Programming/git_projects/phenotips_login.json", "r"
    ) as f:
        login_data = json.load(f)
        email = login_data["email"]
        password = login_data["password"]

    token_response = get_user_tokens(email, password)
    ACCESS_TOKEN = token_response["access_token"]

    return BaserowApi(database_url=DB_URL, token=ACCESS_TOKEN, jwt_token=True)


# --------- private methods ---------
def test_convert_option(baserow_api):
    data = {"status": "active", "tags": ["urgent", "important"]}
    fields = [
        {
            "name": "status",
            "type": "single_select",
            "select_options": [
                {"value": "active", "id": 1},
                {"value": "inactive", "id": 2},
            ],
            "read_only": False,
        },
        {
            "name": "tags",
            "type": "multiple_select",
            "select_options": [
                {"value": "urgent", "id": 1},
                {"value": "important", "id": 2},
            ],
            "read_only": False,
        },
    ]
    converted_data = baserow_api._convert_selects(data, fields)
    assert converted_data == {
        "status": 1,
        "tags": [1, 2],
    }, f"Converted data is {converted_data}"


# --------- public methods ---------
# ATTENTION: A lot of hard-coded IDs are used in the tests. These IDs need to be
# adjusted to the actual IDs in the database.


def test_get_fields(baserow_api):
    table_id = 1053

    fields = baserow_api.get_fields(table_id)
    fields_ids = [field["id"] for field in fields]
    fields_names = [field["name"] for field in fields]
    field_types = [field["type"] for field in fields]

    # Names of the fields
    assert "Sample-ID" in fields_names, f"Fields are {fields}"
    assert "SnakeSplice-Condition-Group" in fields_names, f"Fields are {fields}"
    assert "Number of Reads in Million (FASTQ)" in fields_names, f"Fields are {fields}"

    # IDs of the fields
    assert 10214 in fields_ids, f"Fields are {fields}"
    assert 10215 in fields_ids, f"Fields are {fields}"
    assert 10216 in fields_ids, f"Fields are {fields}"

    # assert type is correct
    assert "text" in field_types, f"Fields are {fields}"
    assert "single_select" in field_types, f"Fields are {fields}"
    assert "number" in field_types, f"Fields are {fields}"


def test_get_writable_fields(baserow_api):
    table_id = 1053

    fields = baserow_api.get_writable_fields(table_id)
    fields_ids = [field["id"] for field in fields]
    fields_names = [field["name"] for field in fields]
    field_types = [field["type"] for field in fields]

    # Names of the fields
    assert "Sample-ID" in fields_names, f"Fields are {fields}"
    assert "SnakeSplice-Condition-Group" in fields_names, f"Fields are {fields}"
    assert "Number of Reads in Million (FASTQ)" in fields_names, f"Fields are {fields}"

    # IDs of the fields
    assert 10214 in fields_ids, f"Fields are {fields}"
    assert 10215 in fields_ids, f"Fields are {fields}"
    assert 10216 in fields_ids, f"Fields are {fields}"

    # assert type is correct
    assert "text" in field_types, f"Fields are {fields}"
    assert "single_select" in field_types, f"Fields are {fields}"
    assert "number" in field_types, f"Fields are {fields}"

    # Assert that
    assert all([not field["read_only"] for field in fields]), f"Fields are {fields}"


def test_get_data_writable1(baserow_api):
    table_id = 1053

    data = baserow_api.get_data(table_id, writable_only=True)
    keys = data.keys()
    assert 1 in keys, f"Data is {data}"
    assert 2 in keys, f"Data is {data}"
    assert 19000 not in keys, f"Data is {data}"

    assert len(data) > 0, f"Data is {data}"
    assert "Sample-ID" in data[1].keys(), f"Data is {data}"
    assert "76660_Ctr_BUD13" in data[1]["Sample-ID"], f"Data is {data}"
    assert "id" not in data[1].keys(), f"Data is {data}"


def test_get_data_writable2(baserow_api):
    table_id = 1054

    data = baserow_api.get_data(table_id, writable_only=True)
    keys = data.keys()
    assert 1 in keys, f"Data is {data}"
    assert 2 in keys, f"Data is {data}"
    assert 19000 not in keys, f"Data is {data}"

    assert len(data) > 0, f"Data is {data}"
    assert "Genename" in data[1].keys(), f"Data is {data}"
    assert "id" not in data[1].keys(), f"Data is {data}"
    # read_only
    assert "HGVS" not in data[1].keys(), f"Data is {data}"


def test_get_data(baserow_api):
    table_id = 1054

    data = baserow_api.get_data(table_id, writable_only=False)
    keys = data.keys()
    assert 1 in keys, f"Data is {data}"
    assert 2 in keys, f"Data is {data}"
    assert 19000 not in keys, f"Data is {data}"

    assert len(data) > 0, f"Data is {data}"
    assert "Genename" in data[1].keys(), f"Data is {data}"
    assert "id" not in data[1].keys(), f"Data is {data}"

    # read_only
    assert "HGVS" in data[1].keys(), f"Data is {data}"


def test_get_data_no_field_names(baserow_api):
    table_id = 1053

    data = baserow_api.get_data(table_id, writable_only=False, user_field_names=False)
    keys = data.keys()
    print("keys", keys)
    print("values", data.values())
    assert 1 in keys, f"Data is {data}"
    assert 2 in keys, f"Data is {data}"
    assert 19000 not in keys, f"Data is {data}"

    assert len(data) > 0, f"Data is {data}"
    assert "Sample-ID" not in data[1].keys(), f"Data is {data}"
    assert "field_10214" in data[1].keys(), f"Data is {data}"


def test_get_entry(baserow_api):
    table_id = 1053
    entry_id = 1

    entry = baserow_api.get_entry(table_id, entry_id)
    assert "Sample-ID" in entry.keys(), f"Entry is {entry}"
    assert (
        "id" not in entry.keys()
    ), f"Entry is {entry}"  # id should not be in the entry


def test_add_data_add_simple_row(baserow_api):
    table_id = 1050

    # Add a simple row
    my_data = {
        "Medgen ID": "Test-MedgenID",
        "Anmerkungen": "TestAnmerkungen",
        "Aktiv": True,
        "WebHook-Trigger": True,
        "Zahl": 12,
    }
    row_id = baserow_api.add_data(table_id, my_data, row_id=None, user_field_names=True)

    # Check if the row was added
    entry = baserow_api.get_entry(table_id, row_id)
    for key, value in my_data.items():
        assert str(entry[key]) == str(value), f"Entry is {entry}"

    # Delete the row again
    baserow_api._delete_row(table_id, row_id)
    # Check if the row was deleted
    try:
        entry = baserow_api.get_entry(table_id, row_id)
    except HTTPError as e:
        assert e.response.status_code == 404, f"Entry is {entry}"


def test_add_data_add_row_no_user_fields(baserow_api):
    table_id = 1050

    # Add a simple row
    my_data = {
        "field_10198": "Test-MedgenID",  # Medgen ID
        "field_10199": "TestAnmerkungen",  # Anmerkungen
        "field_10200": True,  # Aktiv
        "field_10201": True,  # WebHook-Trigger
        "field_10206": 12,  # Zahl
    }
    row_id = baserow_api.add_data(
        table_id, my_data, row_id=None, user_field_names=False
    )

    # Check if the row was added
    entry = baserow_api.get_entry(table_id, row_id, user_field_names=False)
    for key, value in my_data.items():
        assert str(entry[key]) == str(value), f"Entry is {entry}"

    entry = baserow_api.get_entry(table_id, row_id, user_field_names=True)
    for key, value in my_data.items():
        assert entry["Medgen ID"] == my_data["field_10198"], f"Entry is {entry}"
        assert entry["Anmerkungen"] == my_data["field_10199"], f"Entry is {entry}"
        assert entry["Aktiv"] == my_data["field_10200"], f"Entry is {entry}"
        assert entry["WebHook-Trigger"] == my_data["field_10201"], f"Entry is {entry}"
        assert str(entry["Zahl"]) == str(my_data["field_10206"]), f"Entry is {entry}"

    # Delete the row again
    baserow_api._delete_row(table_id, row_id)
    # Check if the row was deleted
    try:
        entry = baserow_api.get_entry(table_id, row_id)
    except HTTPError as e:
        assert e.response.status_code == 404, f"Entry is {entry}"


def test_update_existing_row(baserow_api):
    table_id = 1050
    row_id = 1

    entry = baserow_api.get_entry(table_id, row_id, user_field_names=True)
    assert entry["Medgen ID"] == "Eintrag1", f"Entry is {entry}"

    returned_id = baserow_api.add_data(
        table_id,
        {"Medgen ID": "Eintrag1-geaendert"},
        row_id=row_id,
        user_field_names=True,
    )
    assert returned_id == row_id, f"Returned ID is {returned_id}"

    # Check if the row was updated
    entry = baserow_api.get_entry(table_id, returned_id, user_field_names=True)
    assert entry["Medgen ID"] == "Eintrag1-geaendert", f"Entry is {entry}"

    # Reset the entry
    baserow_api.add_data(
        table_id, {"Medgen ID": "Eintrag1"}, row_id=row_id, user_field_names=True
    )
    entry = baserow_api.get_entry(table_id, row_id, user_field_names=True)
    assert entry["Medgen ID"] == "Eintrag1", f"Entry is {entry}"


def test_add_data_batch_only_new(baserow_api):
    table_id = 1050

    # Batch of 2 rows
    my_data = [
        {
            "Medgen ID": "Test-MedgenID1",
            "Anmerkungen": "TestAnmerkungen",
            "Aktiv": True,
            "WebHook-Trigger": True,
            "Zahl": 12,
        },
        {
            "Medgen ID": "Test-MedgenID2",
            "Anmerkungen": "TestAnmerkungen",
            "Aktiv": True,
            "WebHook-Trigger": True,
            "Zahl": 12,
        },
    ]

    # Add the data
    row_ids, _ = baserow_api.add_data_batch(table_id, my_data, user_field_names=True)
    print("rows: ", row_ids)

    data = baserow_api.get_data(table_id, writable_only=False)
    keys = data.keys()
    for entry in my_data:
        assert entry["Medgen ID"] in [
            data[key]["Medgen ID"] for key in keys
        ], f"Data is {data}"

    # Check also with row_ids
    for row_id in row_ids:
        entry = baserow_api.get_entry(table_id, row_id, user_field_names=True)
        assert entry["Medgen ID"] in [
            entry["Medgen ID"] for entry in my_data
        ], f"Entry is {entry}"

    # Delete the rows again
    for row_id in row_ids:
        baserow_api._delete_row(table_id, row_id)
        # Check if the row was deleted
        try:
            entry = baserow_api.get_entry(table_id, row_id)
        except HTTPError as e:
            assert e.response.status_code == 404, f"Entry is {entry}"


def test_add_data_batch_new_and_update(baserow_api):
    table_id = 1050

    my_original_data = {
        "Medgen ID": "EintragTest",
        "Anmerkungen": "TestAnmerkungen_original",
        "Aktiv": True,
        "WebHook-Trigger": True,
        "Zahl": 12,
    }

    # Batch of 2 rows
    my_update_data = [
        {
            "Medgen ID": "EintragTest",
            "Anmerkungen": "TestAnmerkungen_new",
            "Aktiv": True,
            "WebHook-Trigger": True,
            "Zahl": 12,
        },
        {
            "Medgen ID": "Test-MedgenID2",
            "Anmerkungen": "TestAnmerkungen",
            "Aktiv": True,
            "WebHook-Trigger": True,
            "Zahl": 13,
        },
    ]

    # Add one entry
    row_id = baserow_api.add_data(table_id, my_original_data, user_field_names=True)
    entry = baserow_api.get_entry(table_id, row_id)
    assert entry["Medgen ID"] == my_original_data["Medgen ID"], f"Entry is {entry}"
    assert entry["Anmerkungen"] == my_original_data["Anmerkungen"], f"Entry is {entry}"

    # Add the update data
    row_ids, _ = baserow_api.add_data_batch(
        table_id, my_update_data, user_field_names=True
    )

    # Check if the row was updated
    for k, row_id in enumerate(row_ids):
        entry = baserow_api.get_entry(table_id, row_id, user_field_names=True)
        assert entry["Medgen ID"] == my_update_data[k]["Medgen ID"], f"Entry is {entry}"
        assert (
            entry["Anmerkungen"] == my_update_data[k]["Anmerkungen"]
        ), f"Entry is {entry}"

    # Delete the rows again
    for row_id in row_ids:
        baserow_api._delete_row(table_id, row_id)
        # Check if the row was deleted
        try:
            entry = baserow_api.get_entry(table_id, row_id)
        except HTTPError as e:
            assert e.response.status_code == 404, f"Entry is {entry}"


def test_add_data_batch_with_fail(baserow_api):
    row_ids = []
    table_id = 1050

    new_data = [
        # No formula
        {
            "Medgen ID": "EintragTest",
            "Anmerkungen": "TestAnmerkungen_original",
            "Aktiv": True,
            "WebHook-Trigger": True,
            "Zahl": 12,
        },
        # With formula
        {
            "Medgen ID": "EintragTest",
            "Anmerkungen": "TestAnmerkungen_original",
            "Aktiv": True,
            "WebHook-Trigger": True,
            "Zahl": 12,
            "Formel": "TestFormel",
        },
    ]

    # Add one entry
    row_id = baserow_api.add_data_batch(
        table_id, [new_data[0]], user_field_names=True, fail_on_error=False
    )[0][0]
    entry = baserow_api.get_entry(table_id, row_id)
    row_ids.append(row_id)

    with pytest.raises(Exception) as e_info:
        # Add the same data again
        row_ids.append(
            baserow_api.add_data(
                table_id, [new_data[1]], user_field_names=True, fail_on_error=True
            )[0][0]
        )
        print(e_info)

    # Delete the rows again
    for row_id in row_ids:
        baserow_api._delete_row(table_id, row_id)
        # Check if the row was deleted
        try:
            entry = baserow_api.get_entry(table_id, row_id)
        except HTTPError as e:
            assert e.response.status_code == 404, f"Entry is {entry}"
