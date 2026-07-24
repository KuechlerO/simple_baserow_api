"""Integration tests against an ephemeral baserow.io database."""

import warnings

import pytest
from requests.exceptions import HTTPError

from simple_baserow_api import NAME


def test_base():
    assert NAME == "simple_baserow_api"


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
def test_get_fields(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    fields = baserow_api.get_fields(table_id)
    fields_names = [field["name"] for field in fields]
    field_types = [field["type"] for field in fields]

    assert "Sample-ID" in fields_names, f"Fields are {fields}"
    assert "SnakeSplice-Condition-Group" in fields_names, f"Fields are {fields}"
    assert "Number of Reads in Million (FASTQ)" in fields_names, f"Fields are {fields}"
    assert "text" in field_types, f"Fields are {fields}"
    assert "single_select" in field_types, f"Fields are {fields}"
    assert "number" in field_types, f"Fields are {fields}"


def test_get_writable_fields(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    fields = baserow_api.get_writable_fields(table_id)
    fields_names = [field["name"] for field in fields]
    field_types = [field["type"] for field in fields]

    assert "Sample-ID" in fields_names, f"Fields are {fields}"
    assert "SnakeSplice-Condition-Group" in fields_names, f"Fields are {fields}"
    assert "Number of Reads in Million (FASTQ)" in fields_names, f"Fields are {fields}"
    assert "text" in field_types, f"Fields are {fields}"
    assert "single_select" in field_types, f"Fields are {fields}"
    assert "number" in field_types, f"Fields are {fields}"
    assert all(not field["read_only"] for field in fields), f"Fields are {fields}"


def test_get_data_writable_samples(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    seed_id = test_tables["seed"]["samples_row_id"]

    data = baserow_api.get_data(table_id, writable_only=True)
    assert seed_id in data, f"Data is {data}"
    assert len(data) > 0, f"Data is {data}"
    assert "Sample-ID" in data[seed_id].keys(), f"Data is {data}"
    assert "76660_Ctr_BUD13" in data[seed_id]["Sample-ID"], f"Data is {data}"
    assert "id" not in data[seed_id].keys(), f"Data is {data}"


def test_get_data_writable_findings(baserow_api, test_tables):
    table_id = test_tables["findings"]["id"]
    seed_id = test_tables["seed"]["findings_row_id"]

    data = baserow_api.get_data(table_id, writable_only=True)
    assert seed_id in data, f"Data is {data}"
    assert "Genename" in data[seed_id].keys(), f"Data is {data}"
    assert "id" not in data[seed_id].keys(), f"Data is {data}"
    # formula field is read-only
    assert "HGVS" not in data[seed_id].keys(), f"Data is {data}"


def test_get_data_all_fields(baserow_api, test_tables):
    table_id = test_tables["findings"]["id"]
    seed_id = test_tables["seed"]["findings_row_id"]

    data = baserow_api.get_data(table_id, writable_only=False)
    assert seed_id in data, f"Data is {data}"
    assert "Genename" in data[seed_id].keys(), f"Data is {data}"
    assert "id" not in data[seed_id].keys(), f"Data is {data}"
    assert "HGVS" in data[seed_id].keys(), f"Data is {data}"


def test_get_data_no_field_names(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    seed_id = test_tables["seed"]["samples_row_id"]
    sample_id_field = f"field_{test_tables['samples']['field_ids']['Sample-ID']}"

    data = baserow_api.get_data(table_id, writable_only=False, user_field_names=False)
    assert seed_id in data, f"Data is {data}"
    assert "Sample-ID" not in data[seed_id].keys(), f"Data is {data}"
    assert sample_id_field in data[seed_id].keys(), f"Data is {data}"


def test_get_entry(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    entry_id = test_tables["seed"]["samples_row_id"]

    entry = baserow_api.get_entry(table_id, entry_id)
    assert "Sample-ID" in entry.keys(), f"Entry is {entry}"
    assert "id" not in entry.keys(), f"Entry is {entry}"


def test_add_data_add_simple_row(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    my_data = {
        "Medgen ID": "Test-MedgenID",
        "Anmerkungen": "TestAnmerkungen",
        "Aktiv": True,
        "WebHook-Trigger": True,
        "Zahl": 12,
    }
    row_id = baserow_api.add_data(table_id, my_data, row_id=None, user_field_names=True)

    entry = baserow_api.get_entry(table_id, row_id)
    for key, value in my_data.items():
        assert str(entry[key]) == str(value), f"Entry is {entry}"

    baserow_api._delete_row(table_id, row_id)
    with pytest.raises(HTTPError) as exc:
        baserow_api.get_entry(table_id, row_id)
    assert exc.value.response.status_code == 404


def test_add_data_add_row_no_user_fields(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    field_ids = test_tables["writable"]["field_ids"]
    my_data = {
        f"field_{field_ids['Medgen ID']}": "Test-MedgenID",
        f"field_{field_ids['Anmerkungen']}": "TestAnmerkungen",
        f"field_{field_ids['Aktiv']}": True,
        f"field_{field_ids['WebHook-Trigger']}": True,
        f"field_{field_ids['Zahl']}": 12,
    }
    row_id = baserow_api.add_data(
        table_id, my_data, row_id=None, user_field_names=False
    )

    entry = baserow_api.get_entry(table_id, row_id, user_field_names=False)
    for key, value in my_data.items():
        assert str(entry[key]) == str(value), f"Entry is {entry}"

    entry = baserow_api.get_entry(table_id, row_id, user_field_names=True)
    assert entry["Medgen ID"] == my_data[f"field_{field_ids['Medgen ID']}"]
    assert entry["Anmerkungen"] == my_data[f"field_{field_ids['Anmerkungen']}"]
    assert entry["Aktiv"] == my_data[f"field_{field_ids['Aktiv']}"]
    assert entry["WebHook-Trigger"] == my_data[f"field_{field_ids['WebHook-Trigger']}"]
    assert str(entry["Zahl"]) == str(my_data[f"field_{field_ids['Zahl']}"])

    baserow_api._delete_row(table_id, row_id)
    with pytest.raises(HTTPError) as exc:
        baserow_api.get_entry(table_id, row_id)
    assert exc.value.response.status_code == 404


def test_update_existing_row(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    row_id = test_tables["seed"]["writable_row_id"]

    entry = baserow_api.get_entry(table_id, row_id, user_field_names=True)
    assert entry["Medgen ID"] == "Eintrag1", f"Entry is {entry}"

    returned_id = baserow_api.add_data(
        table_id,
        {"Medgen ID": "Eintrag1-geaendert"},
        row_id=row_id,
        user_field_names=True,
    )
    assert returned_id == row_id, f"Returned ID is {returned_id}"

    entry = baserow_api.get_entry(table_id, returned_id, user_field_names=True)
    assert entry["Medgen ID"] == "Eintrag1-geaendert", f"Entry is {entry}"

    baserow_api.add_data(
        table_id, {"Medgen ID": "Eintrag1"}, row_id=row_id, user_field_names=True
    )
    entry = baserow_api.get_entry(table_id, row_id, user_field_names=True)
    assert entry["Medgen ID"] == "Eintrag1", f"Entry is {entry}"


def test_add_data_batch_only_new(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
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

    row_ids, errors = baserow_api.add_data_batch(
        table_id, my_data, user_field_names=True
    )
    assert not errors, f"Errors: {errors}"

    data = baserow_api.get_data(table_id, writable_only=False)
    medgen_values = [data[key]["Medgen ID"] for key in data]
    for entry in my_data:
        assert entry["Medgen ID"] in medgen_values, f"Data is {data}"

    for row_id in row_ids:
        entry = baserow_api.get_entry(table_id, row_id, user_field_names=True)
        assert entry["Medgen ID"] in [e["Medgen ID"] for e in my_data]

    for row_id in row_ids:
        baserow_api._delete_row(table_id, row_id)
        with pytest.raises(HTTPError) as exc:
            baserow_api.get_entry(table_id, row_id)
        assert exc.value.response.status_code == 404


def test_add_data_batch_new_and_update(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]

    my_original_data = {
        "Medgen ID": "EintragTest",
        "Anmerkungen": "TestAnmerkungen_original",
        "Aktiv": True,
        "WebHook-Trigger": True,
        "Zahl": 12,
    }
    row_id = baserow_api.add_data(table_id, my_original_data, user_field_names=True)
    entry = baserow_api.get_entry(table_id, row_id)
    assert entry["Medgen ID"] == my_original_data["Medgen ID"]
    assert entry["Anmerkungen"] == my_original_data["Anmerkungen"]

    my_update_data = [
        {
            "id": row_id,
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
    row_ids, errors = baserow_api.add_data_batch(
        table_id, my_update_data, user_field_names=True
    )
    assert not errors, f"Errors: {errors}"
    assert row_id in row_ids

    updated = baserow_api.get_entry(table_id, row_id, user_field_names=True)
    assert updated["Anmerkungen"] == "TestAnmerkungen_new"

    for touched_id in row_ids:
        baserow_api._delete_row(table_id, touched_id)
        with pytest.raises(HTTPError) as exc:
            baserow_api.get_entry(table_id, touched_id)
        assert exc.value.response.status_code == 404


def test_add_data_batch_with_fail(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    new_data = [
        {
            "Medgen ID": "EintragTest-fail",
            "Anmerkungen": "ok",
            "Aktiv": True,
            "WebHook-Trigger": True,
            "Zahl": 12,
        },
        {
            "Medgen ID": "EintragTest-fail2",
            "Anmerkungen": "bad",
            "Aktiv": True,
            "WebHook-Trigger": True,
            "Zahl": 12,
            "Formel": "TestFormel",  # read-only formula field
        },
    ]

    row_ids, errors = baserow_api.add_data_batch(
        table_id, [new_data[0]], user_field_names=True, fail_on_error=False
    )
    assert row_ids and not errors
    row_id = row_ids[0]

    with pytest.raises(RuntimeError):
        baserow_api.add_data_batch(
            table_id, [new_data[1]], user_field_names=True, fail_on_error=True
        )

    baserow_api._delete_row(table_id, row_id)
    with pytest.raises(HTTPError) as exc:
        baserow_api.get_entry(table_id, row_id)
    assert exc.value.response.status_code == 404


def test_add_data_check_field_compatibility_warn(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    payload = {
        "Medgen ID": "compat-warn",
        "DoesNotExist": "x",
    }
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        row_id = baserow_api.add_data(
            table_id,
            {"Medgen ID": "compat-warn"},
            check_field_compatibility=True,
            fail_on_error=False,
        )
        # Compatible write should not warn
        assert not any("Missing fields" in str(w.message) for w in caught)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        # Unknown field: warn, still attempt write of known fields only would fail
        # at API if we sent DoesNotExist — validate only path:
        baserow_api._validate_field_compatibility(
            table_id, payload, fail_on_error=False
        )
        assert any("Missing fields" in str(w.message) for w in caught)

    baserow_api._delete_row(table_id, row_id)


def test_add_data_check_field_compatibility_fail(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    payload = {"Medgen ID": "compat-fail", "DoesNotExist": "x"}
    with pytest.raises(RuntimeError, match="Missing fields"):
        baserow_api.add_data(
            table_id,
            payload,
            check_field_compatibility=True,
            fail_on_error=True,
        )


def test_add_data_batch_with_check_field_compatibility(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    good = [{"Medgen ID": "batch-compat-ok", "Zahl": 1}]
    bad = [{"Medgen ID": "batch-compat-bad", "DoesNotExist": "x"}]

    row_ids, errors = baserow_api.add_data_batch(
        table_id,
        good,
        check_field_compatibility=True,
        fail_on_error=True,
    )
    assert row_ids and not errors

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        baserow_api.add_data_batch(
            table_id,
            bad,
            check_field_compatibility=True,
            fail_on_error=False,
        )
        assert any("Missing fields" in str(w.message) for w in caught)

    with pytest.raises(RuntimeError, match="Missing fields"):
        baserow_api.add_data_batch(
            table_id,
            bad,
            check_field_compatibility=True,
            fail_on_error=True,
        )

    for row_id in row_ids:
        baserow_api._delete_row(table_id, row_id)


# --------- include / exclude ----------
def test_include_when_loading_all_rows(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    seed_id = test_tables["seed"]["samples_row_id"]
    data = baserow_api.get_data(table_id, user_field_names=True, include=["Sample-ID"])

    assert "Sample-ID" in data[seed_id].keys(), f"Data is {data[seed_id]}"
    assert "SnakeSplice-Condition-Group" not in data[seed_id].keys()
    assert len(data[seed_id].keys()) == 1, f"Data is {data[seed_id]}"


def test_include_when_loading_single_row(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    entry_id = test_tables["seed"]["samples_row_id"]
    entry = baserow_api.get_entry(
        table_id, entry_id, user_field_names=True, include=["Sample-ID"]
    )

    assert "Sample-ID" in entry.keys(), f"Keys are {entry.keys()}"
    assert "SnakeSplice-Condition-Group" not in entry.keys()
    assert len(entry.keys()) == 1, f"Keys are {entry.keys()}"


def test_exclude_when_loading_all_rows(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    seed_id = test_tables["seed"]["samples_row_id"]
    data = baserow_api.get_data(table_id, user_field_names=True, exclude=["Sample-ID"])

    assert "Sample-ID" not in data[seed_id].keys()
    assert "SnakeSplice-Condition-Group" in data[seed_id].keys()


def test_exclude_when_loading_single_row(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    entry_id = test_tables["seed"]["samples_row_id"]
    entry = baserow_api.get_entry(
        table_id, entry_id, user_field_names=True, exclude=["Sample-ID"]
    )

    assert "Sample-ID" not in entry.keys()
    assert "SnakeSplice-Condition-Group" in entry.keys()


# --------- linked rows ----------
def test_get_entry_linked_rows_with_ids(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    entry_id = test_tables["seed"]["samples_row_id"]
    entry = baserow_api.get_entry(
        table_id,
        entry_id,
        user_field_names=True,
        linked=True,
        use_linked_row_ids=True,
    )
    assert "Splice Findings" in entry.keys(), f"Keys are {entry.keys()}"
    linked_samples = entry["Splice Findings"]
    assert isinstance(linked_samples, list) and len(linked_samples) > 0
    first_linked = linked_samples[0]
    assert isinstance(first_linked, dict)
    assert "Genename" in first_linked.keys()


def test_get_entry_linked_rows_without_ids(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    entry_id = test_tables["seed"]["samples_row_id"]
    entry1 = baserow_api.get_entry(
        table_id,
        entry_id,
        user_field_names=True,
        linked=True,
        use_linked_row_ids=False,
    )
    entry2 = baserow_api.get_entry(
        table_id,
        entry_id,
        user_field_names=True,
        linked=True,
        use_linked_row_ids=True,
    )
    # use_linked_row_ids is ignored when linked=True
    assert entry1 == entry2


def test_get_entry_no_linked_rows(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    entry_id = test_tables["seed"]["samples_row_id"]
    entry1 = baserow_api.get_entry(
        table_id,
        entry_id,
        user_field_names=True,
        linked=False,
        use_linked_row_ids=False,
    )
    entry2 = baserow_api.get_entry(
        table_id,
        entry_id,
        user_field_names=True,
        linked=False,
        use_linked_row_ids=True,
    )
    assert entry1 != entry2
    linked_field1 = entry1["Splice Findings"]
    linked_field2 = entry2["Splice Findings"]
    assert isinstance(linked_field1[0], str)
    assert isinstance(linked_field2[0], int)


def test_get_data_linked_rows_with_ids(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    data = baserow_api.get_data(
        table_id,
        user_field_names=True,
        use_linked_row_ids=True,
    )
    first_entry = data[next(iter(data))]
    linked_samples = first_entry["Splice Findings"]
    assert isinstance(linked_samples, list) and len(linked_samples) > 0
    for entry in linked_samples:
        assert isinstance(entry, int)


def test_get_data_linked_rows_without_ids(baserow_api, test_tables):
    table_id = test_tables["samples"]["id"]
    data = baserow_api.get_data(
        table_id,
        user_field_names=True,
        use_linked_row_ids=False,
    )
    first_entry = data[next(iter(data))]
    linked_samples = first_entry["Splice Findings"]
    assert isinstance(linked_samples, list) and len(linked_samples) > 0
    for entry in linked_samples:
        assert isinstance(entry, str)


# --------- find_entries / synchronize_data ----------
def test_find_entries_by_column(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    seed_id = test_tables["seed"]["writable_row_id"]

    matches = baserow_api.find_entries(table_id, "Medgen ID", "Eintrag1")
    assert seed_id in matches
    assert matches[seed_id]["Medgen ID"] == "Eintrag1"

    none_found = baserow_api.find_entries(table_id, "Medgen ID", "does-not-exist")
    assert none_found == {}


def test_find_entries_unknown_column(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    with pytest.raises(RuntimeError, match="Unknown column"):
        baserow_api.find_entries(table_id, "NoSuchColumn", "x")


def test_synchronize_data_create_and_update(baserow_api, test_tables):
    database_id = test_tables["database_id"]
    source_table_id = test_tables["writable"]["id"]

    # Target mirrors writable (without formula) for a clean sync.
    target = baserow_api.create_table(
        database_id,
        "writable_sync_target",
        primary_field_name="Medgen ID",
        fields=[
            {"name": "Anmerkungen", "type": "text"},
            {"name": "Aktiv", "type": "boolean"},
            {"name": "WebHook-Trigger", "type": "boolean"},
            {
                "name": "Zahl",
                "type": "number",
                "number_decimal_places": 0,
                "number_negative": True,
            },
        ],
    )
    target_id = target["id"]

    source_row_id = baserow_api.add_data(
        source_table_id,
        {
            "Medgen ID": "Sync-Create-1",
            "Anmerkungen": "first",
            "Aktiv": True,
            "WebHook-Trigger": False,
            "Zahl": 7,
        },
    )

    target_row_id, skipped = baserow_api.synchronize_data(
        source_table_id,
        source_row_id,
        target_id,
        identifier_column="Medgen ID",
        fail_on_error=False,
    )
    # Formel is source-only / read-only — may appear in skip list if mapped by name
    # (it exists only on source). Same-name copy only considers source entry keys
    # that exist on target, so Formel should not be in payload; no skip required.
    entry = baserow_api.get_entry(target_id, target_row_id)
    assert entry["Medgen ID"] == "Sync-Create-1"
    assert entry["Anmerkungen"] == "first"
    assert float(entry["Zahl"]) == 7.0

    # Update source, sync again → same target row updated
    baserow_api.add_data(
        source_table_id,
        {"Anmerkungen": "second", "Zahl": 9},
        row_id=source_row_id,
    )
    target_row_id_2, _ = baserow_api.synchronize_data(
        source_table_id,
        source_row_id,
        target_id,
        identifier_column="Medgen ID",
    )
    assert target_row_id_2 == target_row_id
    entry = baserow_api.get_entry(target_id, target_row_id)
    assert entry["Anmerkungen"] == "second"
    assert float(entry["Zahl"]) == 9.0

    baserow_api._delete_row(source_table_id, source_row_id)
    baserow_api.delete_table(target_id)


def test_synchronize_data_with_links(baserow_api, test_tables):
    database_id = test_tables["database_id"]
    source_samples = test_tables["samples"]["id"]

    # Mirror findings + samples on the target side.
    target_findings = baserow_api.create_table(
        database_id,
        "findings_sync_target",
        primary_field_name="Genename",
    )
    target_samples = baserow_api.create_table(
        database_id,
        "samples_sync_target",
        primary_field_name="Sample-ID",
        fields=[
            {
                "name": "SnakeSplice-Condition-Group",
                "type": "single_select",
                "select_options": [
                    {"value": "Group-A", "color": "blue"},
                    {"value": "Group-B", "color": "green"},
                ],
            },
            {
                "name": "Number of Reads in Million (FASTQ)",
                "type": "number",
                "number_decimal_places": 2,
                "number_negative": False,
            },
            {
                "name": "Splice Findings",
                "type": "link_row",
                "link_row_table_id": target_findings["id"],
                "has_related_field": False,
            },
        ],
    )

    # Matching linked entry must exist on the target findings table.
    baserow_api.add_data(target_findings["id"], {"Genename": "GENE1"})

    source_row_id = test_tables["seed"]["samples_row_id"]
    target_row_id, skipped = baserow_api.synchronize_data(
        source_samples,
        source_row_id,
        target_samples["id"],
        identifier_column="Sample-ID",
        fail_on_error=True,
    )
    assert not any("Cannot transfer" in s for s in skipped)

    entry = baserow_api.get_entry(
        target_samples["id"],
        target_row_id,
        use_linked_row_ids=False,
    )
    assert entry["Sample-ID"] == "76660_Ctr_BUD13"
    assert entry["SnakeSplice-Condition-Group"] == "Group-A"
    assert "GENE1" in entry["Splice Findings"]

    baserow_api.delete_table(target_samples["id"])
    baserow_api.delete_table(target_findings["id"])


def test_synchronize_data_fail_on_untransferable(baserow_api, test_tables):
    database_id = test_tables["database_id"]
    source_table_id = test_tables["writable"]["id"]

    # Target lacks Anmerkungen and has a differently typed Zahl → skips / fail
    target = baserow_api.create_table(
        database_id,
        "sync_partial_target",
        primary_field_name="Medgen ID",
        fields=[
            {"name": "Aktiv", "type": "boolean"},
            # text instead of number → type mismatch for Zahl
            {"name": "Zahl", "type": "text"},
        ],
    )

    source_row_id = baserow_api.add_data(
        source_table_id,
        {
            "Medgen ID": "Sync-Fail-1",
            "Anmerkungen": "will-not-copy",
            "Aktiv": True,
            "Zahl": 3,
        },
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        target_row_id, skipped = baserow_api.synchronize_data(
            source_table_id,
            source_row_id,
            target["id"],
            identifier_column="Medgen ID",
            fail_on_error=False,
        )
        assert skipped
        assert any("Anmerkungen" in s or "Zahl" in s for s in skipped)
        assert any("Cannot transfer" in str(w.message) for w in caught)

    entry = baserow_api.get_entry(target["id"], target_row_id)
    assert entry["Medgen ID"] == "Sync-Fail-1"
    assert entry["Aktiv"] is True
    assert "Anmerkungen" not in entry or entry.get("Anmerkungen") in (None, "")

    with pytest.raises(RuntimeError, match="could not transfer"):
        baserow_api.synchronize_data(
            source_table_id,
            source_row_id,
            target["id"],
            identifier_column="Medgen ID",
            fail_on_error=True,
        )

    baserow_api._delete_row(source_table_id, source_row_id)
    baserow_api.delete_table(target["id"])


def test_synchronize_data_missing_link_match(baserow_api, test_tables):
    database_id = test_tables["database_id"]
    source_samples = test_tables["samples"]["id"]

    target_findings = baserow_api.create_table(
        database_id,
        "findings_sync_nomatch",
        primary_field_name="Genename",
    )
    # Intentionally do NOT create GENE1 on the target findings table.
    target_samples = baserow_api.create_table(
        database_id,
        "samples_sync_nomatch",
        primary_field_name="Sample-ID",
        fields=[
            {
                "name": "Splice Findings",
                "type": "link_row",
                "link_row_table_id": target_findings["id"],
                "has_related_field": False,
            },
        ],
    )

    source_row_id = test_tables["seed"]["samples_row_id"]
    with pytest.raises(RuntimeError, match="no match"):
        baserow_api.synchronize_data(
            source_samples,
            source_row_id,
            target_samples["id"],
            identifier_column="Sample-ID",
            fail_on_error=True,
        )

    baserow_api.delete_table(target_samples["id"])
    baserow_api.delete_table(target_findings["id"])


def test_update_row_does_not_mutate_caller_payload(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    row_id = baserow_api.add_data(
        table_id, {"Medgen ID": "mutate-check", "Anmerkungen": "a"}
    )
    payload = {"id": row_id, "Anmerkungen": "b"}
    baserow_api._update_row(table_id, payload, user_field_names=True)
    assert "id" in payload and payload["id"] == row_id
    assert payload["Anmerkungen"] == "b"
    baserow_api._delete_row(table_id, row_id)


def test_find_entries_by_field_id(baserow_api, test_tables):
    table_id = test_tables["writable"]["id"]
    seed_id = test_tables["seed"]["writable_row_id"]
    field_key = f"field_{test_tables['writable']['field_ids']['Medgen ID']}"
    matches = baserow_api.find_entries(table_id, field_key, "Eintrag1")
    assert seed_id in matches


def test_synchronize_data_with_field_mapping(baserow_api, test_tables):
    database_id = test_tables["database_id"]
    source_id = test_tables["writable"]["id"]
    target = baserow_api.create_table(
        database_id,
        "sync_mapped_target",
        primary_field_name="External ID",
        fields=[{"name": "Notes", "type": "text"}],
    )
    source_row = baserow_api.add_data(
        source_id,
        {"Medgen ID": "Map-1", "Anmerkungen": "mapped-note"},
    )
    target_row, skipped = baserow_api.synchronize_data(
        source_id,
        source_row,
        target["id"],
        identifier_column="Medgen ID",
        field_mapping={
            "Medgen ID": "External ID",
            "Anmerkungen": "Notes",
        },
        fail_on_error=True,
    )
    entry = baserow_api.get_entry(target["id"], target_row)
    assert entry["External ID"] == "Map-1"
    assert entry["Notes"] == "mapped-note"
    assert not any("Cannot transfer" in s for s in skipped)
    baserow_api._delete_row(source_id, source_row)
    baserow_api.delete_table(target["id"])


def test_synchronize_data_empty_identifier_raises(baserow_api, test_tables):
    database_id = test_tables["database_id"]
    source_id = test_tables["writable"]["id"]
    target = baserow_api.create_table(
        database_id,
        "sync_empty_id_target",
        primary_field_name="Medgen ID",
    )
    source_row = baserow_api.add_data(source_id, {"Medgen ID": ""})
    with pytest.raises(RuntimeError, match="empty"):
        baserow_api.synchronize_data(
            source_id,
            source_row,
            target["id"],
            identifier_column="Medgen ID",
        )
    baserow_api._delete_row(source_id, source_row)
    baserow_api.delete_table(target["id"])


def test_get_entry_linked_with_field_ids(baserow_api, test_tables):
    """Linked hydration must work when user_field_names=False."""
    table_id = test_tables["samples"]["id"]
    entry_id = test_tables["seed"]["samples_row_id"]
    entry = baserow_api.get_entry(
        table_id,
        entry_id,
        linked=True,
        user_field_names=False,
    )
    link_key = f"field_{test_tables['samples']['field_ids']['Splice Findings']}"
    assert link_key in entry
    assert isinstance(entry[link_key], list) and entry[link_key]
    assert isinstance(entry[link_key][0], dict)


def test_synchronize_ambiguous_link_notice_does_not_fail(baserow_api, test_tables):
    """Ambiguous link matches are soft notices; fail_on_error still succeeds."""
    database_id = test_tables["database_id"]
    source_samples = test_tables["samples"]["id"]

    target_findings = baserow_api.create_table(
        database_id,
        "findings_ambiguous",
        primary_field_name="Genename",
    )
    baserow_api.add_data(target_findings["id"], {"Genename": "GENE1"})
    baserow_api.add_data(target_findings["id"], {"Genename": "GENE1"})

    target_samples = baserow_api.create_table(
        database_id,
        "samples_ambiguous",
        primary_field_name="Sample-ID",
        fields=[
            {
                "name": "Splice Findings",
                "type": "link_row",
                "link_row_table_id": target_findings["id"],
                "has_related_field": False,
            },
        ],
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        target_row, messages = baserow_api.synchronize_data(
            source_samples,
            test_tables["seed"]["samples_row_id"],
            target_samples["id"],
            identifier_column="Sample-ID",
            fail_on_error=True,
        )
        assert target_row
        assert any("Multiple matches" in m for m in messages)
        assert any("Multiple matches" in str(w.message) for w in caught)

    baserow_api.delete_table(target_samples["id"])
    baserow_api.delete_table(target_findings["id"])


def test_synchronize_data_dry_run_does_not_write(baserow_api, test_tables):
    database_id = test_tables["database_id"]
    source_id = test_tables["writable"]["id"]
    target = baserow_api.create_table(
        database_id,
        "sync_dry_run_target",
        primary_field_name="Medgen ID",
        fields=[{"name": "Anmerkungen", "type": "text"}],
    )
    source_row = baserow_api.add_data(
        source_id,
        {"Medgen ID": "Dry-Run-1", "Anmerkungen": "preview"},
    )

    target_row_id, messages = baserow_api.synchronize_data(
        source_id,
        source_row,
        target["id"],
        identifier_column="Medgen ID",
        dry_run=True,
        fail_on_error=False,
    )
    assert target_row_id is None
    assert any("would create" in m for m in messages)
    assert any("dry_run payload fields" in m for m in messages)
    assert baserow_api.find_entries(target["id"], "Medgen ID", "Dry-Run-1") == {}

    real_id, _ = baserow_api.synchronize_data(
        source_id,
        source_row,
        target["id"],
        identifier_column="Medgen ID",
    )
    preview_id, messages2 = baserow_api.synchronize_data(
        source_id,
        source_row,
        target["id"],
        identifier_column="Medgen ID",
        dry_run=True,
    )
    assert preview_id == real_id
    assert any("would update" in m for m in messages2)

    baserow_api._delete_row(source_id, source_row)
    baserow_api.delete_table(target["id"])


def test_synchronize_data_exclude_fields(baserow_api, test_tables):
    database_id = test_tables["database_id"]
    source_id = test_tables["writable"]["id"]
    target = baserow_api.create_table(
        database_id,
        "sync_exclude_target",
        primary_field_name="Medgen ID",
        fields=[
            {"name": "Anmerkungen", "type": "text"},
            {"name": "Aktiv", "type": "boolean"},
        ],
    )
    source_row = baserow_api.add_data(
        source_id,
        {
            "Medgen ID": "Exclude-1",
            "Anmerkungen": "should-skip",
            "Aktiv": True,
        },
    )
    target_row, _ = baserow_api.synchronize_data(
        source_id,
        source_row,
        target["id"],
        identifier_column="Medgen ID",
        exclude_fields=["Anmerkungen"],
        fail_on_error=True,
    )
    entry = baserow_api.get_entry(target["id"], target_row)
    assert entry["Aktiv"] is True
    assert entry.get("Anmerkungen") in (None, "")

    baserow_api._delete_row(source_id, source_row)
    baserow_api.delete_table(target["id"])
