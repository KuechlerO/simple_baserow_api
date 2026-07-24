"""Pytest fixtures for self-contained Baserow.io integration tests.
Pytest auto-loads tests/conftest.py for any tests under tests/.
It defines shared fixtures (setup/teardown helpers) that tests can request by name.

Creates an ephemeral database and tables against ``https://api.baserow.io``,
then deletes them after the test session.
"""

import os
import sys
import uuid

import pytest

from simple_baserow_api import BaserowApi

# Test account on baserow.io (intentionally non-secret). Env vars override.
BASEROW_URL = os.environ.get("BASEROW_URL", "https://api.baserow.io")
BASEROW_EMAIL = os.environ.get("BASEROW_EMAIL", "ljp52207@laoia.com")
BASEROW_PASSWORD = os.environ.get(
    "BASEROW_PASSWORD",
    "k+A98#%4A@mG3zpk~{-M[p*'CYk}hMX:tT#7Xbu3ZO1",
)


# each test runs on cwd to its temp dir
@pytest.fixture(autouse=True)
def go_to_tmpdir(request):
    # Get the fixture dynamically by its name.
    tmpdir = request.getfixturevalue("tmpdir")
    # ensure local test created packages can be imported
    sys.path.insert(0, str(tmpdir))
    # Chdir only for the duration of the test.
    with tmpdir.as_cwd():
        yield


@pytest.fixture(scope="session")
def baserow_api():
    """JWT-authenticated API client for the shared test account."""
    return BaserowApi.from_credentials(
        BASEROW_URL, BASEROW_EMAIL, BASEROW_PASSWORD
    )


@pytest.fixture(scope="session")
def test_tables(baserow_api):
    """Create ephemeral database + tables; tear down after the session.

    Yields a dict::

        {
            "database_id": int,
            "writable": {"id": int, "fields": list, "field_ids": dict},
            "findings": {"id": int, "fields": list, "field_ids": dict},
            "samples": {"id": int, "fields": list, "field_ids": dict},
            "seed": {
                "writable_row_id": int,
                "findings_row_id": int,
                "samples_row_id": int,
            },
        }
    """
    workspaces = baserow_api.list_workspaces()
    assert workspaces, "Test account has no workspaces"
    workspace_id = workspaces[0]["id"]

    db_name = f"pytest-simple-baserow-api-{uuid.uuid4().hex[:10]}"
    database = baserow_api.create_database(workspace_id, db_name)
    database_id = database["id"]

    try:
        # --- writable table (add/update/batch tests) ---
        writable = baserow_api.create_table(
            database_id,
            "writable",
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
                {
                    "name": "Formel",
                    "type": "formula",
                    "formula": "concat(field('Medgen ID'), '')",
                },
            ],
        )

        # --- findings table (link target + formula read-only field) ---
        findings = baserow_api.create_table(
            database_id,
            "findings",
            primary_field_name="Genename",
            fields=[
                {
                    "name": "HGVS",
                    "type": "formula",
                    "formula": "concat(field('Genename'), '-hgvs')",
                },
            ],
        )

        # --- samples table (metadata, select, number, link_row) ---
        samples = baserow_api.create_table(
            database_id,
            "samples",
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
                    "link_row_table_id": findings["id"],
                    "has_related_field": False,
                },
            ],
        )

        def _field_ids(table_info):
            return {f["name"]: f["id"] for f in table_info["fields"]}

        writable["field_ids"] = _field_ids(writable)
        findings["field_ids"] = _field_ids(findings)
        samples["field_ids"] = _field_ids(samples)

        # Seed rows used by read/update tests
        findings_row_id = baserow_api.add_data(
            findings["id"],
            {"Genename": "GENE1"},
            user_field_names=True,
        )
        samples_row_id = baserow_api.add_data(
            samples["id"],
            {
                "Sample-ID": "76660_Ctr_BUD13",
                "SnakeSplice-Condition-Group": "Group-A",
                "Number of Reads in Million (FASTQ)": 12.5,
                "Splice Findings": [findings_row_id],
            },
            user_field_names=True,
        )
        writable_row_id = baserow_api.add_data(
            writable["id"],
            {
                "Medgen ID": "Eintrag1",
                "Anmerkungen": "seed",
                "Aktiv": True,
                "WebHook-Trigger": False,
                "Zahl": 1,
            },
            user_field_names=True,
        )

        # Delete the empty placeholder rows created with each table
        primary_by_table = {
            writable["id"]: "Medgen ID",
            findings["id"]: "Genename",
            samples["id"]: "Sample-ID",
        }
        keep_ids = {writable_row_id, findings_row_id, samples_row_id}
        for table_id, primary_name in primary_by_table.items():
            rows = baserow_api.get_data(table_id, writable_only=False)
            for row_id, row in rows.items():
                if row_id in keep_ids:
                    continue
                if row.get(primary_name) in ("", None):
                    baserow_api._delete_row(table_id, row_id)

        yield {
            "database_id": database_id,
            "writable": writable,
            "findings": findings,
            "samples": samples,
            "seed": {
                "writable_row_id": writable_row_id,
                "findings_row_id": findings_row_id,
                "samples_row_id": samples_row_id,
            },
        }
    finally:
        baserow_api.delete_database(database_id)
