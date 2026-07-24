"""Offline unit tests for helpers and edge-case behaviour (no Baserow network)."""

import warnings

import pytest

from simple_baserow_api.base import (
    BaserowApi,
    _check_for_fields_mismatch,
    _format_value,
)


def test_format_value_single_select():
    field = {"type": "single_select"}
    assert _format_value({"value": "active", "id": 1}, field) == "active"
    assert _format_value(None, field) is None
    with pytest.raises(RuntimeError, match="malformed single_select"):
        _format_value("active", field)


def test_format_value_multiple_select():
    field = {"type": "multiple_select"}
    assert _format_value(
        [{"value": "a", "id": 1}, {"value": "b", "id": 2}], field
    ) == ["a", "b"]
    assert _format_value(None, field) is None
    with pytest.raises(RuntimeError, match="malformed multiple_select"):
        _format_value("a", field)


def test_format_value_link_row():
    field = {"type": "link_row"}
    raw = [{"id": 10, "value": "GENE1"}, {"id": 11, "value": "GENE2"}]
    assert _format_value(raw, field, use_link_ids=True) == [10, 11]
    assert _format_value(raw, field, use_link_ids=False) == ["GENE1", "GENE2"]
    assert _format_value(None, field) is None
    assert _format_value([], field, use_link_ids=True) == []
    with pytest.raises(RuntimeError, match="malformed link_row"):
        _format_value({"id": 1}, field)


def test_format_value_passthrough():
    assert _format_value("hello", {"type": "text"}) == "hello"
    assert _format_value(3.5, {"type": "number"}) == 3.5


def test_check_for_fields_mismatch_missing_and_types():
    table_fields = [
        {"name": "A", "type": "text"},
        {"name": "B", "type": "number"},
    ]
    with pytest.raises(RuntimeError, match="Missing fields"):
        _check_for_fields_mismatch([{"name": "C"}], table_fields)

    with pytest.raises(RuntimeError, match="Type mismatches"):
        _check_for_fields_mismatch(
            [{"name": "A", "type": "number"}],
            table_fields,
        )

    # Name-only: no type warning, no type check
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _check_for_fields_mismatch([{"name": "A"}], table_fields)
        assert not any("Type information" in str(w.message) for w in caught)

    # Formula source type is allowed against a different target type
    _check_for_fields_mismatch(
        [{"name": "A", "type": "formula"}],
        table_fields,
    )


def test_check_for_fields_mismatch_partial_types_warns():
    table_fields = [{"name": "A", "type": "text"}]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _check_for_fields_mismatch(
            [{"name": "A", "type": None}],
            table_fields,
        )
        assert any("Type information is missing" in str(w.message) for w in caught)


def test_baserow_api_requires_token():
    with pytest.raises(ValueError, match="token is required"):
        BaserowApi("https://api.baserow.io")


def test_payload_keys_as_fields_resolves_ids_and_ignores_row_id():
    api = BaserowApi("https://example.invalid", token="dummy")
    table_fields = [{"id": 42, "name": "Medgen ID", "type": "text"}]
    result = api._payload_keys_as_fields(
        {"field_42": "x", "id": 7, "Extra": 1},
        table_fields,
    )
    names = {f["name"] for f in result}
    assert names == {"Medgen ID", "Extra"}


def test_payload_keys_as_fields_batch_union():
    api = BaserowApi("https://example.invalid", token="dummy")
    table_fields = [
        {"id": 1, "name": "A", "type": "text"},
        {"id": 2, "name": "B", "type": "text"},
    ]
    result = api._payload_keys_as_fields(
        [{"A": 1}, {"B": 2, "id": 9}],
        table_fields,
    )
    assert [f["name"] for f in result] == ["A", "B"]


def test_convert_selects_with_field_id_keys_and_bad_multiple():
    api = BaserowApi("https://example.invalid", token="dummy")
    fields = [
        {
            "id": 5,
            "name": "status",
            "type": "single_select",
            "read_only": False,
            "select_options": [{"value": "active", "id": 1}],
        },
        {
            "id": 6,
            "name": "tags",
            "type": "multiple_select",
            "read_only": False,
            "select_options": [{"value": "x", "id": 2}],
        },
    ]
    converted = api._convert_selects({"field_5": "active"}, fields)
    assert converted["field_5"] == 1

    with pytest.raises(RuntimeError, match="must be a list"):
        api._convert_selects({"tags": "x"}, fields)


def test_convert_selects_unknown_option():
    api = BaserowApi("https://example.invalid", token="dummy")
    fields = [
        {
            "id": 1,
            "name": "status",
            "type": "single_select",
            "read_only": False,
            "select_options": [{"value": "active", "id": 1}],
        }
    ]
    with pytest.raises(RuntimeError, match="Could not convert"):
        api._convert_selects({"status": "missing"}, fields)


def test_validate_field_compatibility_warn_vs_fail():
    api = BaserowApi("https://example.invalid", token="dummy")
    table_fields = [{"id": 1, "name": "A", "type": "text", "read_only": False}]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        api._validate_field_compatibility(
            table_id=1,
            data={"B": 1},
            fields=table_fields,
            fail_on_error=False,
        )
        assert any("Missing fields" in str(w.message) for w in caught)

    with pytest.raises(RuntimeError, match="Missing fields"):
        api._validate_field_compatibility(
            table_id=1,
            data={"B": 1},
            fields=table_fields,
            fail_on_error=True,
        )


def test_resolve_field_by_name_and_id():
    api = BaserowApi("https://example.invalid", token="dummy")
    fields = [{"id": 9, "name": "Sample-ID", "type": "text"}]
    assert api._resolve_field(1, "Sample-ID", fields=fields)["id"] == 9
    assert api._resolve_field(1, "field_9", fields=fields)["name"] == "Sample-ID"
    with pytest.raises(RuntimeError, match="Unknown column"):
        api._resolve_field(1, "Nope", fields=fields)
    with pytest.raises(RuntimeError, match="Unknown column"):
        api._resolve_field(1, "field_abc", fields=fields)


def test_add_data_batch_empty_entries():
    api = BaserowApi("https://example.invalid", token="dummy")
    assert api.add_data_batch(1, []) == ([], [])
