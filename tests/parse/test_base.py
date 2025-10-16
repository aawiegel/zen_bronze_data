import pytest
import tempfile
import os

from src.parse.base import CSVTableParser


def test_clean_columns_removes_none_columns():
    parser = CSVTableParser()
    input_data = [
        [None, "A", "B", None],
        [None, 1, 2, None],
        [None, 3, 4, None],
    ]

    output = parser.clean_columns(input_data)
    assert output == [
        ["A", "B"],
        [1, 2],
        [3, 4],
    ]


def test_dedupe_columns_appends_suffixes():
    parser = CSVTableParser()
    input_columns = ["foo", "bar", "bar", "bar", "foo"]
    result = parser._dedupe_columns(input_columns)
    assert result == ["foo", "bar", "bar_1", "bar_2", "foo_1"]


def test_remove_header_finds_first_valid_row():
    parser = CSVTableParser()
    input_data = [
        [None, None, None],
        [None, None, "header"],
        ["A", "B", "C"],
        [1, 2, 3],
    ]

    result = parser.remove_header(input_data, min_found=3)
    assert result == [
        ["A", "B", "C"],
        [1, 2, 3],
    ]


def test_remove_header_raises_if_none_found():
    parser = CSVTableParser()
    data = [
        [None, None],
        [None, "Header"],
    ]

    with pytest.raises(ValueError, match="Could not find header row"):
        parser.remove_header(data, min_found=2)


def test_unpivot_returns_expected_long_format():
    parser = CSVTableParser()
    input_data = [
        ["A", "B"],
        [1, 2],
        [3, 4],
    ]

    result = parser.unpivot(input_data)
    assert result == [
        {
            "row_index": 1,
            "column_index": 1,
            "lab_provided_attribute": "A",
            "lab_provided_value": 1,
        },
        {
            "row_index": 1,
            "column_index": 2,
            "lab_provided_attribute": "B",
            "lab_provided_value": 2,
        },
        {
            "row_index": 2,
            "column_index": 1,
            "lab_provided_attribute": "A",
            "lab_provided_value": 3,
        },
        {
            "row_index": 2,
            "column_index": 2,
            "lab_provided_attribute": "B",
            "lab_provided_value": 4,
        },
    ]


def test_parse_runs_end_to_end():
    """Test full parse workflow with a CSV file"""
    parser = CSVTableParser({"header_detection_threshold": 2})

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(",,Header\n")
        f.write(",A,B\n")
        f.write(",1,2\n")
        f.write(",3,4\n")
        temp_path = f.name

    try:
        result = parser.parse(temp_path)

        assert result == [
            {
                "row_index": 1,
                "column_index": 1,
                "lab_provided_attribute": "A",
                "lab_provided_value": "1",
            },
            {
                "row_index": 1,
                "column_index": 2,
                "lab_provided_attribute": "B",
                "lab_provided_value": "2",
            },
            {
                "row_index": 2,
                "column_index": 1,
                "lab_provided_attribute": "A",
                "lab_provided_value": "3",
            },
            {
                "row_index": 2,
                "column_index": 2,
                "lab_provided_attribute": "B",
                "lab_provided_value": "4",
            },
        ]
    finally:
        os.unlink(temp_path)
