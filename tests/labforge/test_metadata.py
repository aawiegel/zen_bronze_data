import pandas as pd
import os
import tempfile
from src.labforge import metadata


def test_generate_surrogate_key_single_value():
    """Test surrogate key generation with single value"""
    key = metadata.generate_surrogate_key("ph")
    assert isinstance(key, str)
    assert len(key) == 8
    # Should be deterministic
    assert metadata.generate_surrogate_key("ph") == key


def test_generate_surrogate_key_multiple_values():
    """Test surrogate key generation with multiple values"""
    key1 = metadata.generate_surrogate_key("vendor_a", "ph")
    key2 = metadata.generate_surrogate_key("vendor_b", "ph")

    assert isinstance(key1, str)
    assert isinstance(key2, str)
    assert len(key1) == 8
    assert len(key2) == 8
    # Different combinations should produce different keys
    assert key1 != key2


def test_analyte_dimension_structure():
    """Test that analyte dimension has correct structure"""
    assert isinstance(metadata.ANALYTE_DIMENSION, list)
    assert len(metadata.ANALYTE_DIMENSION) > 0

    # Check first analyte has all required fields
    first_analyte = metadata.ANALYTE_DIMENSION[0]
    required_fields = [
        "analyte_id",
        "analyte_name",
        "unit",
        "data_type",
        "min_valid_value",
        "max_valid_value",
        "description",
    ]
    for field in required_fields:
        assert field in first_analyte


def test_vendor_analyte_mapping_structure():
    """Test that vendor-analyte mapping has correct structure"""
    assert isinstance(metadata.VENDOR_ANALYTE_MAPPING, list)
    assert len(metadata.VENDOR_ANALYTE_MAPPING) > 0

    # Check first mapping has all required fields
    first_mapping = metadata.VENDOR_ANALYTE_MAPPING[0]
    required_fields = [
        "vendor_analyte_mapping_id",
        "vendor_id",
        "vendor_column_name",
        "analyte_id",
        "notes",
    ]
    for field in required_fields:
        assert field in first_mapping


def test_generate_analyte_dimension_csv():
    """Test generating analyte dimension CSV"""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "analyte_dimension.csv")
        df = metadata.generate_analyte_dimension_csv(output_path)

        # Check DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(metadata.ANALYTE_DIMENSION)
        assert "analyte_id" in df.columns
        assert "analyte_name" in df.columns

        # Check file was created
        assert os.path.exists(output_path)

        # Read back and verify
        df_read = pd.read_csv(output_path)
        assert len(df_read) == len(df)


def test_generate_vendor_analyte_mapping_csv():
    """Test generating vendor-analyte mapping CSV"""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "vendor_analyte_mapping.csv")
        df = metadata.generate_vendor_analyte_mapping_csv(output_path)

        # Check DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(metadata.VENDOR_ANALYTE_MAPPING)
        assert "vendor_analyte_mapping_id" in df.columns
        assert "vendor_id" in df.columns
        assert "vendor_column_name" in df.columns
        assert "analyte_id" in df.columns

        # Check file was created
        assert os.path.exists(output_path)

        # Read back and verify
        df_read = pd.read_csv(output_path)
        assert len(df_read) == len(df)


def test_all_vendor_columns_mapped():
    """Test that all vendor columns have mapping entries"""
    # This ensures we didn't miss any columns when creating the mapping
    vendor_a_columns = set()
    vendor_b_columns = set()

    for mapping in metadata.VENDOR_ANALYTE_MAPPING:
        if mapping["vendor_id"] == "vendor_a":
            vendor_a_columns.add(mapping["vendor_column_name"])
        elif mapping["vendor_id"] == "vendor_b":
            vendor_b_columns.add(mapping["vendor_column_name"])

    # Vendor A should have 10 measurement columns
    assert len(vendor_a_columns) == 10
    # Vendor B should have 10 measurement columns
    assert len(vendor_b_columns) == 10


def test_surrogate_key_uniqueness():
    """Test that all analyte IDs are unique"""
    analyte_ids = [a["analyte_id"] for a in metadata.ANALYTE_DIMENSION]
    assert len(analyte_ids) == len(set(analyte_ids))


def test_mapping_surrogate_key_uniqueness():
    """Test that all mapping IDs are unique"""
    mapping_ids = [
        m["vendor_analyte_mapping_id"] for m in metadata.VENDOR_ANALYTE_MAPPING
    ]
    assert len(mapping_ids) == len(set(mapping_ids))
