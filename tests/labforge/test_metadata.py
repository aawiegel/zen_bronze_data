import pandas as pd
import os
import tempfile
from src.labforge import metadata


def test_generate_surrogate_key_single_value():
    """Test surrogate key generation with single value"""
    key = metadata.generate_surrogate_key("col_ph")
    assert isinstance(key, str)
    assert len(key) == 8
    # Should be deterministic
    assert metadata.generate_surrogate_key("col_ph") == key


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


def test_canonical_column_definitions_structure():
    """Test that canonical column definitions has correct structure"""
    assert isinstance(metadata.CANONICAL_COLUMN_DEFINITIONS, list)
    assert len(metadata.CANONICAL_COLUMN_DEFINITIONS) > 0

    # Check first column has all required fields
    first_column = metadata.CANONICAL_COLUMN_DEFINITIONS[0]
    required_fields = [
        "canonical_column_id",
        "canonical_column_name",
        "column_category",
        "data_type",
        "description",
    ]
    for field in required_fields:
        assert field in first_column


def test_vendor_column_mapping_structure():
    """Test that vendor column mapping has correct structure"""
    assert isinstance(metadata.VENDOR_COLUMN_MAPPING, list)
    assert len(metadata.VENDOR_COLUMN_MAPPING) > 0

    # Check first mapping has all required fields
    first_mapping = metadata.VENDOR_COLUMN_MAPPING[0]
    required_fields = [
        "vendor_id",
        "vendor_column_name",
        "canonical_column_id",
        "notes",
    ]
    for field in required_fields:
        assert field in first_mapping


def test_generate_canonical_column_definitions_csv():
    """Test generating canonical column definitions CSV"""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "canonical_column_definitions.csv")
        df = metadata.generate_canonical_column_definitions_csv(output_path)

        # Check DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(metadata.CANONICAL_COLUMN_DEFINITIONS)
        assert "canonical_column_id" in df.columns
        assert "canonical_column_name" in df.columns
        assert "column_category" in df.columns

        # Check file was created
        assert os.path.exists(output_path)

        # Read back and verify
        df_read = pd.read_csv(output_path)
        assert len(df_read) == len(df)


def test_generate_vendor_column_mapping_csv():
    """Test generating vendor column mapping CSV"""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "vendor_column_mapping.csv")
        df = metadata.generate_vendor_column_mapping_csv(output_path)

        # Check DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(metadata.VENDOR_COLUMN_MAPPING)
        assert "vendor_id" in df.columns
        assert "vendor_column_name" in df.columns
        assert "canonical_column_id" in df.columns

        # Check file was created
        assert os.path.exists(output_path)

        # Read back and verify
        df_read = pd.read_csv(output_path)
        assert len(df_read) == len(df)


def test_column_categories_present():
    """Test that we have different column categories"""
    categories = set(
        col["column_category"] for col in metadata.CANONICAL_COLUMN_DEFINITIONS
    )
    # Should have measurements and metadata categories
    assert "measurement" in categories
    assert "sample_identifier" in categories


def test_both_vendors_mapped():
    """Test that both vendors have mappings"""
    vendor_ids = set(m["vendor_id"] for m in metadata.VENDOR_COLUMN_MAPPING)
    assert "vendor_a" in vendor_ids
    assert "vendor_b" in vendor_ids


def test_canonical_column_id_uniqueness():
    """Test that all canonical column IDs are unique"""
    column_ids = [
        c["canonical_column_id"] for c in metadata.CANONICAL_COLUMN_DEFINITIONS
    ]
    assert len(column_ids) == len(set(column_ids))


def test_mapping_references_valid_canonical_ids():
    """Test that all mappings reference valid canonical column IDs"""
    valid_ids = set(
        c["canonical_column_id"] for c in metadata.CANONICAL_COLUMN_DEFINITIONS
    )

    for mapping in metadata.VENDOR_COLUMN_MAPPING:
        assert mapping["canonical_column_id"] in valid_ids, (
            f"Mapping for {mapping['vendor_column_name']} references "
            f"invalid canonical_column_id: {mapping['canonical_column_id']}"
        )


def test_typo_mapping_exists():
    """Test that the typo example (sample_barcod) is mapped"""
    typo_mappings = [
        m
        for m in metadata.VENDOR_COLUMN_MAPPING
        if m["vendor_column_name"] == "sample_barcod"
    ]
    assert len(typo_mappings) == 1
    # Should map to same canonical ID as correct spelling
    correct_mappings = [
        m
        for m in metadata.VENDOR_COLUMN_MAPPING
        if m["vendor_column_name"] == "sample_barcode"
    ]
    assert (
        typo_mappings[0]["canonical_column_id"]
        == correct_mappings[0]["canonical_column_id"]
    )


def test_cross_vendor_analyte_mapping():
    """Test that same analyte maps correctly across vendors"""
    # Get pH mappings for both vendors
    vendor_a_ph = [
        m
        for m in metadata.VENDOR_COLUMN_MAPPING
        if m["vendor_id"] == "vendor_a" and m["vendor_column_name"] == "ph"
    ]
    vendor_b_ph = [
        m
        for m in metadata.VENDOR_COLUMN_MAPPING
        if m["vendor_id"] == "vendor_b" and m["vendor_column_name"] == "acidity"
    ]

    assert len(vendor_a_ph) == 1
    assert len(vendor_b_ph) == 1
    # Both should map to the same canonical column
    assert (
        vendor_a_ph[0]["canonical_column_id"] == vendor_b_ph[0]["canonical_column_id"]
    )
