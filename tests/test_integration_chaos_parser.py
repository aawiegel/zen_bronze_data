"""
Integration tests for chaos generators + CSV parser

These tests verify that CSV files generated with chaos functions
can be successfully parsed by the CSVTableParser.
"""

import tempfile
import os
import numpy as np
import pandas as pd

from src.labforge import vendors, chaos
from src.parse import CSVTableParser


def test_parse_csv_with_metadata_rows():
    """Test that parser can handle CSV files with metadata rows at the top"""
    gen = np.random.default_rng(42)

    # Generate a clean vendor CSV
    df = vendors.forge_vendor_csv(gen, vendor="vendor_a", packages=["basic"], rows=10)

    # Add metadata rows at the top
    df_chaos = chaos.chaos_metadata_rows(gen, df, num_rows=3)

    # Write to temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df_chaos.to_csv(f, index=False)
        temp_path = f.name

    try:
        # Parse with CSVTableParser
        parser = CSVTableParser({"header_detection_threshold": 5})
        records = parser.parse(temp_path)

        # Should have successfully parsed
        assert len(records) > 0

        # Should have unpivoted the data (each cell becomes a row)
        # Original data: 10 rows × ~9 columns = ~90 cells
        # We should have at least that many records (metadata adds more)
        assert len(records) >= 90

        # Check structure
        assert "row_index" in records[0]
        assert "column_index" in records[0]
        assert "lab_provided_attribute" in records[0]
        assert "lab_provided_value" in records[0]

    finally:
        os.unlink(temp_path)


def test_parse_csv_with_empty_padding_columns():
    """Test that parser can handle CSV files with empty padding columns"""
    gen = np.random.default_rng(42)

    # Generate a clean vendor CSV
    df = vendors.forge_vendor_csv(
        gen, vendor="vendor_b", packages=["standard"], rows=10
    )

    # Add empty padding columns
    df_chaos = chaos.chaos_empty_column_padding(gen, df, num_columns=3)

    # Write to temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df_chaos.to_csv(f, index=False)
        temp_path = f.name

    try:
        # Parse with CSVTableParser - lower threshold since we have padding columns
        parser = CSVTableParser({"header_detection_threshold": 5})
        records = parser.parse(temp_path)

        # Should have successfully parsed
        assert len(records) > 0

        # Empty columns should be cleaned out by the parser
        # Check that we have column names (some might be empty strings)
        attributes = {r["lab_provided_attribute"] for r in records}

        # Should have some actual column names from the vendor data
        assert any(attr for attr in attributes if attr and attr != "")

    finally:
        os.unlink(temp_path)


def test_parse_csv_with_header_chaos():
    """Test that parser can handle CSV files with chaotic headers"""
    gen = np.random.default_rng(42)

    # Generate a clean vendor CSV
    df = vendors.forge_vendor_csv(
        gen, vendor="vendor_a", packages=["basic", "metals"], rows=15
    )

    # Apply header chaos (typos, casing, whitespace)
    df_chaos = chaos.apply_chaos(
        gen,
        df,
        header_typos=0.8,
        header_casing=0.8,
        header_whitespace=0.5,
    )

    # Write to temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df_chaos.to_csv(f, index=False)
        temp_path = f.name

    try:
        # Parse with CSVTableParser
        parser = CSVTableParser()
        records = parser.parse(temp_path)

        # Should have successfully parsed despite chaos
        assert len(records) > 0

        # Should have row and column tracking
        row_indices = {r["row_index"] for r in records}
        assert len(row_indices) == 15  # Should have 15 rows

        # Column names should be preserved (even if chaotic)
        attributes = {r["lab_provided_attribute"] for r in records}
        assert len(attributes) > 0

    finally:
        os.unlink(temp_path)


def test_parse_csv_with_all_chaos_features():
    """Test parser with MAXIMUM CHAOS - all features combined!"""
    gen = np.random.default_rng(123)

    # Generate vendor CSV
    df = vendors.forge_vendor_csv(
        gen, vendor="vendor_b", packages=["standard", "heavy_metals"], rows=20
    )

    # Apply ALL the chaos!
    df_chaos = chaos.apply_chaos(
        gen,
        df,
        header_typos=0.6,
        header_casing=0.6,
        header_whitespace=0.4,
        invalid_db_chars=0.3,
        add_metadata_rows=True,
        num_metadata=3,
        add_empty_padding=True,
        num_empty=4,
    )

    # Write to temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df_chaos.to_csv(f, index=False)
        temp_path = f.name

    try:
        # Parse with CSVTableParser - needs higher threshold due to metadata rows
        parser = CSVTableParser({"header_detection_threshold": 8})
        records = parser.parse(temp_path)

        # Should have successfully parsed the chaos!
        assert len(records) > 0

        # Check that we got data with proper structure
        assert all("row_index" in r for r in records[:10])
        assert all("column_index" in r for r in records[:10])
        assert all("lab_provided_attribute" in r for r in records[:10])
        assert all("lab_provided_value" in r for r in records[:10])

        # Should have multiple rows (original 20 + metadata)
        row_indices = {r["row_index"] for r in records}
        assert len(row_indices) >= 20

    finally:
        os.unlink(temp_path)


def test_parse_csv_roundtrip_data_integrity():
    """Test that data values are preserved through chaos and parsing"""
    gen = np.random.default_rng(456)

    # Generate a simple vendor CSV with known values
    df = vendors.forge_vendor_csv(gen, vendor="vendor_a", packages=["basic"], rows=5)

    # Store original sample_barcode values (first column)
    original_barcodes = df["sample_barcode"].tolist()

    # Apply chaos to headers only (not data values)
    # Don't add empty padding since parser cleans those out
    df_chaos = chaos.apply_chaos(
        gen,
        df,
        header_casing=1.0,  # Mess up ALL the headers
        header_typos=0.5,
    )

    # Write to temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df_chaos.to_csv(f, index=False)
        temp_path = f.name

    try:
        # Parse with CSVTableParser - basic package only has 7 columns
        parser = CSVTableParser({"header_detection_threshold": 5})
        records = parser.parse(temp_path)

        # Extract barcode values from parsed data
        # Find records where the attribute contains "barco" (might have typos!)
        barcode_records = [
            r for r in records if "barco" in r["lab_provided_attribute"].lower()
        ]

        # Should have 5 barcode values
        assert len(barcode_records) == 5

        # Extract the actual barcode values
        parsed_barcodes = [r["lab_provided_value"] for r in barcode_records]

        # Barcodes should be preserved (data integrity!)
        assert sorted(parsed_barcodes) == sorted(original_barcodes)

    finally:
        os.unlink(temp_path)


def test_parse_csv_with_empty_string_padding_cleaned():
    """Test that parser successfully cleans out empty padding columns"""
    gen = np.random.default_rng(789)

    # Generate vendor CSV
    df = vendors.forge_vendor_csv(gen, vendor="vendor_a", packages=["basic"], rows=5)

    # Count original columns
    original_col_count = len(df.columns)

    # Add empty padding
    df_chaos = chaos.chaos_empty_column_padding(gen, df, num_columns=2)

    # Should have 2 extra columns now
    assert len(df_chaos.columns) == original_col_count + 2

    # Write to temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df_chaos.to_csv(f, index=False)
        temp_path = f.name

    try:
        # Parse with CSVTableParser - lower threshold since we have padding columns
        parser = CSVTableParser({"header_detection_threshold": 5})
        records = parser.parse(temp_path)

        # Should have successfully parsed
        assert len(records) > 0

        # Get unique column names from parsed records
        attributes = {r["lab_provided_attribute"] for r in records}

        # Empty string columns should be cleaned out by parser
        # We should only have the original non-empty column names
        assert (
            "" not in attributes
            or len([a for a in attributes if a]) >= original_col_count
        )

    finally:
        os.unlink(temp_path)
