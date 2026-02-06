"""
Generate metadata tables for the unified column mapping approach.

This module defines canonical column definitions and vendor-specific mappings
for standardizing vendor data in the silver layer. The approach handles ALL
columns uniformly (measurements AND metadata like sample_barcode, lab_id).

Tables:
    - CANONICAL_COLUMN_DEFINITIONS: Defines canonical columns with categories
    - VENDOR_COLUMN_MAPPING: Maps vendor column names to canonical IDs

Note: This is staging-area standardization. Gold layer builds actual star
schema dimensions (analyte dimensions with units/ranges, sample dimensions, etc.)
"""

import hashlib
import pandas as pd


def generate_surrogate_key(*values: str) -> str:
    """
    Generate a surrogate key by hashing one or more string values

    Args:
        *values: One or more strings to concatenate and hash

    Returns:
        8-character hex hash to use as surrogate key

    Example:
        >>> generate_surrogate_key("col_ph")
        'a94a8fe5'
        >>> generate_surrogate_key("vendor_a", "ph")
        'f0e7d6c5'
    """
    combined = "|".join(values)
    return hashlib.sha256(combined.encode()).hexdigest()[:8]


# =============================================================================
# CANONICAL COLUMN DEFINITIONS
# =============================================================================
# Staging area for all column types. Gold layer builds full dimensional models.

CANONICAL_COLUMN_DEFINITIONS = [
    # Measurements (analytes)
    {
        "canonical_column_id": generate_surrogate_key("col_ph"),
        "canonical_column_name": "ph",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Soil pH level",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_copper"),
        "canonical_column_name": "copper_ppm",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Copper concentration in ppm",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_zinc"),
        "canonical_column_name": "zinc_ppm",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Zinc concentration in ppm",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_lead"),
        "canonical_column_name": "lead_ppm",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Lead concentration in ppm",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_iron"),
        "canonical_column_name": "iron_ppm",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Iron concentration in ppm",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_manganese"),
        "canonical_column_name": "manganese_ppm",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Manganese concentration in ppm",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_boron"),
        "canonical_column_name": "boron_ppm",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Boron concentration in ppm",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_molybdenum"),
        "canonical_column_name": "molybdenum_ppm",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Molybdenum concentration in ppm",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_sulfur"),
        "canonical_column_name": "sulfur_ppm",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Sulfur concentration in ppm",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_organic_matter"),
        "canonical_column_name": "organic_matter_pct",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Organic matter percentage",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_ec"),
        "canonical_column_name": "electrical_conductivity",
        "column_category": "measurement",
        "data_type": "numeric",
        "description": "Electrical conductivity in mS/cm",
    },
    # Sample identifiers
    {
        "canonical_column_id": generate_surrogate_key("col_sample_barcode"),
        "canonical_column_name": "sample_barcode",
        "column_category": "sample_identifier",
        "data_type": "string",
        "description": "Unique sample barcode/ID",
    },
    # Lab metadata
    {
        "canonical_column_id": generate_surrogate_key("col_lab_id"),
        "canonical_column_name": "lab_id",
        "column_category": "lab_metadata",
        "data_type": "string",
        "description": "Laboratory identifier",
    },
    # Date columns
    {
        "canonical_column_id": generate_surrogate_key("col_date_received"),
        "canonical_column_name": "date_received",
        "column_category": "date",
        "data_type": "date",
        "description": "Sample receipt date",
    },
    {
        "canonical_column_id": generate_surrogate_key("col_date_analyzed"),
        "canonical_column_name": "date_analyzed",
        "column_category": "date",
        "data_type": "date",
        "description": "Analysis date",
    },
]


# =============================================================================
# VENDOR COLUMN MAPPING
# =============================================================================
# Maps ALL vendor columns to canonical definitions (measurements AND metadata)

VENDOR_COLUMN_MAPPING = [
    # ==========================================================================
    # Vendor A - Measurements
    # ==========================================================================
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "ph",
        "canonical_column_id": generate_surrogate_key("col_ph"),
        "notes": "Direct pH measurement",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "copper_ppm",
        "canonical_column_id": generate_surrogate_key("col_copper"),
        "notes": "Copper in ppm",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "zinc_ppm",
        "canonical_column_id": generate_surrogate_key("col_zinc"),
        "notes": "Zinc in ppm",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "lead_ppm",
        "canonical_column_id": generate_surrogate_key("col_lead"),
        "notes": "Lead in ppm",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "iron_ppm",
        "canonical_column_id": generate_surrogate_key("col_iron"),
        "notes": "Iron in ppm",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "manganese_ppm",
        "canonical_column_id": generate_surrogate_key("col_manganese"),
        "notes": "Manganese in ppm",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "boron_ppm",
        "canonical_column_id": generate_surrogate_key("col_boron"),
        "notes": "Boron in ppm",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "molybdenum_ppm",
        "canonical_column_id": generate_surrogate_key("col_molybdenum"),
        "notes": "Molybdenum in ppm",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "sulfur_ppm",
        "canonical_column_id": generate_surrogate_key("col_sulfur"),
        "notes": "Sulfur in ppm",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "organic_matter_pct",
        "canonical_column_id": generate_surrogate_key("col_organic_matter"),
        "notes": "Organic matter percentage",
    },
    # Vendor A - Metadata columns
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "sample_barcode",
        "canonical_column_id": generate_surrogate_key("col_sample_barcode"),
        "notes": "Sample identifier",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "lab_id",
        "canonical_column_id": generate_surrogate_key("col_lab_id"),
        "notes": "Lab identifier",
    },
    {
        "vendor_id": "vendor_a",
        "vendor_column_name": "received_date",
        "canonical_column_id": generate_surrogate_key("col_date_received"),
        "notes": "Date sample received",
    },
    # ==========================================================================
    # Vendor B - Measurements (different naming conventions)
    # ==========================================================================
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "acidity",
        "canonical_column_id": generate_surrogate_key("col_ph"),
        "notes": "Vendor B calls pH 'acidity'",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "cu_total",
        "canonical_column_id": generate_surrogate_key("col_copper"),
        "notes": "Chemical symbol notation",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "zn_total",
        "canonical_column_id": generate_surrogate_key("col_zinc"),
        "notes": "Chemical symbol notation",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "pb_total",
        "canonical_column_id": generate_surrogate_key("col_lead"),
        "notes": "Chemical symbol notation",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "fe_total",
        "canonical_column_id": generate_surrogate_key("col_iron"),
        "notes": "Chemical symbol notation",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "mn_total",
        "canonical_column_id": generate_surrogate_key("col_manganese"),
        "notes": "Chemical symbol notation",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "b_total",
        "canonical_column_id": generate_surrogate_key("col_boron"),
        "notes": "Chemical symbol notation",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "mo_total",
        "canonical_column_id": generate_surrogate_key("col_molybdenum"),
        "notes": "Chemical symbol notation",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "ec_ms_cm",
        "canonical_column_id": generate_surrogate_key("col_ec"),
        "notes": "Electrical conductivity",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "organic_carbon_pct",
        "canonical_column_id": generate_surrogate_key("col_organic_matter"),
        "notes": "Organic carbon as proxy for organic matter",
    },
    # Vendor B - Metadata columns (with typo example)
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "sample_id",
        "canonical_column_id": generate_surrogate_key("col_sample_barcode"),
        "notes": "Vendor B uses 'sample_id' instead of 'sample_barcode'",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "sample_barcod",
        "canonical_column_id": generate_surrogate_key("col_sample_barcode"),
        "notes": "Typo preserved in bronze, mapped here",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "laboratory",
        "canonical_column_id": generate_surrogate_key("col_lab_id"),
        "notes": "Vendor B uses 'laboratory' instead of 'lab_id'",
    },
    {
        "vendor_id": "vendor_b",
        "vendor_column_name": "analysis_date",
        "canonical_column_id": generate_surrogate_key("col_date_analyzed"),
        "notes": "Analysis date",
    },
]


def generate_canonical_column_definitions_csv(output_path: str) -> pd.DataFrame:
    """
    Generate canonical column definitions CSV file

    Args:
        output_path: Path to write the CSV file

    Returns:
        DataFrame with canonical column definitions

    Example:
        >>> df = generate_canonical_column_definitions_csv("data/canonical.csv")
        >>> print(df.columns.tolist())
        ['canonical_column_id', 'canonical_column_name', 'column_category',
         'data_type', 'description']
    """
    df = pd.DataFrame(CANONICAL_COLUMN_DEFINITIONS)
    df.to_csv(output_path, index=False)
    return df


def generate_vendor_column_mapping_csv(output_path: str) -> pd.DataFrame:
    """
    Generate vendor column mapping CSV file

    Args:
        output_path: Path to write the CSV file

    Returns:
        DataFrame with vendor column mapping data

    Example:
        >>> df = generate_vendor_column_mapping_csv("data/mapping.csv")
        >>> print(df.columns.tolist())
        ['vendor_id', 'vendor_column_name', 'canonical_column_id', 'notes']
    """
    df = pd.DataFrame(VENDOR_COLUMN_MAPPING)
    df.to_csv(output_path, index=False)
    return df
