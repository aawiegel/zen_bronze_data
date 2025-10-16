"""
labforge - Synthetic lab data generation with realistic quality issues

Main modules:
- vendors: Generate vendor-specific CSV files with different schemas
- chaos: Add realistic data quality issues (typos, casing, whitespace, metadata rows, empty columns)
- numeric: Generate numeric data with distributions and precision
- string: Generate barcodes and string identifiers
- temporal: Generate date/time data with relationships
- metadata: Generate analyte dimensions and vendor mappings
"""

# Core vendor data generation
from .vendors import forge_vendor_csv, VENDOR_A_PACKAGES, VENDOR_B_PACKAGES

# Chaos generators for realistic data quality issues
from .chaos import (
    apply_chaos,
    chaos_header_typos,
    chaos_header_casing,
    chaos_header_whitespace,
    chaos_invalid_db_chars,
    chaos_metadata_rows,
    chaos_empty_column_padding,
)

# Metadata generation
from .metadata import (
    generate_analyte_dimension_csv,
    generate_vendor_analyte_mapping_csv,
    generate_surrogate_key,
    ANALYTE_DIMENSION,
    VENDOR_ANALYTE_MAPPING,
    VENDOR_COLUMN_TO_ANALYTE,
)

# Low-level generators (for custom data generation)
from .numeric import forge_distribution
from .string import forge_barcode, forge_barcodes
from .temporal import forge_date_pairs

__all__ = [
    # Vendor data generation
    "forge_vendor_csv",
    "VENDOR_A_PACKAGES",
    "VENDOR_B_PACKAGES",
    # Chaos generators
    "apply_chaos",
    "chaos_header_typos",
    "chaos_header_casing",
    "chaos_header_whitespace",
    "chaos_invalid_db_chars",
    "chaos_metadata_rows",
    "chaos_empty_column_padding",
    # Metadata
    "generate_analyte_dimension_csv",
    "generate_vendor_analyte_mapping_csv",
    "generate_surrogate_key",
    "ANALYTE_DIMENSION",
    "VENDOR_ANALYTE_MAPPING",
    "VENDOR_COLUMN_TO_ANALYTE",
    # Low-level generators
    "forge_distribution",
    "forge_barcode",
    "forge_barcodes",
    "forge_date_pairs",
]
