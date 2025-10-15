"""
Generate metadata tables for analytes and vendor-analyte mappings
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
        >>> generate_surrogate_key("ph")
        'a94a8fe5'
        >>> generate_surrogate_key("vendor_a", "ph")
        'f0e7d6c5'
    """
    combined = "|".join(values)
    return hashlib.sha256(combined.encode()).hexdigest()[:8]


# Analyte dimension table - the source of truth for all measurements
ANALYTE_DIMENSION = [
    {
        "analyte_id": generate_surrogate_key("ph"),
        "analyte_name": "pH",
        "unit": "",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 14.0,
        "description": "Soil pH level measuring acidity/alkalinity (unitless logarithmic scale)",
    },
    {
        "analyte_id": generate_surrogate_key("copper"),
        "analyte_name": "Copper",
        "unit": "ppm",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 100.0,
        "description": "Total copper content in soil",
    },
    {
        "analyte_id": generate_surrogate_key("zinc"),
        "analyte_name": "Zinc",
        "unit": "ppm",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 200.0,
        "description": "Total zinc content in soil",
    },
    {
        "analyte_id": generate_surrogate_key("lead"),
        "analyte_name": "Lead",
        "unit": "ppm",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 50.0,
        "description": "Total lead content in soil",
    },
    {
        "analyte_id": generate_surrogate_key("iron"),
        "analyte_name": "Iron",
        "unit": "ppm",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 1000.0,
        "description": "Total iron content in soil",
    },
    {
        "analyte_id": generate_surrogate_key("manganese"),
        "analyte_name": "Manganese",
        "unit": "ppm",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 500.0,
        "description": "Total manganese content in soil",
    },
    {
        "analyte_id": generate_surrogate_key("boron"),
        "analyte_name": "Boron",
        "unit": "ppm",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 10.0,
        "description": "Total boron content in soil",
    },
    {
        "analyte_id": generate_surrogate_key("molybdenum"),
        "analyte_name": "Molybdenum",
        "unit": "ppm",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 5.0,
        "description": "Total molybdenum content in soil",
    },
    {
        "analyte_id": generate_surrogate_key("sulfur"),
        "analyte_name": "Sulfur",
        "unit": "ppm",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 300.0,
        "description": "Total sulfur content in soil",
    },
    {
        "analyte_id": generate_surrogate_key("organic_matter"),
        "analyte_name": "Organic Matter",
        "unit": "percent",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 100.0,
        "description": "Percentage of organic matter in soil sample",
    },
    {
        "analyte_id": generate_surrogate_key("electrical_conductivity"),
        "analyte_name": "Electrical Conductivity",
        "unit": "mS/cm",
        "data_type": "numeric",
        "min_valid_value": 0.0,
        "max_valid_value": 20.0,
        "description": "Electrical conductivity indicating soil salinity",
    },
]

# Vendor-Analyte mapping (through table)
VENDOR_ANALYTE_MAPPING = [
    # Vendor A - Basic package
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_a", "ph"),
        "vendor_id": "vendor_a",
        "vendor_column_name": "ph",
        "analyte_id": generate_surrogate_key("ph"),
        "notes": "Direct pH measurement",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_a", "copper_ppm"),
        "vendor_id": "vendor_a",
        "vendor_column_name": "copper_ppm",
        "analyte_id": generate_surrogate_key("copper"),
        "notes": "Copper reported in ppm",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_a", "zinc_ppm"),
        "vendor_id": "vendor_a",
        "vendor_column_name": "zinc_ppm",
        "analyte_id": generate_surrogate_key("zinc"),
        "notes": "Zinc reported in ppm",
    },
    # Vendor A - Metals package
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_a", "lead_ppm"),
        "vendor_id": "vendor_a",
        "vendor_column_name": "lead_ppm",
        "analyte_id": generate_surrogate_key("lead"),
        "notes": "Lead reported in ppm",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_a", "iron_ppm"),
        "vendor_id": "vendor_a",
        "vendor_column_name": "iron_ppm",
        "analyte_id": generate_surrogate_key("iron"),
        "notes": "Iron reported in ppm",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key(
            "vendor_a", "manganese_ppm"
        ),
        "vendor_id": "vendor_a",
        "vendor_column_name": "manganese_ppm",
        "analyte_id": generate_surrogate_key("manganese"),
        "notes": "Manganese reported in ppm",
    },
    # Vendor A - Micronutrient package
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_a", "boron_ppm"),
        "vendor_id": "vendor_a",
        "vendor_column_name": "boron_ppm",
        "analyte_id": generate_surrogate_key("boron"),
        "notes": "Boron reported in ppm",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key(
            "vendor_a", "molybdenum_ppm"
        ),
        "vendor_id": "vendor_a",
        "vendor_column_name": "molybdenum_ppm",
        "analyte_id": generate_surrogate_key("molybdenum"),
        "notes": "Molybdenum reported in ppm",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_a", "sulfur_ppm"),
        "vendor_id": "vendor_a",
        "vendor_column_name": "sulfur_ppm",
        "analyte_id": generate_surrogate_key("sulfur"),
        "notes": "Vendor A exclusive - sulfur reported in ppm",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key(
            "vendor_a", "organic_matter_pct"
        ),
        "vendor_id": "vendor_a",
        "vendor_column_name": "organic_matter_pct",
        "analyte_id": generate_surrogate_key("organic_matter"),
        "notes": "Organic matter as percentage",
    },
    # Vendor B - Standard package
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_b", "acidity"),
        "vendor_id": "vendor_b",
        "vendor_column_name": "acidity",
        "analyte_id": generate_surrogate_key("ph"),
        "notes": "pH reported as 'acidity' by Vendor B",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_b", "cu_total"),
        "vendor_id": "vendor_b",
        "vendor_column_name": "cu_total",
        "analyte_id": generate_surrogate_key("copper"),
        "notes": "Total copper using chemical symbol notation",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_b", "zn_total"),
        "vendor_id": "vendor_b",
        "vendor_column_name": "zn_total",
        "analyte_id": generate_surrogate_key("zinc"),
        "notes": "Total zinc using chemical symbol notation",
    },
    # Vendor B - Heavy metals package
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_b", "pb_total"),
        "vendor_id": "vendor_b",
        "vendor_column_name": "pb_total",
        "analyte_id": generate_surrogate_key("lead"),
        "notes": "Total lead using chemical symbol notation",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_b", "fe_total"),
        "vendor_id": "vendor_b",
        "vendor_column_name": "fe_total",
        "analyte_id": generate_surrogate_key("iron"),
        "notes": "Total iron using chemical symbol notation",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_b", "mn_total"),
        "vendor_id": "vendor_b",
        "vendor_column_name": "mn_total",
        "analyte_id": generate_surrogate_key("manganese"),
        "notes": "Total manganese using chemical symbol notation",
    },
    # Vendor B - Trace elements package
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_b", "b_total"),
        "vendor_id": "vendor_b",
        "vendor_column_name": "b_total",
        "analyte_id": generate_surrogate_key("boron"),
        "notes": "Total boron using chemical symbol notation",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_b", "mo_total"),
        "vendor_id": "vendor_b",
        "vendor_column_name": "mo_total",
        "analyte_id": generate_surrogate_key("molybdenum"),
        "notes": "Total molybdenum using chemical symbol notation",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key("vendor_b", "ec_ms_cm"),
        "vendor_id": "vendor_b",
        "vendor_column_name": "ec_ms_cm",
        "analyte_id": generate_surrogate_key("electrical_conductivity"),
        "notes": "Vendor B exclusive - electrical conductivity in mS/cm",
    },
    {
        "vendor_analyte_mapping_id": generate_surrogate_key(
            "vendor_b", "organic_carbon_pct"
        ),
        "vendor_id": "vendor_b",
        "vendor_column_name": "organic_carbon_pct",
        "analyte_id": generate_surrogate_key("organic_matter"),
        "notes": "Organic carbon as proxy for organic matter (reported as percentage)",
    },
]


def generate_analyte_dimension_csv(output_path: str) -> pd.DataFrame:
    """
    Generate analyte dimension CSV file

    Args:
        output_path: Path to write the CSV file

    Returns:
        DataFrame with analyte dimension data

    Example:
        >>> df = generate_analyte_dimension_csv("data/analyte_dimension.csv")
        >>> print(df.columns)
        Index(['analyte_id', 'analyte_name', 'unit', 'data_type',
               'min_valid_value', 'max_valid_value', 'description'], dtype='object')
    """
    df = pd.DataFrame(ANALYTE_DIMENSION)
    df.to_csv(output_path, index=False)
    return df


def generate_vendor_analyte_mapping_csv(output_path: str) -> pd.DataFrame:
    """
    Generate vendor-analyte mapping CSV file (through table)

    Args:
        output_path: Path to write the CSV file

    Returns:
        DataFrame with vendor-analyte mapping data

    Example:
        >>> df = generate_vendor_analyte_mapping_csv("data/vendor_analyte_mapping.csv")
        >>> print(df.columns)
        Index(['vendor_analyte_mapping_id', 'vendor_id', 'vendor_column_name',
               'analyte_id', 'notes'], dtype='object')
    """
    df = pd.DataFrame(VENDOR_ANALYTE_MAPPING)
    df.to_csv(output_path, index=False)
    return df
