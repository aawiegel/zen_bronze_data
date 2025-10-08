"""
Vendor-specific CSV generation with different measurement packages
Demonstrates schema variations across vendors with overlapping but differently-named columns
"""

import pandas as pd
from src.labforge import numeric, string, temporal


# Vendor A: "Premium Soil Labs"
# Known for detailed reporting with specific naming conventions
VENDOR_A_PACKAGES = {
    "basic": {
        "ph": {
            "min_value": 5.0,
            "max_value": 8.0,
            "loc": 6.5,
            "scale": 0.5,
            "precision": 3,
        },
        "copper_ppm": {
            "min_value": 0.1,
            "max_value": 50,
            "precision": 3,
            "distribution": "gamma",
            "shape": 2.0,
            "scale": 2.5,
        },
        "zinc_ppm": {
            "min_value": 0.5,
            "max_value": 100,
            "precision": 3,
            "distribution": "gamma",
            "shape": 2.5,
            "scale": 6.0,
        },
    },
    "metals": {
        "lead_ppm": {
            "min_value": 0.0,
            "max_value": 30,
            "loc": 2,
            "scale": 1.5,
            "precision": 3,
            "null_probability": 0.05,
        },
        "iron_ppm": {
            "min_value": 5,
            "max_value": 500,
            "loc": 100,
            "scale": 50,
            "precision": 3,
        },
        "manganese_ppm": {
            "min_value": 1,
            "max_value": 200,
            "loc": 40,
            "scale": 20,
            "precision": 3,
        },
    },
    "micronutrient": {
        "boron_ppm": {
            "min_value": 0.1,
            "max_value": 5,
            "loc": 1.2,
            "scale": 0.4,
            "precision": 3,
        },
        "molybdenum_ppm": {
            "min_value": 0.01,
            "max_value": 2,
            "loc": 0.3,
            "scale": 0.15,
            "precision": 3,
            "null_probability": 0.1,
        },
        "sulfur_ppm": {
            "min_value": 5,
            "max_value": 150,
            "loc": 30,
            "scale": 15,
            "precision": 3,
        },  # VENDOR A EXCLUSIVE
        "organic_matter_pct": {
            "min_value": 0.5,
            "max_value": 15.0,
            "precision": 3,
            "distribution": "beta",
            "a": 2.0,
            "b": 5.0,
        },  # Beta for percent
    },
}

# Vendor B: "AgriTest Solutions"
# Uses different terminology, has different exclusive measurements
VENDOR_B_PACKAGES = {
    "standard": {
        "acidity": {
            "min_value": 5.0,
            "max_value": 8.0,
            "loc": 6.5,
            "scale": 0.5,
            "precision": 3,
        },  # same as pH
        "cu_total": {
            "min_value": 0.1,
            "max_value": 50,
            "precision": 3,
            "distribution": "gamma",
            "shape": 2.0,
            "scale": 2.5,
        },  # same as copper_ppm
        "zn_total": {
            "min_value": 0.5,
            "max_value": 100,
            "precision": 3,
            "distribution": "gamma",
            "shape": 2.5,
            "scale": 6.0,
        },  # same as zinc_ppm
    },
    "heavy_metals": {
        "pb_total": {
            "min_value": 0.0,
            "max_value": 30,
            "loc": 2,
            "scale": 1.5,
            "precision": 3,
            "null_probability": 0.05,
        },  # same as lead_ppm
        "fe_total": {
            "min_value": 5,
            "max_value": 500,
            "loc": 100,
            "scale": 50,
            "precision": 3,
        },  # same as iron_ppm
        "mn_total": {
            "min_value": 1,
            "max_value": 200,
            "loc": 40,
            "scale": 20,
            "precision": 3,
        },  # same as manganese_ppm
    },
    "trace_elements": {
        "b_total": {
            "min_value": 0.1,
            "max_value": 5,
            "loc": 1.2,
            "scale": 0.4,
            "precision": 3,
        },  # same as boron_ppm
        "mo_total": {
            "min_value": 0.01,
            "max_value": 2,
            "loc": 0.3,
            "scale": 0.15,
            "precision": 3,
            "null_probability": 0.1,
        },  # same as molybdenum_ppm
        "ec_ms_cm": {
            "min_value": 0.1,
            "max_value": 8.0,
            "loc": 2.5,
            "scale": 1.2,
            "precision": 3,
        },  # VENDOR B EXCLUSIVE (electrical conductivity/salinity)
        "organic_carbon_pct": {
            "min_value": 0.3,
            "max_value": 12.0,
            "precision": 3,
            "distribution": "beta",
            "a": 2.0,
            "b": 5.0,
        },  # Beta for percent (similar to organic_matter_pct but different name!)
    },
}

VENDOR_CONFIGS = {
    "vendor_a": {
        "packages": VENDOR_A_PACKAGES,
        "barcode_prefix": "PYB",
        "lab_id_prefix": "PSL-",  # Premium Soil Labs
    },
    "vendor_b": {
        "packages": VENDOR_B_PACKAGES,
        "barcode_prefix": "PYB",
        "lab_id_prefix": "AT",  # AgriTest Solutions
    },
}


def forge_vendor_csv(
    generator, vendor: str = "vendor_a", packages: list[str] = None, rows: int = 50
):
    """
    Generate synthetic vendor CSV data with specified measurement packages

    Args:
        generator: numpy random generator
        vendor: "vendor_a" or "vendor_b"
        packages: List of package names to include (e.g., ["basic", "metals"])
                 If None, includes only the first package for that vendor
        rows: Number of rows to generate

    Returns:
        pd.DataFrame with vendor-specific columns and measurements

    Examples:
        # Vendor A with basic measurements only
        forge_vendor_csv(gen, vendor="vendor_a", packages=["basic"])

        # Vendor B with all packages
        forge_vendor_csv(gen, vendor="vendor_b", packages=["standard", "heavy_metals", "trace_elements"])
    """
    if vendor not in VENDOR_CONFIGS:
        raise ValueError(
            f"Unknown vendor: {vendor}. Must be one of {list(VENDOR_CONFIGS.keys())}"
        )

    config = VENDOR_CONFIGS[vendor]
    available_packages = config["packages"]

    # Default to first package if none specified
    if packages is None:
        packages = [list(available_packages.keys())[0]]

    # Validate packages
    invalid_packages = set(packages) - set(available_packages.keys())
    if invalid_packages:
        raise ValueError(
            f"Invalid packages for {vendor}: {invalid_packages}. Available: {list(available_packages.keys())}"
        )

    # Generate metadata columns (always present)
    dates_received, dates_processed = temporal.forge_date_pairs(generator, size=rows)
    data = {
        "sample_barcode": string.forge_barcodes(
            generator,
            rows,
            prefix=config["barcode_prefix"],
            total_length=14,
            placeholder_location=7,
        ),
        "lab_id": string.forge_barcodes(
            generator,
            rows,
            prefix=config["lab_id_prefix"],
            total_length=12,
            placeholder_location=6,
            placeholder=" ",
        ),
        "date_received": dates_received,
        "date_processed": dates_processed,
    }

    # Add measurements from requested packages
    for package_name in packages:
        package_measurements = available_packages[package_name]
        for measurement_name, params in package_measurements.items():
            data[measurement_name] = numeric.forge_distribution(
                generator, size=rows, **params
            )

    return pd.DataFrame(data)
