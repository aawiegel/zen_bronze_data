from src.labforge import vendors
import pytest


def test_vendor_a_basic_package(np_number_generator):
    """Test Vendor A with only basic package"""
    df = vendors.forge_vendor_csv(
        np_number_generator, vendor="vendor_a", packages=["basic"], rows=25
    )

    assert len(df) == 25

    # Check metadata columns
    assert "sample_barcode" in df.columns
    assert "lab_id" in df.columns
    assert "date_received" in df.columns
    assert "date_processed" in df.columns

    # Check basic package columns
    assert "ph" in df.columns
    assert "copper_ppm" in df.columns
    assert "zinc_ppm" in df.columns

    # Should NOT have other package columns
    assert "lead_ppm" not in df.columns
    assert "sulfur_ppm" not in df.columns


def test_vendor_a_multiple_packages(np_number_generator):
    """Test Vendor A with multiple additive packages"""
    df = vendors.forge_vendor_csv(
        np_number_generator,
        vendor="vendor_a",
        packages=["basic", "metals", "micronutrient"],
        rows=30,
    )

    assert len(df) == 30

    # Should have all columns from all three packages
    assert "ph" in df.columns  # basic
    assert "lead_ppm" in df.columns  # metals
    assert "sulfur_ppm" in df.columns  # micronutrient (exclusive to vendor A)
    assert "boron_ppm" in df.columns


def test_vendor_b_standard_package(np_number_generator):
    """Test Vendor B with only standard package"""
    df = vendors.forge_vendor_csv(
        np_number_generator, vendor="vendor_b", packages=["standard"], rows=25
    )

    assert len(df) == 25

    # Check Vendor B naming conventions (different from Vendor A)
    assert "acidity" in df.columns  # not "ph"
    assert "cu_total" in df.columns  # not "copper_ppm"
    assert "zn_total" in df.columns  # not "zinc_ppm"

    # Should NOT have Vendor A column names
    assert "ph" not in df.columns
    assert "copper_ppm" not in df.columns


def test_vendor_b_with_exclusive_analyte(np_number_generator):
    """Test Vendor B exclusive measurement (ec_ms_cm)"""
    df = vendors.forge_vendor_csv(
        np_number_generator,
        vendor="vendor_b",
        packages=["standard", "trace_elements"],
        rows=20,
    )

    assert "ec_ms_cm" in df.columns  # Vendor B exclusive
    assert "sulfur_ppm" not in df.columns  # Vendor A exclusive


def test_vendor_column_name_differences(np_number_generator):
    """Verify vendors use different column names for same measurements"""
    df_a = vendors.forge_vendor_csv(
        np_number_generator, vendor="vendor_a", packages=["basic"], rows=10
    )
    df_b = vendors.forge_vendor_csv(
        np_number_generator, vendor="vendor_b", packages=["standard"], rows=10
    )

    # Vendor A uses these names
    assert "ph" in df_a.columns
    assert "copper_ppm" in df_a.columns

    # Vendor B uses these names for the SAME measurements
    assert "acidity" in df_b.columns
    assert "cu_total" in df_b.columns

    # They should NOT share these measurement column names
    assert "ph" not in df_b.columns
    assert "acidity" not in df_a.columns


def test_vendor_barcode_prefixes(np_number_generator):
    """Verify vendors have different barcode prefixes"""
    df_a = vendors.forge_vendor_csv(
        np_number_generator, vendor="vendor_a", packages=["basic"], rows=5
    )
    df_b = vendors.forge_vendor_csv(
        np_number_generator, vendor="vendor_b", packages=["standard"], rows=5
    )

    # Vendor A uses PSL prefix
    assert df_a["sample_barcode"].str.startswith("PYB").all()
    assert df_a["lab_id"].str.startswith("PSL-").all()

    # Vendor B uses ATS prefix
    assert df_b["sample_barcode"].str.startswith("PYB").all()
    assert df_b["lab_id"].str.startswith("AT").all()


def test_invalid_vendor_raises_error(np_number_generator):
    """Test that invalid vendor name raises ValueError"""
    with pytest.raises(ValueError, match="Unknown vendor"):
        vendors.forge_vendor_csv(
            np_number_generator, vendor="vendor_c", packages=["basic"]
        )


def test_invalid_package_raises_error(np_number_generator):
    """Test that invalid package name raises ValueError"""
    with pytest.raises(ValueError, match="Invalid packages"):
        vendors.forge_vendor_csv(
            np_number_generator, vendor="vendor_a", packages=["nonexistent_package"]
        )


def test_default_package_behavior(np_number_generator):
    """Test that omitting packages parameter uses first package"""
    df_a = vendors.forge_vendor_csv(np_number_generator, vendor="vendor_a", rows=10)
    df_b = vendors.forge_vendor_csv(np_number_generator, vendor="vendor_b", rows=10)

    # Should get first package for each vendor
    assert "ph" in df_a.columns  # basic package
    assert "acidity" in df_b.columns  # standard package
