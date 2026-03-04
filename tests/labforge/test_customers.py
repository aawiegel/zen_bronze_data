import re
from datetime import datetime

import pandas as pd
import pytest

from src.labforge import customers


# ---------------------------------------------------------------------------
# derive_seed_from_barcode
# ---------------------------------------------------------------------------


def test_derive_seed_is_deterministic():
    barcode = "PYB1234-5678"
    assert customers.derive_seed_from_barcode(barcode) == customers.derive_seed_from_barcode(barcode)


def test_derive_seed_different_barcodes_differ():
    assert customers.derive_seed_from_barcode("PYB0000-0001") != customers.derive_seed_from_barcode(
        "PYB0000-0002"
    )


def test_derive_seed_returns_non_negative_int():
    seed = customers.derive_seed_from_barcode("ANY-BARCODE")
    assert isinstance(seed, int)
    assert seed >= 0


# ---------------------------------------------------------------------------
# forge_customers — field validity
# ---------------------------------------------------------------------------


def test_forge_customers_returns_dataframe(np_number_generator):
    result = customers.forge_customers(3, np_number_generator)
    assert isinstance(result, pd.DataFrame)


def test_forge_customers_correct_row_count(np_number_generator):
    result = customers.forge_customers(5, np_number_generator)
    assert len(result) == 5


def test_forge_customers_all_ids_unique(np_number_generator):
    result = customers.forge_customers(10, np_number_generator)
    assert result["customer_id"].nunique() == 10


def test_forge_customers_customer_id_format(np_number_generator):
    result = customers.forge_customers(5, np_number_generator)
    for customer_id in result["customer_id"]:
        assert customer_id.startswith("CUST-"), f"Unexpected prefix: {customer_id}"
        suffix = customer_id[5:]
        assert len(suffix) == 6, f"Suffix length should be 6: {suffix}"
        assert all(char in "0123456789ABCDEF" for char in suffix), (
            f"Suffix should be uppercase hex: {suffix}"
        )


def test_forge_customers_age_in_valid_range(np_number_generator):
    result = customers.forge_customers(20, np_number_generator)
    assert result["age"].between(25, 75).all(), "All ages should be between 25 and 75"


def test_forge_customers_date_of_birth_format(np_number_generator):
    result = customers.forge_customers(10, np_number_generator)
    for date_of_birth in result["date_of_birth"]:
        parsed = datetime.strptime(date_of_birth, "%Y-%m-%d")
        assert 1951 <= parsed.year <= 2001, (
            f"Birth year {parsed.year} outside expected range for ages 25-75"
        )


def test_forge_customers_email_format(np_number_generator):
    result = customers.forge_customers(10, np_number_generator)
    for email in result["email"]:
        assert "@" in email, f"Email missing @: {email}"
        domain = email.split("@")[1]
        assert domain in customers.EMAIL_DOMAINS, f"Unexpected domain: {domain}"


def test_forge_customers_phone_format(np_number_generator):
    result = customers.forge_customers(10, np_number_generator)
    phone_pattern = re.compile(r"^\(\d{3}\) \d{3}-\d{4}$")
    for phone in result["phone"]:
        assert phone_pattern.match(phone), f"Phone '{phone}' does not match expected format"


def test_forge_customers_state_is_valid(np_number_generator):
    result = customers.forge_customers(10, np_number_generator)
    for state in result["state"]:
        assert state in customers.STATES, f"Unknown state code: {state}"


def test_forge_customers_zip_code_is_five_digits(np_number_generator):
    result = customers.forge_customers(10, np_number_generator)
    for zip_code in result["zip_code"]:
        assert len(zip_code) == 5, f"zip_code should be 5 chars: {zip_code}"
        assert zip_code.isdigit(), f"zip_code should be all digits: {zip_code}"


def test_forge_customers_name_from_curated_lists(np_number_generator):
    result = customers.forge_customers(10, np_number_generator)
    for customer_name in result["customer_name"]:
        parts = customer_name.split(" ")
        assert len(parts) == 2, f"Name should be first + last: {customer_name}"
        assert parts[0] in customers.FIRST_NAMES, f"Unexpected first name: {parts[0]}"
        assert parts[1] in customers.LAST_NAMES, f"Unexpected last name: {parts[1]}"


def test_forge_customers_notes_from_curated_list(np_number_generator):
    result = customers.forge_customers(10, np_number_generator)
    for notes in result["notes"]:
        assert notes in customers.NOTES_PHRASES, f"Unexpected notes value: {notes}"


def test_forge_customers_has_all_required_columns(np_number_generator):
    result = customers.forge_customers(3, np_number_generator)
    expected_columns = {
        "customer_id", "customer_name", "date_of_birth", "age",
        "email", "phone", "street_address", "city", "state", "zip_code", "notes",
    }
    assert expected_columns.issubset(set(result.columns))


# ---------------------------------------------------------------------------
# forge_customers — determinism (no generator fixture needed)
# ---------------------------------------------------------------------------


def test_forge_customers_is_deterministic():
    import numpy as np
    result_one = customers.forge_customers(5, np.random.default_rng(42))
    result_two = customers.forge_customers(5, np.random.default_rng(42))
    pd.testing.assert_frame_equal(result_one, result_two)


# ---------------------------------------------------------------------------
# forge_customer_sample_assignments
# ---------------------------------------------------------------------------


def test_forge_customer_sample_assignments_correct_row_count(np_number_generator):
    customer_df = customers.forge_customers(3, np_number_generator)
    barcodes = ["BC-001", "BC-002", "BC-003", "BC-004"]
    result = customers.forge_customer_sample_assignments(barcodes, customer_df, np_number_generator)
    assert len(result) == 4


def test_forge_customer_sample_assignments_has_required_columns(np_number_generator):
    customer_df = customers.forge_customers(3, np_number_generator)
    barcodes = ["BC-001", "BC-002"]
    result = customers.forge_customer_sample_assignments(barcodes, customer_df, np_number_generator)
    assert set(result.columns) == {"barcode", "customer_id", "crop_type", "sample_date"}


def test_forge_customer_sample_assignments_customer_ids_are_valid(np_number_generator):
    customer_df = customers.forge_customers(3, np_number_generator)
    barcodes = ["BC-001", "BC-002", "BC-003", "BC-004", "BC-005"]
    result = customers.forge_customer_sample_assignments(barcodes, customer_df, np_number_generator)
    assert set(result["customer_id"]).issubset(set(customer_df["customer_id"]))


def test_forge_customer_sample_assignments_barcodes_preserved(np_number_generator):
    customer_df = customers.forge_customers(3, np_number_generator)
    barcodes = ["PYB-ALPHA", "PYB-BETA", "PYB-GAMMA"]
    result = customers.forge_customer_sample_assignments(barcodes, customer_df, np_number_generator)
    assert list(result["barcode"]) == barcodes


def test_forge_customer_sample_assignments_crop_types_valid(np_number_generator):
    customer_df = customers.forge_customers(3, np_number_generator)
    barcodes = [f"BC-{i:03d}" for i in range(10)]
    result = customers.forge_customer_sample_assignments(barcodes, customer_df, np_number_generator)
    for crop_type in result["crop_type"]:
        assert crop_type in customers.CROP_TYPES, f"Unexpected crop type: {crop_type}"


def test_forge_customer_sample_assignments_sample_date_format(np_number_generator):
    customer_df = customers.forge_customers(3, np_number_generator)
    barcodes = ["BC-001", "BC-002"]
    result = customers.forge_customer_sample_assignments(barcodes, customer_df, np_number_generator)
    for sample_date in result["sample_date"]:
        datetime.strptime(sample_date, "%Y-%m-%d")  # Should not raise


def test_forge_customer_sample_assignments_empty_barcodes(np_number_generator):
    customer_df = customers.forge_customers(3, np_number_generator)
    result = customers.forge_customer_sample_assignments([], customer_df, np_number_generator)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
    assert set(result.columns) == {"barcode", "customer_id", "crop_type", "sample_date"}


def test_forge_customer_sample_assignments_raises_on_empty_customers(np_number_generator):
    empty_customers = pd.DataFrame(columns=["customer_id"])
    with pytest.raises(ValueError, match="cannot be empty"):
        customers.forge_customer_sample_assignments(["BC-001"], empty_customers, np_number_generator)
