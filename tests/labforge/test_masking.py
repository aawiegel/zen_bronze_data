import numpy as np
import pandas as pd
import pytest

from src.labforge import customers, masking


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_customer_df(np_number_generator):
    customer_table = customers.forge_customers(10, np_number_generator)
    return customer_table


# ---------------------------------------------------------------------------
# mask_null
# ---------------------------------------------------------------------------


def test_mask_null_replaces_with_none(sample_customer_df):
    masked = masking.mask_null(sample_customer_df, ["email", "phone"])
    assert masked["email"].isna().all()
    assert masked["phone"].isna().all()


def test_mask_null_preserves_unmasked_columns(sample_customer_df):
    masked = masking.mask_null(sample_customer_df, ["email"])
    pd.testing.assert_series_equal(masked["customer_name"], sample_customer_df["customer_name"])


def test_mask_null_does_not_mutate_input(sample_customer_df):
    original_email = sample_customer_df["email"].copy()
    masking.mask_null(sample_customer_df, ["email"])
    pd.testing.assert_series_equal(sample_customer_df["email"], original_email)


# ---------------------------------------------------------------------------
# mask_shuffle
# ---------------------------------------------------------------------------


def test_mask_shuffle_preserves_value_set(sample_customer_df, np_number_generator):
    masked = masking.mask_shuffle(sample_customer_df, ["city"], np_number_generator)
    assert sorted(masked["city"]) == sorted(sample_customer_df["city"])


def test_mask_shuffle_changes_order(sample_customer_df, np_number_generator):
    original_cities = list(sample_customer_df["city"])
    masked = masking.mask_shuffle(sample_customer_df, ["city"], np_number_generator)
    # Astronomically unlikely to be a no-op for n=10
    assert list(masked["city"]) != original_cities


def test_mask_shuffle_preserves_unmasked_columns(sample_customer_df, np_number_generator):
    masked = masking.mask_shuffle(sample_customer_df, ["city"], np_number_generator)
    pd.testing.assert_series_equal(masked["email"], sample_customer_df["email"])


def test_mask_shuffle_does_not_mutate_input(sample_customer_df, np_number_generator):
    original_city = sample_customer_df["city"].copy()
    masking.mask_shuffle(sample_customer_df, ["city"], np_number_generator)
    pd.testing.assert_series_equal(sample_customer_df["city"], original_city)


# ---------------------------------------------------------------------------
# mask_impute
# ---------------------------------------------------------------------------


def test_mask_impute_replaces_column(sample_customer_df):
    imputers = {"email": lambda n: [f"anon{i}@example.com" for i in range(n)]}
    masked = masking.mask_impute(sample_customer_df, ["email"], imputers)
    assert all("anon" in email for email in masked["email"])


def test_mask_impute_correct_row_count(sample_customer_df):
    imputers = {"phone": lambda n: ["(000) 000-0000"] * n}
    masked = masking.mask_impute(sample_customer_df, ["phone"], imputers)
    assert len(masked) == len(sample_customer_df)


def test_mask_impute_raises_for_missing_imputer(sample_customer_df):
    with pytest.raises(ValueError, match="no imputer"):
        masking.mask_impute(sample_customer_df, ["email"], imputers={})


def test_mask_impute_does_not_mutate_input(sample_customer_df):
    original_email = sample_customer_df["email"].copy()
    imputers = {"email": lambda n: ["x@y.com"] * n}
    masking.mask_impute(sample_customer_df, ["email"], imputers)
    pd.testing.assert_series_equal(sample_customer_df["email"], original_email)


# ---------------------------------------------------------------------------
# mask_resample
# ---------------------------------------------------------------------------


def test_mask_resample_produces_values_in_original_range(sample_customer_df, np_number_generator):
    masked = masking.mask_resample(sample_customer_df, ["age"], np_number_generator)
    original_min = sample_customer_df["age"].min()
    original_max = sample_customer_df["age"].max()
    assert masked["age"].between(original_min, original_max).all()


def test_mask_resample_mean_approximately_preserved(sample_customer_df, np_number_generator):
    masked = masking.mask_resample(sample_customer_df, ["age"], np_number_generator)
    original_mean = sample_customer_df["age"].mean()
    resampled_mean = masked["age"].mean()
    # With n=10 and age range 25-75, means within 15 points is a reasonable tolerance
    assert abs(resampled_mean - original_mean) < 15


def test_mask_resample_raises_on_non_numeric_column(sample_customer_df, np_number_generator):
    with pytest.raises(ValueError, match="not numeric"):
        masking.mask_resample(sample_customer_df, ["email"], np_number_generator)


def test_mask_resample_does_not_mutate_input(sample_customer_df, np_number_generator):
    original_age = sample_customer_df["age"].copy()
    masking.mask_resample(sample_customer_df, ["age"], np_number_generator)
    pd.testing.assert_series_equal(sample_customer_df["age"], original_age)


# ---------------------------------------------------------------------------
# mask_hash
# ---------------------------------------------------------------------------


def test_mask_hash_produces_eight_char_strings(sample_customer_df):
    masked = masking.mask_hash(sample_customer_df, ["customer_id"])
    assert masked["customer_id"].str.len().eq(8).all()


def test_mask_hash_is_deterministic(sample_customer_df):
    masked_one = masking.mask_hash(sample_customer_df, ["customer_id"])
    masked_two = masking.mask_hash(sample_customer_df, ["customer_id"])
    pd.testing.assert_series_equal(masked_one["customer_id"], masked_two["customer_id"])


def test_mask_hash_salt_changes_output(sample_customer_df):
    no_salt = masking.mask_hash(sample_customer_df, ["customer_id"], salt="")
    with_salt = masking.mask_hash(sample_customer_df, ["customer_id"], salt="secret")
    assert not (no_salt["customer_id"] == with_salt["customer_id"]).all()


def test_mask_hash_same_value_produces_same_token(sample_customer_df):
    duplicated_row = pd.concat(
        [sample_customer_df.iloc[:1], sample_customer_df.iloc[:1]], ignore_index=True
    )
    masked = masking.mask_hash(duplicated_row, ["customer_id"])
    assert masked["customer_id"].iloc[0] == masked["customer_id"].iloc[1]


def test_mask_hash_does_not_mutate_input(sample_customer_df):
    original_id = sample_customer_df["customer_id"].copy()
    masking.mask_hash(sample_customer_df, ["customer_id"])
    pd.testing.assert_series_equal(sample_customer_df["customer_id"], original_id)


# ---------------------------------------------------------------------------
# apply_masking — end-to-end
# ---------------------------------------------------------------------------


def test_apply_masking_all_five_strategies(sample_customer_df, np_number_generator):
    imputers = {"customer_name": lambda n: ["Anonymous User"] * n}
    mask_config = {
        "phone": "null",
        "city": "shuffle",
        "age": "resample",
        "customer_name": "impute",
        "customer_id": "hash",
    }
    masked = masking.apply_masking(
        sample_customer_df,
        mask_config,
        generator=np_number_generator,
        imputers=imputers,
        salt="test-salt",
    )

    assert masked["phone"].isna().all()
    assert sorted(masked["city"]) == sorted(sample_customer_df["city"])
    assert masked["age"].between(sample_customer_df["age"].min(), sample_customer_df["age"].max()).all()
    assert (masked["customer_name"] == "Anonymous User").all()
    assert masked["customer_id"].str.len().eq(8).all()
    pd.testing.assert_series_equal(masked["email"], sample_customer_df["email"])


def test_apply_masking_raises_on_unknown_strategy(sample_customer_df, np_number_generator):
    with pytest.raises(ValueError, match="Unknown masking"):
        masking.apply_masking(
            sample_customer_df,
            {"email": "encrypt"},
            generator=np_number_generator,
        )


def test_apply_masking_does_not_mutate_input(sample_customer_df, np_number_generator):
    original_email = sample_customer_df["email"].copy()
    masking.apply_masking(
        sample_customer_df,
        {"email": "null"},
        generator=np_number_generator,
    )
    pd.testing.assert_series_equal(sample_customer_df["email"], original_email)
