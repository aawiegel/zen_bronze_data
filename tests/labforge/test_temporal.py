from src.labforge import temporal
import numpy as np


def test_forge_date_pairs(np_number_generator):
    received_dates, processed_dates = temporal.forge_date_pairs(np_number_generator)
    assert (processed_dates > received_dates).all()
    assert np.strings.startswith(received_dates, "2024").all()
    assert received_dates.dtype.kind == "U"
    assert processed_dates.dtype.kind == "U"


def test_forge_date_pairs_size(np_number_generator):
    """Test that size parameter generates correct number of date pairs"""
    size = 50
    received_dates, processed_dates = temporal.forge_date_pairs(
        np_number_generator, size=size
    )
    assert len(received_dates) == size
    assert len(processed_dates) == size


def test_forge_date_pairs_custom_year(np_number_generator):
    """Test that custom year parameter works correctly"""
    year = 2020
    received_dates, processed_dates = temporal.forge_date_pairs(
        np_number_generator, size=10, year=year
    )
    # All received dates should start with the specified year
    assert np.strings.startswith(received_dates, str(year)).all()
    # Check dates are within valid range for that year
    assert all("2020-01-01" <= d <= "2021-12-31" for d in received_dates)


def test_forge_date_pairs_processing_lambda(np_number_generator):
    """Test that processing_lambda affects delay between dates"""
    # Small lambda should have smaller delays
    received_small, processed_small = temporal.forge_date_pairs(
        np_number_generator, size=100, processing_lambda=2
    )
    delays_small = (
        received_small.astype("datetime64[D]") - processed_small.astype("datetime64[D]")
    ).astype(int)

    # Large lambda should have larger delays
    received_large, processed_large = temporal.forge_date_pairs(
        np_number_generator, size=100, processing_lambda=30
    )
    delays_large = (
        received_large.astype("datetime64[D]") - processed_large.astype("datetime64[D]")
    ).astype(int)

    # Mean delay for large lambda should be greater (in absolute value)
    assert np.abs(delays_large).mean() > np.abs(delays_small).mean()


def test_forge_date_pairs_date_format(np_number_generator):
    """Test that dates are in ISO format (YYYY-MM-DD)"""
    received_dates, processed_dates = temporal.forge_date_pairs(
        np_number_generator, size=20
    )

    # Check format by verifying all dates match YYYY-MM-DD pattern
    for date_str in np.concatenate([received_dates, processed_dates]):
        parts = date_str.split("-")
        assert len(parts) == 3
        assert len(parts[0]) == 4  # Year
        assert len(parts[1]) == 2  # Month
        assert len(parts[2]) == 2  # Day


def test_forge_date_pairs_year_boundary(np_number_generator):
    """Test that processed dates can extend into next year"""
    # Generate many dates with large processing delay
    received_dates, processed_dates = temporal.forge_date_pairs(
        np_number_generator, size=500, year=2024, processing_lambda=30
    )

    # At least some processed dates should be in 2025
    has_next_year = any(d.startswith("2025") for d in processed_dates)
    assert has_next_year


def test_forge_date_pairs_minimum_delay(np_number_generator):
    """Test that delay is at least 1 day (due to np.maximum(1, ...))"""
    received_dates, processed_dates = temporal.forge_date_pairs(
        np_number_generator, size=100
    )

    # All processed dates should be strictly greater than received dates
    assert (processed_dates > received_dates).all()
