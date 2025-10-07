import numpy as np
import pytest
from src.labforge import numeric


def test_forge_distribution_sig_figs(np_number_generator):
    """Sig figs work on large numbers too"""
    result = numeric.forge_distribution(
        np_number_generator,
        size=5,
        distribution="uniform",
        low=1000,
        high=10000,
        precision=3,  # 3 sig figs
    )

    # e.g., "6420" -> "6.42e+03" or "6420" (depends on numpy version)
    assert result.dtype.kind == "U"

    for value_str in result:
        value = float(value_str)
        assert 1000 <= value <= 10000


def test_forge_distribution_clips_before_formatting(np_number_generator):
    """Clipping should happen BEFORE sig fig formatting"""
    result = numeric.forge_distribution(
        np_number_generator,
        size=100,
        distribution="normal",
        loc=5,
        scale=3,
        min_value=4.0,
        max_value=6.0,
        precision=3,
    )

    # All values should be strings
    assert result.dtype.kind == "U"

    # When converted back to float, should respect bounds
    float_values = result.astype(float)
    assert float_values.min() >= 4.0
    assert float_values.max() <= 6.0


def test_forge_distribution_with_all_parameters(np_number_generator):
    """Kitchen sink test - everything at once"""
    result = numeric.forge_distribution(
        np_number_generator,
        size=20,
        distribution="gamma",
        shape=2.0,
        scale=0.5,
        min_value=0.5,
        max_value=3.0,
        precision=4,
    )

    assert len(result) == 20
    assert result.dtype.kind == "U"

    float_values = result.astype(float)
    assert float_values.min() >= 0.5
    assert float_values.max() <= 3.0


def test_forge_distribution_null_probability_basic(np_number_generator):
    """Test that null_probability generates expected proportion of nulls"""
    result = numeric.forge_distribution(
        np_number_generator,
        size=1000,
        distribution="normal",
        loc=5.0,
        scale=1.0,
        null_probability=0.3,
    )

    # Should be float array with NaN values
    assert result.dtype.kind == "f"
    null_count = np.isnan(result).sum()
    # With 1000 samples and p=0.3, expect roughly 300 nulls (allow 20-40% range)
    assert 200 <= null_count <= 400


def test_forge_distribution_null_probability_with_precision(np_number_generator):
    """Test that nulls become empty strings when precision is set"""
    result = numeric.forge_distribution(
        np_number_generator,
        size=100,
        distribution="uniform",
        low=0.0,
        high=10.0,
        null_probability=0.5,
        precision=3,
    )

    # Should be string array
    assert result.dtype.kind == "U"
    # Empty strings represent nulls
    empty_count = (result == "").sum()
    # With 100 samples and p=0.5, expect roughly 50 empty strings (allow 30-70% range)
    assert 30 <= empty_count <= 70


def test_forge_distribution_null_probability_zero(np_number_generator):
    """Test that null_probability=0.0 generates no nulls"""
    result = numeric.forge_distribution(
        np_number_generator,
        size=100,
        distribution="normal",
        loc=5.0,
        scale=1.0,
        null_probability=0.0,
    )

    assert not np.isnan(result).any()


def test_forge_distribution_null_probability_one(np_number_generator):
    """Test that null_probability=1.0 generates all nulls"""
    result = numeric.forge_distribution(
        np_number_generator,
        size=50,
        distribution="normal",
        loc=5.0,
        scale=1.0,
        null_probability=1.0,
    )

    assert np.isnan(result).all()


def test_forge_distribution_invalid_null_probability_high(np_number_generator):
    """Test that null_probability > 1.0 raises ValueError"""
    with pytest.raises(ValueError, match="null_probability must be between 0 and 1"):
        numeric.forge_distribution(
            np_number_generator,
            size=10,
            distribution="normal",
            null_probability=1.5,
        )


def test_forge_distribution_invalid_null_probability_negative(np_number_generator):
    """Test that null_probability < 0.0 raises ValueError"""
    with pytest.raises(ValueError, match="null_probability must be between 0 and 1"):
        numeric.forge_distribution(
            np_number_generator,
            size=10,
            distribution="normal",
            null_probability=-0.1,
        )


def test_forge_distribution_invalid_distribution(np_number_generator):
    """Test that invalid distribution name raises ValueError"""
    with pytest.raises(ValueError, match="Unknown distribution"):
        numeric.forge_distribution(
            np_number_generator,
            size=10,
            distribution="invalid_dist",
        )
