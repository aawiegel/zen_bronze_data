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
