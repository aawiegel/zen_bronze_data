import numpy as np


def forge_distribution(
    generator,
    size: int = 1,
    distribution: str = "normal",
    precision: int = None,
    min_value: float = None,
    max_value: float = None,
    null_probability: float = 0.0,
    **distribution_params,
) -> np.ndarray:
    """
    Generate random values with SIGNIFICANT FIGURE formatting! 🔬

    Parameters:
    -----------
    generator: numpy random number generator
    size : int
        Number of values to generate
    distribution : str
        Distribution type: 'normal', 'gamma', 'poisson', 'uniform'
    precision : int
        Number of SIGNIFICANT FIGURES (not decimal places!)
        e.g., 0.001234 with precision=3 becomes "0.00123"
    min_value : float
        Minimum value (clips before formatting)
    max_value : float
        Maximum value (clips before formatting)
    null_probability : float
        Probability (0.0 to 1.0) of generating null values (MCAR)
        Default is 0.0 (no nulls)
    **distribution_params
        Distribution-specific parameters

    Returns:
    --------
    np.ndarray : Array of STRINGS if precision specified, floats otherwise

    Examples:
    ---------
    >>> forge_distribution(3, 'normal', loc=5.5, scale=0.8, precision=3)
    array(['5.42', '5.67', '5.39'], dtype='<U4')
    """
    # Validate null_probability
    if not 0 <= null_probability <= 1:
        raise ValueError(
            f"null_probability must be between 0 and 1, got {null_probability}"
        )

    # Distribution map
    distribution_map = {
        "normal": generator.normal,
        "gamma": generator.gamma,
        "poisson": generator.poisson,
        "uniform": generator.uniform,
        "exponential": generator.exponential,
        "beta": generator.beta,
    }

    if distribution not in distribution_map:
        raise ValueError(
            f"Unknown distribution: {distribution}. "
            f"Choose from: {list(distribution_map.keys())}"
        )

    # Generate random values
    dist_func = distribution_map[distribution]
    array = dist_func(size=size, **distribution_params)

    # Clip to bounds
    array = np.clip(array, min_value, max_value)

    # Inject nulls (MCAR) before formatting (other types of missing data are not implented yet)
    if null_probability > 0:
        null_mask = generator.random(size) < null_probability
        # Ensure null since some distributions produce integers
        array = array.astype(float)
        array[null_mask] = np.nan

    # Format with significant figures if requested
    if precision is not None:
        # np.format_float_positional works on SCALARS, so vectorize it!
        formatter = np.vectorize(
            lambda x: (
                np.format_float_positional(
                    x, precision=precision, unique=False, fractional=False
                )
                if not np.isnan(x)
                else ""
            )
        )
        array = formatter(array)

    return array
