import numpy as np
from datetime import datetime, timedelta


def forge_date_pairs(
    generator, size: int = 1, year: int = 2024, processing_lambda: int = 7
) -> tuple[np.ndarray, np.ndarray]:
    """
    Generate n date pairs with a random delay between them based on the
    Poisson distribution with a scale of processing lambda

    Parameters:
    -----------
    generator: numpy random number generator
    size: int
        Number of values to generate
    year: int
        Calendar year to create dates for
    processing_lambda: int
        Scaling parameter for the Poisson distribution used to calculate delay in dates

    Returns:
    --------
    np.ndarray, np.ndarray : Arrays of date strings pairs
    """
    start_date = datetime(year, 1, 1)
    end_date = datetime(year + 1, 1, 1)
    days_in_year = (end_date - start_date).days

    random_days = generator.integers(0, days_in_year + 1, size=size)

    processing_days = np.maximum(1, np.random.poisson(processing_lambda, size=size))

    base = np.datetime64(start_date)
    received_dates = base + random_days.astype("timedelta64[D]")
    processed_dates = received_dates + processing_days.astype("timedelta64[D]")

    received_strs = np.datetime_as_string(received_dates, unit="D")
    processed_strs = np.datetime_as_string(processed_dates, unit="D")

    return received_strs, processed_strs
