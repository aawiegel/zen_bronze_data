from src.labforge import temporal
import numpy as np


def test_forge_date_pairs(np_number_generator):
    received_dates, processed_dates = temporal.forge_date_pairs(np_number_generator)
    assert processed_dates > received_dates
    assert np.strings.startswith(received_dates, '2024').all()
