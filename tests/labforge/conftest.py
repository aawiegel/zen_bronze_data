import pytest
import numpy as np


@pytest.fixture
def np_number_generator():
    return np.random.default_rng(seed=42)
