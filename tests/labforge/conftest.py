import pytest
import numpy as np

@pytest.fixture
def fixed_numpy_seed():
    np.random.seed(42)