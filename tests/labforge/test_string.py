import random

import pytest

from src.labforge import string

# What will be generated from randint with fixed_seed
SEEDED_BARCODE = "6374626743"


def test_forge_barcode_default(fixed_numpy_seed):
    result = string.forge_barcode()
    assert result == SEEDED_BARCODE, "Incorrect random barcode generated given random seed"  


def test_forge_barcode_with_prefix(fixed_numpy_seed):
    result = string.forge_barcode(prefix="ABC")
    assert result == f"ABC{SEEDED_BARCODE[:7]}"


def test_forge_barcode_with_specific_length(fixed_numpy_seed):
    result = string.forge_barcode(total_length=5)
    assert result == SEEDED_BARCODE[:5]


def test_forge_barcode_with_placeholder(fixed_numpy_seed):
    result = string.forge_barcode(placeholder_location=2, placeholder="-")
    assert result == f"{SEEDED_BARCODE[:2]}-{SEEDED_BARCODE[2:9]}"


def test_forge_barcode_raises_error_with_invalid_params():
    with pytest.raises(ValueError):
        string.forge_barcode(total_length=3, prefix="AB", placeholder_location=1)


def test_forge_barcode_all_features():
    result = string.forge_barcode(prefix="X", total_length=10, placeholder_location=3, placeholder="-")
    assert result.startswith("X")
    assert result[3] == "-"
    assert len(result) == 10
