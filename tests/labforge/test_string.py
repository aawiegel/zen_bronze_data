import random

import pytest

from src.labforge import string

# What will be generated from randint with fixed_seed
SEEDED_BARCODE = "0653370610"


def test_forge_barcode_default(np_number_generator):
    result = string.forge_barcode(np_number_generator)
    assert result == SEEDED_BARCODE, (
        "Incorrect random barcode generated given random seed"
    )


def test_forge_barcode_with_prefix(np_number_generator):
    result = string.forge_barcode(np_number_generator, prefix="ABC")
    assert result == f"ABC{SEEDED_BARCODE[:7]}"


def test_forge_barcode_with_specific_length(np_number_generator):
    result = string.forge_barcode(np_number_generator, total_length=5)
    assert result == SEEDED_BARCODE[:5]


def test_forge_barcode_with_placeholder(np_number_generator):
    result = string.forge_barcode(
        np_number_generator, placeholder_location=2, placeholder="-"
    )
    assert result == f"{SEEDED_BARCODE[:2]}-{SEEDED_BARCODE[2:9]}"


def test_forge_barcode_raises_error_with_invalid_params(np_number_generator):
    with pytest.raises(ValueError):
        string.forge_barcode(
            np_number_generator, total_length=3, prefix="AB", placeholder_location=1
        )


def test_forge_barcode_all_features(np_number_generator):
    result = string.forge_barcode(
        np_number_generator,
        prefix="X",
        total_length=10,
        placeholder_location=3,
        placeholder="-",
    )
    assert result.startswith("X")
    assert result[3] == "-"
    assert len(result) == 10


def test_forge_barcodes_returns_correct_count(np_number_generator):
    result = string.forge_barcodes(np_number_generator, 10)
    assert len(result) == 10


def test_forge_barcodes_with_parameters(np_number_generator):
    result = string.forge_barcodes(
        np_number_generator,
        5,
        prefix="LAB",
        total_length=12,
        placeholder_location=6,
        placeholder="-",
    )
    assert len(result) == 5
    assert all(bc.startswith("LAB") for bc in result)
    assert all("-" in bc for bc in result)
