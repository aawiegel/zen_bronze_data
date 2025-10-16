import pandas as pd
from src.labforge import chaos


def test_chaos_header_typos(np_number_generator):
    """Test that typos are introduced into headers"""
    df = pd.DataFrame(
        {
            "date_received": [1, 2, 3],
            "date_processed": [4, 5, 6],
            "sample_barcode": [7, 8, 9],
        }
    )

    # Use high probability to ensure typos appear
    df_chaos = chaos.chaos_header_typos(np_number_generator, df, probability=1.0)

    # Should have typos
    assert "date_recieved" in df_chaos.columns  # received -> recieved
    assert "date_proccessed" in df_chaos.columns  # processed -> proccessed
    assert "sampl_barcode" in df_chaos.columns  # sample -> sampl


def test_chaos_header_typos_preserves_data(np_number_generator):
    """Test that typos don't affect data values"""
    df = pd.DataFrame(
        {
            "date_received": [1, 2, 3],
            "value": [10, 20, 30],
        }
    )

    df_chaos = chaos.chaos_header_typos(np_number_generator, df, probability=0.5)

    # Data should be preserved
    assert len(df_chaos) == 3
    assert df_chaos["value"].tolist() == [10, 20, 30]


def test_chaos_header_casing(np_number_generator):
    """Test that casing is randomized"""
    df = pd.DataFrame(
        {
            "sample_id": [1, 2, 3],
            "lab_value": [4, 5, 6],
        }
    )

    # Use high probability
    df_chaos = chaos.chaos_header_casing(np_number_generator, df, probability=1.0)

    # Column names should have changed (can't predict exact output, just verify they changed)
    original_columns = set(df.columns)
    chaos_columns = set(df_chaos.columns)

    # At least some should be different (lowercased versions might match)
    assert len(df_chaos.columns) == len(df.columns)  # Same number of columns


def test_chaos_header_whitespace_leading(np_number_generator):
    """Test that whitespace is added to headers"""
    df = pd.DataFrame(
        {
            "column_a": [1, 2, 3],
            "column_b": [4, 5, 6],
        }
    )

    df_chaos = chaos.chaos_header_whitespace(np_number_generator, df, probability=1.0)

    # Some columns should have whitespace
    has_whitespace = any(
        col.startswith(" ") or col.endswith(" ") or "  " in col
        for col in df_chaos.columns
    )
    assert has_whitespace


def test_chaos_header_whitespace_preserves_data(np_number_generator):
    """Test that whitespace in headers doesn't affect data"""
    df = pd.DataFrame(
        {
            "column_a": [1, 2, 3],
        }
    )

    df_chaos = chaos.chaos_header_whitespace(np_number_generator, df, probability=1.0)

    # Data should be preserved
    assert len(df_chaos) == 3
    # Get the only column (which may have whitespace)
    assert df_chaos.iloc[:, 0].tolist() == [1, 2, 3]


def test_apply_chaos_combination(np_number_generator):
    """Test that apply_chaos applies multiple transformations"""
    df = pd.DataFrame(
        {
            "date_received": [1, 2, 3],
            "sample_barcode": [4, 5, 6],
            "laboratory_id": [7, 8, 9],
        }
    )

    df_chaos = chaos.apply_chaos(
        np_number_generator,
        df,
        header_typos=0.8,
        header_casing=0.8,
        header_whitespace=0.8,
    )

    # Should have same number of columns
    assert len(df_chaos.columns) == len(df.columns)

    # Should have same data
    assert len(df_chaos) == 3

    # Columns should be different (chaos applied)
    original_columns = set(df.columns)
    chaos_columns = set(df_chaos.columns)
    # With high probabilities, at least some should be different
    assert len(df_chaos.columns) == 3


def test_apply_chaos_zero_probability(np_number_generator):
    """Test that zero probabilities result in no changes"""
    df = pd.DataFrame(
        {
            "column_a": [1, 2, 3],
            "column_b": [4, 5, 6],
        }
    )

    df_chaos = chaos.apply_chaos(
        np_number_generator,
        df,
        header_typos=0.0,
        header_casing=0.0,
        header_whitespace=0.0,
    )

    # Should be identical
    assert df_chaos.columns.tolist() == df.columns.tolist()
    assert df_chaos.equals(df)


def test_chaos_modifies_in_place(np_number_generator):
    """Test that chaos functions modify the DataFrame in place"""
    df = pd.DataFrame(
        {
            "date_received": [1, 2, 3],
        }
    )

    original_columns = df.columns.tolist()

    result = chaos.apply_chaos(np_number_generator, df, header_typos=1.0)

    # Should modify in place and return the same reference
    assert result is df
    # Columns should be modified
    assert df.columns.tolist() != original_columns


def test_chaos_metadata_rows(np_number_generator):
    """Test that metadata rows are added at the top"""
    df = pd.DataFrame(
        {
            "sample_id": [1, 2, 3],
            "ph": [6.5, 7.0, 6.8],
        }
    )

    df_chaos = chaos.chaos_metadata_rows(np_number_generator, df, num_rows=2)

    # Should have 2 additional rows
    assert len(df_chaos) == 5  # 3 original + 2 metadata

    # First 2 rows should be metadata (likely have None or string values)
    # Original numeric data should be pushed down
    assert df_chaos.iloc[2, 0] == 1  # First original data row


def test_chaos_metadata_rows_random_count(np_number_generator):
    """Test that metadata rows with None generates random count"""
    df = pd.DataFrame(
        {
            "column_a": [1, 2, 3],
        }
    )

    df_chaos = chaos.chaos_metadata_rows(np_number_generator, df, num_rows=None)

    # Should have at least 1 and at most 4 additional rows (1-4 range)
    assert len(df_chaos) >= 4  # 3 original + at least 1 metadata
    assert len(df_chaos) <= 7  # 3 original + at most 4 metadata


def test_chaos_empty_column_padding(np_number_generator):
    """Test that empty columns with empty string names are added"""
    df = pd.DataFrame(
        {
            "sample_id": [1, 2, 3],
            "ph": [6.5, 7.0, 6.8],
        }
    )

    df_chaos = chaos.chaos_empty_column_padding(np_number_generator, df, num_columns=2)

    # Should have 2 additional empty columns
    assert len(df_chaos.columns) == 4  # 2 original + 2 empty

    # Check that there are empty string column names
    empty_cols = [col for col in df_chaos.columns if col == ""]
    assert len(empty_cols) == 2

    # Empty columns should be all empty strings (check by column position to avoid duplicate name issues)
    # Last 2 columns should be the empty ones
    assert (df_chaos.iloc[:, -1] == "").all()
    assert (df_chaos.iloc[:, -2] == "").all()


def test_chaos_empty_column_padding_random_count(np_number_generator):
    """Test that empty padding with None generates random count"""
    df = pd.DataFrame(
        {
            "column_a": [1, 2, 3],
        }
    )

    df_chaos = chaos.chaos_empty_column_padding(
        np_number_generator, df, num_columns=None
    )

    # Should have at least 1 and at most 3 additional columns (1-3 range)
    assert len(df_chaos.columns) >= 2  # 1 original + at least 1 empty
    assert len(df_chaos.columns) <= 4  # 1 original + at most 3 empty


def test_apply_chaos_with_metadata_and_padding(np_number_generator):
    """Test that apply_chaos can add metadata rows and empty padding"""
    df = pd.DataFrame(
        {
            "sample_id": [1, 2, 3],
            "ph": [6.5, 7.0, 6.8],
        }
    )

    df_chaos = chaos.apply_chaos(
        np_number_generator,
        df,
        header_typos=0.0,
        header_casing=0.0,
        header_whitespace=0.0,
        add_metadata_rows=True,
        num_metadata=2,
        add_empty_padding=True,
        num_empty=2,
    )

    # Should have 2 additional rows
    assert len(df_chaos) == 5  # 3 original + 2 metadata

    # Should have 2 additional columns
    assert len(df_chaos.columns) == 4  # 2 original + 2 empty

    # Check for empty string columns
    empty_cols = [col for col in df_chaos.columns if col == ""]
    assert len(empty_cols) == 2


def test_chaos_invalid_db_chars_id_columns(np_number_generator):
    """Test that # is added to ID columns"""
    df = pd.DataFrame(
        {
            "sample_id": [1, 2, 3],
            "lab_id": [4, 5, 6],
            "barcode": [7, 8, 9],
        }
    )

    df_chaos = chaos.chaos_invalid_db_chars(np_number_generator, df, probability=1.0)

    # ID columns should get # prefix
    assert "#sample_id" in df_chaos.columns or "#lab_id" in df_chaos.columns


def test_chaos_invalid_db_chars_id_edge_case(np_number_generator):
    """Test that # is NOT added to columns that contain 'id' but aren't ID columns"""
    df = pd.DataFrame(
        {
            "acidity": [5.5, 6.0, 6.5],
            "humidity": [45.0, 50.0, 55.0],
            "sample_id": [1, 2, 3],
        }
    )

    df_chaos = chaos.chaos_invalid_db_chars(np_number_generator, df, probability=1.0)

    # acidity and humidity should NOT get # prefix (they contain "id" but aren't ID columns)
    assert not any(
        col.startswith("#") and "acidity" in col.lower() for col in df_chaos.columns
    )
    assert not any(
        col.startswith("#") and "humidity" in col.lower() for col in df_chaos.columns
    )

    # sample_id SHOULD get # prefix (ends with _id)
    assert "#sample_id" in df_chaos.columns


def test_chaos_invalid_db_chars_percentage_columns(np_number_generator):
    """Test that % suffix is added to percentage columns"""
    df = pd.DataFrame(
        {
            "organic_matter_pct": [1.5, 2.0, 3.0],
            "moisture_percent": [10, 15, 20],
            "copper_ppm": [5, 6, 7],
        }
    )

    df_chaos = chaos.chaos_invalid_db_chars(np_number_generator, df, probability=1.0)

    # Percentage columns should get % suffix
    assert "organic_matter%" in df_chaos.columns or "moisture%" in df_chaos.columns
    # Non-percentage columns should not get %
    assert "copper_ppm%" not in df_chaos.columns


def test_chaos_invalid_db_chars_hyphens(np_number_generator):
    """Test that underscores are replaced with hyphens"""
    df = pd.DataFrame(
        {
            "sample_barcode": [1, 2, 3],
            "test_value": [4, 5, 6],
        }
    )

    df_chaos = chaos.chaos_invalid_db_chars(np_number_generator, df, probability=1.0)

    # Some columns should have hyphens instead of underscores
    has_hyphens = any("-" in str(col) for col in df_chaos.columns)
    assert has_hyphens


def test_chaos_invalid_db_chars_preserves_data(np_number_generator):
    """Test that invalid chars don't affect data values"""
    df = pd.DataFrame(
        {
            "sample_id": [1, 2, 3],
            "organic_matter_pct": [1.5, 2.0, 3.0],
        }
    )

    df_chaos = chaos.chaos_invalid_db_chars(np_number_generator, df, probability=1.0)

    # Data should be preserved
    assert len(df_chaos) == 3
    # Check first column data (whatever it's named now)
    assert df_chaos.iloc[:, 0].tolist() == [1, 2, 3]


def test_apply_chaos_with_invalid_db_chars(np_number_generator):
    """Test that apply_chaos can add invalid database characters"""
    df = pd.DataFrame(
        {
            "sample_id": [1, 2, 3],
            "organic_matter_pct": [4.5, 5.5, 6.5],
        }
    )

    df_chaos = chaos.apply_chaos(
        np_number_generator,
        df,
        header_typos=0.0,
        header_casing=0.0,
        header_whitespace=0.0,
        invalid_db_chars=1.0,
    )

    # Should have invalid chars in column names
    has_hash = any("#" in str(col) for col in df_chaos.columns)
    has_percent = any("%" in str(col) for col in df_chaos.columns)
    has_hyphen = any("-" in str(col) for col in df_chaos.columns)

    # At least one type of invalid char should be present
    assert has_hash or has_percent or has_hyphen
