"""
Chaos generators for creating realistic messy CSV headers
Takes clean DataFrames and introduces common header quality issues
Focuses on column name variations that make schema mapping challenging
"""

import pandas as pd


# Common typos mapping for headers
COMMON_TYPOS = {
    "received": "recieved",
    "processed": "proccessed",
    "sample": "sampl",
    "barcode": "barcod",
    "laboratory": "laborator",
    "total": "totl",
    "organic": "orgainc",
    "carbon": "carban",
    "matter": "mattr",
}


def chaos_header_typos(
    generator, df: pd.DataFrame, probability: float = 0.3
) -> pd.DataFrame:
    """
    Introduce random typos into column names

    Args:
        generator: numpy random generator
        df: Input DataFrame
        probability: Probability of introducing a typo for each matching column

    Returns:
        DataFrame with renamed columns containing typos
    """
    new_columns = []
    for col in df.columns:
        new_col = col
        # Check if any typo keywords are in the column name
        for correct, typo in COMMON_TYPOS.items():
            if correct in col.lower() and generator.random() < probability:
                new_col = col.replace(correct, typo)
                break
        new_columns.append(new_col)

    df.columns = new_columns
    return df


def chaos_header_casing(
    generator, df: pd.DataFrame, probability: float = 0.4
) -> pd.DataFrame:
    """
    Randomly change column name casing

    Args:
        generator: numpy random generator
        df: Input DataFrame
        probability: Probability of changing casing for each column

    Returns:
        DataFrame with inconsistent column casing
    """
    new_columns = []
    for col in df.columns:
        if generator.random() < probability:
            choice = generator.choice(["upper", "lower", "title", "mixed"])
            if choice == "upper":
                new_col = col.upper()
            elif choice == "lower":
                new_col = col.lower()
            elif choice == "title":
                new_col = col.title()
            else:  # mixed - random caps
                new_col = "".join(
                    c.upper() if generator.random() > 0.5 else c.lower() for c in col
                )
            new_columns.append(new_col)
        else:
            new_columns.append(col)

    df.columns = new_columns
    return df


def chaos_header_whitespace(
    generator, df: pd.DataFrame, probability: float = 0.2
) -> pd.DataFrame:
    """
    Add random leading/trailing/internal whitespace to column names

    Args:
        generator: numpy random generator
        df: Input DataFrame
        probability: Probability of adding whitespace to each column

    Returns:
        DataFrame with whitespace in column names
    """
    new_columns = []
    for col in df.columns:
        if generator.random() < probability:
            choice = generator.choice(["leading", "trailing", "both", "internal"])
            if choice == "leading":
                new_col = " " + col
            elif choice == "trailing":
                new_col = col + " "
            elif choice == "both":
                new_col = " " + col + " "
            else:  # internal - add space in middle
                if len(col) > 1:
                    split_point = len(col) // 2
                    new_col = col[:split_point] + " " + col[split_point:]
                else:
                    new_col = col
            new_columns.append(new_col)
        else:
            new_columns.append(col)

    df.columns = new_columns
    return df


def chaos_invalid_db_chars(
    generator, df: pd.DataFrame, probability: float = 0.3
) -> pd.DataFrame:
    """
    Add invalid database characters to column names

    Simulates common spreadsheet practices:
    - # prefix for ID columns (e.g., "id" → "#id")
    - % suffix for percentage columns (e.g., "organic_matter_pct" → "organic_matter%")
    - Hyphens replacing underscores (e.g., "sample_id" → "sample-id")

    Args:
        generator: numpy random generator
        df: Input DataFrame
        probability: Probability of modifying each column

    Returns:
        DataFrame with database-unfriendly column names

    Example:
        >>> gen = np.random.default_rng(42)
        >>> df = pd.DataFrame({"sample_id": [1, 2], "cu_pct": [5.2, 6.1]})
        >>> df_chaos = chaos_invalid_db_chars(gen, df, probability=0.8)
        >>> # Might produce: "sample-id", "cu%"
    """
    new_columns = []
    for col in df.columns:
        if generator.random() < probability:
            new_col = col
            col_lower = col.lower()

            # # prefix for ID columns (must end with _id or be exactly "id")
            if (col_lower.endswith("_id") or col_lower == "id") and not col.startswith(
                "#"
            ):
                new_col = "#" + new_col
            # % suffix for percentage/pct columns
            elif "pct" in col_lower or "percent" in col_lower:
                new_col = new_col.replace("_pct", "%").replace("_percent", "%")
            # Replace underscores with hyphens
            elif "_" in new_col:
                new_col = new_col.replace("_", "-")

            new_columns.append(new_col)
        else:
            new_columns.append(col)

    df.columns = new_columns
    return df


def chaos_metadata_rows(
    generator, df: pd.DataFrame, num_rows: int = None
) -> pd.DataFrame:
    """
    Add metadata/header rows at the top of the DataFrame

    Simulates common vendor CSV files that include metadata like:
    - "Report Generated: 2024-10-15"
    - "Lab Name: Premium Soil Labs"
    - "Contact: lab@example.com"
    - Empty rows before the actual data header

    Args:
        generator: numpy random generator
        df: Input DataFrame
        num_rows: Number of metadata rows to add (if None, randomly choose 1-4)

    Returns:
        DataFrame with metadata rows prepended

    Example:
        >>> gen = np.random.default_rng(42)
        >>> df = pd.DataFrame({"sample_id": [1, 2], "ph": [6.5, 7.0]})
        >>> df_chaos = chaos_metadata_rows(gen, df, num_rows=2)
        >>> # First 2 rows might be: ["Report Date: 2024-10-15", None, ...] and [None, None, ...]
    """
    import numpy as np

    if num_rows is None:
        num_rows = generator.integers(1, 5)  # Random 1-4 metadata rows

    metadata_options = [
        ["Lab Report", None, None],
        ["Generated:", "2024-10-15", None],
        ["Lab Name:", "Premium Soil Testing", None],
        ["Contact:", "lab@testing.com", None],
        ["", "", ""],  # Empty row
        [None, None, None],  # Completely null row
        ["Analysis Date", None, None],
        ["Report ID:", f"RPT{generator.integers(1000, 9999)}", None],
    ]

    # Create metadata rows
    metadata_rows = []
    for _ in range(num_rows):
        # Pick a random metadata pattern
        base_row = list(generator.choice(metadata_options))
        # Pad or truncate to match DataFrame width
        if len(base_row) < len(df.columns):
            base_row.extend([None] * (len(df.columns) - len(base_row)))
        else:
            base_row = base_row[: len(df.columns)]
        metadata_rows.append(base_row)

    # Create DataFrame from metadata rows
    metadata_df = pd.DataFrame(metadata_rows, columns=df.columns)

    # Concatenate metadata rows on top of the original DataFrame
    result = pd.concat([metadata_df, df], ignore_index=True)

    return result


def chaos_empty_column_padding(
    generator, df: pd.DataFrame, num_columns: int = None
) -> pd.DataFrame:
    """
    Add completely empty columns with empty string names

    This simulates when vendors have empty columns with no names, just blank
    column headers. Uses df.insert() to add multiple columns with "" as the name.

    Args:
        generator: numpy random generator
        df: Input DataFrame
        num_columns: Number of empty columns to add (if None, randomly choose 1-3)

    Returns:
        DataFrame with additional empty columns (empty string names, None values)

    Example:
        >>> gen = np.random.default_rng(42)
        >>> df = pd.DataFrame({"sample_id": [1, 2], "ph": [6.5, 7.0]})
        >>> df_chaos = chaos_empty_column_padding(gen, df, num_columns=2)
        >>> # Adds 2 columns with "" as column name, all values None
    """
    import numpy as np

    if num_columns is None:
        num_columns = generator.integers(1, 4)  # Random 1-3 empty columns

    # Add empty columns at the end using insert()
    # Allow duplicate empty string column names
    for i in range(num_columns):
        # Insert at the end (current length is the next index position)
        df.insert(
            len(df.columns),
            "",
            pd.Series([None] * len(df), dtype=object),
            allow_duplicates=True,
        )

    return df


def apply_chaos(
    generator,
    df: pd.DataFrame,
    header_typos: float = 0.3,
    header_casing: float = 0.4,
    header_whitespace: float = 0.2,
    invalid_db_chars: float = 0.0,
    add_metadata_rows: bool = False,
    num_metadata: int = None,
    add_empty_padding: bool = False,
    num_empty: int = None,
) -> pd.DataFrame:
    """
    Apply chaos transformations to a DataFrame

    Args:
        generator: numpy random generator
        df: Input DataFrame
        header_typos: Probability of header typos (0.0 to 1.0)
        header_casing: Probability of header casing changes (0.0 to 1.0)
        header_whitespace: Probability of header whitespace (0.0 to 1.0)
        invalid_db_chars: Probability of invalid database characters (0.0 to 1.0)
        add_metadata_rows: If True, add metadata rows at the top
        num_metadata: Number of metadata rows to add (if None and add_metadata_rows=True, random 1-4)
        add_empty_padding: If True, add empty columns with "" names
        num_empty: Number of empty columns to add (if None and add_empty_padding=True, random 1-3)

    Returns:
        DataFrame with chaotic headers, rows, and columns

    Example:
        >>> from src.labforge import vendors, chaos
        >>> import numpy as np
        >>> gen = np.random.default_rng(42)
        >>> df_clean = vendors.forge_vendor_csv(gen, vendor="vendor_a", packages=["basic"])
        >>> df_messy = chaos.apply_chaos(
        ...     gen, df_clean,
        ...     add_metadata_rows=True,
        ...     add_empty_padding=True
        ... )
        >>> # Now df_messy has: typos, inconsistent casing, whitespace,
        >>> # metadata rows at top, AND empty padding columns!
    """
    # Apply metadata rows FIRST (before header chaos) so the chaos affects metadata too
    if add_metadata_rows:
        df = chaos_metadata_rows(generator, df, num_rows=num_metadata)

    # Apply header chaos transformations
    if header_typos > 0:
        df = chaos_header_typos(generator, df, probability=header_typos)
    if header_casing > 0:
        df = chaos_header_casing(generator, df, probability=header_casing)
    if header_whitespace > 0:
        df = chaos_header_whitespace(generator, df, probability=header_whitespace)
    if invalid_db_chars > 0:
        df = chaos_invalid_db_chars(generator, df, probability=invalid_db_chars)

    # Add empty column padding
    if add_empty_padding:
        df = chaos_empty_column_padding(generator, df, num_columns=num_empty)

    return df
