# Coding Standards for zen_bronze_data

This project generates synthetic vendor CSV data with realistic quality issues for demonstrating data engineering challenges. Keep the code clean, tested, and just a little bit sassy.

## Code Formatting

- **Use Black** for all Python code formatting
  - Run `black src/ tests/ notebooks/` before committing
  - Line length: 88 characters (Black default)
  - Follow PEP 8 conventions

## Type Hints

- **Always include type hints** for function parameters and return values
- Use modern Python type hints (e.g., `list[str]` instead of `List[str]`)
- Examples:
  ```python
  def forge_vendor_csv(
      generator,
      vendor: str = "vendor_a",
      packages: list[str] = None,
      rows: int = 50
  ) -> pd.DataFrame:
  ```

## Documentation

### Docstrings

- **Every function must have a docstring** with:
  - Brief description of what it does
  - Args section with parameter descriptions
  - Returns section describing the output
  - Example usage when helpful

- **Format:**
  ```python
  def chaos_header_typos(generator, df: pd.DataFrame, probability: float = 0.3) -> pd.DataFrame:
      """
      Introduce random typos into column names

      Args:
          generator: numpy random generator
          df: Input DataFrame
          probability: Probability of introducing a typo for each matching column

      Returns:
          DataFrame with renamed columns containing typos

      Example:
          >>> gen = np.random.default_rng(42)
          >>> df = pd.DataFrame({"received_date": [1, 2, 3]})
          >>> df_messy = chaos_header_typos(gen, df, probability=0.8)
      """
  ```

## Variable Naming

- Use **descriptive, semantic names** that make the code self-documenting
- A little personality is encouraged, but keep it professional
- Good examples:
  - `df_chaos` (descriptive + adds character)
  - `forge_vendor_csv` (action-oriented, clear purpose)
  - `COMMON_TYPOS` (constants in CAPS)
  - `new_columns` (clear and purposeful)
- Avoid:
  - Single letters (except in loops: `i`, `col`)
  - Overly cute names that obscure meaning
  - Names that require comments to understand

## Testing

- **Write tests for all new functions**
- Use pytest conventions
- Test files mirror source structure: `src/labforge/vendors.py` → `tests/labforge/test_vendors.py`
- Include:
  - Happy path tests
  - Edge cases
  - Error conditions (when applicable)
- Aim for clear, descriptive test names:
  ```python
  def test_chaos_header_typos_preserves_data(np_number_generator):
      """Test that typos don't affect data values"""
  ```

## Random Number Generation

- **Always use the new numpy Generator pattern**, not legacy `np.random` methods
- Pass generator as parameter, don't create inside functions
- Example:
  ```python
  # Good
  def forge_data(generator, size: int) -> np.ndarray:
      return generator.normal(loc=5, scale=1, size=size)

  # Bad
  def forge_data(size: int) -> np.ndarray:
      return np.random.normal(loc=5, scale=1, size=size)
  ```

## Data Handling

- **Avoid unnecessary DataFrame copies** when modifying in place
- When functions modify DataFrames, they should modify in place and return the reference
- Example:
  ```python
  def chaos_header_typos(generator, df: pd.DataFrame, probability: float = 0.3) -> pd.DataFrame:
      new_columns = [...]
      df.columns = new_columns  # Modify in place
      return df  # Return reference
  ```

## Module Organization

- Keep related functionality together
- Use clear module names that indicate purpose:
  - `vendors.py` - Vendor-specific data generation
  - `chaos.py` - Data quality issue generators
  - `numeric.py` - Numeric data generation
  - `string.py` - String/barcode generation
  - `temporal.py` - Date/time generation

## Comments

- Code should be self-documenting through good naming
- Add comments for:
  - Complex algorithms
  - Non-obvious business logic
  - Workarounds or known limitations
- Don't comment obvious things:
  ```python
  # Bad
  x = x + 1  # Increment x

  # Good
  # Vendor B uses different assay methods for the same analyte
  "cu_total": {...}  # same as copper_ppm from Vendor A
  ```

## Precision and Scientific Notation

- Use **at least 3 significant figures** for all analyte measurements
- This reflects realistic lab reporting precision
- Specify precision in distribution configs:
  ```python
  "ph": {"min_value": 5.0, "max_value": 8.0, "loc": 6.5, "scale": 0.5, "precision": 3}
  ```

## Before Committing

1. Run `black src/ tests/ notebooks/`
2. Run `pytest tests/` - all tests must pass
3. Ensure new functions have type hints and docstrings
4. Update tests if you modified functionality

---

*Remember: Keep it clean, keep it tested, and keep it just a little bit fabulous.* ✨
