# zen_bronze_data

Synthetic vendor CSV data generator with realistic quality issues for demonstrating data engineering challenges and medallion architecture patterns.

## Overview

This project generates realistic biotech/lab vendor CSV files with intentional data quality issues (typos, metadata rows, schema variations) to demonstrate production data engineering patterns without exposing real vendor data.

**Key Features:**
- **LabForge**: Synthetic data generation with realistic chaos
- **Medallion Architecture**: Bronze → Silver → Gold layer demonstrations
- **Schema Variations**: Multiple vendors with different column naming conventions
- **Databricks Asset Bundles**: Easy deployment to Databricks Community Edition

## Project Structure

```
zen_bronze_data/
├── src/
│   ├── labforge/          # Synthetic data generation
│   │   ├── vendors.py     # Vendor-specific schemas and packages
│   │   ├── chaos.py       # Data quality issue generators
│   │   ├── numeric.py     # Numeric measurement generation
│   │   ├── string.py      # Barcode and ID generation
│   │   ├── temporal.py    # Date/time generation
│   │   └── metadata.py    # Dimension and mapping tables
│   └── parse/
│       └── base.py        # CSV parsing utilities
├── notebooks/
│   ├── generate_sample_files.py      # Generate test data
│   └── part1_medallion_demo.py       # Medallion architecture demo
├── tests/                 # Pytest test suite
├── example_data/          # Sample CSV files for local development
├── posts/                 # Blog post content
└── databricks.yml         # Asset Bundle configuration
```

## Getting Started

### Local Development Setup

1. **Install uv** (fast Python package manager):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Create virtual environment and install dependencies**:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv pip install -r requirements/dev.txt
   ```

3. **Run tests**:
   ```bash
   uv run pytest tests/
   ```

4. **Format and lint code**:
   ```bash
   uv run ruff format src/ tests/ notebooks/
   uv run ruff check src/ tests/ notebooks/
   ```

### Databricks Deployment

This project uses **Databricks Asset Bundles** for easy deployment to Databricks Community Edition (or any Databricks workspace).

#### Prerequisites

1. **Databricks CLI** installed and configured:
   You'll need:
   - Workspace URL (e.g., `https://YOUR-WORKSPACE-URL.cloud.databricks.com`)
   - A personal access token
   - Configure databricks dev profile with access token

2. **Update workspace URL** in `databricks.yml`:
   ```yaml
   targets:
     dev:
       workspace:
         host: https://YOUR-WORKSPACE-URL.cloud.databricks.com
   ```

#### Deploy the Bundle

```bash
# Validate the bundle configuration
databricks bundle validate

# Deploy to your workspace
databricks bundle deploy

# Verify deployment
databricks bundle resources list
```

This will create:
- **Schemas**: `workspace.dev_bronze`, `workspace.dev_silver`, `workspace.dev_gold`
- **Volume**: `workspace.dev_bronze.incoming` (for CSV file storage)
- **Job**: "Generate Sample CSV Files" (generates test data)

#### Generate Sample Data

The bundle includes a job that generates all test data (clean files, messy files with typos, metadata rows, etc.):

**Via CLI:**
```bash
databricks jobs run-now --job-id $(databricks bundle resources list -t dev | grep generate_sample_files | awk '{print $2}')
```

**Via UI:**
1. Navigate to **Workflows** → "Generate Sample CSV Files"
2. Click **Run Now**
3. Wait for completion (generates ~10 vendor CSV files with various quality issues)

This creates all necessary files in `/Volumes/workspace/bronze/incoming/`, including:
- Clean vendor files (`vendor_a_basic_clean.csv`, `vendor_b_standard_clean.csv`)
- Messy variants (typos, casing issues, whitespace problems)
- Excel nightmares (metadata rows, empty padding columns)
- Metadata tables (analyte dimensions, vendor-analyte mappings)

**Alternative: Upload Example Files**

If you want to use the pre-generated example data instead:

```bash
# Upload the example CSV files to the incoming volume
databricks fs cp example_data/vendor_a_basic_clean.csv \
  dbfs:/Volumes/workspace/dev_bronze/incoming/vendor_a_basic_clean.csv

databricks fs cp example_data/vendor_b_standard_clean.csv \
  dbfs:/Volumes/workspace/dev_bronze/incoming/vendor_b_standard_clean.csv
```

#### Run the Demo Notebook

Once data is generated:

1. Navigate to **Workspace** → your username → `.bundle/zen_bronze_data/dev/files/notebooks`
2. Open `part1_medallion_demo.py`
3. Attach to a cluster or serverless compute (or create a new one)
4. **Run All** to see Bronze → Silver → Gold in action!

#### Destroy the Bundle (cleanup)

When you're done experimenting:

```bash
databricks bundle destroy -t dev
```

This removes all schemas, volumes, and jobs created by the bundle.

## Development Workflow

### Before Committing

Run the pre-commit checklist:

```bash
# 1. Format code with Ruff
uv run ruff format src/ tests/ notebooks/

# 2. Check for linting issues
uv run ruff check src/ tests/ notebooks/

# 3. Run tests
uv run pytest tests/

# 4. Verify type hints and docstrings are present
```

See `CLAUDE.md` for complete coding standards.

## Project Philosophy

This project demonstrates:
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
- **Schema Evolution**: Handling vendor data with unstable schemas
- **Data Quality**: Realistic test data with intentional issues
- **Privacy-First**: Synthetic data generation for shareable examples

For detailed standards and the full Claud-ia persona guide, see [CLAUDE.md](CLAUDE.md).

## Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Blog Series: Medallion Architecture](posts/)
- [LabForge Data Generation](src/labforge/)

---

*Keep it clean, keep it tested, and keep it just a little bit fabulous.* ✨
