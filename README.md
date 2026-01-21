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
   ```bash
   # Install Databricks CLI
   pip install databricks-cli

   # Configure authentication
   databricks configure --token
   ```
   You'll need:
   - Workspace URL (e.g., `https://dbc-81f4a118-bb2c.cloud.databricks.com`)
   - Personal Access Token (generate in User Settings → Access Tokens)

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
databricks bundle deploy -t dev

# Verify deployment
databricks bundle resources list -t dev
```

This will create:
- **Schemas**: `workspace.bronze`, `workspace.silver`, `workspace.gold`
- **Volume**: `workspace.bronze.incoming` (for CSV file storage)
- **Job**: "Generate Sample CSV Files" (generates test data)

#### Upload Sample Data

Upload the example CSV files to the incoming volume:

```bash
# Using Databricks CLI
databricks fs cp example_data/vendor_a_basic_clean.csv \
  dbfs:/Volumes/workspace/bronze/incoming/vendor_a_basic_clean.csv

databricks fs cp example_data/vendor_b_standard_clean.csv \
  dbfs:/Volumes/workspace/bronze/incoming/vendor_b_standard_clean.csv
```

Or use the Databricks UI:
1. Navigate to **Catalog** → **workspace** → **bronze** → **incoming**
2. Click **Upload** and select your CSV files

#### Run the Notebooks

1. **Generate Sample Files** (optional - creates messy test data):
   ```bash
   databricks jobs run-now --job-id $(databricks bundle resources list -t dev | grep generate_sample_files | awk '{print $2}')
   ```

   Or via UI: **Workflows** → "Generate Sample CSV Files" → **Run Now**

2. **Medallion Demo Notebook**:
   - Navigate to **Workspace** → your username → `.bundle/zen_bronze_data/dev/files/notebooks`
   - Open `part1_medallion_demo.py`
   - Attach to a cluster and **Run All**

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
