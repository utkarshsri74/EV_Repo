# Schema Evolution Process

This document describes the process for handling schema changes in the Electric Vehicle Population Data pipeline, ensuring controlled updates to the data schema while maintaining data integrity.

## Overview

The pipeline validates the schema of incoming JSON data against a predefined set of expected columns (defined in `constants.py` as `EXPECTED_COLUMNS`). If new or unexpected columns are detected, the pipeline fails, logs the issue, and requires manual intervention to approve schema changes.

## Schema Change Process

1. **Detection of New Columns**:
   - During the silver layer pipeline (`silver.py`), the `validate_schema` function (`utils.py`) compares DataFrame columns against `EXPECTED_COLUMNS`.
   - If unexpected columns are found (i.e., columns not in `EXPECTED_COLUMNS`), the pipeline:
     - Logs the unexpected columns to `schema_evolution_log.txt` in ADLS at `abfss://raw@studydatalakedevsa.dfs.core.windows.net/schema_evolution_log.txt`.
     - Raises a `ValueError` with a message like: "Unexpected columns [list]. Please review schema_evolution_log.txt and update EXPECTED_COLUMNS."
     - Halts execution to prevent unapproved data from proceeding.

2. **Review and Approval**:
   - The data engineer reviews the `schema_evolution_log.txt` file to identify new columns.
   - The engineer assesses the new columns for relevance and impact:
     - Are the columns valid additions (e.g., new data fields like "Battery Type")?
     - Do they require changes to downstream transformations or curated layer logic?
   - If approved, the engineer updates `EXPECTED_COLUMNS` in `constants.py` to include the new columns.

3. **Implementation**:
   - Update `constants.py` with the new columns (e.g., append `"Battery Type"` to `EXPECTED_COLUMNS`).
   - If necessary, modify downstream logic (e.g., `curated_tfm.py`) to handle new columns in aggregations or derived fields.
   - Test the pipeline in a development environment to ensure the new schema is processed correctly.

4. **Deployment**:
   - Commit changes to the Git repository.
   - Deploy the updated code to UAT for validation.
   - Promote to production after successful testing.

## Example Scenario

- **Event**: A new JSON file includes a "Battery Type" column.
- **Pipeline Response**:
  - `validate_schema` detects "Battery Type" as unexpected.
  - Logs to `schema_evolution_log.txt`: "2025-04-11 10:00:00: Unexpected columns detected: ['Battery Type']"
  - Pipeline fails with error: "Unexpected columns ['Battery Type']. Please review schema_evolution_log.txt and update EXPECTED_COLUMNS."
- **Engineer Action**:
  - Reviews log and confirms "Battery Type" is a valid addition.
  - Updates `EXPECTED_COLUMNS` in `constants.py`:
    ```python
    EXPECTED_COLUMNS = [
        ..., # Existing columns
        "Battery Type"
    ]