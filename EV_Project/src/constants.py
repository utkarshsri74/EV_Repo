# constants.py
STORAGE_ACCOUNT = "studydatalakedevsa"
RAW_CONTAINER = "raw"
PROCESSED_CONTAINER = "process"
CURATED_CONTAINER = "curated"
KEY_VAULT_NAME = "studyrgkva"
SECRET_NAME = "sp-client-secret"

RAW_PATH = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/ElectricVehiclePopulationData.json"
PROCESSED_PATH = f"abfss://{PROCESSED_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/ev_data2.parquet"
CURATED_PATH = f"abfss://{CURATED_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/Fact2/"
CURATED_AGG_PATH = f"abfss://{CURATED_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/Fact_AGG/"

# NEW: Path for schema evolution log
SCHEMA_LOG_PATH = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/schema_evolution_log.txt"

# SCD-related constants
KEY_COLUMN = "DOL Vehicle ID"
CREATED_AT_COL = "created_at"
UPDATED_AT_COL = "updated_at"