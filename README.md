# Oracle Data Export Script - MERGE Statement Generator

This script connects to an Oracle database, extracts data from specified tables based on user-provided primary key values, and generates MERGE statements for data migration.

## Features

- YAML-based configuration (no more command-line flags!)
- Support for multiple tables in a single run
- Interactive primary key value filtering
- Shared column values across tables
- Automatic MERGE statement generation
- OWNER.TABLE format support

## Requirements

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Create a `config.yaml` file (or use the provided sample):

```yaml
# Database Connection Settings
database:
  username: Dual
  password: dual
  dsn: 11.111.111.120:1521/OraceleDB  # format: host:port/service_name

# Output Configuration
output:
  file: ./result.sql

# Tables to Export (OWNER.TABLE format)
tables:
  - SYSTEM.CLIENTS
  - SYSTEM.ACCOUNTS
  - SYSTEM.USERS
  # Add more tables here...

# Shared Column Values (Optional)
# These values will be automatically used when exporting tables that have these columns
shared_values:
  # Example:
  # CLIENT_ID: 12345
  # START_DATE: 2024-01-01
```

### Configuration Sections

#### `database` (required)
- `username`: Oracle database username
- `password`: Oracle database password
- `dsn`: Database connection string in format `host:port/service_name`

#### `output` (optional)
- `file`: Path to the output SQL file (default: `./oracle_merge_exports/merge_export.sql`)

#### `tables` (required)
- List of tables to export
- **Must be in OWNER.TABLE format** (e.g., `SYSTEM.USERS`)
- Tables are processed in the order listed

#### `shared_values` (optional)
- Column names and values that apply across multiple tables
- Values are automatically used when a table has matching column names
- You can still add more shared values interactively when the script runs

## Usage

### Basic Usage

Run with default `config.yaml`:

```bash
python fastExportV2.py
```

### Custom Configuration File

Specify a custom configuration file:

```bash
python fastExport.py -c custom_config.yaml
```

Or:

```bash
python fastExport.py --config custom_config.yaml
```

## Interactive Prompts

When the script runs, it will:

1. **Load configuration** from the YAML file
2. **Connect to the database** using credentials from config
3. **Display shared values** from config (if any)
4. **Prompt for additional shared values** (optional)
5. **Process each table**:
   - Show primary key columns
   - Apply shared values automatically
   - Prompt for additional filters (if needed)
   - Generate MERGE statements

## Example Workflow

```bash
$ python fastExportV2.py

======================================================================
Oracle Data Export Script - MERGE Statement Generator
======================================================================

Successfully loaded configuration from config.yaml
  Database: 11.125.111.120:1521/OracleDB
  Output file: ./results.sql
  Tables to export: 38
Successfully connected to Oracle Database 19.0.0.0.0

=== SHARED COLUMN VALUES FROM CONFIG ===
  • CLIENT_ID = 12345

=== SHARED COLUMN VALUES SETUP ===
Enter column names and values that may be shared across multiple tables.
...
```

## Output

The script generates a SQL file containing:
- Header with generation timestamp
- MERGE statements for each exported table
- Final COMMIT statement
- Summary comments

## Troubleshooting

### "Config file must contain 'database' section"
- Ensure your YAML file has a `database:` section with `username`, `password`, and `dsn`

### "Table must be in OWNER.TABLE format"
- All tables must include the schema owner (e.g., `SYSTEM.USERS`, not just `USERS`)

### "Configuration file 'config.yaml' not found"
- Create a `config.yaml` file in the current directory, or specify the path with `-c`

## Notes

- The script validates all table names before processing
- Duplicate tables in the list will trigger additional prompts
- Press ENTER when prompted for a PK value to skip filtering on that column
- The output file is overwritten on each run

---

# Oracle Data Deletion Script (fastDelete.py)

A safe DELETE statement generator for Oracle databases. This script connects to your database, previews data to be deleted, and **generates DELETE statements to a file for manual review and execution**.

## Key Safety Features

- **DOES NOT execute deletions** - only generates SQL statements to a file
- Preview of data before generating DELETE statements
- Interactive primary key filtering
- Row count preview for each table
- All DELETE statements saved to file for review before execution

## Usage

### Basic Usage

Generate DELETE statements using default config.yaml:

```bash
python fastDelete.py
```

### Custom Configuration

```bash
python fastDelete.py -c custom_config.yaml
```

### Custom Output File

```bash
python fastDelete.py -o my_deletions.sql
```

Or combine options:

```bash
python fastDelete.py -c custom_config.yaml -o my_deletions.sql
```

## How It Works

1. Connects to the database using credentials from `config.yaml`
2. For each table:
   - Displays primary key columns
   - Prompts for filter values (press ENTER to skip a column)
   - Shows preview of rows that would be deleted
   - Generates DELETE statement with filters
3. Writes all DELETE statements to output file
4. You manually review and execute the SQL file when ready

## Configuration

fastDelete.py uses the same `config.yaml` format as fastExport.py:

```yaml
database:
  username: Dual
  password: dual
  dsn: 11.111.121.120:1521/OracleDB

output:
  file: ./oracle_delete_statements/delete_statements.sql

tables:
  - SYSTEM.CLIENTS
  - SYSTEM.ACCOUNTS
  - SYSTEM.USERS
```

## Command-Line Options

| Option | Description |
|--------|-------------|
| `-c, --config FILE` | Path to YAML configuration file (default: config.yaml) |
| `-o, --output-file FILE` | Output file for DELETE statements (overrides config) |

## Interactive Prompts

1. **Primary Key Filter Configuration**: For each table, enter values for primary key columns
   - Press ENTER to skip a column (DELETE statement will target ALL values for that column)
   - Only provide values for columns you want to filter on
   - Date format: YYYY-MM-DD

2. **Preview**: The script shows:
   - Total number of rows that would be deleted
   - Preview of first 5 rows that match the criteria

3. **Statement Generation**: DELETE statement is generated with your filters

## Example Output

```bash
$ python fastDelete.py

================================================================================
Oracle Data Deletion Script - DELETE Statement Generator
(DRY-RUN ONLY - Generates SQL file, does NOT execute deletions)
================================================================================

Successfully loaded configuration from config.yaml
  Database: 11.120.111.123:1521/OracleDB
  Tables to process: 3

============================================================
CONFIGURING DELETION FILTERS FOR: SYSTEM.CLIENTS
============================================================

Primary Key Columns:
  1. CLIENT_ID (NUMBER)

⚠️  DELETION FILTER CONFIGURATION:
   • Press ENTER to skip a primary key column (will DELETE ALL rows for that column)
   • Only specify values for columns you want to filter on

🔑 Enter value for primary key column CLIENT_ID (NUMBER) [ENTER to skip]: 12345
  → ✅ Will filter CLIENT_ID = 12345

🎯 DELETION SUMMARY FOR SYSTEM.CLIENTS:
   • Will filter on 1 primary key column(s)
     - CLIENT_ID = 12345

📊 DELETION PREVIEW for SYSTEM.CLIENTS:
   • Total rows to be deleted: 1
   • Preview of first few rows to be deleted:
     CLIENT_ID | CLIENT_NAME | DATE_CREATED
     ---------------------------------
     12345 | Test Client | 2024-01-15

[... continues for other tables ...]

✅ Successfully generated 3 DELETE statements in ./oracle_delete_statements/delete_statements.sql

🔌 Database connection closed.

================================================================================
Summary:
================================================================================
DELETE statements generated for 3 tables
Output file: ./oracle_delete_statements/delete_statements.sql

⚠️  IMPORTANT: Review the generated SQL file before executing!
================================================================================
```

## Generated SQL File Format

The output SQL file contains:

```sql
-- Oracle DELETE statements
-- Generated on 2024-01-15 10:30:00

SET DEFINE OFF;

-- DELETE statement for SYSTEM.CLIENTS
-- Generated on 2024-01-15 10:30:00
DELETE FROM SYSTEM.CLIENTS WHERE CLIENT_ID = 12345;

-- DELETE statement for SYSTEM.ACCOUNTS
-- Generated on 2024-01-15 10:30:01
DELETE FROM SYSTEM.ACCOUNTS WHERE ACCOUNT_ID = 67890;

-- Total DELETE statements generated: 2
-- COMMIT; -- Uncomment to commit the deletions
-- ROLLBACK; -- Uncomment to rollback the deletions
```

## Executing the Generated SQL

After reviewing the generated SQL file:

1. **Review carefully** - ensure the DELETE statements target the correct rows
2. **Connect to your database** using SQL*Plus, SQL Developer, or your preferred tool
3. **Execute the SQL file** or copy/paste statements
4. **Verify deletions** by checking row counts
5. **COMMIT or ROLLBACK** as appropriate

Example using SQL*Plus:
```bash
sqlplus username/password@database
SQL> @oracle_delete_statements/delete_statements.sql
SQL> -- Review what was deleted
SQL> COMMIT;  -- or ROLLBACK if something is wrong
```

## Comparison with fastExport.py

| Feature | fastExport.py | fastDelete.py |
|---------|---------------|---------------|
| Purpose | Extract data with MERGE statements | Generate DELETE statements |
| Database Operation | SELECT (read-only) | Generates DELETE SQL (no execution) |
| Output | MERGE statements for data migration | DELETE statements for manual execution |
| Safety | Read-only, no risk | Safe - requires manual SQL execution |
| Use Case | Data migration, backup | Data cleanup |
