#!/usr/bin/env python3
"""
Oracle Data Export Script - MERGE Statement Generator
This script connects to an Oracle database, extracts data from specified tables based on
user-provided primary key , and generates MERGE statements for data migration.

Updated to use the newer oracledb package instead of cx_Oracle.
Now uses YAML configuratiovaluesn file instead of command-line flags.

Usage:
    python fastExportV2.py                    # Uses config.yaml in current directory
    python fastExportV2.py -c custom.yaml     # Uses custom configuration file

"""

import os
import oracledb
import datetime
import argparse
import yaml
from typing import List, Dict, Any, Tuple


def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)

        # Validate required sections
        if 'database' not in config:
            raise ValueError("Config file must contain 'database' section")
        if 'tables' not in config or not config['tables']:
            raise ValueError("Config file must contain 'tables' section with at least one table")

        # Validate database section
        db_config = config['database']
        required_db_fields = ['username', 'password', 'dsn']
        for field in required_db_fields:
            if field not in db_config:
                raise ValueError(f"Database configuration must include '{field}'")

        # Set default output file if not specified
        if 'output' not in config:
            config['output'] = {'file': './oracle_merge_exports/merge_export.sql'}
        elif 'file' not in config['output']:
            config['output']['file'] = './oracle_merge_exports/merge_export.sql'

        # Load shared values if present
        if 'shared_values' not in config or config['shared_values'] is None:
            config['shared_values'] = {}

        print(f"Successfully loaded configuration from {config_file}")
        print(f"  Database: {db_config['dsn']}")
        print(f"  Output file: {config['output']['file']}")
        print(f"  Tables to export: {len(config['tables'])}")
        if config['shared_values']:
            print(f"  Shared values configured: {len(config['shared_values'])}")

        return config

    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found")
        raise
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file '{config_file}': {e}")
        raise
    except Exception as e:
        print(f"Error loading configuration: {e}")
        raise


def connect_to_database(username: str, password: str, dsn: str) -> oracledb.Connection:
    """Establish connection to Oracle database"""
    try:
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        print(f"Successfully connected to Oracle Database {connection.version}")
        return connection
    except oracledb.Error as error:
        print(f"Error connecting to Oracle: {error}")
        raise


def get_table_columns(connection: oracledb.Connection, owner: str, table_name: str) -> List[Dict[str, Any]]:
    """Get column information including primary key status for a table"""
    cursor = connection.cursor()
    
    query = """
    SELECT DISTINCT
        c.column_name, 
        c.data_type,
        c.data_length,
        c.data_precision,
        c.data_scale,
        c.nullable,
        CASE WHEN EXISTS (
            SELECT 1 
            FROM all_cons_columns cc
            INNER JOIN all_constraints con ON cc.constraint_name = con.constraint_name
            WHERE con.constraint_type = 'P'
              AND cc.table_name = :table_name
              AND cc.owner = :owner
              AND cc.column_name = c.column_name
        ) THEN 1 ELSE 0 END AS is_pk,
        c.column_id
    FROM 
        all_tab_columns c
    WHERE 
        c.table_name = :table_name
        AND c.owner = :owner
    ORDER BY 
        c.column_id
    """
    
    cursor.execute(query, table_name=table_name.upper(), owner=owner.upper())
    columns = []
    
    for row in cursor:
        columns.append({
            'name': row[0],
            'data_type': row[1],
            'data_length': row[2],
            'data_precision': row[3],
            'data_scale': row[4],
            'nullable': row[5] == 'Y',
            'is_pk': row[6] == 1
        })
    
    cursor.close()
    return columns


def prompt_for_shared_columns(config_shared_values: Dict[str, Any] = None) -> Dict[str, Any]:
    """Prompt the user to enter shared column values that may be used across multiple tables

    Args:
        config_shared_values: Pre-configured shared values from config file

    Returns:
        Dictionary of shared column values (uppercase keys)
    """
    shared_values = {}

    # Start with values from config file
    if config_shared_values:
        for key, value in config_shared_values.items():
            if value is not None:  # Skip None values
                shared_values[key.upper()] = value

        if shared_values:
            print("\n=== SHARED COLUMN VALUES FROM CONFIG ===")
            for col, val in shared_values.items():
                print(f"  • {col} = {val}")
            print()

    print("\n=== SHARED COLUMN VALUES SETUP ===")
    print("Enter column names and values that may be shared across multiple tables.")
    print("These values will be automatically used when exporting tables that have these columns.")

    if shared_values:
        print("(Additional values to supplement those from config file)")

    print("Press ENTER when done adding shared columns.\n")

    while True:
        column_name = input("Enter column name (or ENTER to finish): ").strip()
        if not column_name:
            break

        column_value = input(f"Enter value for '{column_name}': ").strip()
        if column_value:
            shared_values[column_name.upper()] = column_value
            print(f"  → Added shared value: {column_name.upper()} = {column_value}")
        else:
            print("  → Empty value, skipping...")

    if shared_values:
        print(f"\nFinal shared column values:")
        for col, val in shared_values.items():
            print(f"  • {col} = {val}")
        print(f"These will be automatically used when found in table primary keys.\n")
    else:
        print("No shared column values configured.\n")

    return shared_values


def prompt_for_pk_values(pk_columns: List[Dict[str, Any]], shared_values: Dict[str, Any] = None,
                         table_name: str = "", processed_tables: set = None,
                         row_count: int = 0) -> Dict[str, Any]:
    """Prompt the user to enter values for primary key columns"""
    pk_values = {}
    shared_values = shared_values or {}
    processed_tables = processed_tables or set()

    # Check if we should prompt based on conditions:
    # 1. Table is repeated in source file
    # 2. No rows were fetched (row_count == 0)
    # 3. No shared values available for any PK columns

    is_repeated_table = table_name in processed_tables
    no_rows_fetched = row_count == 0

    # Check if any PK columns have shared values
    pk_columns_with_shared = [col for col in pk_columns if col['name'].upper() in shared_values]

    # Auto-apply shared values first
    for col in pk_columns:
        col_name_upper = col['name'].upper()
        if col_name_upper in shared_values:
            # Apply basic type conversion for shared values
            shared_val = shared_values[col_name_upper]
            try:
                if col['data_type'] == 'NUMBER':
                    if '.' in str(shared_val):
                        processed_value = float(shared_val)
                    else:
                        processed_value = int(shared_val)
                    pk_values[col['name']] = processed_value
                else:
                    pk_values[col['name']] = shared_val
                print(f"  → Using shared value for {col['name']}: {shared_val}")
            except ValueError:
                print(f"  ⚠️  Invalid shared value for {col['name']} ({col['data_type']}): {shared_val} - will prompt for input")
                # Don't add to pk_values so user will be prompted

    # Determine if we need to prompt
    need_to_prompt = is_repeated_table or no_rows_fetched or not pk_columns_with_shared

    if not need_to_prompt:
        print(f"\nUsing shared values for {table_name} (no additional input needed)")
        return pk_values

    # Show reason for prompting
    if is_repeated_table:
        print(f"\n⚠️  Table {table_name} appears multiple times in the source file.")
    if no_rows_fetched:
        print(f"\n⚠️  No rows found with current filters for {table_name}.")
    if not pk_columns_with_shared:
        print(f"\n⚠️  No shared values available for primary key columns in {table_name}.")

    print("\nNote: Press ENTER to skip a primary key column (will export ALL values for that column)")
    print("Only specify values for columns you want to filter on.")
    print("Date format: YYYY-MM-DD (e.g., 2024-12-25)\n")
    print('To terminate to prompting for a table enter X')
    
    for col in pk_columns:
        col_name = col['name']
        data_type = col['data_type']

        valid_input = False
        terminate = False
        while not valid_input and not terminate:
            if data_type in ('DATE', 'TIMESTAMP'):
                value = input(f"Enter value for primary key column {col_name} ({data_type}) [YYYY-MM-DD or ENTER to skip]: ")
            else:
                value = input(f"Enter value for primary key column {col_name} ({data_type}) [ENTER to skip]: ")
            
            if not value.strip():
                print(f"  → Skipping {col_name} - will export ALL values for this column")
                valid_input = True
                continue

            if value.upper().strip() == 'X':
                terminate =  True
                
            try:
                if data_type == 'NUMBER':
                    # Try to convert to a number
                    if '.' in value:
                        processed_value = float(value)
                    else:
                        processed_value = int(value)
                    pk_values[col_name] = processed_value
                elif data_type in ('DATE', 'TIMESTAMP'):
                    # Validate date format
                    if len(value) == 10 and value[4] == '-' and value[7] == '-':
                        # Try to parse the date to validate it
                        try:
                            datetime.datetime.strptime(value, '%Y-%m-%d')
                            pk_values[col_name] = value
                        except ValueError:
                            print("Invalid date. Please use format YYYY-MM-DD (e.g., 2024-12-25)")
                            continue
                    else:
                        print("Date format should be YYYY-MM-DD (e.g., 2024-12-25)")
                        continue
                else:
                    # Treat as string for all other data types
                    pk_values[col_name] = value
                
                valid_input = True
                print(f"  → Will filter {col_name} = {pk_values[col_name]} (manual)")
            except ValueError:
                print(f"Invalid input for {data_type}. Please try again.")
        if terminate: return pk_values    
    return pk_values


def build_where_clause(pk_values: Dict[str, Any], pk_columns: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    """Build a WHERE clause from primary key values (only for specified values)"""
    where_conditions = []
    bind_params = {}
    
    for col in pk_columns:
        col_name = col['name']
        # Only add to WHERE clause if user provided a value
        if col_name in pk_values:
            if col['data_type'] in ('DATE', 'TIMESTAMP'):
                # For date columns, use TO_DATE function in the WHERE clause
                where_conditions.append(f"{col_name} = TO_DATE(:{col_name}, 'YYYY-MM-DD')")
            else:
                where_conditions.append(f"{col_name} = :{col_name}")
            bind_params[col_name] = pk_values[col_name]
    
    where_clause = " AND ".join(where_conditions) if where_conditions else ""
    return where_clause, bind_params

def format_value_for_sql(value: Any, data_type: str) -> str:
    """Format a value based on its data type for inclusion in SQL"""
    if value is None:
        return 'NULL'
    
    # Handle different Oracle data types
    if data_type in ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR', 'CLOB', 'NCLOB'):
        # Escape single quotes in the string
        return f"'{str(value).replace('\'', '\'\'')}'"
    elif data_type == 'NUMBER':
        return str(value)
    elif data_type == 'DATE':
        # Handle both datetime objects and string dates
        if isinstance(value, datetime.datetime):
            return f"TO_DATE('{value.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
        else:
            return f"TO_DATE('{value}', 'YYYY-MM-DD')"
    elif data_type.startswith('TIMESTAMP'):
        if isinstance(value, datetime.datetime):
            return f"TO_TIMESTAMP('{value.strftime('%Y-%m-%d %H:%M:%S.%f')}', 'YYYY-MM-DD HH24:MI:SS.FF')"
        else:
            return f"TO_TIMESTAMP('{value}', 'YYYY-MM-DD HH24:MI:SS.FF')"
    else:
        # Default handling for other data types
        return f"'{str(value)}'"


def parse_table_name(table_spec: str) -> tuple[str, str]:
    """Parse table name to extract owner and table name - OWNER.TABLE format required"""
    if '.' not in table_spec:
        raise ValueError(f"Table specification must be in OWNER.TABLE format. Got: {table_spec}")
    
    parts = table_spec.split('.', 1)
    if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
        raise ValueError(f"Invalid table specification. Expected OWNER.TABLE format. Got: {table_spec}")
    
    owner, table = parts
    return owner.upper().strip(), table.upper().strip()


def generate_merge_statements(connection: oracledb.Connection, table_spec: str,
                              shared_values: Dict[str, Any] = None,
                              processed_tables: set = None) -> List[str]:
    """Generate MERGE statements for a table with user-specified PK values"""
    shared_values = shared_values or {}
    processed_tables = processed_tables or set()
    # Parse table specification (must include owner)
    try:
        owner, table_name = parse_table_name(table_spec)
    except ValueError as e:
        print(f"Error: {e}")
        raise
    
    qualified_table_name = f"{owner}.{table_name}"
    
    # Get column information
    columns = get_table_columns(connection, owner, table_name)
    
    if not columns:
        raise ValueError(f"Table {qualified_table_name} not found or no access")
    
    # Identify primary key columns and non-primary key columns
    pk_columns = [col for col in columns if col['is_pk']]
    non_pk_columns = [col for col in columns if not col['is_pk']]
    
    # If no primary keys defined, use all columns as matching criteria
    if not pk_columns:
        print(f"Warning: No primary key found for {qualified_table_name}. Using all columns for matching.")
        pk_columns = columns
        non_pk_columns = []
    
    # Build WHERE clause for initial row count check
    temp_where_clause, temp_bind_params = "", {}

    # First, apply any available shared values to get initial row count
    # Apply proper type conversion to avoid ORA-01722 errors
    temp_pk_values = {}
    for col in pk_columns:
        col_name_upper = col['name'].upper()
        if col_name_upper in shared_values:
            shared_val = shared_values[col_name_upper]
            try:
                if col['data_type'] == 'NUMBER':
                    if '.' in str(shared_val):
                        temp_pk_values[col['name']] = float(shared_val)
                    else:
                        temp_pk_values[col['name']] = int(shared_val)
                else:
                    # For VARCHAR2 and other types, ensure it's a string
                    temp_pk_values[col['name']] = str(shared_val)
            except (ValueError, TypeError):
                # If conversion fails, skip this shared value for the count query
                print(f"  ⚠️  Skipping shared value {col_name_upper} for count query due to type mismatch")
                continue

    if temp_pk_values:
        temp_where_clause, temp_bind_params = build_where_clause(temp_pk_values, pk_columns)

    # Get initial row count to determine if we need to prompt
    cursor = connection.cursor()
    count_query = f"SELECT /*+ NO_PARALLEL */ COUNT(*) FROM {qualified_table_name}"
    if temp_where_clause:
        count_query += f" WHERE {temp_where_clause}"

    try:
        if temp_bind_params:
            cursor.execute(count_query, temp_bind_params)
        else:
            cursor.execute(count_query)
        initial_row_count = cursor.fetchone()[0]
    except oracledb.DatabaseError as e:
        # Handle ORA-01722 (invalid number) and other database errors
        error_obj, = e.args
        print(f"  ⚠️  Warning: Could not count rows - {str(error_obj).split(chr(10))[0]}")
        print(f"  ⚠️  This may indicate data quality issues in the table.")
        print(f"  ⚠️  Proceeding with manual filter entry...")
        initial_row_count = 0
    finally:
        cursor.close()

    # Prompt for primary key values
    print(f"\nTable: {qualified_table_name}")
    if temp_pk_values:
        print(f"Found {initial_row_count} rows with shared column values.")
    print("Enter primary key values to filter rows for export.")
    pk_values = prompt_for_pk_values(pk_columns, shared_values, qualified_table_name,
                                   processed_tables, initial_row_count)

    # Add this table to processed tables set
    processed_tables.add(qualified_table_name)
    
    # Ask for confirmation if no filters specified
    if not pk_values:
        response = input(f"\nWarning: No filters specified. This will export ALL rows from {qualified_table_name}. Continue? (y/N): ")
        if response.lower() not in ('y', 'yes'):
            print("Export cancelled.")
            return []
    
    # Build WHERE clause for filtering rows
    where_clause, bind_params = build_where_clause(pk_values, pk_columns)
    
    # Build ON clause for the MERGE statement
    on_clause = " AND ".join([f"target.{col['name']} = source.{col['name']}" for col in pk_columns])
    
    # Build UPDATE clause if there are non-primary key columns
    update_clause = ""
    if non_pk_columns:
        update_clause = "WHEN MATCHED THEN UPDATE SET " + \
                       ", ".join([f"target.{col['name']} = source.{col['name']}" for col in non_pk_columns])
    
    # Column names for INSERT clause
    all_column_names = [col['name'] for col in columns]
    
    # Create cursor for data extraction
    cursor = connection.cursor()
    
    # Construct the query with WHERE clause - use qualified table name
    query = f"SELECT /*+ NO_PARALLEL */ * FROM {qualified_table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
        print(f"Executing query: {query}")
        print(f"With parameters: {bind_params}")
    else:
        print(f"Executing query: {query} (no filters)")
    
    # Execute the query with bind parameters (if any)
    if bind_params:
        cursor.execute(query, bind_params)
    else:
        cursor.execute(query)
    
    rows = cursor.fetchall()
    total_rows = len(rows)
    
    if where_clause:
        print(f"Retrieved {total_rows} rows matching the specified primary key values")
    else:
        print(f"Retrieved ALL {total_rows} rows from the table")
    
    # Dictionary to map column names to positions
    column_index = {col['name']: idx for idx, col in enumerate(columns)}
    
    # List to store all MERGE statements
    merge_statements = []
    
    # Process rows
    for row in rows:
        # Start building the MERGE statement - use qualified table name
        merge_stmt = [f"MERGE INTO {qualified_table_name} target"]
        merge_stmt.append("USING (SELECT ")
        
        # Add column values
        values = []
        for col in columns:
            col_name = col['name']
            col_idx = column_index[col_name]
            value = row[col_idx]
            formatted_value = format_value_for_sql(value, col['data_type'])
            values.append(f"{formatted_value} AS {col_name}")
        
        merge_stmt.append(", ".join(values))
        merge_stmt.append("FROM dual) source")
        
        # Add ON clause
        merge_stmt.append(f"ON ({on_clause})")
        
        # Add UPDATE clause if applicable
        if update_clause:
            merge_stmt.append(update_clause)
        
        # Add INSERT clause
        merge_stmt.append("WHEN NOT MATCHED THEN INSERT (")
        merge_stmt.append(", ".join(all_column_names))
        merge_stmt.append(") VALUES (")
        merge_stmt.append(", ".join([f"source.{col_name}" for col_name in all_column_names]))
        merge_stmt.append(");")
        
        # Add the complete statement to our list
        merge_statements.append("\n".join(merge_stmt))
    
    cursor.close()
    return merge_statements


def write_merge_statements_to_file(merge_statements: List[str], table_name: str, 
                                  output_file: str) -> None:
    """Append table MERGE statements to a file"""
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'a') as file:
        # Write table header
        file.write(f"\n-- MERGE statements for {table_name}\n")
        file.write(f"-- Generated on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Write all MERGE statements
        for stmt in merge_statements:
            file.write(f"{stmt}\n\n")
        
        file.write(f"-- {len(merge_statements)} rows exported for {table_name}\n\n")
    
    print(f"Successfully wrote {len(merge_statements)} MERGE statements for {table_name} to {output_file}")


def export_table_as_merge(connection: oracledb.Connection, table_spec: str,
                         output_file: str, shared_values: Dict[str, Any] = None,
                         processed_tables: set = None) -> None:
    """Export a table as MERGE statements"""
    shared_values = shared_values or {}
    processed_tables = processed_tables or set()
    try:
        print(f"\nExporting {table_spec}...")
        merge_statements = generate_merge_statements(connection, table_spec, shared_values, processed_tables)
        write_merge_statements_to_file(merge_statements, table_spec, output_file)
    except Exception as e:
        print(f"Error exporting {table_spec}: {e}")
        raise


def validate_tables(tables: List[str]) -> List[str]:
    """Validate that all tables are in OWNER.TABLE format

    Args:
        tables: List of table names to validate

    Returns:
        List of validated table names

    Raises:
        ValueError: If any table name is invalid
    """
    validated_tables = []

    for idx, table_name in enumerate(tables, 1):
        table_name = str(table_name).strip()

        if not table_name:
            continue  # Skip empty entries

        # Validate OWNER.TABLE format
        if '.' not in table_name:
            raise ValueError(f"Table #{idx}: Must be in OWNER.TABLE format. Got: {table_name}")

        parts = table_name.split('.', 1)
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            raise ValueError(f"Table #{idx}: Invalid table specification. Expected OWNER.TABLE format. Got: {table_name}")

        validated_tables.append(table_name)

    if not validated_tables:
        raise ValueError("No valid table names found in configuration")

    print(f"\nTables to export ({len(validated_tables)}):")
    for table in validated_tables:
        print(f"  - {table}")
    print()

    return validated_tables


def main():
    """Main function to run the script"""
    parser = argparse.ArgumentParser(
        description='Generate Oracle MERGE statements for data migration using YAML configuration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fastExportV2.py                    # Uses config.yaml in current directory
  python fastExportV2.py -c custom.yaml     # Uses custom configuration file
        """
    )
    parser.add_argument('--config', '-c', default='config.yaml',
                        help='Path to YAML configuration file (default: config.yaml)')

    args = parser.parse_args()

    try:
        # Load configuration from YAML file
        print(f"\n{'='*70}")
        print("Oracle Data Export Script - MERGE Statement Generator")
        print(f"{'='*70}\n")

        config = load_config(args.config)

        # Extract configuration values
        db_config = config['database']
        tables = validate_tables(config['tables'])
        output_file = config['output']['file']
        config_shared_values = config.get('shared_values', {})

        # Connect to the database
        connection = connect_to_database(
            db_config['username'],
            db_config['password'],
            db_config['dsn']
        )

        # Prompt for shared column values (merges config values with interactive input)
        shared_values = prompt_for_shared_columns(config_shared_values)
        processed_tables = set()

        # Create or overwrite the output file with header
        output_dir = os.path.dirname(output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        with open(output_file, 'w') as file:
            file.write(f"-- Oracle MERGE statements export\n")
            file.write(f"-- Generated on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            file.write(f"-- Configuration: {args.config}\n")
            file.write("SET DEFINE OFF;\n\n")

        # Export each table to the same file
        for table in tables:
            export_table_as_merge(connection, table, output_file, shared_values, processed_tables)

        # Add final COMMIT statement
        with open(output_file, 'a') as file:
            file.write("\nCOMMIT;\n")
            file.write(f"-- End of export for {len(tables)} tables\n")

        # Print summary
        print(f"\n{'='*70}")
        print("Export Summary:")
        print(f"{'='*70}")
        print(f"Successfully exported {len(tables)} tables to {output_file}")
        print(f"{'='*70}\n")

        # Close the connection
        connection.close()
        print("Database connection closed.\n")

    except Exception as e:
        print(f"\nError during export: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())