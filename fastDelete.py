#!/usr/bin/env python3
"""
Oracle Data Deletion Script - DELETE Statement Generator
This script connects to an Oracle database, identifies data from specified tables based on
user-provided primary key values, and generates DELETE statements to a file for review.

SAFETY: This script ONLY generates DELETE statements to a file. It does NOT execute deletions.
You must manually review and execute the generated SQL statements.

Features:
- Preview of data that would be deleted
- Interactive primary key filtering
- Generates DELETE statements to file for manual review and execution

Updated to use YAML configuration file instead of command-line flags.

Usage:
    python fastDelete.py                    # Uses config.yaml in current directory
    python fastDelete.py -c custom.yaml     # Uses custom configuration file
    python fastDelete.py -o output.sql      # Specify custom output file
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

        # Set default output file if not specified (for dry-run mode)
        if 'output' not in config:
            config['output'] = {'file': './oracle_delete_statements/delete_statements.sql'}
        elif 'file' not in config['output']:
            config['output']['file'] = './oracle_delete_statements/delete_statements.sql'

        print(f"Successfully loaded configuration from {config_file}")
        print(f"  Database: {db_config['dsn']}")
        print(f"  Tables to process: {len(config['tables'])}")

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


def prompt_for_pk_values(pk_columns: List[Dict[str, Any]], table_name: str) -> Dict[str, Any]:
    """Prompt the user to enter values for primary key columns"""
    pk_values = {}
    
    print(f"\n{'='*60}")
    print(f"CONFIGURING DELETION FILTERS FOR: {table_name}")
    print(f"{'='*60}")
    print("\nPrimary Key Columns:")
    for i, col in enumerate(pk_columns, 1):
        print(f"  {i}. {col['name']} ({col['data_type']})")
    
    print("\n⚠️  DELETION FILTER CONFIGURATION:")
    print("   • Press ENTER to skip a primary key column (will DELETE ALL rows for that column)")
    print("   • Only specify values for columns you want to filter on")
    print("   • Date format: YYYY-MM-DD (e.g., 2024-12-25)")
    print("   • BE VERY CAREFUL - This will permanently delete data!\n")
    
    for col in pk_columns:
        col_name = col['name']
        data_type = col['data_type']
        
        valid_input = False
        while not valid_input:
            if data_type in ('DATE', 'TIMESTAMP'):
                value = input(f"🔑 Enter value for primary key column {col_name} ({data_type}) [YYYY-MM-DD or ENTER to skip]: ")
            else:
                value = input(f"🔑 Enter value for primary key column {col_name} ({data_type}) [ENTER to skip]: ")
            
            # Handle empty input - skip this column
            if not value.strip():
                print(f"  → ⚠️  Skipping {col_name} - will target ALL values for this column")
                valid_input = True
                continue
                
            # Basic type validation
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
                            print("❌ Invalid date. Please use format YYYY-MM-DD (e.g., 2024-12-25)")
                            continue
                    else:
                        print("❌ Date format should be YYYY-MM-DD (e.g., 2024-12-25)")
                        continue
                else:
                    # Treat as string for all other data types
                    pk_values[col_name] = value
                
                valid_input = True
                print(f"  → ✅ Will filter {col_name} = {pk_values[col_name]}")
            except ValueError:
                print(f"❌ Invalid input for {data_type}. Please try again.")
    
    # Summary of what will be deleted
    print(f"\n🎯 DELETION SUMMARY FOR {table_name}:")
    if pk_values:
        print(f"   • Will filter on {len(pk_values)} primary key column(s)")
        print(f"   • Will target ALL values for {len(pk_columns) - len(pk_values)} primary key column(s)")
        for col_name, value in pk_values.items():
            print(f"     - {col_name} = {value}")
    else:
        print(f"   • ⚠️  NO FILTERS SPECIFIED - WILL DELETE ALL ROWS FROM THE TABLE!")
    
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


def parse_table_name(table_spec: str) -> tuple[str, str]:
    """Parse table name to extract owner and table name - OWNER.TABLE format required"""
    if '.' not in table_spec:
        raise ValueError(f"Table specification must be in OWNER.TABLE format. Got: {table_spec}")
    
    parts = table_spec.split('.', 1)
    if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
        raise ValueError(f"Invalid table specification. Expected OWNER.TABLE format. Got: {table_spec}")
    
    owner, table = parts
    return owner.upper().strip(), table.upper().strip()


def preview_data_to_delete(connection: oracledb.Connection, table_spec: str, 
                          where_clause: str, bind_params: Dict[str, Any]) -> int:
    """Preview the data that will be deleted and return row count"""
    try:
        owner, table_name = parse_table_name(table_spec)
    except ValueError as e:
        print(f"Error: {e}")
        raise
    
    qualified_table_name = f"{owner}.{table_name}"
    cursor = connection.cursor()
    
    # Get row count
    count_query = f"SELECT COUNT(*) FROM {qualified_table_name}"
    if where_clause:
        count_query += f" WHERE {where_clause}"
    
    if bind_params:
        cursor.execute(count_query, bind_params)
    else:
        cursor.execute(count_query)
    
    row_count = cursor.fetchone()[0]
    
    if row_count == 0:
        print(f"📊 No rows found matching the criteria in {qualified_table_name}")
        cursor.close()
        return 0
    
    # Preview first few rows
    preview_query = f"SELECT * FROM {qualified_table_name}"
    if where_clause:
        preview_query += f" WHERE {where_clause}"
    preview_query += " AND ROWNUM <= 5"  # Limit to first 5 rows for preview
    
    print(f"\n📊 DELETION PREVIEW for {qualified_table_name}:")
    print(f"   • Total rows to be deleted: {row_count}")
    
    if bind_params:
        cursor.execute(preview_query, bind_params)
    else:
        cursor.execute(preview_query)
    
    # Get column names
    columns = get_table_columns(connection, owner, table_name)
    column_names = [col['name'] for col in columns]
    
    print(f"   • Preview of first few rows to be deleted:")
    print(f"     {' | '.join(column_names)}")
    print(f"     {'-' * (len(' | '.join(column_names)))}")
    
    rows = cursor.fetchall()
    for row in rows:
        formatted_row = []
        for i, value in enumerate(row):
            if value is None:
                formatted_row.append("NULL")
            elif isinstance(value, str) and len(value) > 20:
                formatted_row.append(value[:17] + "...")
            else:
                formatted_row.append(str(value))
        print(f"     {' | '.join(formatted_row)}")
    
    if row_count > 5:
        print(f"     ... and {row_count - 5} more rows")
    
    cursor.close()
    return row_count


def confirm_deletion(table_spec: str, row_count: int, where_clause: str) -> bool:
    """Ask user to confirm deletion"""
    print(f"\n⚠️  DELETION CONFIRMATION REQUIRED ⚠️")
    print(f"Table: {table_spec}")
    print(f"Rows to delete: {row_count}")
    if where_clause:
        print(f"Filter conditions: {where_clause}")
    else:
        print("Filter conditions: NONE (ALL ROWS WILL BE DELETED)")
    
    print(f"\n❗ This action cannot be undone unless you rollback the transaction!")
    
    while True:
        response = input(f"\nDo you want to DELETE {row_count} rows from {table_spec}? (yes/no): ").lower().strip()
        if response in ('yes', 'y'):
            return True
        elif response in ('no', 'n'):
            return False
        else:
            print("Please enter 'yes' or 'no'")


def generate_delete_statement(table_spec: str, where_clause: str) -> str:
    """Generate DELETE statement"""
    try:
        owner, table_name = parse_table_name(table_spec)
    except ValueError as e:
        print(f"Error: {e}")
        raise
    
    qualified_table_name = f"{owner}.{table_name}"
    
    delete_stmt = f"DELETE FROM {qualified_table_name}"
    if where_clause:
        delete_stmt += f" WHERE {where_clause}"
    
    return delete_stmt


def execute_deletion(connection: oracledb.Connection, table_spec: str, 
                    where_clause: str, bind_params: Dict[str, Any], 
                    dry_run: bool = False, auto_commit: bool = False) -> int:
    """Execute the deletion or generate DELETE statement"""
    delete_stmt = generate_delete_statement(table_spec, where_clause)
    
    if dry_run:
        print(f"\n📝 Generated DELETE statement:")
        print(f"   {delete_stmt}")
        if bind_params:
            print(f"   Bind parameters: {bind_params}")
        return 0
    
    cursor = connection.cursor()
    
    try:
        print(f"\n🔄 Executing: {delete_stmt}")
        if bind_params:
            print(f"   With parameters: {bind_params}")
            cursor.execute(delete_stmt, bind_params)
        else:
            cursor.execute(delete_stmt)
        
        rows_deleted = cursor.rowcount
        print(f"✅ Successfully deleted {rows_deleted} rows from {table_spec}")
        
        # Auto-commit if requested
        if auto_commit:
            connection.commit()
            print(f"✅ Changes committed for {table_spec}")
        
        cursor.close()
        return rows_deleted
        
    except oracledb.Error as e:
        print(f"❌ Error deleting from {table_spec}: {e}")
        cursor.close()
        raise


def process_table_deletion(connection: oracledb.Connection, table_spec: str, 
                          dry_run: bool = False, auto_confirm: bool = False, 
                          auto_commit: bool = False) -> int:
    """Process deletion for a single table"""
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
    
    # Identify primary key columns
    pk_columns = [col for col in columns if col['is_pk']]
    
    # If no primary keys defined, use all columns as filtering criteria
    if not pk_columns:
        print(f"⚠️  Warning: No primary key found for {qualified_table_name}.")
        pk_columns = columns[:5]  # Use first 5 columns to avoid overwhelming user
        print(f"Using first {len(pk_columns)} columns for filtering.")
    
    # Prompt for primary key values to filter deletion
    pk_values = prompt_for_pk_values(pk_columns, qualified_table_name)
    
    # Build WHERE clause for filtering rows
    where_clause, bind_params = build_where_clause(pk_values, pk_columns)
    
    # Preview data to be deleted
    row_count = preview_data_to_delete(connection, table_spec, where_clause, bind_params)
    
    if row_count == 0:
        print(f"⚠️  No data to delete from {qualified_table_name}")
        return 0
    
    # Ask for confirmation unless auto-confirm is enabled
    if not auto_confirm:
        if not confirm_deletion(qualified_table_name, row_count, where_clause):
            print(f"❌ Deletion cancelled for {qualified_table_name}")
            return 0
    
    # Execute deletion
    return execute_deletion(connection, table_spec, where_clause, bind_params, dry_run, auto_commit)


def write_delete_statements_to_file(connection: oracledb.Connection, tables: List[str], 
                                   output_file: str) -> None:
    """Generate DELETE statements and write to file"""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as file:
        file.write(f"-- Oracle DELETE statements\n")
        file.write(f"-- Generated on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        file.write("-- WARNING: These statements will permanently delete data!\n")
        file.write("-- Review carefully before execution\n\n")
        file.write("SET DEFINE OFF;\n\n")
        
        total_statements = 0
        
        for table in tables:
            try:
                print(f"\n🔄 Processing {table} for DELETE statement generation...")
                
                # Get deletion configuration from user
                try:
                    owner, table_name = parse_table_name(table)
                except ValueError as e:
                    print(f"Error: {e}")
                    continue
                
                qualified_table_name = f"{owner}.{table_name}"
                columns = get_table_columns(connection, owner, table_name)
                
                if not columns:
                    print(f"⚠️  Table {qualified_table_name} not found or no access")
                    continue
                
                pk_columns = [col for col in columns if col['is_pk']]
                if not pk_columns:
                    pk_columns = columns[:5]
                
                pk_values = prompt_for_pk_values(pk_columns, qualified_table_name)
                where_clause, bind_params = build_where_clause(pk_values, pk_columns)
                
                # Generate DELETE statement
                delete_stmt = generate_delete_statement(table, where_clause)
                
                # Write to file
                file.write(f"-- DELETE statement for {qualified_table_name}\n")
                file.write(f"-- Generated on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                if bind_params:
                    file.write(f"-- Note: This statement uses bind parameters that need to be substituted\n")
                    for param, value in bind_params.items():
                        if isinstance(value, str):
                            delete_stmt = delete_stmt.replace(f":{param}", f"'{value}'")
                        else:
                            delete_stmt = delete_stmt.replace(f":{param}", str(value))
                
                file.write(f"{delete_stmt};\n\n")
                total_statements += 1
                
            except Exception as e:
                print(f"❌ Error processing {table}: {e}")
                file.write(f"-- ERROR processing {table}: {e}\n\n")
        
        file.write(f"\n-- Total DELETE statements generated: {total_statements}\n")
        file.write("-- COMMIT; -- Uncomment to commit the deletions\n")
        file.write("-- ROLLBACK; -- Uncomment to rollback the deletions\n")
    
    print(f"\n✅ Successfully generated {total_statements} DELETE statements in {output_file}")


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

    print(f"\n📋 Tables to process ({len(validated_tables)}):")
    for table in validated_tables:
        print(f"   • {table}")
    print()

    return validated_tables


def main():
    """Main function to run the script"""
    parser = argparse.ArgumentParser(
        description='Generate Oracle DELETE statements to file (DOES NOT EXECUTE DELETIONS)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fastDelete.py                    # Uses config.yaml, outputs to default file
  python fastDelete.py -c custom.yaml     # Uses custom configuration file
  python fastDelete.py -o output.sql      # Specify custom output file

SAFETY: This script only generates DELETE statements to a file for review.
        It does NOT execute any deletions. You must manually execute the SQL.
        """
    )
    parser.add_argument('--config', '-c', default='config.yaml',
                        help='Path to YAML configuration file (default: config.yaml)')
    parser.add_argument('--output-file', '-o',
                        help='Output file for DELETE statements (overrides config file)')

    args = parser.parse_args()

    try:
        # Load configuration from YAML file
        print(f"\n{'='*80}")
        print("Oracle Data Deletion Script - DELETE Statement Generator")
        print("(DRY-RUN ONLY - Generates SQL file, does NOT execute deletions)")
        print(f"{'='*80}\n")

        config = load_config(args.config)

        # Extract configuration values
        db_config = config['database']
        tables = validate_tables(config['tables'])

        # Determine output file (command line overrides config file)
        output_file = args.output_file if args.output_file else config['output']['file']

        # Connect to the database
        connection = connect_to_database(
            db_config['username'],
            db_config['password'],
            db_config['dsn']
        )

        # Generate DELETE statements to file
        write_delete_statements_to_file(connection, tables, output_file)

        # Close the connection
        connection.close()
        print("\n🔌 Database connection closed.")

        # Print final summary
        print(f"\n{'='*80}")
        print("Summary:")
        print(f"{'='*80}")
        print(f"DELETE statements generated for {len(tables)} tables")
        print(f"Output file: {output_file}")
        print(f"\n⚠️  IMPORTANT: Review the generated SQL file before executing!")
        print(f"{'='*80}\n")

    except Exception as e:
        print(f"❌ Error during statement generation: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())