"""
Table mirroring implementation for copy data from ODBC to SQLite.
"""

import os
import sys
import sqlite3
import pyodbc
import logging
import decimal
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger("mirror-mcp")

class TableMirror:
    """Handles mirroring tables from ODBC to SQLite."""
    
    def __init__(self, odbc_config: Dict[str, Any], sqlite_db_path: str, max_rows: int = 10000):
        """Initialize the mirror with configuration."""
        self.odbc_config = odbc_config
        self.sqlite_db_path = sqlite_db_path
        self.max_rows = max_rows
        self.odbc_connections = {}  # Cache for ODBC connections
        
        print(f"TableMirror initialized with:", file=sys.stderr)
        print(f"  SQLite DB: {sqlite_db_path}", file=sys.stderr)
        print(f"  ODBC connections: {list(odbc_config['connections'].keys())}", file=sys.stderr)
        print(f"  Default connection: {odbc_config['default_connection']}", file=sys.stderr)
    
    def get_odbc_connection(self, connection_name: Optional[str] = None) -> pyodbc.Connection:
        """
        Get a database connection by name or use the default.
        
        Args:
            connection_name: Name of the connection to use, or None for default
            
        Returns:
            pyodbc.Connection: Active database connection
        """
        # Use default if not specified
        if connection_name is None:
            if self.odbc_config["default_connection"] is None:
                if len(self.odbc_config["connections"]) == 1:
                    # If only one connection is defined, use it
                    connection_name = list(self.odbc_config["connections"].keys())[0]
                else:
                    raise ValueError("No default connection specified and multiple connections exist")
            else:
                connection_name = self.odbc_config["default_connection"]
                
        # Check if connection exists
        if connection_name not in self.odbc_config["connections"]:
            raise ValueError(f"Connection '{connection_name}' not found in configuration")
            
        # Return existing connection if available
        if connection_name in self.odbc_connections:
            try:
                # Test the connection with a simple query
                self.odbc_connections[connection_name].cursor().execute("SELECT 1")
                return self.odbc_connections[connection_name]
            except Exception as e:
                print(f"Error testing existing connection: {e}", file=sys.stderr)
                # Connection is stale, close it
                try:
                    self.odbc_connections[connection_name].close()
                except Exception:
                    pass
                del self.odbc_connections[connection_name]
                
        # Create new connection
        connection_config = self.odbc_config["connections"][connection_name]
        
        # Build connection string from config
        conn_parts = []
        
        if 'dsn' in connection_config:
            conn_parts.append(f"DSN={connection_config['dsn']}")
        
        if 'username' in connection_config:
            conn_parts.append(f"UID={connection_config['username']}")
            
        if 'password' in connection_config:
            conn_parts.append(f"PWD={connection_config['password']}")
            
        # Add any other parameters that aren't reserved
        for key, value in connection_config.items():
            if key.lower() not in ['dsn', 'username', 'password', 'readonly']:
                conn_parts.append(f"{key}={value}")
                
        conn_str = ';'.join(conn_parts)
        
        print(f"Connecting to ODBC with connection string: {conn_str}", file=sys.stderr)
        
        # Special handling for ProvideX
        is_providex = 'PROVIDEX' in conn_str.upper() or connection_name.upper() == 'SAGE100'
        
        try:
            if is_providex:
                # For ProvideX, set autocommit during connection
                print("Using special ProvideX connection mode with autocommit=True", file=sys.stderr)
                connection = pyodbc.connect(conn_str, autocommit=True)
            else:
                # For other drivers, use standard approach
                connection = pyodbc.connect(conn_str)
                connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
                connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
                connection.setencoding(encoding='utf-8')
                
            self.odbc_connections[connection_name] = connection
            return connection
        except Exception as e:
            err_msg = f"Failed to connect to '{connection_name}': {str(e)}"
            print(f"Error: {err_msg}", file=sys.stderr)
            raise ConnectionError(err_msg)
    
    def get_sqlite_connection(self) -> sqlite3.Connection:
        """Get a connection to the SQLite database."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.sqlite_db_path)), exist_ok=True)
            
            # Connect to SQLite
            connection = sqlite3.connect(self.sqlite_db_path)
            
            # Enable foreign keys
            connection.execute("PRAGMA foreign_keys = ON")
            
            # Return connection
            return connection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to SQLite database: {str(e)}")
    
    def get_table_schema(self, table_name: str, connection_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Name of the table
            connection_name: Name of the connection to use, or None for default
            
        Returns:
            List of dictionaries with column information
        """
        connection = self.get_odbc_connection(connection_name)
        cursor = connection.cursor()
        
        # Try to extract schema and table name
        schema_parts = table_name.split('.')
        if len(schema_parts) > 1:
            schema_name = schema_parts[0]
            table_name = schema_parts[1]
        else:
            schema_name = None
            
        columns = []
        try:
            # Use metadata API if available
            column_metadata = cursor.columns(table=table_name, schema=schema_name)
            for column in column_metadata:
                columns.append({
                    "name": column.column_name,
                    "type": column.type_name,
                    "size": column.column_size,
                    "nullable": column.nullable == 1,
                    "position": column.ordinal_position
                })
                
            # If we got column info, return it
            if columns:
                return columns
                
            # Otherwise, try SQL approach
            raise Exception("No columns found")
        except Exception as e:
            print(f"Error getting schema via metadata: {e}", file=sys.stderr)
            # Try SQL approach for drivers that don't support metadata
            try:
                sql = f"SELECT * FROM {table_name} WHERE 1=0"
                print(f"Attempting to get schema via SQL: {sql}", file=sys.stderr)
                cursor.execute(sql)
                
                columns = []
                for i, column in enumerate(cursor.description):
                    columns.append({
                        "name": column[0],
                        "type": self._get_type_name(column[1]),
                        "size": column[3],
                        "nullable": column[6] == 1,
                        "position": i+1
                    })
                return columns
            except Exception as e:
                raise ValueError(f"Failed to get schema for table '{table_name}': {str(e)}")
    
    def _get_type_name(self, type_code: int) -> str:
        """Convert ODBC type code to type name."""
        type_map = {
            pyodbc.SQL_CHAR: "CHAR",
            pyodbc.SQL_VARCHAR: "VARCHAR",
            pyodbc.SQL_LONGVARCHAR: "LONGVARCHAR",
            pyodbc.SQL_WCHAR: "WCHAR",
            pyodbc.SQL_WVARCHAR: "WVARCHAR",
            pyodbc.SQL_WLONGVARCHAR: "WLONGVARCHAR",
            pyodbc.SQL_DECIMAL: "DECIMAL",
            pyodbc.SQL_NUMERIC: "NUMERIC",
            pyodbc.SQL_SMALLINT: "SMALLINT",
            pyodbc.SQL_INTEGER: "INTEGER",
            pyodbc.SQL_REAL: "REAL",
            pyodbc.SQL_FLOAT: "FLOAT",
            pyodbc.SQL_DOUBLE: "DOUBLE",
            pyodbc.SQL_BIT: "BIT",
            pyodbc.SQL_TINYINT: "TINYINT",
            pyodbc.SQL_BIGINT: "BIGINT",
            pyodbc.SQL_BINARY: "BINARY",
            pyodbc.SQL_VARBINARY: "VARBINARY",
            pyodbc.SQL_LONGVARBINARY: "LONGVARBINARY",
            pyodbc.SQL_TYPE_DATE: "DATE",
            pyodbc.SQL_TYPE_TIME: "TIME",
            pyodbc.SQL_TYPE_TIMESTAMP: "TIMESTAMP"
        }
        return type_map.get(type_code, f"UNKNOWN({type_code})")
    
    def _map_to_sqlite_type(self, odbc_type: str) -> str:
        """Map ODBC data type to SQLite data type."""
        odbc_type = odbc_type.upper()
        
        # SQLite has only a few types: TEXT, INTEGER, REAL, BLOB, and NULL
        if odbc_type in ('CHAR', 'VARCHAR', 'LONGVARCHAR', 'WCHAR', 'WVARCHAR', 'WLONGVARCHAR', 'LCHAR', 'LVARCHAR'):
            return 'TEXT'
        elif odbc_type in ('INT', 'INTEGER', 'SMALLINT', 'TINYINT', 'BIT', 'BOOLEAN', 'BIGINT'):
            return 'INTEGER'
        elif odbc_type in ('DECIMAL', 'NUMERIC', 'FLOAT', 'REAL', 'DOUBLE'):
            return 'REAL'
        elif odbc_type in ('BINARY', 'VARBINARY', 'LONGVARBINARY'):
            return 'BLOB'
        elif odbc_type in ('DATE', 'TIME', 'DATETIME', 'TIMESTAMP'):
            return 'TEXT'  # Store dates as text in ISO format for compatibility
        else:
            return 'TEXT'  # Default to TEXT for unknown types
    
    def create_sqlite_table(self, sqlite_cursor: sqlite3.Cursor, 
                           table_name: str, columns: List[Dict[str, Any]],
                           overwrite: bool = False) -> bool:
        """
        Create a table in SQLite based on the schema.
        
        Args:
            sqlite_cursor: SQLite cursor
            table_name: Name of the table to create
            columns: List of column definitions
            overwrite: Whether to drop and recreate if the table exists
            
        Returns:
            bool: True if table was created, False if it already existed
        """
        # Check if table exists
        sqlite_cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        table_exists = sqlite_cursor.fetchone() is not None
        
        if table_exists:
            if overwrite:
                # Drop the table
                sqlite_cursor.execute(f"DROP TABLE {table_name}")
            else:
                # Table exists and we're not overwriting
                return False
        
        # Create the table
        column_defs = []
        for column in columns:
            sqlite_type = self._map_to_sqlite_type(column["type"])
            null_part = "NULL" if column["nullable"] else "NOT NULL"
            column_defs.append(f'"{column["name"]}" {sqlite_type} {null_part}')
        
        create_sql = f"CREATE TABLE {table_name} ({', '.join(column_defs)})"
        print(f"Creating SQLite table with: {create_sql}", file=sys.stderr)
        sqlite_cursor.execute(create_sql)
        
        return True
    
    def _convert_value_for_sqlite(self, value: Any) -> Any:
        """Convert values to types that SQLite can handle."""
        if value is None:
            return None
        elif isinstance(value, decimal.Decimal):
            # Convert Decimal to float
            return float(value)
        elif isinstance(value, bytes):
            # Convert bytes to string representation
            return str(value)
        else:
            return value
    
    def _convert_row_for_sqlite(self, row: tuple) -> tuple:
        """Convert all values in a row to SQLite-compatible types."""
        return tuple(self._convert_value_for_sqlite(value) for value in row)
    
    def mirror_table(self, source_table: str, dest_table: Optional[str] = None,
                    connection_name: Optional[str] = None, overwrite: bool = False) -> Dict[str, Any]:
        """
        Mirror a table from ODBC to SQLite.
        
        Args:
            source_table: Name of the source table in ODBC
            dest_table: Name of the destination table in SQLite (defaults to source name)
            connection_name: Name of the ODBC connection to use
            overwrite: Whether to overwrite existing data
            
        Returns:
            Dict with result information
        """
        if dest_table is None:
            dest_table = source_table
        
        print(f"Mirroring table: {source_table} to {dest_table}", file=sys.stderr)
        print(f"Connection: {connection_name or '(default)'}, Overwrite: {overwrite}", file=sys.stderr)
        
        # Get schema
        try:
            columns = self.get_table_schema(source_table, connection_name)
            if not columns:
                raise ValueError(f"No columns found for table {source_table}")
                
            print(f"Got schema with {len(columns)} columns", file=sys.stderr)
        except Exception as e:
            print(f"Schema error: {e}", file=sys.stderr)
            return {
                "success": False,
                "error": f"Failed to get schema: {str(e)}"
            }
            
        # Get connections
        try:
            odbc_conn = self.get_odbc_connection(connection_name)
            sqlite_conn = self.get_sqlite_connection()
            
            print(f"Successfully connected to both databases", file=sys.stderr)
        except Exception as e:
            print(f"Connection error: {e}", file=sys.stderr)
            return {
                "success": False,
                "error": f"Connection error: {str(e)}"
            }
            
        try:
            # Create SQLite cursor
            sqlite_cursor = sqlite_conn.cursor()
            
            # Create table if needed
            created = self.create_sqlite_table(sqlite_cursor, dest_table, columns, overwrite)
            
            # Clear existing data if overwriting
            if not created and overwrite:
                print(f"Clearing existing data from {dest_table}", file=sys.stderr)
                sqlite_cursor.execute(f"DELETE FROM {dest_table}")
            
            # Begin transaction
            sqlite_conn.commit()  # Ensure previous operations are committed
            
            # Get column names
            column_names = [column["name"] for column in columns]
            
            # Create ODBC cursor and fetch data
            odbc_cursor = odbc_conn.cursor()
            
            print(f"Executing query: SELECT * FROM {source_table}", file=sys.stderr)
            odbc_cursor.execute(f"SELECT * FROM {source_table}")
            
            # Copy data in batches
            total_rows = 0
            batch_size = 100  # Insert 100 rows at a time
            
            # Create insert statement with explicit column names
            placeholders = ", ".join(["?"] * len(column_names))
            column_list = ", ".join([f'"{col}"' for col in column_names])
            insert_sql = f'INSERT INTO {dest_table} ({column_list}) VALUES ({placeholders})'
            
            print(f"Insert SQL: {insert_sql}", file=sys.stderr)
            
            # Begin transaction
            sqlite_conn.execute("BEGIN TRANSACTION")
            
            rows = odbc_cursor.fetchmany(batch_size)
            while rows and total_rows < self.max_rows:
                print(f"Processing batch of {len(rows)} rows", file=sys.stderr)
                
                # Convert decimal.Decimal values to float for SQLite compatibility
                converted_rows = [self._convert_row_for_sqlite(row) for row in rows]
                
                sqlite_cursor.executemany(insert_sql, converted_rows)
                total_rows += len(rows)
                rows = odbc_cursor.fetchmany(batch_size)
                
                # Commit every 1000 rows
                if total_rows % 1000 == 0:
                    print(f"Committing at {total_rows} rows", file=sys.stderr)
                    sqlite_conn.commit()
                    sqlite_conn.execute("BEGIN TRANSACTION")
            
            # Commit the final batch
            print(f"Final commit with {total_rows} total rows", file=sys.stderr)
            sqlite_conn.commit()
            
            return {
                "success": True,
                "source_table": source_table,
                "destination_table": dest_table,
                "rows_copied": total_rows,
                "table_created": created,
                "max_rows_reached": total_rows >= self.max_rows
            }
            
        except Exception as e:
            # Rollback on error
            print(f"Error during mirroring: {e}", file=sys.stderr)
            try:
                sqlite_conn.rollback()
            except:
                pass
                
            return {
                "success": False,
                "error": f"Mirror operation failed: {str(e)}"
            }
        finally:
            # Close SQLite connection
            try:
                sqlite_conn.close()
            except:
                pass