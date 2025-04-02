"""
Configuration management for the Mirror MCP Server.
Handles loading configuration for ODBC and SQLite connections.
"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import Optional, Dict, Any
import configparser
from pydantic import BaseModel


class MirrorConfig(BaseModel):
    """Configuration for the mirror MCP server."""
    odbc_config_path: str  # Path to ODBC config file
    sqlite_db_path: str    # Path to SQLite database
    max_rows: int = 10000  # Default max rows for mirroring
    

def load_config() -> MirrorConfig:
    """
    Load configuration from command line arguments.
    """
    parser = argparse.ArgumentParser(description="Mirror MCP Server")
    parser.add_argument(
        "--odbc-config", 
        required=True,
        help="Path to ODBC configuration file"
    )
    parser.add_argument(
        "--sqlite-db", 
        required=True,
        help="Path to SQLite database file"
    )
    parser.add_argument(
        "--max-rows", 
        type=int,
        default=10000,
        help="Maximum rows to transfer per operation"
    )
    
    args = parser.parse_args()
    
    # Validate that the files exist
    if not os.path.exists(args.odbc_config):
        raise FileNotFoundError(f"ODBC configuration file not found: {args.odbc_config}")
    
    # For SQLite, parent directory should exist (file might be created)
    sqlite_path = Path(args.sqlite_db)
    if not sqlite_path.parent.exists():
        raise FileNotFoundError(f"SQLite database directory not found: {sqlite_path.parent}")
    
    return MirrorConfig(
        odbc_config_path=args.odbc_config,
        sqlite_db_path=args.sqlite_db,
        max_rows=args.max_rows
    )


def load_odbc_config(config_path: str) -> Dict[str, Any]:
    """
    Load ODBC configuration from a file.
    
    Returns a dictionary with connection details.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    print(f"Loading ODBC config from: {config_path}", file=sys.stderr)
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    connections = {}
    default_connection = None
    
    # Extract server config
    if 'SERVER' in config:
        server_config = config['SERVER']
        default_connection = server_config.get('default_connection')
    
    # Extract connection configs
    for section in config.sections():
        if section == 'SERVER':
            continue
            
        # This is a connection section
        connections[section] = dict(config[section])
        print(f"Found connection: {section}", file=sys.stderr)
    
    if not connections:
        print(f"WARNING: No connections found in config file", file=sys.stderr)
    
    return {
        "connections": connections,
        "default_connection": default_connection
    }