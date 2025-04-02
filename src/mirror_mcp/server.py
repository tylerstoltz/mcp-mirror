"""
MCP Server implementation for mirroring tables from ODBC to SQLite.
"""

import asyncio
import os
import sys
import json
import logging
from typing import Dict, List, Any, Optional

from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
import mcp.types as types

from .config import load_config, load_odbc_config, MirrorConfig
from .mirror import TableMirror


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr)
    ]
)

logger = logging.getLogger("mirror-mcp-server")


class MirrorMCPServer:
    """
    MCP Server that provides tools for mirroring tables from ODBC to SQLite.
    """
    
    def __init__(self):
        """Initialize the server with configuration."""
        try:
            # Load configuration
            self.config = load_config()
            
            # Load ODBC configuration
            self.odbc_config = load_odbc_config(self.config.odbc_config_path)
            
            # Create mirror handler
            self.mirror = TableMirror(
                self.odbc_config,
                self.config.sqlite_db_path,
                self.config.max_rows
            )
            
            # Create MCP server
            self.server = Server("mirror-mcp-server")
            
            # Register tool handlers
            self._register_tools()
            
            logger.info(f"Initialized Mirror MCP Server connecting ODBC to SQLite")
            logger.info(f"ODBC Config: {self.config.odbc_config_path}")
            logger.info(f"SQLite DB: {self.config.sqlite_db_path}")
            logger.info(f"Max Rows: {self.config.max_rows}")
        except Exception as e:
            logger.error(f"Failed to initialize server: {e}")
            raise
            
    def _register_tools(self):
        """Register all MCP tools."""
        @self.server.list_tools()
        async def list_tools() -> List[types.Tool]:
            """List available tools for the MCP client."""
            return [
                types.Tool(
                    name="mirror-table",
                    description="Mirror a table from ODBC to SQLite",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source_table": {
                                "type": "string",
                                "description": "Name of the source table in ODBC"
                            },
                            "dest_table": {
                                "type": "string",
                                "description": "Name of the destination table in SQLite (optional, defaults to source name)"
                            },
                            "connection_name": {
                                "type": "string",
                                "description": "Name of the ODBC connection to use (optional, uses default if not specified)"
                            },
                            "overwrite": {
                                "type": "boolean",
                                "description": "Whether to overwrite existing data (optional, defaults to false)"
                            }
                        },
                        "required": ["source_table"]
                    }
                ),
                types.Tool(
                    name="list-odbc-connections",
                    description="List all configured ODBC connections",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                )
            ]
            
        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[types.TextContent]:
            """Handle tool execution requests."""
            arguments = arguments or {}
            
            try:
                if name == "mirror-table":
                    source_table = arguments.get("source_table")
                    if not source_table:
                        raise ValueError("Source table is required")
                        
                    dest_table = arguments.get("dest_table")
                    connection_name = arguments.get("connection_name")
                    overwrite = arguments.get("overwrite", False)
                    
                    result = self.mirror.mirror_table(
                        source_table=source_table,
                        dest_table=dest_table,
                        connection_name=connection_name,
                        overwrite=overwrite
                    )
                    
                    if result["success"]:
                        message = f"## Table Mirroring Successful\n\n"
                        message += f"- Source table: `{result['source_table']}`\n"
                        message += f"- Destination table: `{result['destination_table']}`\n"
                        message += f"- Rows copied: {result['rows_copied']}\n"
                        
                        if result['table_created']:
                            message += f"- SQLite table was created\n"
                        else:
                            message += f"- SQLite table already existed\n"
                            
                        if result['max_rows_reached']:
                            message += f"\n**Note:** Maximum row limit of {self.config.max_rows} was reached. "
                            message += f"Some rows may not have been copied."
                    else:
                        message = f"## Table Mirroring Failed\n\n"
                        message += f"Error: {result['error']}"
                        
                    return [types.TextContent(type="text", text=message)]
                    
                elif name == "list-odbc-connections":
                    connections = list(self.odbc_config["connections"].keys())
                    default_connection = self.odbc_config["default_connection"]
                    
                    message = f"## Available ODBC Connections\n\n"
                    
                    if default_connection:
                        message += f"Default connection: `{default_connection}`\n\n"
                    
                    message += "### Connections\n\n"
                    for connection in connections:
                        message += f"- `{connection}`"
                        if connection == default_connection:
                            message += " (default)"
                        message += "\n"
                        
                    return [types.TextContent(type="text", text=message)]
                    
                else:
                    raise ValueError(f"Unknown tool: {name}")
                    
            except Exception as e:
                logger.error(f"Error executing tool {name}: {e}")
                error_message = f"Error executing {name}: {str(e)}"
                return [types.TextContent(type="text", text=error_message)]
                
    async def run(self):
        """Run the MCP server."""
        try:
            initialization_options = InitializationOptions(
                server_name="mirror-mcp-server",
                server_version="0.1.0",
                capabilities=self.server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            )
            
            async with stdio_server() as (read_stream, write_stream):
                logger.info("Starting Mirror MCP Server")
                await self.server.run(
                    read_stream,
                    write_stream,
                    initialization_options,
                )
        except Exception as e:
            logger.error(f"Server error: {e}")
            raise