# Mirror MCP Server

A simple MCP (Model Context Protocol) server that provides a tool for mirroring tables from ODBC to SQLite. This allows Claude Desktop to move data between databases without loading large datasets into its context window.

## Features

- Single purpose: mirror tables from ODBC to SQLite
- Works with any ODBC-compatible database
- Automatically creates SQLite tables with appropriate schema
- Handles large tables with batch processing
- Preserves data types during transfer
- Special handling for ProvideX/Sage 100 connections

## Prerequisites

- Python 3.10 or higher
- UV package manager
- ODBC drivers for your source database
- Claude Desktop with both ODBC and SQLite MCP servers configured

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/mirror-mcp-server.git
cd mirror-mcp-server

# Install with UV
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .
```

## Configuration

This server requires both an ODBC configuration file and a path to a SQLite database:

```bash
mirror-mcp-server --odbc-config /path/to/odbc-config.ini --sqlite-db /path/to/sqlite.db
```

### Claude Desktop Integration

Add the Mirror MCP server to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "mirror": {
      "command": "uv",
      "args": [
        "--directory",
        "C:\\path\\to\\mirror-mcp-server",
        "run",
        "mirror-mcp-server",
        "--odbc-config", 
        "C:\\path\\to\\odbc-config.ini",
        "--sqlite-db",
        "C:\\path\\to\\sqlite.db"
      ]
    }
  }
}
```

## Usage

Once the server is running and connected to Claude Desktop, you can use the following tools:

### 1. mirror-table

Copies a table from ODBC to SQLite:

```
# Example query in Claude
Mirror the Customer table from ODBC to SQLite
```

Parameters:
- `source_table`: Name of the source table in ODBC (required)
- `dest_table`: Name of the destination table in SQLite (optional, defaults to source name)
- `connection_name`: Name of the ODBC connection to use (optional, uses default if not specified)
- `overwrite`: Whether to overwrite existing data (optional, defaults to false)

### 2. list-odbc-connections

Lists all available ODBC connections from the config file:

```
# Example query in Claude
List the available ODBC connections
```

## Example Workflows

1. **Mirror a customer table and then analyze it**:
   ```
   First, mirror the AR_Customer table from the ODBC database to SQLite.
   Then, show me the total number of customers by region from the SQLite database.
   ```

2. **Create a reporting database**:
   ```
   Mirror the following tables from ODBC to SQLite:
   - Sales_Header
   - Sales_Detail
   - Product
   - Customer
   
   Then create a sales summary report by month using the SQLite data.
   ```

## Troubleshooting

- **Connection errors**: Verify that the ODBC configuration file is correct
- **Table not found**: Check that the table name is spelled correctly and accessible to the ODBC user
- **Permission issues**: Ensure the SQLite database path is writable

## License

MIT License