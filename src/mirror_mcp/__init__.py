"""
Mirror MCP Server package.
Provides MCP tools for mirroring tables from ODBC to SQLite.
"""

import asyncio
import sys
from .server import MirrorMCPServer


def main():
    """Main entry point for the package."""
    try:
        server = MirrorMCPServer()
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("Server shutting down...")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


# Expose key classes at package level
from .config import MirrorConfig
from .mirror import TableMirror

__all__ = ['MirrorMCPServer', 'MirrorConfig', 'TableMirror', 'main']