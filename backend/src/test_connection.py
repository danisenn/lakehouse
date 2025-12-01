#!/usr/bin/env python3
"""
Standalone test script to verify Dremio lakehouse connection using ADBC Flight SQL.
Run this to diagnose connection issues.
"""

import os
import polars as pl
from dotenv import load_dotenv


def test_connection():
    """Test the lakehouse connection and display diagnostic information."""
    
    print("=" * 60)
    print("DREMIO LAKEHOUSE CONNECTION TEST (ADBC Flight SQL)")
    print("=" * 60)
    
    # Step 1: Check environment variables
    print("\n1. Checking environment variables...")
    load_dotenv()
    
    env_vars = {
        'DREMIO_USER': os.getenv('DREMIO_USER'),
        'DREMIO_PASSWORD': os.getenv('DREMIO_PASSWORD'),
        'DREMIO_HOST': os.getenv('DREMIO_HOST', '10.28.1.180'),
        'DREMIO_PORT': os.getenv('DREMIO_PORT', '32010'),
        'DREMIO_USE_TLS': os.getenv('DREMIO_USE_TLS', 'false'),
    }
    
    for key, value in env_vars.items():
        if key == 'DREMIO_PASSWORD':
            # Mask password
            display_value = '***' if value else 'NOT SET'
        else:
            display_value = value if value else 'NOT SET'
        
        status = "✓" if value else "✗"
        print(f"   {status} {key}: {display_value}")
    
    # Check if required vars are set
    if not env_vars['DREMIO_USER'] or not env_vars['DREMIO_PASSWORD']:
        print("\n❌ ERROR: DREMIO_USER and DREMIO_PASSWORD must be set in .env file")
        return False
    
    # Step 2: Build ADBC Flight SQL connection
    print("\n2. Building ADBC Flight SQL connection...")
    user = env_vars['DREMIO_USER']
    password = env_vars['DREMIO_PASSWORD']
    host = env_vars['DREMIO_HOST']
    port = env_vars['DREMIO_PORT']
    use_tls = env_vars['DREMIO_USE_TLS'].lower() == 'true'
    
    protocol = 'grpc+tls' if use_tls else 'grpc'
    uri = f'{protocol}://{host}:{port}'
    
    print(f"   ✓ Connection URI: {uri}")
    print(f"   ✓ Username: {user}")
    print(f"   ✓ TLS Enabled: {use_tls}")
    
    # Step 3: Test basic connectivity with a simple query
    print("\n3. Testing connection with simple query...")
    test_query = "SELECT 1 as test_column"
    
    try:
        import adbc_driver_manager as manager
        import adbc_driver_flightsql as flightsql

        with manager.AdbcDatabase(driver=flightsql, uri=uri, username=user, password=password) as db:
            with manager.AdbcConnection(db) as conn:
                with manager.AdbcStatement(conn) as stmt:
                    stmt.set_sql_query(test_query)
                    stmt.execute_query()
                    reader = stmt.get_arrow_reader()
                    table = reader.read_all()
                    df = pl.from_arrow(table)
        print(f"   ✓ Connection successful!")
        print(f"   ✓ Query result: {df}")
    except Exception as e:
        print(f"   ✗ Connection failed: {e}")
        print("\n   Common issues:")
        print("   - Check if Dremio server is running on Flight SQL port")
        print("   - Verify DREMIO_HOST is accessible from Docker container")
        print(f"   - Confirm port {port} is the correct Flight SQL port (usually 32010)")
        print("   - Validate username and password are correct")
        print("   - If using TLS, ensure DREMIO_USE_TLS=true in .env")
        print("\n   If running Dremio on this Mac (outside Docker):")
        print("   - Set DREMIO_HOST=host.docker.internal in .env")
        print("\n   Network troubleshooting:")
        print(f"   - Try pinging the host: ping {host}")
        print(f"   - Check if port is open: nc -zv {host} {port}")
        return False
    
    # Step 4: Try to list available schemas
    print("\n4. Listing available schemas...")
    schema_query = 'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA."SCHEMATA"'
    
    try:
        with manager.AdbcDatabase(driver=flightsql, uri=uri, username=user, password=password) as db:
            with manager.AdbcConnection(db) as conn:
                with manager.AdbcStatement(conn) as stmt:
                    stmt.set_sql_query(schema_query)
                    stmt.execute_query()
                    reader = stmt.get_arrow_reader()
                    table = reader.read_all()
                    schemas_df = pl.from_arrow(table)
        schemas = schemas_df["SCHEMA_NAME"].to_list()
        print(f"   ✓ Found {len(schemas)} schemas:")
        for schema in schemas[:10]:  # Show first 10
            print(f"     - {schema}")
        if len(schemas) > 10:
            print(f"     ... and {len(schemas) - 10} more")
    except Exception as e:
        print(f"   ⚠ Could not list schemas: {e}")
        print("   (This may be a permission issue, but basic connection works)")
    
    print("\n" + "=" * 60)
    print("✅ CONNECTION TEST PASSED!")
    print("=" * 60)
    print("\nYour lakehouse connection is properly configured.")
    print("You can now use the assistant system to query your data.")
    
    return True


if __name__ == "__main__":
    import sys
    success = test_connection()
    sys.exit(0 if success else 1)
