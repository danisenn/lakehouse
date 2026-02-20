#!/usr/bin/env python3
"""
Test script to verify Dremio lakehouse connection from Docker container.
Run this to diagnose connection issues.
"""

import os
import sys
from pathlib import Path

# Add src to path so we can import connection module
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.connection.connection import get_connection
import polars as pl


def _to_adbc_uri(conn: dict | str) -> str:
    """Normalize connection from get_connection() to an ADBC Flight SQL URI string."""
    from urllib.parse import quote
    if isinstance(conn, str):
        return conn
    uri = conn.get('uri')
    username = conn.get('username')
    password = conn.get('password')
    if not uri or not username or password is None:
        raise ValueError("Invalid lakehouse connection: missing uri/username/password")
    if uri.startswith('grpc+tls://'):
        host_port = uri.replace('grpc+tls://', '')
    elif uri.startswith('grpc://'):
        host_port = uri.replace('grpc://', '')
    else:
        host_port = uri
    
    user_enc = quote(str(username), safe='')
    pass_enc = quote(str(password), safe='')
    
    adbc_uri = f"flightsql://{user_enc}:{pass_enc}@{host_port}"
    if uri.startswith('grpc://'):
        adbc_uri += "?use_encryption=false"
        
    return adbc_uri

def test_connection():
    """Test the lakehouse connection and display diagnostic information."""
    
    print("=" * 60)
    print("DREMIO LAKEHOUSE CONNECTION TEST")
    print("=" * 60)
    
    # Step 1: Check environment variables
    print("\n1. Checking environment variables...")
    from dotenv import load_dotenv
    load_dotenv()
    
    env_vars = {
        'DREMIO_USER': os.getenv('DREMIO_USER'),
        'DREMIO_PASSWORD': os.getenv('DREMIO_PASSWORD'),
        'DREMIO_HOST': os.getenv('DREMIO_HOST', '10.28.1.180'),
        'DREMIO_PORT': os.getenv('DREMIO_PORT', '32010'),
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
        
    # Step 1.5: Check TCP connectivity
    print("\n1.5. Checking network connectivity...")
    import socket
    
    def check_port(host, port, timeout=3):
        try:
            with socket.create_connection((host, int(port)), timeout=timeout):
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False

    target_host = env_vars['DREMIO_HOST']
    flight_port = env_vars['DREMIO_PORT']
    ui_port = '9047'  # Standard UI port
    
    print(f"   Testing connectivity to {target_host}...")
    
    # Check Flight SQL Port
    if check_port(target_host, flight_port):
        print(f"   ✓ Port {flight_port} (Flight SQL) is reachable")
    else:
        print(f"   ✗ Port {flight_port} (Flight SQL) is NOT reachable")
        print("     -> This is required for the Python client to work.")
        
    # Check UI Port (diagnostic only)
    if check_port(target_host, ui_port):
        print(f"   ✓ Port {ui_port} (Web UI) is reachable")
    else:
        print(f"   ! Port {ui_port} (Web UI) is NOT reachable")
        
    if not check_port(target_host, flight_port):
        print("\n   ⚠ WARNING: Flight SQL port is unreachable. Connection will likely fail.")
        print("   Possible causes:")
        print("   - VPN is not active or blocking Docker traffic")
        print("   - Dremio server is not listening on port 32010")
        print("   - Firewall rules on the server or your machine")
    
    # Step 2: Get connection string and connect
    print("\n2. Connecting to Lakehouse...")
    try:
        conn_dict = get_connection()
        uri = conn_dict.get('uri')
        username = conn_dict.get('username')
        password = conn_dict.get('password')
        
        # Use grpc:// for plaintext (default from get_connection if no TLS)
        # If TLS is enabled, it would be grpc+tls://
        print(f"   Using URI: {uri}")
        
        import adbc_driver_flightsql.dbapi as flight_sql
        
        # Connect using DBAPI
        conn = flight_sql.connect(uri, db_kwargs={
            "username": username,
            "password": password,
        })
        print(f"   ✓ Connected to Dremio via ADBC Flight SQL")
        
    except Exception as e:
        print(f"   ✗ Connection failed: {e}")
        return False
    
    # Step 3: Test basic connectivity with a simple query
    print("\n3. Testing connection with simple query...")
    test_query = "SELECT 1 as test_column"
    
    try:
        # Use read_database with the connection object
        df = pl.read_database(query=test_query, connection=conn)
        print(f"   ✓ Query successful!")
        print(f"   ✓ Query result: {df}")
    except Exception as e:
        print(f"   ✗ Query failed: {e}")
        print("\n   Common issues:")
        print("   - Check if Dremio server is running")
        print("   - Verify DREMIO_HOST is accessible from Docker container")
        print("   - Confirm port 32010 is not blocked by firewall")
        print("   - Validate username and password are correct")
        return False
    
    # Step 4: Try to list available schemas
    print("\n4. Listing available schemas...")
    schema_query = 'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA."SCHEMATA"'
    
    try:
        schemas_df = pl.read_database(query=schema_query, connection=conn)
        schemas = schemas_df["SCHEMA_NAME"].to_list()
        print(f"   ✓ Found {len(schemas)} schemas:")
        for schema in schemas[:10]:  # Show first 10
            print(f"     - {schema}")
        if len(schemas) > 10:
            print(f"     ... and {len(schemas) - 10} more")
    except Exception as e:
        print(f"   ⚠ Could not list schemas: {e}")
        print("   (This may be a permission issue, but basic connection works)")
    
    # Close connection
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ CONNECTION TEST PASSED!")
    print("=" * 60)
    print("\nYour lakehouse connection is properly configured.")
    print("You can now use the assistant system to query your data.")
    
    return True


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
