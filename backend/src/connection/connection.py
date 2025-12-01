import os
from dotenv import load_dotenv

def get_connection():
    load_dotenv()
    # Dremio connection details for ADBC Flight SQL
    user = os.getenv('DREMIO_USER')
    password = os.getenv('DREMIO_PASSWORD')
    host = os.getenv('DREMIO_HOST', '10.28.1.180')
    port = os.getenv('DREMIO_PORT', '32010')
    
    # Return connection string for ADBC Flight SQL
    # Format: grpc+tls://host:port or grpc://host:port for non-TLS
    # We'll use non-TLS (grpc://) as default, but this can be configured
    use_tls = os.getenv('DREMIO_USE_TLS', 'false').lower() == 'true'
    protocol = 'grpc+tls' if use_tls else 'grpc'
    
    return {
        'uri': f'{protocol}://{host}:{port}',
        'username': user,
        'password': password
    }
