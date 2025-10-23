import os

from dremio_simple_query.connect import get_token, DremioConnection
from dotenv import load_dotenv
import polars as pl

def get_connection():
    load_dotenv()
    # Dremio login details
    token = get_token(uri="http://10.28.1.180:9047/apiv2/login", payload={"userName": os.getenv('DREMIO_USER'), "password": os.getenv('DREMIO_PASSWORD')})

    try:
        dremio = DremioConnection(token, "grpc://10.28.1.180:32010")
    except Exception as e:
        print(f'connection went wrong: {e}')
    return dremio

