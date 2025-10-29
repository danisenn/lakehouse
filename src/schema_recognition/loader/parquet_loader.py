import pyarrow.parquet as pq
from src.utils.logger import logger # Annahme: zentraler Logger

def load_parquet_schema(file_path: str):
    """
    Lädt ein Parquet-File und gibt das Schema zurück.
    :param file_path: Pfad zur Parquet-Datei
    :return: pyarrow.Schema Objekt
    """
    try:
        logger.info(f"Lade Parquet-Datei: {file_path}")
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema
        logger.info(f"Schema erfolgreich geladen: {schema}")
        return schema
    except Exception as e:
        logger.error(f"Fehler beim Laden der Parquet-Datei: {e}")
        raise

def load_parquet_table(file_path: str):
    """
    Lädt gesamte Parquet-Tabelle und gibt sie als PyArrow Table zurück.
    :param file_path: Pfad zur Parquet-Datei
    :return: pyarrow.Table Objekt
    """
    try:
        logger.info(f"Lade Parquet-Tabelle: {file_path}")
        table = pq.read_table(file_path)
        logger.info(f"Parquet-Tabelle erfolgreich geladen mit {table.num_rows} Zeilen")
        return table
    except Exception as e:
        logger.error(f"Fehler beim Laden der Parquet-Tabelle: {e}")
        raise

if __name__ == "__main__":
    # Beispielaufruf
    test_file = "data/samples/beispiel.parquet"
    schema = load_parquet_schema(test_file)
    print(schema)
    table = load_parquet_table(test_file)
    print(table)
