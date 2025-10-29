import os

def load_csv_samples(directory: str):
    """
    Listet alle CSV-Dateien im Verzeichnis und gibt ihre Pfade zurück.
    """
    csv_files = []
    for file in os.listdir(directory):
        if file.lower().endswith('.csv'):
            csv_files.append(os.path.join(directory, file))
    return csv_files