import configparser
from logger import get_logger
import os
from dotenv import load_dotenv

# Laden der Umgebungsvariablen aus der .env-Datei
load_dotenv()
KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

# Konfiguration des Loggers
logger = get_logger(__name__)
logger.info("Dies ist eine Info-Nachricht")
logger.error("Hier ist ein Fehler aufgetreten!")


def load_config(file_path='dwh.cfg'):
    """Lädt die Konfiguration aus einer Datei."""
    config = configparser.ConfigParser()

    try:
        config.read_file(open(file_path))
        logger.info(f'Konfigurationsdatei {file_path} erfolgreich geladen.')
    except Exception as e:
        logger.error(f'Fehler beim Laden der Konfiguration: {e}')
        raise

    # AWS-Zugangsdaten
    # KEY = config.get('AWS', 'KEY', fallback=None)
    # SECRET = config.get('AWS', 'SECRET', fallback=None)

    if not KEY or not SECRET:
        logger.error("Fehlende AWS-Zugangsdaten! Bitte überprüfe die Konfiguration.")
        raise ValueError("AWS-Zugangsdaten fehlen.")

    # DWH-Parameter
    dwh_params = {key: config.get("DWH", key) for key in [
        "DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", 
        "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", 
        "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"
    ]}

    return KEY, SECRET, dwh_params

# Test-Ausgabe
if __name__ == "__main__":
    try:
        KEY, SECRET, dwh_params = load_config()
        logger.info(f"Geladene Datenbank-Konfiguration: {dwh_params['DWH_DB_USER']}, {dwh_params['DWH_DB_PASSWORD']}, {dwh_params['DWH_DB']}")
    except Exception as e:
        logger.error(f"Fehler beim Laden der Konfiguration: {e}")
