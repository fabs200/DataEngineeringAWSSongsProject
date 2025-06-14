import logging

def get_logger(name=__name__):
    """Initialisiert und gibt einen zentralen Logger zurück."""
    logger = logging.getLogger(name)
    if not logger.handlers:  # Verhindert doppelte Handler beim mehrfachen Import
        logger.setLevel(logging.INFO)
        
        # Log-Format definieren
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

        # Konsolen-Handler hinzufügen
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger
