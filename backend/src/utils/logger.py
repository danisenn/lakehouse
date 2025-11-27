import logging
import sys

logger_name = 'lakehouse'

# Create logger
logger = logging.getLogger(logger_name)
logger.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter("{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M:%S")

# File Handler
file_handler = logging.FileHandler(f"{logger_name}.log", encoding="utf-8", mode="a")
file_handler.setFormatter(formatter)

# Console Handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

# Add handlers
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)