import logging

logger_name='lakehouse'
logging.basicConfig(
    filename=f"{logger_name}.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    )