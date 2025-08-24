import logging

def setup_logger(LEVEL):
    logging.basicConfig(
        level=LEVEL,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("ws")