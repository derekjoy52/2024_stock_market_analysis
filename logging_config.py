import logging

def setup_logging():
    #Configure logging
    logging.basicConfig(
        level=logging.INFO, #Set the logging level
        format="%(asctime)s - %(levelname)s - %(message)s", #Define log format
        handlers=[
            logging.FileHandler("app.log"), #Log to a file
            logging.StreamHandler() #Log to the console
        ]
    )
    logging.info("Logging is configured.")