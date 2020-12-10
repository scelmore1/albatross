import logging
import os


class MyLogger:

    def __init__(self, name, file_handler, log_level, file_mode='w'):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        # remove existing handlers
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s:\t%(message)s')
        # create handlers
        if file_handler is not None:
            if not os.path.exists(file_handler):
                os.makedirs(os.path.dirname(file_handler), exist_ok=True)
            file_handler = logging.FileHandler(file_handler, mode=file_mode)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def getLogger(self):
        return self.logger