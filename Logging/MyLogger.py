import logging
import os


class MyLogger:

    def __init__(self, name, log_level, file_handler_dir=None, file_mode='w'):
        """Create a logger with the given name and log level, create initial handlers"""
        self._logger = logging.getLogger(name)
        self._logger.setLevel(log_level)
        self._formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s:\t%(message)s')
        self._file_handler = None

        # remove existing handlers
        if self._logger.hasHandlers():
            self._logger.handlers.clear()

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self._formatter)
        self._logger.addHandler(console_handler)

        if file_handler_dir is not None:
            if not os.path.exists(file_handler_dir):
                os.makedirs(os.path.dirname(file_handler_dir), exist_ok=True)
            file_handler = logging.FileHandler(file_handler_dir, mode=file_mode)
            file_handler.setFormatter(self._formatter)
            self._logger.addHandler(file_handler)
            self._file_handler = file_handler

    def getLogger(self):
        """Return the logger to calling object"""
        return self._logger

    def replaceFileHandler(self, file_handler_dir, file_mode='w'):
        """Remove existing file handler and create new one for the file_handler directory"""
        # remove existing handler
        if self._file_handler is not None:
            self._logger.removeHandler(self._file_handler)

        # create handlers
        if not os.path.exists(file_handler_dir):
            os.makedirs(os.path.dirname(file_handler_dir), exist_ok=True)
        file_handler = logging.FileHandler(file_handler_dir, mode=file_mode)
        file_handler.setFormatter(self._formatter)

        self._logger.addHandler(file_handler)
