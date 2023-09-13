""" 
    Created By: Rohit Abhishek 
    Function: This module is responsible to raise and tailor message for exception condition 
"""

import logging
import traceback

friday_logger = logging.getLogger(__name__)


class ValidationException(Exception):
    """Validation Exception for TRAVIS Application."""

    def __init__(self, message):
        self.message = message
        Exception.__init__(self, self.message)
        friday_logger.error(traceback.format_exc())

    def __str__(self):
        friday_logger.critical(self.message)
        return repr(self.message)


class ProcessingException(Exception):
    """Processing Exception for TRAVIS Application."""

    def __init__(self, message):
        self.message = message
        Exception.__init__(self, self.message)
        friday_logger.error(traceback.format_exc())

    def __str__(self):
        friday_logger.critical(self.message)
        return repr(self.message)