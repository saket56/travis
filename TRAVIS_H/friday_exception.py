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
        # sys.excepthook = exception_hook
        return repr(self.message)


class ProcessingException(Exception):
    """Processing Exception for TRAVIS Application."""

    def __init__(self, message):
        self.message = message
        Exception.__init__(self, self.message)
        friday_logger.error(traceback.format_exc())

    def __str__(self):
        friday_logger.critical(self.message)
        # sys.excepthook = exception_hook

        return repr(self.message)


# def exception_hook(exc_type, exc_value, tb):
#     """ Exception hook for traceback """

#     print('Traceback:')
#     filename = tb.tb_frame.f_code.co_filename
#     name = tb.tb_frame.f_code.co_name
#     line_no = tb.tb_lineno
#     print(f"File {filename} line {line_no}, in {name}")

#     # Exception type and value
#     print(f"{exc_type.__name__}, Message: {exc_value}")
