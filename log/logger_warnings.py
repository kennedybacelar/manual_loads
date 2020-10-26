import logging

logger_warnings_file_path = '../log/log_warnings.log'

logger_warnings = logging.getLogger(__name__) #Allow us to work with different log classes
logger_warnings.propagate = False
logger_warnings.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
fh = logging.FileHandler(logger_warnings_file_path)
fh.setFormatter(formatter)
logger_warnings.addHandler(fh)
logger_warnings.propagate = False

logger_warnings.setLevel(logging.ERROR)
logger_warnings.setLevel(logging.INFO)