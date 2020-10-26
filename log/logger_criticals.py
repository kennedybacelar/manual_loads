import logging

logger_criticals_file_path = '../log/log_criticals.log'

logger_criticals = logging.getLogger(__name__) #Allow us to work with different log classes
logger_criticals.propagate = False
logger_criticals.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
fh = logging.FileHandler(logger_criticals_file_path)
fh.setFormatter(formatter)
logger_criticals.addHandler(fh)
logger_criticals.propagate = False

logger_criticals.setLevel(logging.ERROR)
logger_criticals.setLevel(logging.INFO)