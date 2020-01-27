import logging
import watchtower


class CallCounted:
    """Decorator to determine number of calls for a method"""

    def __init__(self, method):
        self.method = method
        self.counter = 0

    def __call__(self, *args, **kwargs):
        self.counter += 1
        return self.method(*args, **kwargs)


# create logger for application log
logger = logging.getLogger("xapi-batch-delete")
logger.setLevel(logging.DEBUG)

# create logger for process log
process_log = logging.getLogger("process_monitor")
process_log.setLevel(logging.DEBUG)

# create handlers and set level to debug
consolehandler = logging.StreamHandler()
cloudwatch_handler = watchtower.CloudWatchLogHandler(log_group='datahub')

# create formatter - see LogRecord attributes for full list
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(inst)s - %(message)s')

# add formatter to handlers - these can be different
consolehandler.setFormatter(formatter)
cloudwatch_handler.setFormatter(formatter)

# add handlers to logger
logger.addHandler(consolehandler)
logger.addHandler(cloudwatch_handler)
process_log.addHandler(consolehandler)
process_log.addHandler(cloudwatch_handler)

logger.error = CallCounted(logger.error)
logger.error.counter = 0

logger = logging.LoggerAdapter(logger, extra={"inst": "Process"})
