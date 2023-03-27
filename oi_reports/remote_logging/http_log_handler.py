import logging
import os

from remote_logging.datadog_ops import LogPublisher


class CustomHttpHandler(logging.Handler):
    log_publisher: LogPublisher

    def __init__(self, log_publisher):
        super().__init__()
        self.log_publisher = log_publisher

    def emit(self, record: logging.LogRecord) -> None:
        log_entry = "{0} - {1} - {2}:{3}: {4}".format(
            record.threadName,
            record.levelname,
            record.module,
            record.funcName,
            self.format(record))
        if "DEPLOYMENT" in os.environ and os.environ["DEPLOYMENT"] == "local":
            extra_tags = {"test:true"}
        else:
            extra_tags = {"test:false"}
        self.log_publisher.publish_log_to_datadog(log_entry, record.levelname, extra_tags)
