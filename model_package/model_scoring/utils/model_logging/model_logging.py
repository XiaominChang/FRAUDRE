import logging
import structlog
import sys
from typing import Optional

class log_utility:
    """
    A structured logging utility built on top of structlog.

    Features:
        - JSON-formatted logs suitable for GCP Cloud Logging (stdout captured automatically by Vertex AI).
        - Adds log level, timestamps, model_id, component, and pipeline_job.
        - Supports exception logging with stack traces.
        - Portable: works both locally and in Vertex AI pipelines.
    """

    _is_configured = False

    def __init__(self, model_id: str, component: str, pipeline_job: Optional[str] = None, human_readable: bool = False):
        self.model_id = model_id
        self.component = component
        self.pipeline_job = pipeline_job
        self.human_readable = human_readable

        if not log_utility._is_configured:
            self._configure(human_readable=self.human_readable)
            log_utility._is_configured = True

        self.logger = structlog.get_logger().bind(
            model_id=self.model_id,
            component=self.component,
            pipeline_job=self.pipeline_job,
        )

    def _configure(self, human_readable: bool = False):
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=logging.INFO,
        )

        renderer = structlog.dev.ConsoleRenderer() if human_readable else structlog.processors.JSONRenderer()

        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                renderer,
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    # Logging methods
    def debug(self, message: str, **kwargs):
        self.logger.debug(message, **kwargs)

    def info(self, message: str, **kwargs):
        self.logger.info(message, **kwargs)

    def warning(self, message: str, **kwargs):
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs):
        self.logger.error(message, **kwargs)

    def critical(self, message: str, **kwargs):
        self.logger.critical(message, **kwargs)

    def exception(self, message: str, **kwargs):
        self.logger.error(message, exc_info=True, **kwargs)