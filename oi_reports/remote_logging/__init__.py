from libs.app_constants import APPLICATION_NAME, SERVICE_NAME, SERVICE_TYPE, TEAM
from remote_logging.datadog_ops import LogPublisher
from remote_logging.http_log_handler import CustomHttpHandler

__all__ = [
    'build_dd_api_key_json',
    'init_remote_log_handler',
    'http_log_handler'
]


def build_dd_api_key_json(api_key: str):
    return {"apiKeyAuth": api_key}


def init_remote_log_handler(dd_api_kv_json) -> CustomHttpHandler:
    dd_log_publisher = LogPublisher(api_key=dd_api_kv_json,
                                    application_name=APPLICATION_NAME,
                                    svc_name=SERVICE_NAME,
                                    svc_type=SERVICE_TYPE,
                                    team=TEAM)
    return CustomHttpHandler(dd_log_publisher)
