import string
from typing import Union

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.content_encoding import ContentEncoding
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem


class LogPublisher:
    def __init__(self,
                 application_name: str,
                 svc_name: str,
                 svc_type: str,
                 team: str,
                 api_key: dict):
        self.api_key = api_key
        self.application_name = application_name
        self.svc_name = svc_name
        self.svc_type = svc_type
        self.team = team
        self.configuration = Configuration(api_key=api_key)
        self.api_client = ApiClient(self.configuration)
        self.api_instance = LogsApi(self.api_client)

    def construct_final_tags(self, extra_tags: set = None) -> Union[str]:
        tags = {
            'application:{}'.format(self.application_name),
            'service_type: {}'.format(self.svc_type),
            'service: {}'.format(self.svc_name),
            'team: {}'.format(self.team)
        }

        if extra_tags is None:
            extra_tags = {}

        final_tags = tags.union(extra_tags)
        return ', '.join(final_tags)

    def construct_payload(self, log_msg: str, log_level: str, extra_tags: set = None) -> HTTPLog:
        body = HTTPLog(
            [
                HTTPLogItem(
                    message=log_msg,
                    service=self.svc_name,
                    status=log_level,
                    ddtags=self.construct_final_tags(extra_tags)
                )
            ]
        )
        return body

    def publish_log_to_datadog(self, log_msg: string, log_level: str = "INFO", extra_tags: set = None):
        body = self.construct_payload(log_msg, log_level, extra_tags)
        response = self.api_instance.submit_log(content_encoding=ContentEncoding.GZIP, body=body)


