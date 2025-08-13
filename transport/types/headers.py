# Kafka 헤더 키를 상수화한 Enum
from enum import Enum


class HeaderKey(str, Enum):
    REGION = "region"
    EXCHANGE = "exchange"
    EVENT_KIND = "event_kind"
    REQUEST_TYPE = "request_type"
    SCHEMA_VERSION = "schema_version"
    CONTENT_TYPE = "content_type"

    def b(self) -> bytes:
        """바이트 표현 (Kafka 헤더 값으로 편리하게 사용)"""
        return self.value.encode("utf-8")


KafkaHeader = list[tuple[str, bytes]]
