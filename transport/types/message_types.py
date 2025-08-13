from typing import TypedDict, NotRequired
from core.types import ExchangeSocketConfig


class ExchangeMetadata(TypedDict):
    """이벤트의 출처(메타데이터)를 표현.

    - region: 지역 (예: "korea")
    - exchange: 거래소 (예: "bithumb")
    - request_type: 요청 타입 (예: "ticker", "orderbook", "trade")
    """

    region: str
    exchange: str
    request_type: str


class ConnectMessageTD(TypedDict):
    """Connect + Projection 메시지의 전체 스키마"""

    event_kind: str
    schema_version: str
    symbols: list[str]
    source: ExchangeMetadata
    connection: ExchangeSocketConfig
    projection: list[str]
    ts_issue: int
    ts_ingest: int
    expiry_ms: NotRequired[int]
