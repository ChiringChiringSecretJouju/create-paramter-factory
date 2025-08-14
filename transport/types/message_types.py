from typing import TypedDict, NotRequired, Literal
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


class RoutingTD(TypedDict, total=False):
    out_topics: dict[Literal["raw", "norm"], str]
    ack_topic: str
    error_topic: str


class RetryPolicyTD(TypedDict, total=False):
    max_retries: int
    backoff_ms: int
    multiplier: float
    jitter_ms: int


class ReliabilityTD(TypedDict, total=False):
    ack_required: bool
    ack_timeout_ms: int
    retry_policy: RetryPolicyTD
    non_retryable_codes: list[str]


class ConnectMessageTD(TypedDict, total=False):
    """Connect + Projection 메시지의 전체 스키마"""

    type: NotRequired[Literal["command"]]
    action: NotRequired[Literal["connect", "connect_and_subscribe"]]
    ticket_id: NotRequired[str]
    ttl_ms: NotRequired[int]
    routing: NotRequired[RoutingTD]
    reliability: NotRequired[ReliabilityTD]

    schema_version: str
    symbols: list[str]
    target: ExchangeMetadata
    connection: ExchangeSocketConfig
    projection: list[str]
    ts_issue: int
    ts_ingest: int
    expiry_ms: NotRequired[int]


def default_routing(region: str, exchange: str, req_type: str) -> RoutingTD:
    # 초기엔 고정 규칙으로 충분. 나중에 티켓에서 오버라이드 허용.
    return RoutingTD(
        out_topics={
            "raw": f"market.raw.{exchange}.{req_type}",
            "norm": f"market.norm.{region}.{req_type}",
        },
        ack_topic="ws.status",
        error_topic="ws.error",
    )


DEFAULT_RELIABILITY = ReliabilityTD(
    ack_required=True,
    ack_timeout_ms=1500,
    retry_policy=RetryPolicyTD(
        max_retries=3,
        backoff_ms=500,
        multiplier=2.0,
        jitter_ms=150,
    ),
    non_retryable_codes=["PARAM_INVALID", "WS_AUTH_FAILED"],
)
