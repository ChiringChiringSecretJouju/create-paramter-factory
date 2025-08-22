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
    """라우팅 설정을 정의하는 타입.

    - out_topics: 출력 토픽 (원시 및 정규화된 데이터용)
    - ack_topic: 확인 응답 토픽
    - error_topic: 오류 메시지 토픽
    """

    out_topics: dict[Literal["raw", "norm"], str]
    ack_topic: str
    error_topic: str


class RetryPolicyTD(TypedDict, total=False):
    """재시도 정책 설정.

    - max_retries: 최대 재시도 횟수
    - backoff_ms: 초기 대기 시간(밀리초)
    - multiplier: 백오프 승수(각 재시도마다 대기 시간 증가율)
    - jitter_ms: 무작위 지터 시간(밀리초)
    """

    max_retries: int
    backoff_ms: int
    multiplier: float
    jitter_ms: int


class ReliabilityTD(TypedDict, total=False):
    """메시지 신뢰성 설정.

    - ack_required: 확인 응답 필요 여부
    - ack_timeout_ms: 확인 응답 타임아웃(밀리초)
    - retry_policy: 재시도 정책
    - non_retryable_codes: 재시도하지 않을 오류 코드 목록
    """

    ack_required: bool
    ack_timeout_ms: int
    retry_policy: RetryPolicyTD
    non_retryable_codes: list[str]


class ConnectMessageTD(TypedDict, total=False):
    """Connect + Projection 메시지의 전체 스키마

    - type: 메시지 유형 (command)
    - action: 수행할 작업 (connect 또는 connect_and_subscribe)
    - ticket_id: 메시지 추적용 티켓 ID
    - ttl_ms: 메시지 유효 시간(밀리초)
    - routing: 라우팅 설정
    - reliability: 신뢰성 설정
    - schema_version: 스키마 버전
    - symbols: 구독할 심볼 목록
    - target: 대상 거래소 메타데이터
    - connection: 연결 설정
    - projection: 데이터 투영 필드 목록
    - ts_issue: 메시지 발행 타임스탬프
    - ts_ingest: 메시지 수집 타임스탬프
    - expiry_ms: 메시지 만료 시간(밀리초)
    """

    type: NotRequired[str]
    action: NotRequired[str]
    ticket_id: NotRequired[str]
    ttl_ms: NotRequired[int]
    routing: NotRequired[RoutingTD]

    schema_version: str
    symbols: list[str]
    target: ExchangeMetadata
    connection: ExchangeSocketConfig
    projection: list[str]
    ts_issue: int
    ts_ingest: int
    expiry_ms: NotRequired[int]


def default_routing(region: str, exchange: str, req_type: str) -> RoutingTD:
    """기본 라우팅 설정을 생성합니다.

    Args:
        region: 지역 코드
        exchange: 거래소 코드
        req_type: 요청 유형

    Returns:
        RoutingTD: 기본 라우팅 설정
    """
    # 초기엔 고정 규칙으로 충분. 나중에 티켓에서 오버라이드 허용.
    return RoutingTD(
        out_topics={
            "raw": f"market.raw.{exchange}.{req_type}",
            "norm": f"market.norm.{region}.{req_type}",
        }
    )


# 기본 메시지 신뢰성 설정
# - ack_required: 확인 응답 요청 여부
# - ack_timeout_ms: 확인 응답 대기 시간(ms)
# - retry_policy: 재시도 정책 (최대 재시도 횟수, 초기 백오프, 백오프 증가율, 지터)
# - non_retryable_codes: 재시도하지 않을 오류 코드 목록
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
