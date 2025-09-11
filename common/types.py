from __future__ import annotations

from typing import Any, Awaitable, Callable, ParamSpec, TypeVar, TypeAlias, TypedDict
from typing import NotRequired

# 비동기 함수 타입 별칭을 위한 제네릭 파라미터
T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")

# 모듈 간에 공유되는 공통 비동기 함수 타입 별칭
AsyncFn: TypeAlias = Callable[P, Awaitable[R]]
AsyncFnWithErrDict: TypeAlias = Callable[P, Awaitable[R | dict[str, Any]]]
HandleExDecorator: TypeAlias = Callable[[AsyncFn], AsyncFnWithErrDict]


class ExchangeSocketConfig(TypedDict):
    """거래소 소켓 구성 정보 타입"""

    url: str
    socket_params: dict[str, Any] | list[dict[str, Any]]


class ExchangeMetadata(TypedDict):
    """이벤트의 출처(메타데이터)를 표현.

    - region: 지역 (예: "korea")
    - exchange: 거래소 (예: "bithumb")
    - request_type: 요청 타입 (예: "ticker", "orderbook", "trade")
    """

    region: str
    exchange: str
    symbols: list[str]
    request_type: str


# 에러 이벤트 타입 (Kafka ws.error 전송 포맷)
class WsErrorEventTD(TypedDict):
    timestamp: str
    version: str
    type: str
    source: ExchangeMetadata
    error: str
    error_type: str
    original_error: NotRequired[str]
    original_error_type: NotRequired[str]
