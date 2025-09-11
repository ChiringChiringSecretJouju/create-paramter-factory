from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from functools import wraps
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Sequence, Awaitable, Callable

from kafka.errors import KafkaConnectionError, KafkaProtocolError, NoBrokersAvailable

from common.logger import PipelineLogger
from common.types import (
    AsyncFn,
    AsyncFnWithErrDict,
    HandleExDecorator,
    ExchangeMetadata,
    WsErrorEventTD,
    P,
    R,
)

# 제네릭 타입 정의 (공통 타입 별칭은 common/types.py에 보관)

logger = PipelineLogger.get_logger("exchange_exceptions", "exceptions")

# 이벤트 스키마 버전 (ws.error 등 공용 이벤트에 사용)
ERROR_SCHEMA_VERSION = "1.0.0"

# 주입 가능한 퍼블리셔 콜백 시그니처
PublisherFn = Callable[[WsErrorEventTD], Awaitable[None]]


async def _log_ws_error(payload: WsErrorEventTD) -> None:
    """기본 퍼블리셔: 카프카 미연결 환경에서 JSON 로그만 남김."""
    try:
        logger.error(
            "ws.error event: %s",
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )
    except Exception:
        # 로깅 실패는 무시
        pass


"""
# 예상되는 비동기 예외 타입

AsyncException 예외 목록 
- asyncio.CancelledError: 태스크 취소시 발생
- asyncio.TimeoutError: 작업 타임아웃시 발생
- ValueError: 데이터 값 관련 오류
- TypeError: 데이터 타입 관련 오류
- KeyError: 데이터 구조 키 접근 오류
- AttributeError: 객체 속성 접근 오류

KafkaException 예외 목록
- NoBrokersAvailable: Kafka broker 연결 실패
- KafkaConnectionError: Kafka 연결 오류
- KafkaProtocolError: Kafka 프로토콜 오류
"""
AsyncException: tuple[type[Exception], ...] = (
    asyncio.CancelledError,
    asyncio.TimeoutError,
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
)
KafkaException: tuple[type[Exception], ...] = (
    NoBrokersAvailable,
    KafkaProtocolError,
    KafkaConnectionError,
)

# 두 그룹을 하나로 합친 예외 튜플 (except 절에서 튜플 중첩을 피하기 위해)
HANDLED_EXCEPTIONS: tuple[type[Exception], ...] = AsyncException + KafkaException


@dataclass(slots=True, frozen=True)
class ExchangeException(Exception):
    """거래소 관련 기본 예외 클래스"""

    region: str
    exchange_name: str
    req_type: str
    symbols: list[str]
    message: str
    original_exception: Exception | None = None

    def __post_init__(self) -> None:
        # super()는 frozen dataclass + Exception 상속에서 타입 에러를 유발할 수 있음
        # 직접 기반 클래스 초기화를 호출하여 메시지를 설정
        Exception.__init__(self, f"[{self.exchange_name}] {self.message}")

    def to_dict(self) -> WsErrorEventTD:
        """예외 정보를 이벤트 데이터로 변환"""
        result: WsErrorEventTD = {
            "timestamp": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
            "version": ERROR_SCHEMA_VERSION,
            "type": "error",
            "source": {
                "region": self.region,
                "exchange": self.exchange_name,
                "request_type": self.req_type,
            },
            "symbols": self.symbols,
            "error": self.message,
            "error_type": self.__class__.__name__,
        }

        if self.original_exception:
            result["original_error"] = str(self.original_exception)
            result["original_error_type"] = self.original_exception.__class__.__name__

        return result


# 특화된 예외 클래스들
class ConnectionException(ExchangeException):
    """소켓 연결 과정에서 발생한 예외의 베이스."""

    pass


class ConnectionTimeoutException(ConnectionException):
    """소켓 연결 타임아웃 발생."""

    pass


class MessageProcessingException(ExchangeException):
    """소켓 메시지 처리 과정에서 발생한 예외의 베이스."""

    pass


class JSONParsingException(MessageProcessingException):
    """JSON 파싱 과정에서 발생한 예외."""

    pass


# ---------------- URL Resolve specific exceptions ---------------- #
class UrlResolveException(ExchangeException):
    """URL 조회/해석 과정에서 발생한 예외의 베이스."""


class RegionNotRegisteredException(UrlResolveException):
    """지정된 지역이 설정에 존재하지 않을 때."""

    pass


class MarketNotRegisteredException(UrlResolveException):
    """해당 지역에 지정된 거래소가 존재하지 않을 때."""

    pass


async def publish_url_error_event(
    *,
    region: str,
    exchange_name: str,
    req_type: str,
    symbols: list[str] | None,
    message: str,
    original_exception: Exception | None = None,
    publisher: PublisherFn | None = None,
) -> None:
    """URL 조회 실패를 ws.error 이벤트로 퍼블리시합니다.

    Note:
        - 퍼블리셔가 주입되지 않으면 로컬 에러 로그(_log_ws_error)로 대체됩니다.
        - 호출 측에서 적절한 예외를 다시 raise 하거나 흐름을 결정하세요.
    """
    exc = UrlResolveException(
        region=region,
        exchange_name=exchange_name,
        req_type=req_type,
        symbols=list(symbols or []),
        message=message,
        original_exception=original_exception,
    )
    payload = exc.to_dict()
    if publisher is not None:
        await publisher(payload)
    else:
        await _log_ws_error(payload)


# 예외처리 데코레이터
def handle_exchange_exceptions(
    exchange_name_attr: str,
    region_attr: str,
    req_type_attr: str,
    symbols_attr: str,
    exception_mapping: dict[type[Exception], type[ExchangeException]] | None = None,
    publisher: PublisherFn | None = None,
) -> HandleExDecorator:
    """
    거래소 관련 예외 처리를 위한 데코레이터.

    Args:
        exchange_name_attr: 인스턴스에서 거래소 이름을 읽을 속성명
        region_attr: 인스턴스에서 지역(region)을 읽을 속성명
        req_type_attr: 인스턴스에서 요청 타입을 읽을 속성명
        symbols_attr: 인스턴스에서 심볼 목록을 읽을 속성명
        exception_mapping: 일반 예외 → 도메인 예외 매핑 딕셔너리
    """
    # 기본 예외 매핑
    if exception_mapping is None:
        exception_mapping = {
            asyncio.TimeoutError: ConnectionTimeoutException,
            json.JSONDecodeError: JSONParsingException,
            ValueError: MessageProcessingException,
            TypeError: MessageProcessingException,
            KeyError: MessageProcessingException,
            NoBrokersAvailable: ConnectionException,
            KafkaProtocolError: ConnectionException,
            KafkaConnectionError: ConnectionException,
        }

    def decorator(func: AsyncFn) -> AsyncFnWithErrDict:
        @wraps(func)
        async def wrapper(
            self, *args: P.args, **kwargs: P.kwargs
        ) -> R | dict[str, Any]:
            # 컨텍스트 정보 가져오기
            exchange_name: str = getattr(self, exchange_name_attr, "")
            region: str = getattr(self, region_attr, "")
            req_type: str = getattr(self, req_type_attr, "")
            symbols: list[str] = list(getattr(self, symbols_attr, []) or [])

            try:
                return await func(self, *args, **kwargs)

            except asyncio.CancelledError:
                # 태스크 취소는 그대로 전파하여 상위 레벨에서 안전하게 중단되도록 함
                raise

            except ExchangeException as e:
                # 에러 이벤트 전송
                if publisher is not None:
                    await publisher(e.to_dict())
                else:
                    await _log_ws_error(e.to_dict())
                raise e

            except HANDLED_EXCEPTIONS as e:  # 정의된 예외 그룹만 처리
                # 매핑된 ExchangeException으로 변환 (컨텍스트 포함)
                exchange_exc: ExchangeException = map_exception(
                    e,
                    region=region,
                    exchange_name=exchange_name,
                    req_type=req_type,
                    symbols=symbols,
                    mapping=exception_mapping,
                )
                # 에러 이벤트 전송
                if publisher is not None:
                    await publisher(exchange_exc.to_dict())
                else:
                    await _log_ws_error(exchange_exc.to_dict())
                raise exchange_exc

            except Exception as e:
                # 심각한 예외는 로깅하고 다시 발생
                logger.critical(f"Critical error in {exchange_name}: {str(e)}")
                raise  # 처리되지 않은 예외는 상위로 전파

        return wrapper

    return decorator


# 헬퍼 함수들
def map_exception(
    exc: Exception,
    *,
    region: str,
    exchange_name: str,
    req_type: str,
    symbols: Sequence[str],
    mapping: dict[type[Exception], type[ExchangeException]],
) -> ExchangeException:
    """일반 예외를 ExchangeException으로 변환 (컨텍스트 포함)"""
    for base_exc, exc_cls in mapping.items():
        if isinstance(exc, base_exc):
            return exc_cls(
                region=region,
                exchange_name=exchange_name,
                req_type=req_type,
                symbols=list(symbols),
                message=str(exc),
                original_exception=exc,
            )

    # 매핑되지 않은 예외는 기본 ExchangeException 사용
    return ExchangeException(
        region=region,
        exchange_name=exchange_name,
        req_type=req_type,
        symbols=list(symbols),
        message=f"예상치 못한 오류: {str(exc)}",
        original_exception=exc,
    )
