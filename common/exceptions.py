from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from functools import wraps
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Awaitable, Callable, ParamSpec, Sequence, TypeVar, TypeAlias

from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaConnectionError, KafkaProtocolError, NoBrokersAvailable

from common.logger import PipelineLogger
from common.serde import to_bytes
from transport.utils.projection import load_kafka_config

# 제네릭 타입 정의
T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")
AsyncFn: TypeAlias = Callable[P, Awaitable[R]]
AsyncFnWithErrDict: TypeAlias = Callable[P, Awaitable[R | dict[str, Any]]]
HandleExDecorator: TypeAlias = Callable[[AsyncFn], AsyncFnWithErrDict]

logger = PipelineLogger.get_logger("exchange_exceptions", "exceptions")

# ws.error 토픽 전송을 위한 프로듀서 (지연 초기화)
ERROR_TOPIC = "ws.error"
ERROR_SCHEMA_VERSION = "1.0.0"
_producer_cfg = load_kafka_config()


def _make_error_key(source: dict[str, Any]) -> bytes:
    region: str = str(source.get("region", ""))
    exchange: str = str(source.get("exchange", ""))
    req_type: str = str(source.get("request_type", ""))
    return f"{region}|{exchange}|{req_type}".encode("utf-8")


async def _publish_ws_error(payload: dict[str, Any]) -> None:
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=_producer_cfg.bootstrap_servers,
            acks=_producer_cfg.acks,
            linger_ms=_producer_cfg.linger_ms,
            max_batch_size=_producer_cfg.max_batch_size,
            max_request_size=_producer_cfg.max_request_size,
        )
        await producer.start()
        source = payload.get("source", {})
        await producer.send_and_wait(
            topic=ERROR_TOPIC,
            key=_make_error_key(source),
            value=to_bytes(payload),
        )
    except Exception as send_err:
        # 에러 전송 실패는 애플리케이션 흐름을 막지 않도록 로그만 남김
        logger.warning(
            f"Failed to publish ws.error event: {send_err}",
            exchange=source.get("exchange", "global"),
        )
    finally:
        await producer.stop()


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
        super().__init__(f"[{self.exchange_name}] {self.message}")

    def to_dict(self) -> dict[str, Any]:
        """예외 정보를 이벤트 데이터로 변환"""
        result = {
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
    pass


class ConnectionTimeoutException(ConnectionException):
    pass


class MessageProcessingException(ExchangeException):
    pass


class JSONParsingException(MessageProcessingException):
    pass


# 예외처리 데코레이터
def handle_exchange_exceptions(
    exchange_name_attr: str,
    region_attr: str,
    req_type_attr: str,
    symbols_attr: str,
    exception_mapping: dict[type[Exception], type[ExchangeException]] | None = None,
    return_as_dict: bool = True,
) -> HandleExDecorator:
    """
    거래소 관련 예외 처리를 위한 데코레이터.

    Args:
        exchange_name_attr: 인스턴스에서 거래소 이름을 읽을 속성명
        region_attr: 인스턴스에서 지역(region)을 읽을 속성명
        req_type_attr: 인스턴스에서 요청 타입을 읽을 속성명
        symbols_attr: 인스턴스에서 심볼 목록을 읽을 속성명
        exception_mapping: 일반 예외 → 도메인 예외 매핑 딕셔너리
        return_as_dict: True면 예외를 dict로 반환, False면 도메인 예외를 raise
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
                await _publish_ws_error(e.to_dict())
                if return_as_dict:
                    return e.to_dict()
                raise e

            except (AsyncException, KafkaException) as e:  # 정의된 예외 그룹만 처리
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
                await _publish_ws_error(exchange_exc.to_dict())
                if return_as_dict:
                    return exchange_exc.to_dict()
                raise exchange_exc

            except Exception as e:
                # 심각한 예외는 로깅하고 다시 발생
                logger.critical(
                    f"Critical error in {exchange_name}: {str(e)}", exc_info=e
                )
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
