# common/exceptions.py 파일
from functools import wraps
from typing import Any, Callable, TypeVar
import json
import logging
import asyncio
from kafka.errors import NoBrokersAvailable, KafkaConnectionError, KafkaProtocolError
from dataclasses import dataclass
from common.logger import PipelineLogger

from common.setting.types import Err

# 제네릭 타입 정의
T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])
logger = PipelineLogger.get_logger("exchange_exceptions", "exceptions")

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
- KafkaConnectionError: Kafka 연결 오류
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
    KafkaConnectionError,
    KafkaProtocolError,
    KafkaConnectionError,
)


@dataclass
class ExchangeException(Exception):
    """거래소 관련 기본 예외 클래스"""

    exchange_name: str
    message: str
    original_exception: Exception | None = None

    def __post_init__(self) -> None:
        super().__init__(f"[{self.exchange_name}] {self.message}")

    def to_dict(self) -> dict[str, Any]:
        """예외 정보를 이벤트 데이터로 변환"""
        result = {
            "exchange": self.exchange_name,
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
    exchange_name_attr: str = "exchange_name",
    exception_mapping: dict[type[Exception], type[ExchangeException]] = None,
    log_level: int = logging.ERROR,
):
    """
    거래소 관련 예외 처리를 위한 데코레이터

    Args:
        exchange_name_attr: 거래소 이름을 가진 속성 이름
        exception_mapping: 일반 예외와 거래소 예외 간의 매핑
        log_level: 로그 레벨
    """
    # 기본 예외 매핑
    if exception_mapping is None:
        exception_mapping = {
            asyncio.TimeoutError: ConnectionTimeoutException,
            json.JSONDecodeError: JSONParsingException,
        }

    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # 거래소 이름 가져오기
            exchange_name = getattr(self, exchange_name_attr)

            try:
                return await func(self, *args, **kwargs)

            except ExchangeException as e:
                # 이미 ExchangeException인 경우 그대로 사용
                log_exception(e, log_level)
                return Err(e.to_dict())

            except AsyncException + KafkaException as e:  # 정의된 예외 그룹만 처리
                # 매핑된 ExchangeException으로 변환
                exchange_exc = map_exception(e, exchange_name, exception_mapping)
                log_exception(exchange_exc, log_level)
                return Err(exchange_exc.to_dict())

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
    exchange_name: str,
    mapping: dict[type[Exception], type[ExchangeException]],
) -> ExchangeException:
    """일반 예외를 ExchangeException으로 변환"""
    for base_exc, exchange_exc in mapping.items():
        if isinstance(exc, base_exc):
            return exchange_exc(exchange_name, str(exc), exc)

    # 매핑되지 않은 예외는 기본 ExchangeException 사용
    return ExchangeException(exchange_name, f"예상치 못한 오류: {str(exc)}", exc)


def log_exception(exc: ExchangeException, level: int) -> None:
    """예외 로깅"""
    if exc.original_exception and level <= logging.ERROR:
        logger.debug(
            f"원인 예외: {exc.original_exception}", exc_info=exc.original_exception
        )
