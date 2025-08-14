from typing import TypedDict, TypeVar, Generic, Any, Callable, Literal


T = TypeVar("T")  # 성공 타입
E = TypeVar("E")  # 오류 타입
NationalMarketURLs = dict[str, str]
SocketRequestType = Literal["ticker", "orderbook", "trade"]


class Ok(Generic[T]):
    def __init__(self, ok: T) -> None:
        self.correct = ok

    def ok(self) -> T:
        """값을 반환합니다."""
        return self.correct


class Err(Generic[E]):
    def __init__(self, error: E) -> None:
        self.error = error

    def err(self) -> E:
        """오류 값을 반환합니다."""
        return self.error


# Result는 "이미 래핑된" Ok/Err 타입을 제네릭 인자로 받는 파라메트릭 타입 별칭입니다.
# 사용 예: Result[Ok[str], Err[str]]
type Result[A, B] = A | B


# 각 지역에 대한 URL 구조 정의
class KoreaRegionURLs(TypedDict):
    upbit: str
    bithumb: str
    korbit: str
    coinone: str


# 전체 URL 구조 정의
class AllMarketURLs(TypedDict):
    korea: KoreaRegionURLs


class ExchangeSocketConfig(TypedDict):
    """거래소 소켓 구성 정보 타입"""

    url: str
    socket_params: dict[str, Any] | list[dict[str, Any]]


def safe_result_call(
    func: Callable, error_msg: str, *args, **kwargs
) -> Result[Ok, Err]:
    """안전하게 함수를 실행하고 Result 형태로 반환하는 공통 유틸 함수"""
    result = func(*args, **kwargs)
    # 함수가 이미 Result(Ok/Err)를 반환하는 경우 그대로 전달
    if isinstance(result, (Ok, Err)):
        return result
    # 그 외에는 truthy/falsy 기준으로 Ok/Err 포장
    if not result:
        return Err(error_msg)
    return Ok(result)
