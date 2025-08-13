from typing import TypedDict, TypeVar, Generic, Any


T = TypeVar("T")  # 성공 타입
E = TypeVar("E")  # 오류 타입
NationalMarketURLs = dict[str, str]


class Ok(Generic[T]):
    def __init__(self, ok: T) -> None:
        self.ok = ok

    def value(self) -> T:
        """값을 반환합니다."""
        return self.ok


class Err(Generic[E]):
    def __init__(self, error: E) -> None:
        self.error = error

    def value(self) -> E:
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


class ExchangeMetadata(TypedDict):
    region: str
    url: str
    exchange_name: str
    request_type: str


class ExchangeSocketConfig(TypedDict):
    """거래소 소켓 구성 정보 타입"""

    url: str
    socket_params: dict[str, Any] | list[dict[str, Any]]
