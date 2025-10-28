from typing import TypedDict, TypeVar, Literal
from dataclasses import dataclass


T = TypeVar("T")  # 성공 타입
E = TypeVar("E")  # 오류 타입
SocketRequestType = Literal["ticker", "orderbook", "trade"]


# 각 지역에 대한 URL 구조 정의
class KoreaRegionURLs(TypedDict):
    upbit: str
    bithumb: str
    korbit: str
    coinone: str
    # gopax: str


class AsiaRegionURLs(TypedDict):
    binance: str
    bybit: str
    okx: str
    huobi: str
    gateio: str
    # mexc: str


class EuropeRegionURLs(TypedDict):
    bitfinex: str


class NorthAmericaRegionURLs(TypedDict):
    coinbase: str
    kraken: str


# 전체 URL 구조 정의
class AllMarketURLs(TypedDict):
    korea: KoreaRegionURLs
    asia: AsiaRegionURLs
    eu: EuropeRegionURLs
    na: NorthAmericaRegionURLs


@dataclass(frozen=True, slots=True)
class SocketConnectMetaData:
    """Connect 이벤트 생성을 위한 입력 메타데이터.

    - region: 지역 (예: "korea")
    - exchange: 거래소 (예: "upbit")
    - req_type: 요청 타입 (예: SocketRequestType.TICKER)
    - symbols: 심볼 목록 (예: ["KRW-BTC", "KRW-ETH"]) 또는 심볼 코드 축약형(호출부 규칙 따름)
    - expiry_ms: 선택적 만료 시간(ms)
    """

    region: str
    exchange: str
    symbols: list[str]
    req_type: SocketRequestType
    expiry_ms: int | None = None
