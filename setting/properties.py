import configparser
from pathlib import Path
from typing import Literal, Callable
from setting.types import (
    AllMarketURLs,
    KoreaRegionURLs,
    NationalMarketURLs,
    ExchangeSocketConfig,
    Result,
    Ok,
    Err,
)
from core.market import SocketParameterFactory

# ConfigParser 설정
path = Path(__file__).parent
parser = configparser.ConfigParser()
parser.read(f"{path}/_urls.conf")


def _safe_result_call(
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


# URL 관리 클래스 (비공개)
class _ExchangeURLManager:
    """거래소 URL 관리 클래스

    거래소 URL을 지역 및 유형별로 관리하고 조회하는 기능을 제공합니다.
    40개 이상의 거래소 연결 확장성을 고려하여 구현되었습니다.
    """

    def __init__(self, config: configparser.ConfigParser | None = None) -> None:
        """ExchangeURLManager 초기화

        Args:
            config: 설정 파서 객체 (기본값: None, None일 경우 전역 parser 사용)
        """
        self.parser = config or parser

    def get_exchange_urls(self, uri_type: str) -> AllMarketURLs:
        """모든 거래소 URL 정보를 반환합니다.

        Args:
            uri_type (str): URL 종류 (socket, rest)

        Returns:
            URLs: URL 정보 (socket, rest)
        """
        return AllMarketURLs(
            korea=KoreaRegionURLs(
                upbit=self.parser.get(f"{uri_type}URL", "UPBIT"),
                bithumb=self.parser.get(f"{uri_type}URL", "BITHUMB"),
                korbit=self.parser.get(f"{uri_type}URL", "KORBIT"),
                coinone=self.parser.get(f"{uri_type}URL", "COINONE"),
            )
        )

    def get_symbol_collect_url(
        self, market: str, location: str, url_type: str
    ) -> Result[Ok[str], Err[str]]:
        """특정 거래소와 지역에 대한 URL을 반환합니다.

        Args:
            market (str): 거래소 이름
            location (str): 지역 정보
            url_type (str): URL 종류 (socket, rest)

        Returns:
            Result[Ok[str], Err[str]]: 매칭된 URL (성공, 실패)
        """
        # location에 해당하는 딕셔너리 가져오기
        urls: AllMarketURLs = self.get_exchange_urls(url_type.upper())
        region_urls: dict[str, str] = urls.get(location)

        # 1. 지역 URL이 존재하는지 확인
        if not region_urls:
            return Err(f"지역이 등록되지 않았습니다: {location}")

        # 2. 거래소 URL이 존재하는지 확인
        ex_urls: str | None = region_urls.get(market)
        if not ex_urls:
            return Err(f"{location} 지역에서 등록되지 않은 거래소입니다: {market}")

        # 3. 모든 조건이 만족되면 URI 반환
        return Ok(ex_urls)

    def get_region_urls(
        self, region: str, uri_type: str
    ) -> Result[Ok[dict[str, str]], Err[str]]:
        """특정 지역의 모든 거래소 URL을 반환합니다.

        Args:
            region (str): 지역 이름 (korea, asia, ne)
            uri_type (str): URL 종류 (socket, rest)

        Returns:
            Result: 해당 지역의 거래소 URL 정보
        """
        urls: AllMarketURLs = self.get_exchange_urls(uri_type.upper())
        region_urls = urls.get(region)
        if not region_urls:
            return Err(f"지역이 등록되지 않았습니다: {region}")
        return Ok(region_urls)


class _ExchangeConfigManager:
    """거래소 구성 정보 통합 관리 클래스

    URL 관리자와 소켓 파라미터 팩토리를 통합하여
    거래소별 웹소켓 연결에 필요한 모든 정보를 단일 JSON 구조로 제공합니다.
    """

    def __init__(self) -> None:
        self._url_manager = _ExchangeURLManager()
        self._socket_factory = SocketParameterFactory
        self._supported_exchanges = list(self._socket_factory._creators.keys())

    @property
    def supported_exchanges(self) -> list[str]:
        return self._supported_exchanges

    def get_exchange_config(
        self,
        exchange: str,
        symbols: list[str],
        req_type: Literal["ticker", "orderbook", "trade"],
        region: str = "korea",
    ) -> Result[Ok[ExchangeSocketConfig], Err[str]]:
        url_result = self._url_manager.get_symbol_collect_url(
            exchange, region, "socket"
        )
        if isinstance(url_result, Err):
            return Err(f"URL 정보를 가져오는데 실패했습니다: {url_result.error}")

        try:
            socket_params = self._socket_factory.create_socket_parameter(
                exchange=exchange,
                symbols=symbols,
                req_type=req_type,
            )
        except ValueError as e:
            return Err(f"소켓 파라미터 생성에 실패했습니다: {str(e)}")

        url_str: str = url_result.value()
        config: ExchangeSocketConfig = {
            "url": url_str,
            "socket_params": socket_params,
        }
        return Ok(config)

    def get_all_exchange_configs(
        self,
        symbols: list[str],
        req_type: Literal["ticker", "orderbook", "trade"],
        region: str = "korea",
    ) -> Result[Ok[dict[str, ExchangeSocketConfig]], Err[str]]:
        urls_result = self._url_manager.get_region_urls(region, "socket")
        if isinstance(urls_result, Err):
            return Err(f"지역 URL 정보를 가져오는데 실패했습니다: {urls_result.error}")

        region_urls = urls_result.value()
        configs: dict[str, ExchangeSocketConfig] = {}
        for exchange in self._supported_exchanges:
            if exchange not in region_urls:
                continue
            try:
                socket_params = self._socket_factory.create_socket_parameter(
                    exchange=exchange,
                    symbols=symbols,
                    req_type=req_type,
                )
                configs[exchange] = {
                    "url": region_urls[exchange],
                    "socket_params": socket_params,
                }
            except ValueError:
                continue
        if not configs:
            return Err(f"지원되는 거래소 구성 정보가 없습니다: {region} 지역")
        return Ok(configs)


class _ExchangeService:
    """헬퍼 메서드 집합

    - URL 조회 기능: `_ExchangeURLManager` 의 래퍼
    - 구성 조회 기능: `_ExchangeConfigManager` 의 래퍼
    """

    def __init__(self) -> None:
        self._url_manager = _ExchangeURLManager()
        self._config_manager = _ExchangeConfigManager()

    # ------------------------ URL helpers --------------------- #
    def get_symbol_collect_url(
        self, market: str, location: str, url_type: str
    ) -> Result[Ok[str], Err[str]]:
        err = f"거래소 '{market}', 지역 '{location}', 유형 '{url_type}'의 URL을 찾을 수 없습니다."
        return _safe_result_call(
            self._url_manager.get_symbol_collect_url, err, market, location, url_type
        )

    def get_all_region_urls(
        self, region: str, url_type: str
    ) -> Result[Ok[NationalMarketURLs], Err[str]]:
        err = f"지역 '{region}'의 URL을 찾을 수 없습니다."
        return _safe_result_call(
            self._url_manager.get_region_urls, err, region, url_type.upper()
        )

    def get_all_urls(self, url_type: str) -> Result[Ok[AllMarketURLs], Err[str]]:
        err = f"유형 '{url_type}'의 URL을 찾을 수 없습니다."
        return _safe_result_call(
            self._url_manager.get_exchange_urls, err, url_type.upper()
        )

    # ------------------------ Config helpers ------------------ #
    def get_exchange_config(
        self,
        exchange: str,
        symbols: list[str],
        req_type: Literal["ticker", "orderbook", "trade"],
        region: str = "korea",
    ) -> Result[Ok[ExchangeSocketConfig], Err[str]]:
        if exchange.lower() == "all":
            return self.get_all_exchange_configs(symbols, req_type, region)
        return self._config_manager.get_exchange_config(
            exchange, symbols, req_type, region
        )

    def get_all_exchange_configs(
        self,
        symbols: list[str],
        req_type: Literal["ticker", "orderbook", "trade"],
        region: str = "korea",
    ) -> Result[Ok[dict[str, ExchangeSocketConfig]], Err[str]]:
        return self._config_manager.get_all_exchange_configs(symbols, req_type, region)


# 모듈 전역에서 재사용할 단일 서비스 인스턴스
_exchange_service = _ExchangeService()
