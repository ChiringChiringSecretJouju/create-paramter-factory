from core.types import (
    AllMarketURLs,
    NationalMarketURLs,
    ExchangeSocketConfig,
    SocketRequestType,
    Result,
    Ok,
    Err,
)
from core.socket_params import SocketParameterFactory
from core.socket_uri.uri_builder import ExchangeURLManager
from core.types import safe_result_call


class ExchangeConfigManager:
    """거래소 구성 정보 통합 관리 클래스

    URL 관리자와 소켓 파라미터 팩토리를 통합하여
    거래소별 웹소켓 연결에 필요한 모든 정보를 단일 JSON 구조로 제공합니다.
    """

    def __init__(self) -> None:
        self._url_manager = ExchangeURLManager()
        self._socket_factory = SocketParameterFactory
        self._supported_exchanges = list(self._socket_factory._creators.keys())

    @property
    def supported_exchanges(self) -> list[str]:
        return self._supported_exchanges

    def get_exchange_config(
        self,
        exchange: str,
        symbols: list[str],
        req_type: SocketRequestType,
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
        req_type: SocketRequestType,
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


class ExchangeService:
    """헬퍼 메서드 집합

    - URL 조회 기능: `ExchangeURLManager` 의 래퍼
    - 구성 조회 기능: `ExchangeConfigManager` 의 래퍼
    """

    def __init__(self) -> None:
        self._url_manager = ExchangeURLManager()
        self._config_manager = ExchangeConfigManager()

    # ------------------------ URL helpers --------------------- #
    def get_symbol_collect_url(
        self, market: str, location: str, url_type: str
    ) -> Result[Ok[str], Err[str]]:
        err = f"거래소 '{market}', 지역 '{location}', 유형 '{url_type}'의 URL을 찾을 수 없습니다."
        return safe_result_call(
            self._url_manager.get_symbol_collect_url, err, market, location, url_type
        )

    def get_all_region_urls(
        self, region: str, url_type: str
    ) -> Result[Ok[NationalMarketURLs], Err[str]]:
        err = f"지역 '{region}'의 URL을 찾을 수 없습니다."
        return safe_result_call(
            self._url_manager.get_region_urls, err, region, url_type.upper()
        )

    def get_all_urls(self, url_type: str) -> Result[Ok[AllMarketURLs], Err[str]]:
        err = f"유형 '{url_type}'의 URL을 찾을 수 없습니다."
        return safe_result_call(
            self._url_manager.get_exchange_urls, err, url_type.upper()
        )

    # ------------------------ Config helpers ------------------ #
    def get_exchange_config(
        self,
        exchange: str,
        symbols: list[str],
        req_type: SocketRequestType,
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
        req_type: SocketRequestType,
        region: str = "korea",
    ) -> Result[Ok[dict[str, ExchangeSocketConfig]], Err[str]]:
        return self._config_manager.get_all_exchange_configs(symbols, req_type, region)


# 모듈 전역에서 재사용할 단일 서비스 인스턴스
_exchange_service = ExchangeService()
