from core.types import (
    AllMarketURLs,
    NationalMarketURLs,
    ExchangeSocketConfig,
    SocketRequestType,
    SocketConnectMetaData,
    Err,
)
from core.socket_params import SocketParameterFactory
from core.socket_uri.uri_builder import ExchangeURLManager


class ExchangeConfigManager:
    """거래소 구성 정보 통합 관리 클래스

    URL 관리자와 소켓 파라미터 팩토리를 통합하여
    거래소별 웹소켓 연결에 필요한 모든 정보를 단일 JSON 구조로 제공합니다.
    """

    def __init__(self) -> None:
        """
        거래소 URL 관리자와 소켓 파라미터 팩토리를 초기화합니다.
        _supported_exchanges: 지원되는 거래소 목록
        _url_manager: 거래소 URL 관리자
        _socket_factory: 소켓 파라미터 팩토리
        """
        self._url_manager = ExchangeURLManager()
        self._socket_factory = SocketParameterFactory
        self._supported_exchanges = list(self._socket_factory._creators.keys())

    @property
    def supported_exchanges(self) -> list[str]:
        """지원되는 거래소 목록을 반환합니다."""
        return self._supported_exchanges

    def get_exchange_config(self, spec: SocketConnectMetaData) -> ExchangeSocketConfig:
        """특정 거래소의 구성 정보를 반환합니다.

        Args:
            spec: SocketConnectMetaData
        Returns:
            ExchangeSocketConfig: 구성 정보
        """
        url_result = self._url_manager.get_symbol_collect_url(
            market=spec.exchange,
            location=spec.region,
            url_type="socket",
        )
        if isinstance(url_result, Err):
            raise RuntimeError(
                f"URL 정보를 가져오는데 실패했습니다: {url_result.error}"
            )

        try:
            socket_params = self._socket_factory.create_socket_parameter(
                exchange=spec.exchange,
                symbols=list(spec.symbols),
                req_type=spec.req_type,
            )
        except ValueError as e:
            raise RuntimeError(f"소켓 파라미터 생성에 실패했습니다: {str(e)}")

        config: ExchangeSocketConfig = {
            "url": url_result,
            "socket_params": socket_params,
        }
        return config

    def get_all_exchange_configs(
        self,
        spec: SocketConnectMetaData,
    ) -> dict[str, ExchangeSocketConfig]:
        """모든 거래소의 구성 정보를 반환합니다.

        Args:
            spec: SocketConnectMetaData
        Returns:
            dict[str, ExchangeSocketConfig]: 구성 정보
        """
        urls_result: dict[str, str] = self._url_manager.get_region_urls(
            spec.region, "socket"
        )
        if not urls_result:
            raise RuntimeError(
                f"지역 URL 정보를 가져오는데 실패했습니다: {spec.region}"
            )

        configs: dict[str, ExchangeSocketConfig] = {}
        for exchange in self._supported_exchanges:
            if exchange not in urls_result:
                continue
            try:
                socket_params = self._socket_factory.create_socket_parameter(
                    exchange=exchange,
                    symbols=list(spec.symbols),
                    req_type=spec.req_type,
                )
                configs[exchange] = {
                    "url": urls_result[exchange],
                    "socket_params": socket_params,
                }
            except ValueError:
                continue
        if not configs:
            raise RuntimeError(
                f"지원되는 거래소 구성 정보가 없습니다: {spec.region} 지역"
            )
        return configs

    # ------------------------ Spec wrappers ------------------ #
    def get_exchange_config_from(
        self, spec: SocketConnectMetaData
    ) -> ExchangeSocketConfig:
        """SocketConnectMetaData 기반 구성 정보를 반환합니다."""
        return self.get_exchange_config(spec)

    def get_all_exchange_configs_from(
        self, spec: SocketConnectMetaData
    ) -> dict[str, ExchangeSocketConfig]:
        """SocketConnectMetaData 기반 모든 거래소 구성 정보를 반환합니다."""
        return self.get_all_exchange_configs(spec)


class ExchangeService:
    """헬퍼 메서드 집합

    - URL 조회 기능: `ExchangeURLManager` 의 래퍼
    - 구성 조회 기능: `ExchangeConfigManager` 의 래퍼
    """

    def __init__(self) -> None:
        """
        거래소 URL 관리자와 구성 관리자를 초기화합니다.
        _url_manager: 거래소 URL 관리자
        _config_manager: 구성 관리자
        """
        self._url_manager = ExchangeURLManager()
        self._config_manager = ExchangeConfigManager()

    # ------------------------ URL helpers --------------------- #
    def get_symbol_collect_url(self, market: str, location: str, url_type: str) -> str:
        """특정 거래소와 지역에 대한 URL을 반환합니다.

        Args:
            market: 거래소
            location: 지역
            url_type: URL 유형
        Returns:
            str: URL
        """
        res = self._url_manager.get_symbol_collect_url(market, location, url_type)
        if not res:
            raise RuntimeError(
                f"거래소 '{market}', 지역 '{location}', 유형 '{url_type}'의 URL을 찾을 수 없습니다."
            )
        return res

    def get_all_region_urls(self, region: str, url_type: str) -> NationalMarketURLs:
        """지역에 대한 URL을 반환합니다.

        Args:
            region: 지역
            url_type: URL 유형
        Returns:
            NationalMarketURLs: URL
        """
        res = self._url_manager.get_region_urls(region, url_type.upper())
        if not res:
            raise RuntimeError(f"지역 '{region}'의 URL을 찾을 수 없습니다.")
        return res

    def get_all_urls(self, url_type: str) -> AllMarketURLs:
        """모든 거래소의 URL을 반환합니다.

        Args:
            url_type: URL 유형
        Returns:
            AllMarketURLs: URL
        """
        res = self._url_manager.get_exchange_urls(url_type.upper())
        if not res:
            raise RuntimeError(f"유형 '{url_type}'의 URL을 찾을 수 없습니다.")
        return res

    # ------------------------ Config helpers ------------------ #
    def get_exchange_config(
        self,
        exchange: str,
        symbols: list[str],
        req_type: SocketRequestType,
        region: str = "korea",
    ) -> ExchangeSocketConfig:
        """특정 거래소의 구성 정보를 반환합니다.

        Args:
            exchange: 거래소
            symbols: 심볼 목록
            req_type: 요청 타입
            region: 지역
        Returns:
            ExchangeSocketConfig: 구성 정보
        """
        if exchange.lower() == "all":
            raise ValueError(
                "'all'은 단일 구성 조회에 사용할 수 없습니다. get_all_exchange_configs를 사용하세요."
            )
        spec = SocketConnectMetaData(
            region=region,
            exchange=exchange,
            req_type=req_type,
            symbols=symbols,
        )
        return self._config_manager.get_exchange_config_from(spec)

    def get_all_exchange_configs(
        self, spec: SocketConnectMetaData
    ) -> dict[str, ExchangeSocketConfig]:
        """모든 거래소의 구성 정보를 반환합니다.

        Args:
            spec: SocketConnectMetaData
        Returns:
            dict[str, ExchangeSocketConfig]: 구성 정보
        """
        return self._config_manager.get_all_exchange_configs(spec)

    # ------------------------ Spec helpers ------------------- #
    def get_exchange_config_from(
        self, spec: SocketConnectMetaData
    ) -> ExchangeSocketConfig:
        """SocketConnectMetaData 기반 구성 정보를 반환합니다."""
        return self._config_manager.get_exchange_config_from(spec)

    def get_all_exchange_configs_from(
        self, spec: SocketConnectMetaData
    ) -> dict[str, ExchangeSocketConfig]:
        """SocketConnectMetaData 기반 모든 거래소 구성 정보를 반환합니다."""
        return self._config_manager.get_all_exchange_configs_from(spec)


# 모듈 전역에서 재사용할 단일 서비스 인스턴스
_exchange_service = ExchangeService()
