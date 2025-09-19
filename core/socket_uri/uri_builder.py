import configparser
from pathlib import Path
from core.types import (
    AllMarketURLs,
    KoreaRegionURLs,
    AsiaRegionURLs,
    EuropeRegionURLs,
    NorthAmericaRegionURLs,
)
from common.exceptions import (
    RegionNotRegisteredException,
    MarketNotRegisteredException,
)

# ConfigParser 설정 (core/socket_uri/builder.py 기준으로 core/_urls.conf를 읽음)
path = Path(__file__).parent.parent.parent
parser = configparser.ConfigParser()
parser.read(f"{path}/setting/config/urls.conf")


class ExchangeURLManager:
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
            ),
            asia=AsiaRegionURLs(
                binance=self.parser.get(f"{uri_type}URL", "BINANCE"),
                bybit=self.parser.get(f"{uri_type}URL", "BYBIT"),
                okx=self.parser.get(f"{uri_type}URL", "OKX"),
                huobi=self.parser.get(f"{uri_type}URL", "HUOBI"),
                gateio=self.parser.get(f"{uri_type}URL", "GATEIO"),
            ),
            europe=EuropeRegionURLs(
                bitfinex=self.parser.get(f"{uri_type}URL", "BITFINEX"),
            ),
            north_america=NorthAmericaRegionURLs(
                coinbase=self.parser.get(f"{uri_type}URL", "COINBASE"),
                kraken=self.parser.get(f"{uri_type}URL", "KRAKEN"),
            ),
        )

    def get_symbol_collect_url(self, market: str, location: str, url_type: str) -> str:
        """특정 거래소와 지역에 대한 URL을 반환합니다."""
        urls: AllMarketURLs = self.get_exchange_urls(url_type.upper())
        region_urls: dict[str, str] = urls.get(location)

        if not region_urls:
            raise RegionNotRegisteredException(
                region=location,
                exchange_name=market,
                req_type=url_type,
                symbols=[],
                message=f"지역이 등록되지 않았습니다: {location}",
            )

        ex_urls: str | None = region_urls.get(market)
        if not ex_urls:
            raise MarketNotRegisteredException(
                region=location,
                exchange_name=market,
                req_type=url_type,
                symbols=[],
                message=f"{location} 지역에서 등록되지 않은 거래소입니다: {market}",
            )

        return ex_urls

    def get_region_urls(self, region: str, uri_type: str) -> dict[str, str]:
        """특정 지역의 모든 거래소 URL을 반환합니다."""
        urls: AllMarketURLs = self.get_exchange_urls(uri_type.upper())
        region_urls = urls.get(region)
        if not region_urls:
            raise RegionNotRegisteredException(
                region=region,
                exchange_name="",
                req_type=uri_type,
                symbols=[],
                message=f"지역이 등록되지 않았습니다: {region}",
            )
        return region_urls
