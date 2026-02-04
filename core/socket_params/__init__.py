from core.socket_params.base import SocketParameterCreator

from core.socket_params.market.korea import (
    UpbitSocketParameter,
    BithumbSocketParameter,
    KorbitSocketParameter,
    CoinoneSocketParameter,
)


from core.socket_params.market.asia import (
    BinanceSocketParameter,
    OKXSocketParameter,
    BybitSocketParameter,
)


class SocketParameterFactory:
    """거래소별 소켓 파라미터 생성기 팩토리

    팩토리 패턴을 사용하여 거래소별 특화된 파라미터 생성기를 제공함
    """

    _creators = {
        # 한국 지역
        "upbit": UpbitSocketParameter,
        "bithumb": BithumbSocketParameter,
        "korbit": KorbitSocketParameter,
        "coinone": CoinoneSocketParameter,
        # 아시아 지역
        "binance": BinanceSocketParameter,
        "okx": OKXSocketParameter,
        "bybit": BybitSocketParameter,
    }

    @classmethod
    def get_creator(cls, exchange: str) -> SocketParameterCreator:
        """거래소 이름에 따라 적절한 파라미터 생성기 반환

        Args:
            exchange (str): 거래소 이름 (소문자)

        Returns:
            SocketParameterCreator: 해당 거래소의 파라미터 생성기 인스턴스

        Raises:
            ValueError: 지원하지 않는 거래소인 경우
        """
        if exchange not in cls._creators:
            supported = ", ".join(cls._creators.keys())
            raise ValueError(
                f"지원하지 않는 거래소: {exchange}. 지원 거래소: {supported}"
            )

        creator_class = cls._creators[exchange]
        return creator_class()

    @classmethod
    def create_socket_parameter(
        cls,
        exchange: str,
        symbols: list[str],
        req_type: str,
    ) -> dict | list[dict]:
        """지정된 거래소에 대한 소켓 파라미터 생성

        편의 메서드로, 팩토리에서 생성기를 가져와 바로 파라미터를 생성함

        Args:
            exchange (str): 거래소 이름
            symbols (list[str]): 심볼 목록
            req_type (str): 요청 타입 (ticker, orderbook 등)

        Returns:
            dict | list[dict]: 생성된 소켓 파라미터
        """
        if exchange == "all":
            return cls._create_all_socket_parameter(symbols, req_type)
        creator = cls.get_creator(exchange)
        return creator.create_parameters(symbols, req_type)

    @classmethod
    def _create_all_socket_parameter(
        cls,
        symbols: list[str],
        req_type: str,
    ) -> dict | list[dict]:
        """모든 거래소에 대한 소켓 파라미터 생성"""
        return [
            cls.get_creator(exchange).create_parameters(symbols, req_type)
            for exchange in cls._creators
        ]
