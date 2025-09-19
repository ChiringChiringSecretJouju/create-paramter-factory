from __future__ import annotations

import uuid
from core.socket_params.base import SocketParameterCreator


class BitfinexSocketParameter(SocketParameterCreator):
    """Bitfinex 거래소 파라미터 생성기.

    Bitfinex는 심볼별 개별 구독이 필요하며,
    심볼 형식은 t{BASE}{QUOTE} (예: tBTCUSD, tETHUSD)를 사용합니다.
    """

    def __init__(self) -> None:
        super().__init__(exchange="bitfinex", region="europe")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """Bitfinex 전용 파라미터 생성.

        Args:
            symbols: 심볼 목록 (예: ["BTC_USD", "ETH_USD"])
            req_type: 요청 타입 (ticker, orderbook 등)

        Returns:
            dict: 생성된 소켓 파라미터

        Raises:
            ValueError: 지원하지 않는 요청 타입인 경우
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        template = dict(self.template[req_type])
        result = dict(template)

        # Bitfinex 심볼 형식으로 변환: BTC_USD → tBTCUSD
        if "symbol" in result and "{bitfinex_symbol}" in str(result["symbol"]):
            # 첫 번째 심볼만 사용 (Bitfinex는 개별 구독)
            bitfinex_symbol = self._format_symbol(symbols[0])
            result["symbol"] = bitfinex_symbol

        return result

    def _format_symbol(self, symbol: str) -> str:
        """심볼을 Bitfinex 형식으로 변환.

        Args:
            symbol: 표준 심볼 (예: "BTC_USD")

        Returns:
            str: Bitfinex 형식 심볼 (예: "tBTCUSD")
        """
        if "_" in symbol:
            base, quote = symbol.split("_", 1)
            return f"t{base.upper()}{quote.upper()}"
        return f"t{symbol.upper()}"

    def create_multiple_parameters(
        self, symbols: list[str], req_type: str
    ) -> list[dict]:
        """여러 심볼에 대한 파라미터 생성 (Bitfinex는 개별 구독 필요).

        Args:
            symbols: 심볼 목록
            req_type: 요청 타입

        Returns:
            list[dict]: 각 심볼별 파라미터 리스트
        """
        return [self.create_parameters([symbol], req_type) for symbol in symbols]
