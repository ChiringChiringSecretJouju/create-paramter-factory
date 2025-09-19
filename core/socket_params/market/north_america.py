from __future__ import annotations

import time
from core.socket_params.base import SocketParameterCreator


class CoinbaseSocketParameter(SocketParameterCreator):
    """Coinbase Exchange 거래소 파라미터 생성기.
    
    Coinbase Exchange WebSocket API 사용
    심볼 형식: ETH-USD (대문자 하이픈)
    채널: ticker, level2
    """

    def __init__(self) -> None:
        super().__init__(exchange="coinbase", region="north_america")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """Coinbase 전용 파라미터 생성.
        
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

        result = dict(self.template[req_type])
        
        # product_ids 처리
        if "product_ids" in result and "{coinbase_products}" in str(result["product_ids"]):
            result["product_ids"] = [self._format_symbol(s) for s in symbols]

        return result

    def _format_symbol(self, symbol: str) -> str:
        """심볼을 Coinbase 형식으로 변환.
        
        Args:
            symbol: 표준 심볼 (예: "BTC_USD")
            
        Returns:
            str: Coinbase 형식 심볼 (예: "BTC-USD")
        """
        if "_" in symbol:
            return symbol.replace("_", "-").upper()
        return symbol.upper()


class KrakenSocketParameter(SocketParameterCreator):
    """Kraken 거래소 파라미터 생성기.
    
    Kraken WebSocket V2 API 사용
    심볼 형식: BTC/USD (슬래시), BTC→XBT 교정
    채널: ticker, book
    """

    def __init__(self) -> None:
        super().__init__(exchange="kraken", region="north_america")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """Kraken 전용 파라미터 생성.
        
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

        result = dict(self.template[req_type])
        
        # params.symbol 처리
        if "params" in result and isinstance(result["params"], dict):
            if "symbol" in result["params"] and "{kraken_symbols}" in str(result["params"]["symbol"]):
                result["params"]["symbol"] = [self._format_symbol(s) for s in symbols]

        return result

    def _format_symbol(self, symbol: str) -> str:
        """심볼을 Kraken 형식으로 변환.
        
        Args:
            symbol: 표준 심볼 (예: "BTC_USD")
            
        Returns:
            str: Kraken 형식 심볼 (예: "XBT/USD")
        """
        if "_" in symbol:
            base, quote = symbol.split("_", 1)
            # BTC → XBT 교정
            if base.upper() == "BTC":
                base = "XBT"
            return f"{base.upper()}/{quote.upper()}"
        return symbol.upper()
