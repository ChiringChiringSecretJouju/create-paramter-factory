import time
from typing import Any

from core.socket_params.base import SocketParameterCreator


class BinanceSocketParameter(SocketParameterCreator):
    """바이낸스 거래소 웹소켓 파라미터 생성기"""

    def __init__(self):
        super().__init__(exchange="binance", region="asia")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict[str, Any]:
        """바이낸스 전용 파라미터 생성
        Format: {"method": "SUBSCRIBE", "params": ["btcusdt@ticker"], "id": 1}
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        result = dict(self.template[req_type])
        
        # params 초기화
        if "params" not in result or not isinstance(result["params"], list):
            result["params"] = []

        # id 생성
        if result.get("id") == "{req_id}":
            result["id"] = int(time.time() * 1000) % 1000000

        stream_suffix = self._get_stream_suffix(req_type)

        formatted_streams = []
        for symbol in symbols:
            # Binance: 소문자, 구분자 없음 (BTC-USDT -> btcusdt)
            formatted_symbol = symbol.lower().replace("-", "").replace("_", "")
            # USDT가 없으면 붙여줌 (입력이 BTC일 경우)
            if not formatted_symbol.endswith("usdt"):
                 formatted_symbol += "usdt"
                 
            stream = f"{formatted_symbol}@{stream_suffix}"
            formatted_streams.append(stream)

        result["params"] = formatted_streams
        return result

    def _get_stream_suffix(self, req_type: str) -> str:
        suffix_map = {
            "ticker": "ticker",
            "orderbook": "depth20",  # default to depth20
            "trade": "trade",
        }
        return suffix_map.get(req_type, "ticker")


class BybitSocketParameter(SocketParameterCreator):
    """바이비트 거래소 웹소켓 파라미터 생성기 (V5)"""

    def __init__(self):
        super().__init__(exchange="bybit", region="asia")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict[str, Any]:
        """바이비트 전용 파라미터 생성
        Format: {"op": "subscribe", "args": ["tickers.BTCUSDT"], "req_id": "..."}
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        result = dict(self.template[req_type])
        
        if result.get("req_id") == "{req_id}":
            result["req_id"] = f"bybit-{int(time.time() * 1000)}"

        topic = self._get_topic(req_type)
        formatted_args = []
        for symbol in symbols:
            # Bybit: 대문자, 구분자 없음 (BTC -> BTCUSDT)
            s = symbol.upper().replace("-", "").replace("_", "")
            if not s.endswith("USDT"):
                s += "USDT"
            
            formatted_args.append(f"{topic}.{s}")

        result["args"] = formatted_args
        return result

    def _get_topic(self, req_type: str) -> str:
        topic_map = {
            "ticker": "tickers",
            "orderbook": "orderbook.50",
            "trade": "publicTrade",
        }
        return topic_map.get(req_type, "tickers")


class OKXSocketParameter(SocketParameterCreator):
    """OKX 거래소 웹소켓 파라미터 생성기"""

    def __init__(self):
        super().__init__(exchange="okx", region="asia")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict[str, Any]:
        """OKX 전용 파라미터 생성
        Format: {"op": "subscribe", "args": [{"channel": "tickers", "instId": "BTC-USDT"}]}
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        result = dict(self.template[req_type])
        
        channel = self._get_channel(req_type)
        args = []
        for symbol in symbols:
            # OKX: 대문자, 하이픈 구분 (BTC -> BTC-USDT)
            s = symbol.upper().replace("_", "-")
            if "-" not in s:
                s += "-USDT"
            
            args.append({
                "channel": channel,
                "instId": s
            })

        result["args"] = args
        return result

    def _get_channel(self, req_type: str) -> str:
        channel_map = {
            "ticker": "tickers",
            "orderbook": "books",
            "trade": "trades",
        }
        return channel_map.get(req_type, "tickers")

