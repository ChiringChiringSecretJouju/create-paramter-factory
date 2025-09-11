import time
from core.socket_params.base import SocketParameterCreator


class BinanceSocketParameter(SocketParameterCreator):
    """바이낸스 거래소 웹소켓 파라미터 생성기"""

    def __init__(self):
        super().__init__(exchange="binance", region="asia")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """바이낸스 전용 파라미터 생성

        Binance WebSocket API에 맞게 형식을 생성합니다.
        - method: SUBSCRIBE 또는 UNSUBSCRIBE
        - params: ["btcusdt@ticker", "ethusdt@ticker"] 형식의 스트림 배열
        - id: 요청 고유 ID
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        # 템플릿에서 기본 구조 복사
        result = dict(self.template[req_type])

        # id 플레이스홀더 처리
        if result.get("id") == "{req_id}":
            result["id"] = int(time.time() * 1000) % 1000000

        # params 플레이스홀더 처리
        if "params" in result and "{binance_streams}" in str(result["params"]):
            result["params"] = self._format_symbols(symbols, req_type)

        return result

    def _format_symbols(self, symbols: list[str], req_type: str) -> list[str]:
        """심볼을 바이낸스 스트림 형식으로 포맷팅"""
        stream_suffix = self._get_stream_suffix(req_type)
        formatted_streams = []

        for symbol in symbols:
            # 심볼을 소문자로 변환하고 '_' 제거 (BTC_USDT -> btcusdt)
            formatted_symbol = symbol.lower().replace("_", "")
            # 스트림 형식 구성 (예: btcusdt@ticker)
            stream = f"{formatted_symbol}@{stream_suffix}"
            formatted_streams.append(stream)

        return formatted_streams

    def _get_stream_suffix(self, req_type: str) -> str:
        """요청 타입에 따른 스트림 접미사 반환"""
        suffix_map = {
            "ticker": "ticker",
            "orderbook": "depth",
            "unsubscribe_ticker": "ticker",
            "unsubscribe_orderbook": "depth",
        }
        return suffix_map.get(req_type, "ticker")
