from __future__ import annotations

import time
import uuid
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
        if "params" in result:
            # 템플릿의 스트림 접미사(ticker/depth 등)에 관계없이 항상 치환
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
            stream = f"{formatted_symbol}usdt@{stream_suffix}"
            formatted_streams.append(stream)

        return formatted_streams

    def _get_stream_suffix(self, req_type: str) -> str:
        """요청 타입에 따른 스트림 접미사 반환"""
        suffix_map = {
            "ticker": "ticker",
            "orderbook": "depth",
            "trade": "trade",
            "unsubscribe_ticker": "ticker",
            "unsubscribe_orderbook": "depth",
            "unsubscribe_trade": "trade",
        }
        return suffix_map.get(req_type, "ticker")


class BybitSocketParameter(SocketParameterCreator):
    """Bybit 거래소 파라미터 생성기.

    Bybit V5 API 사용, op: "subscribe"/"unsubscribe" 형식
    심볼 형식: BTCUSDT (대문자 연결)
    토픽 형식: tickers.BTCUSDT, orderbook.50.BTCUSDT
    """

    def __init__(self) -> None:
        super().__init__(exchange="bybit", region="asia")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """Bybit 전용 파라미터 생성.

        Args:
            symbols: 심볼 목록 (예: ["BTC_USDT", "ETH_USDT"])
            req_type: 요청 타입 (ticker, orderbook 등)

        Returns:
            dict: 생성된 소켓 파라미터

        Raises:
            ValueError: 지원하지 않는 요청 타입인 경우
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        result = dict(self.template[req_type])

        # req_id 처리
        if "req_id" in result and result["req_id"] == "{uuid}":
            result["req_id"] = str(uuid.uuid4())

        # args 플레이스홀더 처리
        ticker: bool = req_type == "ticker" and "{bybit_tickers}" in str(result["args"])
        orderbook: bool = req_type == "orderbook" and "{bybit_orderbooks}" in str(
            result["args"]
        )
        trade: bool = req_type == "trade" and "{bybit_trades}" in str(result["args"])
        unsubscribe: bool = "unsubscribe" in req_type

        # 심볼 포맷팅
        # fmt: off
        ticker_symbols: list[str] = [f"tickers.{self._format_symbol(s)}" for s in symbols]
        orderbook_symbols: list[str] = [f"orderbook.50.{self._format_symbol(s)}" for s in symbols]
        trade_symbols: list[str] = [f"publicTrade.{self._format_symbol(s)}" for s in symbols]

        # fmt: on
        if "args" in result:
            if ticker:
                result["args"] = ticker_symbols
            elif orderbook:
                result["args"] = orderbook_symbols
            elif trade:
                result["args"] = trade_symbols
            elif unsubscribe:
                # unsubscribe는 subscribe와 동일한 args 사용
                if "ticker" in req_type:
                    result["args"] = ticker_symbols
                elif "orderbook" in req_type:
                    result["args"] = orderbook_symbols
                elif "trade" in req_type:
                    result["args"] = trade_symbols

        return result

    def _format_symbol(self, symbol: str) -> str:
        """심볼을 Bybit 형식으로 변환.

        Args:
            symbol: 표준 심볼 (예: "BTC_USDT" 또는 "BTC")

        Returns:
            str: Bybit 형식 심볼 (예: "BTCUSDT")
        """
        if "_" in symbol:
            # "BTC_USDT" → "BTCUSDT"
            return symbol.replace("_", "").upper()
        else:
            # "BTC" → "BTCUSDT" (기본적으로 USDT 페어 가정)
            return f"{symbol.upper()}USDT"


class OKXSocketParameter(SocketParameterCreator):
    """OKX 거래소 파라미터 생성기.

    OKX V5 API 사용, op: "subscribe"/"unsubscribe" 형식
    심볼 형식: BTC-USDT (대문자 하이픈)
    채널: tickers, books
    """

    def __init__(self) -> None:
        super().__init__(exchange="okx", region="asia")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """OKX 전용 파라미터 생성.

        Args:
            symbols: 심볼 목록 (예: ["BTC_USDT", "ETH_USDT"])
            req_type: 요청 타입 (ticker, orderbook 등)

        Returns:
            dict: 생성된 소켓 파라미터

        Raises:
            ValueError: 지원하지 않는 요청 타입인 경우
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        result = dict(self.template[req_type])

        # args 처리 - 단일 심볼용 템플릿을 다중 심볼로 확장
        if "args" in result and len(result["args"]) == 1:
            template_arg = result["args"][0]
            if isinstance(template_arg, dict):
                # 채널별 args 생성
                channel = template_arg.get("channel")
                if channel == "tickers":
                    result["args"] = [
                        {"channel": "tickers", "instId": self._format_symbol(s)}
                        for s in symbols
                    ]
                elif channel == "books":
                    result["args"] = [
                        {"channel": "books", "instId": self._format_symbol(s)}
                        for s in symbols
                    ]
                elif channel == "trades":
                    result["args"] = [
                        {"channel": "trades", "instId": self._format_symbol(s)}
                        for s in symbols
                    ]

        return result

    def _format_symbol(self, symbol: str) -> str:
        if "_" in symbol:
            # "BTC_USDT" → "BTC-USDT"
            return symbol.replace("_", "-").upper()
        else:
            # "BTC" → "BTC-USDT" (기본적으로 USDT 페어 가정)
            return f"{symbol.upper()}-USDT"


class HuobiSocketParameter(SocketParameterCreator):
    """Huobi (HTX) 거래소 파라미터 생성기.

    Huobi V1 API 사용, sub/unsub 키워드
    심볼 형식: btcusdt (소문자 연결)
    토픽 형식: market.btcusdt.ticker, market.btcusdt.depth.step0
    """

    def __init__(self) -> None:
        super().__init__(exchange="huobi", region="asia")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """Huobi 전용 파라미터 생성.

        Args:
            symbols: 심볼 목록 (예: ["BTC_USDT", "ETH_USDT"])
            req_type: 요청 타입 (ticker, orderbook 등)

        Returns:
            dict: 생성된 소켓 파라미터

        Raises:
            ValueError: 지원하지 않는 요청 타입인 경우
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        result = dict(self.template[req_type])

        # id 처리
        if "id" in result and result["id"] == "{req_id}":
            result["id"] = str(int(time.time() * 1000))

        # sub/unsub 토픽 처리 - 첫 번째 심볼만 사용 (Huobi는 개별 구독)
        symbol = symbols[0] if symbols else "btcusdt"
        formatted_symbol = f"{self._format_symbol(symbol)}"

        # 토픽 포맷팅
        # fmt: off
        sub_result_ticker = "sub" in result and "{huobi_ticker_topics}" in str(result["sub"])
        sub_result_orderbook = "sub" in result and "{huobi_depth_topics}" in str(result["sub"])
        sub_result_trade = "sub" in result and "{huobi_trade_topics}" in str(result["sub"])
        unsub_result_ticker = "unsub" in result and "{huobi_ticker_topics}" in str(result["unsub"])
        unsub_result_orderbook = "unsub" in result and "{huobi_depth_topics}" in str(result["unsub"])
        unsub_result_trade = "unsub" in result and "{huobi_trade_topics}" in str(result["unsub"])

        # fmt: on
        if sub_result_ticker:
            result["sub"] = f"market.{formatted_symbol}.ticker"
        elif sub_result_orderbook:
            result["sub"] = f"market.{formatted_symbol}.depth.step0"
        elif sub_result_trade:
            result["sub"] = f"market.{formatted_symbol}.trade.detail"
        elif unsub_result_ticker:
            result["unsub"] = f"market.{formatted_symbol}.ticker"
        elif unsub_result_orderbook:
            result["unsub"] = f"market.{formatted_symbol}.depth.step0"
        elif unsub_result_trade:
            result["unsub"] = f"market.{formatted_symbol}.trade.detail"

        return result

    def _format_symbol(self, symbol: str) -> str:
        """심볼을 Huobi 형식으로 변환.

        Args:
            symbol: 표준 심볼 (예: "BTC_USDT")

        Returns:
            str: Huobi 형식 심볼 (예: "btcusdt")
        """
        if "_" in symbol:
            return symbol.replace("_", "").lower()
        return f"{symbol.lower()}usdt"


class GateIOSocketParameter(SocketParameterCreator):
    """Gate.io 거래소 파라미터 생성기.

    Gate.io V4 API 사용, event: "subscribe"/"unsubscribe" 형식
    심볼 형식: BTC_USDT (대문자 언더스코어 유지)
    채널: spot.tickers, spot.order_book
    """

    def __init__(self) -> None:
        super().__init__(exchange="gateio", region="asia")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """Gate.io 전용 파라미터 생성.

        Args:
            symbols: 심볼 목록 (예: ["BTC_USDT", "ETH_USDT"])
            req_type: 요청 타입 (ticker, orderbook 등)

        Returns:
            dict: 생성된 소켓 파라미터

        Raises:
            ValueError: 지원하지 않는 요청 타입인 경우
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        result = dict(self.template[req_type])

        # time 처리 (epoch seconds)
        if "time" in result and result["time"] == "{epoch_sec}":
            result["time"] = int(time.time())

        # payload 처리
        if "payload" in result and "{gateio_pairs}" in str(result["payload"]):
            result["payload"] = [f"{self._format_symbol(s)}_USDT" for s in symbols]

        return result

    def _format_symbol(self, symbol: str) -> str:
        """심볼을 Gate.io 형식으로 변환.

        Args:
            symbol: 표준 심볼 (예: "BTC_USDT")

        Returns:
            str: Gate.io 형식 심볼 (예: "BTC_USDT")
        """
        # Gate.io는 언더스코어 형식 그대로 사용
        return f"{symbol.upper()}USDT"


class MEXCSocketParameter(SocketParameterCreator):
    """MEXC 거래소 파라미터 생성기.

    MEXC WebSocket API v3 사용
    심볼 형식: BTCUSDT (대문자 연결)
    공통 프리픽스: spot@public
    채널 형식:
        - Ticker: spot@public.miniTicker.v3.api@{symbol}@UTC+8
        - Orderbook: spot@public.limit.depth.v3.api@{symbol}@20
    메서드: SUBSCRIPTION / UNSUBSCRIPTION
    공식 문서: https://www.mexc.com/api-docs/spot-v3/websocket-market-streams
    """

    def __init__(self) -> None:
        super().__init__(exchange="mexc", region="asia")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """MEXC 전용 파라미터 생성.

        Args:
            symbols: 심볼 목록 (예: ["BTC_USDT", "ETH_USDT"])
            req_type: 요청 타입 (ticker, orderbook 등)

        Returns:
            dict: 생성된 소켓 파라미터

        Raises:
            ValueError: 지원하지 않는 요청 타입인 경우
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        result = dict(self.template[req_type])

        # params 플레이스홀더 처리
        if "params" in result and len(result["params"]) > 0:
            # 템플릿의 첫 번째 params를 채널 템플릿으로 사용
            channel_template = result["params"][0]
            result["params"] = [
                channel_template.replace("{symbol}", self._format_symbol(s))
                for s in symbols
            ]

        return result

    def _format_symbol(self, symbol: str) -> str:
        """심볼을 MEXC 형식으로 변환.

        Args:
            symbol: 표준 심볼 (예: "BTC_USDT" 또는 "BTC")

        Returns:
            str: MEXC 형식 심볼 (예: "BTCUSDT")
        """
        if "_" in symbol:
            return symbol.replace("_", "").upper()
        return f"{symbol.upper()}USDT"
