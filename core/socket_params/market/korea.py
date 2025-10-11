import uuid
from abc import abstractmethod
from core.socket_params.base import SocketParameterCreator


class KRWExchangeSocketParameter(SocketParameterCreator):
    """한국 원화 기반 거래소 공통 파라미터 생성 베이스."""

    def create_parameters(self, symbols: list[str], req_type: str) -> list[dict]:
        """공통 파라미터 생성."""
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        template_list = list(self.template[req_type])
        result = []

        ticket_uuid = str(uuid.uuid4())
        result.append({"ticket": ticket_uuid})

        for item in template_list:
            item_copy = dict(item)

            if "ticket" in item_copy:
                del item_copy["ticket"]

            if "codes" in item_copy:
                placeholder = self._get_codes_placeholder()
                if placeholder in str(item_copy["codes"]):
                    item_copy["codes"] = self._format_symbols(symbols)

            result.append(item_copy)

        return result

    def _format_symbols(self, symbols: list[str]) -> list[str]:
        """심볼 포맷팅 (자식 클래스에서 구현)"""
        return [f"KRW-{s.split('_')[0]}" for s in symbols]

    @abstractmethod
    def _get_codes_placeholder(self) -> str:
        """코드 플레이스홀더 반환 (자식 클래스에서 구현)"""
        pass


class UpbitSocketParameter(KRWExchangeSocketParameter):
    """업비트 거래소 파라미터 생성기."""

    def __init__(self):
        super().__init__(exchange="upbit", region="korea")

    def _get_codes_placeholder(self) -> str:
        return "{upbit_codes}"


class BithumbSocketParameter(KRWExchangeSocketParameter):
    """빗썸 거래소 파라미터 생성기."""

    def __init__(self):
        super().__init__(exchange="bithumb", region="korea")

    def _get_codes_placeholder(self) -> str:
        return "{bithumb_symbols}"


class KorbitSocketParameter(SocketParameterCreator):
    """코빗 거래소 파라미터 생성기."""

    def __init__(self):
        super().__init__(exchange="korbit", region="korea")

    def create_parameters(self, symbols: list[str], req_type: str) -> list[dict]:
        """코빗 전용 파라미터 생성."""
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        template_list = list(self.template[req_type])
        result = []

        for item in template_list:
            item_copy = dict(item)

            if "symbols" in item_copy and "{korbit_symbols}" in str(
                item_copy["symbols"]
            ):
                item_copy["symbols"] = [f"{s.lower()}_krw" for s in symbols]

            result.append(item_copy)

        return result


class CoinoneSocketParameter(SocketParameterCreator):
    """코인원 거래소 파라미터 생성기."""

    def __init__(self):
        super().__init__(exchange="coinone", region="korea")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """코인원 전용 파라미터 생성 (심볼당 1요청)."""
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        template = dict(self.template[req_type])
        return self._create_single_parameter(template, symbols[0])

    def _create_single_parameter(self, template: dict, symbol: str) -> dict:
        """단일 심볼 요청 파라미터 생성."""
        result = dict(template)

        for key, value in result.items():
            if isinstance(value, dict) and "target_currency" in value:
                currency = symbol.split("_")[0].lower()
                result[key]["target_currency"] = currency.upper()

        return result


class GopaxSocketParameter(SocketParameterCreator):
    """Gopax 거래소 파라미터 생성기.

    Gopax WebSocket API 특징:
    - Ticker: 전체 티커 일괄 구독 (symbols 무시)
    - Orderbook: tradingPairNames 배열로 다중 심볼 구독 가능
    심볼 형식: BTC-KRW (대문자 하이픈)
    메시지 형식: {"n": "SubscribeToTickers", "o": {...}}
    """

    def __init__(self):
        super().__init__(exchange="gopax", region="korea")

    def create_parameters(self, symbols: list[str], req_type: str) -> dict:
        """Gopax 전용 파라미터 생성.

        Args:
            symbols: 심볼 목록 (예: ["BTC", "ETH"])
            req_type: 요청 타입 (ticker, orderbook 등)

        Returns:
            dict: 생성된 소켓 파라미터

        Raises:
            ValueError: 지원하지 않는 요청 타입인 경우
        """
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        result = dict(self.template[req_type])

        # ticker는 전체 구독이므로 템플릿 그대로 반환
        if req_type in ("ticker", "unsubscribe_ticker"):
            return result

        # orderbook의 경우 tradingPairNames 플레이스홀더 처리
        if "o" in result and isinstance(result["o"], dict):
            if "tradingPairNames" in result["o"]:
                # 플레이스홀더 리스트를 실제 심볼 리스트로 교체
                result["o"]["tradingPairNames"] = [
                    self._format_symbol(s) for s in symbols
                ]

        return result

    def _format_symbol(self, symbol: str) -> str:
        """심볼을 Gopax 형식으로 변환.

        Args:
            symbol: 표준 심볼 (예: "BTC" 또는 "BTC_KRW")

        Returns:
            str: Gopax 형식 심볼 (예: "BTC-KRW")
        """
        if "_" in symbol:
            # "BTC_KRW" → "BTC-KRW"
            parts = symbol.split("_")
            return f"{parts[0].upper()}-{parts[1].upper()}"
        # "BTC" → "BTC-KRW" (기본적으로 KRW 페어 가정)
        return f"{symbol.upper()}-KRW"
