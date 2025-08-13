import uuid
from abc import abstractmethod
from setting.template._base_create_paramteter import BaseSocketParameter


class KRWExchangeSocketParameter(BaseSocketParameter):
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


class KorbitSocketParameter(BaseSocketParameter):
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


class CoinoneSocketParameter(BaseSocketParameter):
    """코인원 거래소 파라미터 생성기."""

    def __init__(self):
        super().__init__(exchange="coinone", region="korea")

    def create_parameters(self, symbols: list[str], req_type: str) -> list[dict]:
        """코인원 전용 파라미터 생성 (심볼당 1요청)."""
        if req_type not in self.template:
            raise ValueError(f"지원하지 않는 요청 타입: {req_type}")

        template = dict(self.template[req_type])

        return self._create_single_parameter(template, symbols[0], req_type)

    def _create_single_parameter(
        self, template: dict, symbol: str, req_type: str
    ) -> dict:
        """단일 심볼 요청 파라미터 생성."""
        result = dict(template)

        for key, value in result.items():
            if isinstance(value, dict) and "target_currency" in value:
                currency = symbol.split("_")[0].lower()
                result[key]["target_currency"] = currency.upper()

        return result
