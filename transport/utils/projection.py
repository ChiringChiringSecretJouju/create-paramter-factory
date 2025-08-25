import asyncio
from pathlib import Path

import yaml
from common.types import ExchangeMetadata
from core.properties import SocketRequestType


async def load_projection_async(
    exchange: str, req_type: SocketRequestType, template_dir: str
) -> list[str]:
    """YAML 템플릿에서 projection 필드를 비동기로 로드

    - 파일명 규칙: _market_all_{req_type}.yml
    - 섹션 키: exchange 소문자 우선 조회, 없으면 원본 키
    - 반환: 리스트 (없으면 빈 리스트)
    """
    file_name = f"_market_all_{req_type}.yml"
    path = Path(template_dir) / file_name

    def _read_yaml() -> list[str]:
        try:
            with path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            section = data.get(exchange.lower()) or data.get(exchange)
            params = section.get("parameter") if section else None
            if isinstance(params, list):
                return [str(p) for p in params]
        except FileNotFoundError:
            return []
        return []

    return await asyncio.to_thread(_read_yaml)


def make_exchange_metadata(
    region: str, exchange: str, req_type: SocketRequestType
) -> ExchangeMetadata:
    """동기 메타데이터 생성 헬퍼.

    ConnectMessageTD의 `source` 필드 구성을 캡슐화한다.

    Args:
        region: 지역
        exchange: 거래소
        req_type: 요청 타입
    Returns:
        ExchangeMetadata: 메타데이터
    """
    return {
        "region": region,
        "exchange": exchange,
        "request_type": str(req_type),
    }
