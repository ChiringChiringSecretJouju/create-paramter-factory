from __future__ import annotations


import asyncio
from pathlib import Path

import logging
import yaml
import configparser
from dataclasses import dataclass
from transport.types.message_types import ExchangeMetadata
from core.properties import SocketRequestType


@dataclass(slots=True)
class ProducerConfig:
    bootstrap_servers: str = "localhost:9092"
    acks: int | str = 1
    linger_ms: int = 0
    max_batch_size: int = 1_048_576
    max_request_size: int = 1_048_576

    @classmethod
    def from_file(cls, path: Path) -> ProducerConfig:
        """설정 파일에서 Kafka 프로듀서 설정 로드"""
        parser = configparser.ConfigParser()
        parser.read(path.as_posix())

        bs = parser.get("KAFKA", "bootstrap_servers")
        acks_raw = parser.get("KAFKA", "acks", fallback="1")

        try:
            acks: int | str = int(acks_raw)
        except (TypeError, ValueError):
            acks = acks_raw

        # kafka 설정 파일에서 linger_ms, max_batch_size, max_request_size 로드
        linger_ms = int(parser.get("KAFKA", "linger_ms", fallback="0"))
        max_batch_size = int(parser.get("KAFKA", "max_batch_size", fallback="1048576"))
        max_request_size = int(
            parser.get("KAFKA", "max_request_size", fallback="1048576")
        )
        return cls(
            bootstrap_servers=bs,
            acks=acks,
            linger_ms=linger_ms,
            max_batch_size=max_batch_size,
            max_request_size=max_request_size,
        )


async def load_projection_async(
    exchange: str, req_type: str, template_dir: str
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


def load_kafka_config() -> ProducerConfig:
    """레포 루트 기준 setting/config/kafka_config.conf에서 Kafka 설정 로드

    레거시 경로 폴백 제거, 단일 경로만 사용
    """
    root = Path(__file__).parent.parent.parent
    conf_path = root / "setting" / "config" / "kafka_config.conf"
    logger = logging.getLogger(__name__)
    if conf_path.exists():
        try:
            return ProducerConfig.from_file(conf_path)
        except Exception as e:  # 파일 손상/파싱 오류 등 방어
            logger.warning(
                "Kafka 설정 파일을 읽는 데 실패했습니다(%s). 기본값을 사용합니다.", e
            )
    logger.warning(
        "Kafka 설정 파일을 찾지 못했습니다: %s. 기본값을 사용합니다.",
        conf_path,
    )
    return ProducerConfig()


def make_exchange_metadata(
    *,
    region: str,
    exchange: str,
    req_type: SocketRequestType,
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
    return ExchangeMetadata(
        region=region,
        exchange=exchange,
        request_type=str(req_type),
    )
