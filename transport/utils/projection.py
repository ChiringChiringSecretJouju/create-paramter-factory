from __future__ import annotations


import asyncio
from pathlib import Path

import yaml
import configparser
from dataclasses import dataclass


@dataclass(slots=True)
class ProducerConfig:
    bootstrap_servers: str = "localhost:9092"
    topic: str = "market_connect_v1"
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
        topic = parser.get("KAFKA", "topic", fallback="market_connect_v1")
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
            topic=topic,
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

    레거시 경로(setting/_kafka_config.conf)도 폴백 지원
    """
    root = Path(__file__).parents[1]
    conf_path = root / "setting" / "config" / "kafka_config.conf"
    if conf_path.exists():
        return ProducerConfig.from_file(conf_path)

    # Fallback to legacy path to be tolerant
    legacy = root / "setting" / "_kafka_config.conf"
    if legacy.exists():
        return ProducerConfig.from_file(legacy)
    return ProducerConfig()
