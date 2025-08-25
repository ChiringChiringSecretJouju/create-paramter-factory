from __future__ import annotations

import configparser
import logging
from dataclasses import dataclass
from pathlib import Path


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

        linger_ms = int(parser.get("KAFKA", "linger_ms", fallback="0"))
        max_batch_size = int(parser.get("KAFKA", "max_batch_size", fallback="1048576"))
        max_request_size = int(parser.get("KAFKA", "max_request_size", fallback="1048576"))

        return cls(
            bootstrap_servers=bs,
            acks=acks,
            linger_ms=linger_ms,
            max_batch_size=max_batch_size,
            max_request_size=max_request_size,
        )


def load_kafka_config() -> ProducerConfig:
    """레포 루트 기준 setting/config/kafka_config.conf에서 Kafka 설정 로드"""
    root = Path(__file__).parent.parent
    conf_path = root / "setting" / "config" / "kafka_config.conf"
    logger = logging.getLogger(__name__)

    if conf_path.exists():
        try:
            return ProducerConfig.from_file(conf_path)
        except Exception as e:
            logger.warning("Kafka 설정 파일을 읽는 데 실패했습니다(%s). 기본값을 사용합니다.", e)

    logger.warning("Kafka 설정 파일을 찾지 못했습니다: %s. 기본값을 사용합니다.", conf_path)
    return ProducerConfig()
