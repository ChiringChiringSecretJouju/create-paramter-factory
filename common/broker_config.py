from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class ProducerConfig:
    bootstrap_servers: str = "kafka1:19092,kafka2:29092,kafka3:39092"
    acks: int | str = 1
    linger_ms: int = 0
    max_batch_size: int = 1_048_576
    max_request_size: int = 1_048_576

    @classmethod
    def from_env_file(cls, path: Path) -> ProducerConfig:
        """환경변수 파일에서 Kafka 프로듀서 설정 로드"""
        # .env 파일 직접 읽기
        env_vars = {}
        try:
            with open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        env_vars[key.strip()] = value.strip()
        except Exception:
            pass

        # 환경변수 우선순위: 실제 환경변수 > .env 파일 > 기본값
        bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or env_vars.get(
            "KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers
        )
        acks_raw = os.getenv("KAFKA_ACKS") or env_vars.get("KAFKA_ACKS", "1")

        try:
            acks: int | str = int(acks_raw)
        except (TypeError, ValueError):
            acks = acks_raw

        linger_ms = int(
            os.getenv("KAFKA_LINGER_MS") or env_vars.get("KAFKA_LINGER_MS", "0")
        )
        max_batch_size = int(
            os.getenv("KAFKA_MAX_BATCH_SIZE")
            or env_vars.get("KAFKA_MAX_BATCH_SIZE", "1048576")
        )
        max_request_size = int(
            os.getenv("KAFKA_MAX_REQUEST_SIZE")
            or env_vars.get("KAFKA_MAX_REQUEST_SIZE", "1048576")
        )

        return cls(
            bootstrap_servers=bs,
            acks=acks,
            linger_ms=linger_ms,
            max_batch_size=max_batch_size,
            max_request_size=max_request_size,
        )


def load_kafka_config() -> ProducerConfig:
    """레포 루트 기준 setting/config/.env에서 Kafka 설정 로드"""
    root = Path(__file__).parent.parent
    conf_path = root / "setting" / "config" / ".env"
    logger = logging.getLogger(__name__)

    if conf_path.exists():
        try:
            return ProducerConfig.from_env_file(conf_path)
        except Exception as e:
            logger.warning(
                "Kafka 설정 파일을 읽는 데 실패했습니다(%s). 기본값을 사용합니다.", e
            )

    logger.warning(
        "Kafka 설정 파일을 찾지 못했습니다: %s. 기본값을 사용합니다.", conf_path
    )
    return ProducerConfig()
