from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProducerConfig(BaseSettings):
    """Kafka Producer 설정 클래스 (Pydantic Settings 기반)"""

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        env_file="setting/config/.env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    bootstrap_servers: str = Field(
        description="Kafka 브로커 서버 주소 목록 (쉼표로 구분)"
    )
    acks: int | Literal["all"] = Field(
        description="프로듀서 확인 응답 설정 (0, 1, 'all')"
    )
    linger_ms: int = Field(description="배치 전송 대기 시간 (밀리초)")
    max_batch_size: int = Field(description="최대 배치 크기 (바이트)")
    max_request_size: int = Field(description="최대 요청 크기 (바이트)")

    @field_validator("acks", mode="before")
    @classmethod
    def validate_acks(cls, v: str | int) -> int | str:
        """acks 값 검증 및 변환"""
        if isinstance(v, str):
            if v.lower() == "all":
                return "all"
            try:
                return int(v)
            except ValueError:
                raise ValueError(f"Invalid acks value: {v}. Must be 0, 1, or 'all'")
        return v

    @classmethod
    def from_file(cls, path: Path) -> ProducerConfig:
        """설정 파일에서 Kafka 프로듀서 설정 로드 (하위 호환성 유지)"""
        return cls(_env_file=path)
