from __future__ import annotations

from typing import Any, Sequence

from aiokafka import AIOKafkaConsumer

from core.types import SocketRequestType
from common.broker_config import ProducerConfig

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class RequestEnvelope(BaseModel):
    socket_mode: SocketRequestType
    symbols: list[str]
    orderbook_depth: int | None = None
    realtime_only: bool | None = None
    correlation_id: str | None = None

    model_config = ConfigDict(extra="ignore")

    @model_validator(mode="before")
    @classmethod
    def _normalize_socket_mode(cls, data: Any) -> Any:
        """외부 JSON 페이로드에서 `socket_mode`를 추출/소문자화합니다.

        Args:
            data (Any): Kafka/외부 호출로부터 온 임의의 JSON. dict가 아닐 수도 있어 Any 사용.

        Return:
            Any: `socket_mode`가 정규화된 dict 또는 원본 값.
        """
        if not isinstance(data, dict):
            return data
        target = data.get("target")
        if isinstance(target, dict) and "request_type" in target:
            mode = str(target.get("request_type", "")).strip().lower()
            data = {**data, "socket_mode": mode}
        elif "socket_mode" in data and data["socket_mode"] is not None:
            data = {**data, "socket_mode": str(data["socket_mode"]).strip().lower()}
        return data

    @model_validator(mode="before")
    @classmethod
    def _lift_nested_fields(cls, data: Any) -> Any:
        """중첩 필드(`connection.socket_params.symbols`)를 상위로 승격합니다.

        Args:
            data (Any): 외부 JSON 페이로드.

        Return:
            Any: 누락된 상위 필드를 보완한 dict 또는 원본 값.
        """
        if not isinstance(data, dict):
            return data
        if "symbols" not in data:
            socket_params = (data.get("connection") or {}).get("socket_params") or {}
            if isinstance(socket_params, dict) and "symbols" in socket_params:
                data = {**data, "symbols": socket_params.get("symbols")}
        return data

    @field_validator("symbols", mode="before")
    @classmethod
    def _normalize_symbols(cls, v: Any) -> list[str]:
        """`symbols`를 표준 `list[str]`로 변환합니다.

        Args:
            v (Any): 단일 문자열, 문자열 시퀀스 또는 임의의 JSON. 유연한 입력 수용을 위해 Any 사용.

        Return:
            list[str]: 정규화된 심볼 리스트.
        """
        if isinstance(v, str):
            return [v]
        if isinstance(v, Sequence):
            return [str(s) for s in v]
        raise ValueError("symbols must be a string or a list of strings")

    @staticmethod
    def parse(payload: dict[str, Any]) -> RequestEnvelope:
        """페이로드를 검증/파싱하여 `RequestEnvelope`로 반환합니다.

        Args:
            payload (dict[str, Any]): 외부 JSON 페이로드.

        Return:
            RequestEnvelope: 검증된 모델 인스턴스.
        """
        return RequestEnvelope.model_validate(payload)


class AioKafkaRequestConsumer:
    """마켓 연결 요청을 소비만 합니다. 메시지 처리는 호출 측에서 수행하세요.

    입력 JSON 예시:
    {
        "socket_mode": "ticker|orderbook|trade",
        "symbols": "BTC" | ["BTC", "XRP"],
        "orderbook_depth": 15,
        "realtime_only": true,
        "correlation_id": "req-2025-08-14-0001"
    }

    참고:
    - `RequestEnvelope.parse()`를 사용해 호출 측에서 검증/파싱할 수 있습니다.
    """

    def __init__(
        self,
        topic: str,
        group_id: str = "create-parameter-factory-consumer",
        cfg: ProducerConfig | None = None,
    ) -> None:
        """Kafka 요청 컨슈머를 초기화합니다.

        Args:
            topic: 구독할 Kafka 토픽 이름
            group_id: 컨슈머 그룹 ID
            cfg: Kafka 설정, None이면 기본값 사용

        """
        self._topic = topic
        self._cfg = cfg or ProducerConfig()
        self._group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        """Kafka 컨슈머를 시작합니다."""
        if self._consumer is not None:
            return
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._cfg.bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=True,
            value_deserializer=lambda v: v,
            key_deserializer=lambda v: v,
        )
        await self._consumer.start()

    async def stop(self) -> None:
        """Kafka 컨슈머를 중지합니다."""
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    def __aiter__(self):
        """비동기 이터레이터 프로토콜을 구현합니다.

        Returns:
            AsyncIterator: 비동기 이터레이터 객체
        """
        if self._consumer is None:
            raise RuntimeError("consumer not started")
        return self._iterate()

    async def _iterate(self):
        """내부 비동기 이터레이션 메서드입니다.

        Returns:
            AsyncGenerator: Kafka 레코드를 생성하는 제너레이터
        """
        assert self._consumer is not None
        async for record in self._consumer:
            yield record

    # async def run(self) -> None:
    #     """컨슈머를 실행하여 메시지를 소비합니다"""
    #     if self._consumer is None:
    #         raise RuntimeError("consumer not started")
    #     try:
    #         async for _ in self:
    #             # 호출 측에서 직접 순회하며 처리하는 것을 권장합니다.
    #             pass
    #     finally:
    #         await self.stop()
