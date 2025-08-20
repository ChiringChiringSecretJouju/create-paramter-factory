from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from aiokafka import AIOKafkaConsumer

from core.types import SocketRequestType
from transport.utils.projection import ProducerConfig, load_kafka_config


@dataclass(slots=True)
class RequestEnvelope:
    socket_mode: SocketRequestType
    symbols: list[str]
    orderbook_depth: int | None = None
    realtime_only: bool | None = None
    correlation_id: str | None = None

    @staticmethod
    def parse(payload: dict[str, Any]) -> RequestEnvelope:
        mode = payload.get("target", {}).get("request_type", "").strip().lower()
        if mode not in ("ticker", "orderbook", "trade"):
            raise ValueError("socket_mode must be one of: ticker, orderbook, trade")
        raw_symbols = payload.get("symbols", [])
        if isinstance(raw_symbols, str):
            symbols: list[str] = [raw_symbols]
        elif isinstance(raw_symbols, Sequence):
            symbols = [str(s) for s in raw_symbols]
        else:
            raise ValueError("symbols must be a string or a list of strings")
        depth = payload.get("orderbook_depth")
        realtime = payload.get("realtime_only")
        corr = payload.get("correlation_id")
        return RequestEnvelope(
            socket_mode=mode,  # type: ignore[assignment]
            symbols=symbols,
            orderbook_depth=int(depth) if depth is not None else None,
            realtime_only=bool(realtime) if realtime is not None else None,
            correlation_id=str(corr) if corr is not None else None,
        )


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
        topic: str = "market_connect_request_v1",
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
        self._cfg = cfg or load_kafka_config()
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
