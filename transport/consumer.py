from __future__ import annotations

import asyncio
import threading
import queue
from typing import Any, Sequence
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Consumer, KafkaError, Message

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


class ConfluentKafkaRecord:
    """confluent-kafka Message를 aiokafka 호환 레코드로 변환하는 래퍼"""

    def __init__(self, message: Message) -> None:
        self.topic = message.topic()
        self.partition = message.partition()
        self.offset = message.offset()
        self.key = message.key()
        self.value = message.value()
        self.headers = message.headers() or []
        self.timestamp = message.timestamp()


class AioKafkaRequestConsumer:
    """confluent-kafka 기반 비동기 컨슈머 (스레드 + 큐 활용)

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
        self._consumer: Consumer | None = None
        self._message_queue: queue.Queue[ConfluentKafkaRecord | None] = queue.Queue()
        self._executor: ThreadPoolExecutor | None = None
        self._consumer_thread: threading.Thread | None = None
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def _create_consumer_config(self) -> dict[str, Any]:
        """confluent-kafka Consumer 설정을 생성합니다."""
        return {
            "bootstrap.servers": self._cfg.bootstrap_servers,
            "group.id": self._group_id,
            # 오프셋/커밋
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 3000,
            # 세션/하트비트
            "session.timeout.ms": 45000,
            "heartbeat.interval.ms": 15000,
            "max.poll.interval.ms": 900000,
            # 페치(← librdkafka 키 주의)
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 500,  # ✅ (Java: fetch.max.wait.ms)
            "fetch.max.bytes": 52428800,
            # 안정성/네트워크
            "socket.timeout.ms": 60000,
            "reconnect.backoff.ms": 200,
            "reconnect.backoff.max.ms": 5000,
            "socket.keepalive.enable": True,
            # 리밸런스/할당
            "partition.assignment.strategy": "cooperative-sticky",
            # 기타
            "enable.partition.eof": True,
            "allow.auto.create.topics": False,
        }

    def _consumer_worker(self) -> None:
        """별도 스레드에서 실행되는 컨슈머 워커"""
        try:
            consumer = Consumer(self._create_consumer_config())
            consumer.subscribe([self._topic])

            while self._running:
                try:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            print(f"Consumer error: {msg.error()}")
                            continue

                    # 메시지를 큐에 추가
                    record = ConfluentKafkaRecord(msg)
                    self._message_queue.put(record)

                except Exception as e:
                    print(f"Error in consumer worker: {e}")

        except Exception as e:
            print(f"Failed to create consumer: {e}")
        finally:
            try:
                consumer.close()
            except:
                pass
            # 종료 신호를 큐에 추가
            self._message_queue.put(None)

    async def start(self) -> None:
        """Kafka 컨슈머를 시작합니다."""
        if self._running:
            return

        self._running = True
        self._loop = asyncio.get_running_loop()
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="kafka-consumer"
        )

        # 컨슈머 스레드 시작
        self._consumer_thread = threading.Thread(
            target=self._consumer_worker, name="kafka-consumer-thread", daemon=True
        )
        self._consumer_thread.start()

    async def stop(self) -> None:
        """Kafka 컨슈머를 중지합니다."""
        if not self._running:
            return

        self._running = False

        # 스레드 종료 대기
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=5.0)

        # ThreadPoolExecutor 정리
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

        self._consumer_thread = None
        self._loop = None

    def __aiter__(self):
        """비동기 이터레이터 프로토콜을 구현합니다.

        Returns:
            AsyncIterator: 비동기 이터레이터 객체
        """
        if not self._running:
            raise RuntimeError("consumer not started")
        return self._iterate()

    async def _iterate(self):
        """내부 비동기 이터레이션 메서드입니다.

        Returns:
            AsyncGenerator: Kafka 레코드를 생성하는 제너레이터
        """
        while self._running:
            try:
                # 큐에서 메시지 대기 (타임아웃 1초)
                try:
                    record = self._message_queue.get(timeout=1.0)
                except queue.Empty:
                    # 타임아웃은 정상적인 상황 (메시지가 없을 때)
                    continue

                # None은 종료 신호
                if record is None:
                    break

                yield record

            except Exception as e:
                print(f"Error in consumer iteration: {e}")
                break

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
