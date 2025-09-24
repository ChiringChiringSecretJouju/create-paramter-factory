from __future__ import annotations

import asyncio
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from confluent_kafka import Producer, KafkaError

from common.broker_config import ProducerConfig
from common.exceptions import handle_exchange_exceptions
from common.serde import to_bytes
from common.types import ExchangeSocketConfig
from core.properties import ExchangeService
from transport.di.producer_factory import AiokafkaProducerFactory, KafkaProducerFactory
from transport.di.ws_error_publisher import publish_ws_error
from transport.types.headers import HeaderKey, KafkaHeader
from transport.types.message_types import (
    ConnectMessageTD,
    ExchangeMetadata,
    default_routing,
)
from transport.types.specs import SocketConnectMetaData as SCMeta
from transport.utils.projection import load_projection_async, make_exchange_metadata
from transport.utils.time import now_ms_kst

SCHEMA_VERSION = "1.0.0"
DEFAULT_TTL_MS = 30_000


class ConnectMessageBuilder:
    """서비스/템플릿을 이용하여 Connect + Projection 메시지를 생성"""

    def __init__(self, template_dir: str = "setting/templates") -> None:
        self.template_dir = template_dir
        self.svc = ExchangeService()
        # 예외 데코레이터 컨텍스트
        self._exchange_name: str = ""
        self._region: str = ""
        self._req_type: str = ""
        self._symbols: list[str] = []

    @handle_exchange_exceptions(
        exchange_name_attr="_exchange_name",
        region_attr="_region",
        req_type_attr="_req_type",
        symbols_attr="_symbols",
        publisher=publish_ws_error,
    )
    async def create_ticket(
        self,
        type: str,
        action: str,
        source: ExchangeMetadata,
        symbols: list[str],
    ) -> ConnectMessageTD:
        """Connect + Projection 메시지를 빌드합니다.

        Args:
            source: ExchangeMetadata (예: ExchangeMetadata(region="korea", exchange="upbit", request_type="ticker"))
            symbols: 심볼 목록 (예: ["KRW-BTC", "KRW-ETH"])
            expiry_ms: 메시지 만료 시간(밀리초) (선택적) // 2025년 8월 22일 보류

        Returns:
            ConnectMessageTD: 생성된 Connect 메시지

        Raises:
            RuntimeError: 구성 생성에 실패한 경우
        """
        # 데코레이터 컨텍스트 세팅
        region: str = source["region"]
        exchange: str = source["exchange"]
        req_type_str: str = source["request_type"]
        self._exchange_name = exchange
        self._region = region
        self._req_type = req_type_str
        self._symbols = list(symbols)

        # URL 및 소켓 파라미터 구성 (동기)
        config: ExchangeSocketConfig = self.svc.get_exchange_config(
            exchange=exchange,
            symbols=list(symbols),
            req_type=req_type_str,
            region=region,
        )
        projection: list[str] = await load_projection_async(
            exchange=exchange,
            req_type=req_type_str,
            template_dir=self.template_dir,
        )
        now: int = now_ms_kst()
        msg: ConnectMessageTD = {
            "type": type,
            "action": action,
            "ttl_ms": DEFAULT_TTL_MS,
            "routing": default_routing(region, exchange, req_type_str),
            "schema_version": SCHEMA_VERSION,
            "target": source,
            "symbols": list(symbols),
            "connection": config,
            "projection": projection,
            "ts_issue": now,
            "ts_ingest": now,
        }

        # 일단 보류
        # if expiry_ms is not None:
        #     msg.expiry_ms = int(expiry_ms)
        return msg

    @handle_exchange_exceptions(
        exchange_name_attr="_exchange_name",
        region_attr="_region",
        req_type_attr="_req_type",
        symbols_attr="_symbols",
        publisher=publish_ws_error,
    )
    async def build_from_spec(self, spec: SCMeta) -> ConnectMessageTD:
        """ConnectSpec를 받아 메시지를 생성합니다.

        Args:
            spec: ConnectSpec
        Returns:
            ConnectMessageTD: 생성된 Connect 메시지
        Raises:
            ExchangeException: 구성 생성에 실패한 경우
        """
        # 데코레이터 컨텍스트 세팅 (spec 기반)
        self._exchange_name = spec.exchange
        self._region = spec.region
        self._req_type = spec.req_type
        self._symbols = list(spec.symbols)

        # SCMeta는 dataclass이므로 속성 접근 사용
        source: ExchangeMetadata = make_exchange_metadata(
            region=spec.region,
            exchange=spec.exchange,
            req_type=spec.req_type,
        )
        return await self.create_ticket(
            type="status",
            action="connect_and_subscribe",
            source=source,
            symbols=list(spec.symbols),
            # expiry_ms=spec.expiry_ms,
        )


class AioKafkaConnectProducer:
    """confluent-kafka 기반 비동기 프로듀서 (스레드 + 큐 활용)

    Args:
        cfg: ProducerConfig
        producer_factory: KafkaProducerFactory

    사용 예:
        prod = AioKafkaConnectProducer(ProducerConfig())
        await prod.start()
        await prod.produce_connect(region="korea", exchange="bithumb", req_type="ticker", symbols=["KRW-BTC"])
        await prod.stop()
    """

    def __init__(
        self,
        topic: str,
        cfg: ProducerConfig | None = None,
        producer_factory: KafkaProducerFactory | None = None,
    ) -> None:
        self.topic = topic
        self.cfg = cfg or ProducerConfig()
        self._producer: Producer | None = None
        self._builder = ConnectMessageBuilder()
        # factory는 더 이상 사용하지 않음 (confluent-kafka 직접 사용)
        self._send_queue: queue.Queue[tuple[str, bytes, bytes, list[tuple[str, bytes]]] | None] = queue.Queue()
        self._executor: ThreadPoolExecutor | None = None
        self._producer_thread: threading.Thread | None = None
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def _create_producer_config(self) -> dict[str, Any]:
        """confluent-kafka Producer 설정을 생성합니다."""
        config = {
            'bootstrap.servers': self.cfg.bootstrap_servers,
            'acks': str(self.cfg.acks),
            'linger.ms': self.cfg.linger_ms,
            'batch.size': self.cfg.max_batch_size,
            'message.max.bytes': self.cfg.max_request_size,
            'compression.type': 'lz4',
            'retries': 3,
            'retry.backoff.ms': 100,
        }
        return config

    def _producer_worker(self) -> None:
        """별도 스레드에서 실행되는 프로듀서 워커"""
        try:
            producer = Producer(self._create_producer_config())
            
            while self._running:
                try:
                    # 큐에서 전송할 메시지 대기 (타임아웃 1초)
                    try:
                        send_data = self._send_queue.get(timeout=1.0)
                    except queue.Empty:
                        continue
                        
                    # None은 종료 신호
                    if send_data is None:
                        break
                        
                    topic, key, value, headers = send_data
                    
                    # 메시지 전송
                    producer.produce(
                        topic=topic,
                        key=key,
                        value=value,
                        headers=headers,
                        callback=self._delivery_callback
                    )
                    
                    # 주기적으로 flush (배치 처리)
                    producer.poll(0)
                    
                except Exception as e:
                    print(f"Error in producer worker: {e}")
                    
        except Exception as e:
            print(f"Failed to create producer: {e}")
        finally:
            try:
                # 남은 메시지 flush
                producer.flush(timeout=5.0)
            except:
                pass

    def _delivery_callback(self, err: KafkaError | None, msg) -> None:
        """메시지 전송 결과 콜백"""
        if err is not None:
            print(f"Message delivery failed: {err}")
        # 성공적인 전송은 로깅하지 않음 (성능상 이유)

    async def start(self) -> None:
        """프로듀서를 시작합니다."""
        if self._running:
            return
            
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka-producer")
        
        # 프로듀서 스레드 시작
        self._producer_thread = threading.Thread(
            target=self._producer_worker,
            name="kafka-producer-thread",
            daemon=True
        )
        self._producer_thread.start()

    async def stop(self) -> None:
        """프로듀서를 중지합니다."""
        if not self._running:
            return
            
        self._running = False
        
        # 종료 신호 전송
        self._send_queue.put(None)
        
        # 스레드 종료 대기
        if self._producer_thread and self._producer_thread.is_alive():
            self._producer_thread.join(timeout=5.0)
            
        # ThreadPoolExecutor 정리
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
            
        self._producer_thread = None
        self._loop = None

    def _make_key(self, source: ExchangeMetadata) -> bytes:
        """
        Key: region|exchange|first_symbol
        Args:
            source: ExchangeMetadata(region="korea", exchange="bithumb", request_type="ticker")
        Returns:
            bytes: 키
        """
        region: str = source["region"]
        exchange: str = source["exchange"]
        req_type: str = source["request_type"]
        return f"{region}|{exchange}|{req_type}".encode("utf-8")

    def _make_headers(self, source: ExchangeMetadata) -> KafkaHeader:
        return [
            (HeaderKey.REGION.value, source["region"].encode("utf-8")),
            (HeaderKey.EXCHANGE.value, source["exchange"].encode("utf-8")),
            (HeaderKey.EVENT_KIND.value, b"connect"),
            (HeaderKey.REQUEST_TYPE.value, source["request_type"].encode("utf-8")),
            (HeaderKey.SCHEMA_VERSION.value, SCHEMA_VERSION.encode("utf-8")),
            (HeaderKey.CONTENT_TYPE.value, b"application/json"),
        ]

    async def send_connect(self, msg: ConnectMessageTD) -> None:
        """Connect 메시지를 Kafka로 전송합니다. start() 선행 필요."""
        if not self._running:
            raise RuntimeError("Producer is not started. Call start() first.")
            
        source: ExchangeMetadata = msg["target"]
        topic = f"{self.topic}.{source['region']}"
        key = self._make_key(source)
        value = to_bytes(msg)
        headers = self._make_headers(source)
        
        # 메시지를 큐에 추가
        self._send_queue.put((topic, key, value, headers))

    @handle_exchange_exceptions(
        exchange_name_attr="_exchange_name",
        region_attr="_region",
        req_type_attr="_req_type",
        symbols_attr="_symbols",
        publisher=publish_ws_error,
    )
    async def produce_connect(self, msg: ConnectMessageTD) -> None:
        """Kafka로 메시지를 전송합니다.

        Args:
            msg: ConnectMessageTD (이미 빌드된 메시지)

        Raises:
            RuntimeError: Producer가 시작되지 않은 경우
        """
        # 컨텍스트 속성 먼저 세팅 (예외 발생 전 확보)
        source: ExchangeMetadata = msg["target"]
        self._exchange_name: str = source["exchange"]
        self._region: str = source["region"]
        self._req_type: str = source["request_type"]
        self._symbols: list[str] = list(msg.get("symbols", []))

        # 이미 빌드된 메시지 전송
        await self.send_connect(msg)
