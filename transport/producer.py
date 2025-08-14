from __future__ import annotations

from typing import Sequence

from aiokafka import AIOKafkaProducer

from common.serde import to_bytes
from core.properties import ExchangeService
from core.types import ExchangeSocketConfig
from transport.types.message_types import (
    ConnectMessageTD,
    ExchangeMetadata,
    default_routing,
    DEFAULT_RELIABILITY,
)
from transport.types.specs import SocketConnectMetaData as SCMeta
from transport.types.headers import HeaderKey, KafkaHeader
from transport.utils.time import now_ms_kst
from transport.utils.projection import (
    load_projection_async,
    load_kafka_config,
    make_exchange_metadata,
    ProducerConfig,
)
from transport.di.producer_factory import KafkaProducerFactory, AiokafkaProducerFactory


SCHEMA_VERSION = "1.0.0"
DEFAULT_TTL_MS = 30_000


class ConnectMessageBuilder:
    """서비스/템플릿을 이용하여 Connect + Projection 메시지를 생성"""

    def __init__(self, template_dir: str = "setting/templates") -> None:
        self.template_dir = template_dir
        self.svc = ExchangeService()

    async def build(
        self,
        source: ExchangeMetadata,
        symbols: Sequence[str],
        expiry_ms: int | None = None,
    ) -> ConnectMessageTD:
        """Connect + Projection 메시지를 빌드합니다.

        Args:
            source: ExchangeMetadata (예: ExchangeMetadata(region="korea", exchange="upbit", request_type="ticker"))
            symbols: 심볼 목록 (예: ["KRW-BTC", "KRW-ETH"])
            expiry_ms: 메시지 만료 시간(밀리초) (선택적)

        Returns:
            ConnectMessageTD: 생성된 Connect 메시지

        Raises:
            RuntimeError: 구성 생성에 실패한 경우
        """
        # URL 및 소켓 파라미터 구성 (동기)
        region = source["region"]
        exchange = source["exchange"]
        req_type_str = source["request_type"]
        config: ExchangeSocketConfig = self.svc.get_exchange_config(
            exchange, list(symbols), req_type_str, region
        )
        projection: list[str] = await load_projection_async(
            exchange, req_type_str, self.template_dir
        )
        now: int = now_ms_kst()
        msg = ConnectMessageTD(
            type="command",
            action="connect_and_subscribe",
            ttl_ms=DEFAULT_TTL_MS,
            routing=default_routing(region, exchange, req_type_str),
            reliability=DEFAULT_RELIABILITY,
            schema_version=SCHEMA_VERSION,
            symbols=list(symbols),
            target=source,
            connection=config,
            projection=projection,
            ts_issue=now,
            ts_ingest=now,
        )
        if expiry_ms is not None:
            msg.expiry_ms = int(expiry_ms)
        return msg

    async def build_from_spec(self, spec: SCMeta) -> ConnectMessageTD:
        """ConnectSpec를 받아 메시지를 생성합니다.

        Args:
            spec: ConnectSpec
        Returns:
            ConnectMessageTD: 생성된 Connect 메시지
        Raises:
            RuntimeError: 구성 생성에 실패한 경우
        """
        source: ExchangeMetadata = make_exchange_metadata(
            region=spec.region, exchange=spec.exchange, req_type=spec.req_type
        )
        return await self.build(
            source=source,
            symbols=spec.symbols,
            expiry_ms=spec.expiry_ms,
        )


class AioKafkaConnectProducer:
    """aiokafka 기반 Connect 메시지 프로듀서

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
        cfg: ProducerConfig | None = None,
        producer_factory: KafkaProducerFactory | None = None,
    ) -> None:
        self.cfg = cfg or load_kafka_config()
        self._producer: AIOKafkaProducer | None = None
        self._builder = ConnectMessageBuilder()
        self._producer_factory: KafkaProducerFactory = (
            producer_factory or AiokafkaProducerFactory()
        )

    async def start(self) -> None:
        if self._producer is not None:
            return

        self._producer = self._producer_factory.create(self.cfg)
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def produce_connect(self, spec: SCMeta) -> None:
        """ConnectSpec 기반으로 Connect 메시지를 전송합니다.

        Args:
            spec: ConnectSpec
        Raises:
            RuntimeError: Producer가 시작되지 않은 경우
        """
        if self._producer is None:
            raise RuntimeError("Producer is not started. Call start() first.")
        msg: ConnectMessageTD = await self._builder.build_from_spec(spec)
        await self.send_connect(msg=msg)

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
        """Kafka로 메시지를 전송합니다.

        Args:
            msg: ConnectMessageTD

        Raises:
            RuntimeError: Producer가 시작되지 않은 경우
        """
        if self._producer is None:
            raise RuntimeError("Producer is not started. Call start() first.")

        source: ExchangeMetadata = msg["target"]
        await self._producer.send_and_wait(
            topic=self.cfg.topic,
            key=self._make_key(source),
            value=to_bytes(msg),
            headers=self._make_headers(source),
        )
