from __future__ import annotations

from aiokafka import AIOKafkaProducer
from typing import Any

from common.serde import to_bytes
from core.properties import ExchangeService
from core.types import ExchangeSocketConfig
from transport.types.message_types import (
    ConnectMessageTD,
    ExchangeMetadata,
    default_routing,
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
from common.exceptions import handle_exchange_exceptions


SCHEMA_VERSION = "1.0.0"
DEFAULT_TTL_MS = 30_000


class ConnectMessageBuilder:
    """서비스/템플릿을 이용하여 Connect + Projection 메시지를 생성"""

    def __init__(self, template_dir: str = "setting/templates") -> None:
        self.template_dir = template_dir
        self.svc = ExchangeService()

    async def build(
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
        # URL 및 소켓 파라미터 구성 (동기)
        region: str = source["region"]
        exchange: str = source["exchange"]
        req_type_str: str = source["request_type"]
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
        msg = ConnectMessageTD(
            type=type,
            action=action,
            ttl_ms=DEFAULT_TTL_MS,
            routing=default_routing(region, exchange, req_type_str),
            schema_version=SCHEMA_VERSION,
            target=source,
            symbols=list(symbols),
            connection=config,
            projection=projection,
            ts_issue=now,
            ts_ingest=now,
        )

        # 일단 보류
        # if expiry_ms is not None:
        #     msg.expiry_ms = int(expiry_ms)
        return msg

    @handle_exchange_exceptions(
        exchange_name_attr="_exchange_name",
        region_attr="_region",
        req_type_attr="_req_type",
        symbols_attr="_symbols",
        return_as_dict=True,
    )
    async def build_from_spec(self, spec: SCMeta) -> ConnectMessageTD | dict[str, Any]:
        """ConnectSpec를 받아 메시지를 생성합니다.

        Args:
            spec: ConnectSpec
        Returns:
            ConnectMessageTD: 생성된 Connect 메시지
        Raises:
            RuntimeError: 구성 생성에 실패한 경우
        """
        print("심볼", spec["symbols"])
        # 컨텍스트 속성 먼저 세팅 (예외 발생 전 확보)
        self._exchange_name = spec["target"]["exchange"]
        self._region = spec["target"]["region"]
        self._req_type = spec["target"]["request_type"]
        self._symbols = list(spec["symbols"]) if spec.get("symbols") is not None else []

        source: ExchangeMetadata = make_exchange_metadata(
            region=self._region,
            exchange=self._exchange_name,
            req_type=self._req_type,
        )
        return await self.build(
            type="status",
            action="connect_and_subscribe",
            source=source,
            symbols=self._symbols,
            # expiry_ms=spec.expiry_ms,
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
        topic: str,
        cfg: ProducerConfig | None = None,
        producer_factory: KafkaProducerFactory | None = None,
    ) -> None:
        self.topic = topic
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

    @handle_exchange_exceptions(
        exchange_name_attr="_exchange_name",
        region_attr="_region",
        req_type_attr="_req_type",
        symbols_attr="_symbols",
        return_as_dict=True,
    )
    async def produce_connect(self, spec: SCMeta) -> dict[str, Any] | None:
        """Kafka로 메시지를 전송합니다.

        Args:
            spec: ConnectSpec

        Raises:
            RuntimeError: Producer가 시작되지 않은 경우
        """
        if self._producer is None:
            raise RuntimeError("Producer is not started. Call start() first.")

        # 컨텍스트 속성 먼저 세팅 (예외 발생 전 확보)
        target = spec["target"]
        self._exchange_name = target["exchange"]
        self._region = target["region"]
        self._req_type = target["request_type"]
        self._symbols = list(spec["symbols"])

        # Connect 메시지 생성
        msg = await self._builder.build_from_spec(spec)
        # build_from_spec가 데코레이터에 의해 에러 dict를 반환한 경우, 즉시 반환하여 중복 처리 방지
        if isinstance(msg, dict):
            return msg

        # 메시지 전송
        source: ExchangeMetadata = msg["target"]
        await self._producer.send_and_wait(
            topic=self.topic,
            key=self._make_key(source),
            value=to_bytes(msg),
            headers=self._make_headers(source),
        )
