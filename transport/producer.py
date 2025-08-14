from __future__ import annotations

from typing import Sequence

from aiokafka import AIOKafkaProducer

from common.serde import to_bytes
from core.properties import ExchangeService, SocketRequestType
from core.types import Err
from transport.types.message_types import ConnectMessageTD, ExchangeMetadata
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


class ConnectMessageBuilder:
    """서비스/템플릿을 이용하여 Connect + Projection 메시지를 생성"""

    def __init__(self, template_dir: str = "setting/templates") -> None:
        self.template_dir = template_dir
        self.svc = ExchangeService()

    async def build(
        self,
        *,
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
        # region: 지역, exchange: 거래소 req_type_str: request 타입
        region = source["region"]
        exchange = source["exchange"]
        req_type_str = source["request_type"]

        # 서비스는 문자열 타입의 요청 타입도 처리 가능해야 함
        res = self.svc.get_exchange_config(
            exchange, list(symbols), req_type_str, region
        )

        if isinstance(res, Err):
            raise RuntimeError(f"구성 생성 실패: {res.error}")

        # Projection 필드 비동기 로드
        projection: list[str] = await load_projection_async(
            exchange, req_type_str, self.template_dir
        )

        now: int = now_ms_kst()
        msg = ConnectMessageTD(
            event_kind="connect",
            schema_version=SCHEMA_VERSION,
            symbols=list(symbols),
            source=source,
            connection=res.ok(),
            projection=projection,
            ts_issue=now,
            ts_ingest=now,
        )

        if expiry_ms is not None:
            msg.expiry_ms = int(expiry_ms)

        return msg


class AioKafkaConnectProducer:
    """aiokafka 기반 Connect 메시지 프로듀서

    사용 예:
        prod = AioKafkaConnectProducer(ProducerConfig())
        await prod.start()
        await prod.produce_connect(region="korea", exchange="bithumb", req_type="ticker", symbols=["KRW-BTC"])
        await prod.stop()
    """

    def __init__(
        self,
        cfg: ProducerConfig | None = None,
        *,
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

    async def produce_connect(
        self,
        *,
        region: str,
        exchange: str,
        req_type: SocketRequestType,
        symbols: Sequence[str],
        expiry_ms: int | None = None,
    ) -> None:
        """
        Connect 메시지를 Kafka로 전송합니다.
        Args:
            region: 지역
            exchange: 거래소
            req_type: 요청 타입
            symbols: 심볼 목록
            expiry_ms: 메시지 만료 시간(밀리초)
        """
        if self._producer is None:
            raise RuntimeError("Producer is not started. Call start() first.")
        # 메시지 구성 및 전송을 단계별로 위임하여 책임 축소
        source: ExchangeMetadata = make_exchange_metadata(
            region=region, exchange=exchange, req_type=req_type
        )
        msg: ConnectMessageTD = await self.build_connect_message(
            source=source, symbols=symbols, expiry_ms=expiry_ms
        )
        await self.send_connect(source=source, msg=msg)

    async def build_connect_message(
        self,
        *,
        source: ExchangeMetadata,
        symbols: Sequence[str],
        expiry_ms: int | None = None,
    ) -> ConnectMessageTD:
        """빌더 위임: Connect 메시지를 생성합니다."""
        return await self._builder.build(
            source=source, symbols=symbols, expiry_ms=expiry_ms
        )

    def _make_key(self, *, region: str, exchange: str, req_type: str) -> bytes:
        """
        Key: region|exchange|first_symbol
        Args:
            region: 지역
            exchange: 거래소
            symbols: 심볼 목록
        Returns:
            bytes: 키
        """
        return f"{region}|{exchange}|{req_type}".encode("utf-8")

    def _make_headers(self, *, source: ExchangeMetadata) -> KafkaHeader:
        return [
            (HeaderKey.REGION.value, source["region"].encode("utf-8")),
            (HeaderKey.EXCHANGE.value, source["exchange"].encode("utf-8")),
            (HeaderKey.EVENT_KIND.value, b"connect"),
            (HeaderKey.REQUEST_TYPE.value, source["request_type"].encode("utf-8")),
            (HeaderKey.SCHEMA_VERSION.value, SCHEMA_VERSION.encode("utf-8")),
            (HeaderKey.CONTENT_TYPE.value, b"application/json"),
        ]

    async def send_connect(
        self, *, source: ExchangeMetadata, msg: ConnectMessageTD
    ) -> None:
        """
        Connect 메시지를 Kafka로 전송합니다.
        Args:
            source: ExchangeMetadata
            msg: ConnectMessageTD
        """
        if self._producer is None:
            raise RuntimeError("Producer is not started. Call start() first.")

        key = self._make_key(
            region=source["region"],
            exchange=source["exchange"],
            req_type=source["request_type"],
        )
        headers = self._make_headers(source=source)
        await self._producer.send_and_wait(
            topic=self.cfg.topic,
            key=key,
            value=to_bytes(msg),
            headers=headers,
        )
