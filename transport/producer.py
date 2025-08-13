from __future__ import annotations

from typing import Sequence

from aiokafka import AIOKafkaProducer

from common.serde import to_bytes
from core.properties import ExchangeService, SocketRequestType
from core.types import Err
from transport.types.message_types import ConnectMessageTD, ExchangeMetadata
from transport.types.headers import HeaderKey, KafkaHeader
from transport.utils.time import now_ms_kst
from transport.utils.projection import load_projection_async, load_kafka_config
from transport.utils.projection import ProducerConfig


SCHEMA_VERSION = "1.0.0"


class ConnectMessageBuilder:
    """서비스/템플릿을 이용하여 Connect + Projection 메시지를 생성"""

    def __init__(self) -> None:
        self.template_dir = "setting/templates"
        self.svc = ExchangeService()

    async def build(
        self,
        *,
        region: str,
        exchange: str,
        req_type: SocketRequestType,
        symbols: Sequence[str],
        expiry_ms: int | None = None,
    ) -> ConnectMessageTD:
        """Connect + Projection 메시지를 빌드합니다.

        Args:
            region: 지역 코드 (예: "korea")
            exchange: 거래소 이름 (예: "upbit", "bithumb")
            req_type: 요청 타입 (ticker, orderbook, trade)
            symbols: 심볼 목록 (예: ["KRW-BTC", "KRW-ETH"])
            expiry_ms: 메시지 만료 시간(밀리초) (선택적)

        Returns:
            ConnectMessageTD: 생성된 Connect 메시지

        Raises:
            RuntimeError: 구성 생성에 실패한 경우
        """
        # URL 및 소켓 파라미터 구성 (동기)
        res = self.svc.get_exchange_config(exchange, list(symbols), req_type, region)
        if isinstance(res, Err):
            raise RuntimeError(f"구성 생성 실패: {res.error}")

        # Projection 필드 비동기 로드
        projection: list[str] = await load_projection_async(
            exchange, req_type, self.template_dir
        )

        now: int = now_ms_kst()
        msg = ConnectMessageTD(
            event_kind="connect",
            schema_version=SCHEMA_VERSION,
            symbols=list(symbols),
            source=ExchangeMetadata(
                region=region,
                exchange=exchange,
                request_type=str(req_type),
            ),
            connection=res.value(),
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

    def __init__(self, cfg: ProducerConfig | None = None) -> None:
        self.cfg = cfg or load_kafka_config()
        self._producer: AIOKafkaProducer | None = None
        self._builder = ConnectMessageBuilder()

    async def start(self) -> None:
        if self._producer is not None:
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.cfg.bootstrap_servers,
            acks=self.cfg.acks,
            linger_ms=self.cfg.linger_ms,
            max_batch_size=self.cfg.max_batch_size,
            max_request_size=self.cfg.max_request_size,
        )
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
        if self._producer is None:
            raise RuntimeError("Producer is not started. Call start() first.")

        # 메시지 구성 (빌더는 비동기)
        msg: ConnectMessageTD = await self._builder.build(
            region=region,
            exchange=exchange,
            req_type=req_type,
            symbols=symbols,
            expiry_ms=expiry_ms,
        )

        # Key: region|exchange|first_symbol
        key = f"{region}|{exchange}|{(list(symbols) or [''])[0]}".encode("utf-8")

        headers: KafkaHeader = [
            (HeaderKey.REGION.value, region.encode("utf-8")),
            (HeaderKey.EXCHANGE.value, exchange.encode("utf-8")),
            (HeaderKey.EVENT_KIND.value, b"connect"),
            (HeaderKey.REQUEST_TYPE.value, str(req_type).encode("utf-8")),
            (HeaderKey.SCHEMA_VERSION.value, SCHEMA_VERSION.encode("utf-8")),
            (HeaderKey.CONTENT_TYPE.value, b"application/json"),
        ]

        await self._producer.send_and_wait(
            topic=self.cfg.topic,
            key=key,
            value=to_bytes(msg),
            headers=headers,
        )
