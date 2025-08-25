from __future__ import annotations

import asyncio
import json
from transport.types.message_types import ConnectMessageTD
from transport.producer import (
    AioKafkaConnectProducer,
    ConnectMessageBuilder,
)
from transport.consumer import AioKafkaRequestConsumer, RequestEnvelope


class ConnectForwarder:
    """Kafka 컨슈머 → Connect 메시지 빌더 → Kafka 프로듀서 파이프라인.

    역할을 다음 메서드로 분리합니다.
    - _extract_request_fields: 페이로드에서 region/exchange/req_type/symbols 추출
    - _build_connect_message: ConnectMessageTD 생성
    - run: 레코드를 소비하고 생성된 메시지를 전달

    """

    def __init__(self) -> None:
        self.consumer = AioKafkaRequestConsumer(
            topic="ws.command",
            group_id="create-parameter-factory-consumer",
        )
        self.builder = ConnectMessageBuilder()
        self.producer = AioKafkaConnectProducer(topic="ws.status")

    @staticmethod
    def _extract_request_fields(payload: dict) -> tuple[str, str, str, list[str]]:
        """페이로드에서 필드(region, exchange, req_type, symbols)를 추출합니다.

        Args:
            payload: 수신한 원시 JSON 디코드 결과(dict)

        Returns:
            tuple[str, str, str, list[str]]: (region, exchange, req_type, symbols)
        """
        region: str = payload.get("target", {}).get("region", "")
        exchange: str = payload.get("target", {}).get("exchange", "")
        req_type: str = payload.get("target", {}).get("request_type", "")
        symbols: list[str] = (
            payload.get("connection", {}).get("socket_params", {}).get("symbols", [])
        )
        return region, exchange, req_type, symbols

    async def _build_connect_message(
        self, payload: dict, req: RequestEnvelope
    ) -> ConnectMessageTD:
        """추출된 필드로 ConnectMessageTD를 생성합니다.

        Args:
            payload: 수신 페이로드(dict)
            req: 파싱된 요청 엔벨로프(RequestEnvelope)

        Returns:
            ConnectMessageTD: 생성된 Connect 메시지
        """
        region, exchange, req_type, symbols = self._extract_request_fields(payload)
        msg: ConnectMessageTD = await self.builder.create_ticket(
            type="status",
            action="connect_and_subscribe",
            source={
                "region": region,
                "exchange": exchange,
                "request_type": req_type,
            },
            symbols=symbols,
        )
        if req.correlation_id:
            msg["ticket_id"] = req.correlation_id
        return msg

    async def handle_record(self, raw_value: bytes) -> None:
        """단일 Kafka 레코드를 처리하고 Connect 메시지를 전송합니다.

        Args:
            raw_value: Kafka에서 수신한 바이트 값

        Returns:
            None
        """
        payload = json.loads(raw_value.decode("utf-8"))
        req = RequestEnvelope.parse(payload)
        msg = await self._build_connect_message(payload, req)
        await self.producer.produce_connect(msg)

    async def run(self) -> None:
        """컨슈머를 실행하여 레코드를 소비하고 메시지를 전달합니다.

        Args:
            None

        Returns:
            None
        """
        await self.consumer.start()
        await self.producer.start()
        try:
            async for record in self.consumer:
                await self.handle_record(record.value)

        finally:
            await self.consumer.stop()
            await self.producer.stop()


def main() -> None:
    asyncio.run(ConnectForwarder().run())


if __name__ == "__main__":
    main()
