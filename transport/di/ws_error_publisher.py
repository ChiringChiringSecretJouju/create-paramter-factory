from __future__ import annotations

from aiokafka import AIOKafkaProducer

from common.broker_config import load_kafka_config
from common.serde import to_bytes
from common.types import WsErrorEventTD

ERROR_TOPIC = "ws.error"
_cfg = load_kafka_config()


def _make_key(payload: WsErrorEventTD) -> bytes:
    src = payload.get("source", {})
    region = str(src.get("region", ""))
    exchange = str(src.get("exchange", ""))
    req_type = str(src.get("request_type", ""))
    return f"{region}|{exchange}|{req_type}".encode("utf-8")


async def publish_ws_error(payload: WsErrorEventTD) -> None:
    """Publish ws.error payload to Kafka (per-call producer start/stop)"""
    producer = AIOKafkaProducer(
        bootstrap_servers=_cfg.bootstrap_servers,
        acks=_cfg.acks,
        linger_ms=_cfg.linger_ms,
        max_batch_size=_cfg.max_batch_size,
        max_request_size=_cfg.max_request_size,
    )
    await producer.start()
    try:
        await producer.send_and_wait(
            topic=ERROR_TOPIC,
            key=_make_key(payload),
            value=to_bytes(payload),
        )
    finally:
        await producer.stop()
