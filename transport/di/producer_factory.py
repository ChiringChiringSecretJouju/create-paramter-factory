from __future__ import annotations
from typing import Protocol, runtime_checkable
from aiokafka import AIOKafkaProducer
from transport.utils.projection import ProducerConfig


@runtime_checkable
class KafkaProducerFactory(Protocol):
    """Factory interface to create Kafka producer instances.

    Abstracts producer creation for DI, testing, and observability wrappers.
    """

    def create(
        self, cfg: ProducerConfig
    ) -> AIOKafkaProducer:  # pragma: no cover - type contract
        ...


class AiokafkaProducerFactory:
    """Default factory that builds AIOKafkaProducer from ProducerConfig."""

    def create(self, cfg: ProducerConfig) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=cfg.bootstrap_servers,
            acks=cfg.acks,
            linger_ms=cfg.linger_ms,
            max_batch_size=cfg.max_batch_size,
            max_request_size=cfg.max_request_size,
        )
