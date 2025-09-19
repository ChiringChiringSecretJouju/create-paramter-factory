from __future__ import annotations
from abc import ABC, abstractmethod
from aiokafka import AIOKafkaProducer
from common.broker_config import ProducerConfig


class KafkaProducerFactory(ABC):
    """Kafka 프로듀서 인스턴스를 생성하는 팩토리 인터페이스.

    DI, 테스팅, 그리고 관찰성 래퍼를 위한 프로듀서 생성을 추상화합니다.
    """

    @abstractmethod
    def create(self, cfg: ProducerConfig) -> AIOKafkaProducer: ...


class AiokafkaProducerFactory(KafkaProducerFactory):
    """ProducerConfig로부터 AIOKafkaProducer를 생성하는 기본 팩토리."""

    def create(self, cfg: ProducerConfig) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=cfg.bootstrap_servers,
            acks=cfg.acks,
            linger_ms=cfg.linger_ms,
            max_batch_size=cfg.max_batch_size,
            max_request_size=cfg.max_request_size,
        )
