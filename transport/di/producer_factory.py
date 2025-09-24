from __future__ import annotations
from abc import ABC, abstractmethod
from confluent_kafka import Producer
from common.broker_config import ProducerConfig


class KafkaProducerFactory(ABC):
    """Kafka 프로듀서 인스턴스를 생성하는 팩토리 인터페이스.

    DI, 테스팅, 그리고 관찰성 래퍼를 위한 프로듀서 생성을 추상화합니다.
    """

    @abstractmethod
    def create(self, cfg: ProducerConfig) -> Producer: ...


class ConfluentProducerFactory(KafkaProducerFactory):
    """ProducerConfig로부터 confluent-kafka Producer를 생성하는 기본 팩토리."""

    def create(self, cfg: ProducerConfig) -> Producer:
        config = {
            'bootstrap.servers': cfg.bootstrap_servers,
            'acks': str(cfg.acks),
            'linger.ms': cfg.linger_ms,
            'batch.size': cfg.max_batch_size,
            'message.max.bytes': cfg.max_request_size,
            'compression.type': 'lz4',
            'retries': 3,
            'retry.backoff.ms': 100,
        }
        return Producer(config)


# 하위 호환성을 위한 별칭
AiokafkaProducerFactory = ConfluentProducerFactory
