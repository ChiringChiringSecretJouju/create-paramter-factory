# transport/

외부 I/O 계층(메시지 브로커 등)과의 상호작용을 담당합니다. Kafka 컨슈머/프로듀서, 메시지 스키마, DI, 유틸 등을 포함합니다.

## 1) 이 폴더의 역할
- 요청 수신 → 메시지 변환 → 전송 파이프라인 구현
- 전송 타입/헤더/스펙을 정의하여 계층 간 계약을 명확히 함

## 2) 포함 기능
- `transport/consumer.py`: `AioKafkaRequestConsumer`, `RequestEnvelope`
- `transport/producer.py`: `AioKafkaConnectProducer`, `ConnectMessageBuilder`
- `transport/di/producer_factory.py`: Kafka 프로듀서 팩토리(DI)
- `transport/types/`: 메시지/헤더/스펙 타입 정의
- `transport/utils/`: 프로젝션/시간 유틸, Kafka 설정 로딩

## 3) 날짜
- 2025-08-20
