# Avro Schemas

이 디렉토리는 WebSocket 연결 명령에 대한 Avro Schema 정의를 포함합니다.

## 📋 Schema 파일

### 1. `connect_command.avsc` (v1.0)
기본 버전의 ConnectCommand 스키마입니다.

**주요 특징:**
- 문자열 기반 필드 (유연성 높음)
- 기본적인 검증 규칙
- 간단한 구조

**사용 사례:**
- 프로토타입 개발
- 빠른 구현이 필요한 경우
- 새로운 거래소/타입 추가가 빈번한 경우

### 2. `connect_command_v2.avsc` (v2.0)
확장된 버전의 ConnectCommand 스키마입니다.

**주요 특징:**
- Enum 기반 필드 (타입 안정성 높음)
- 추가 옵션 필드 (depth, interval, connection options)
- 엄격한 검증 규칙
- 스키마 진화 지원

**사용 사례:**
- 프로덕션 환경
- 엄격한 타입 검증이 필요한 경우
- 스키마 호환성이 중요한 경우

## 🚀 사용 방법

### Python에서 사용

```python
from common.avro_schemas import ConnectCommandSchema

# 메시지 생성
message = ConnectCommandSchema.create_message(
    exchange="upbit",
    region="korea",
    request_type="ticker",
    subscribe_type="ticker",
    symbols=["BTC", "ETH", "XRP"],
    correlation_id="test-001"
)

# 검증
is_valid = ConnectCommandSchema.validate_message(message)

# 직렬화
serialized = ConnectCommandSchema.serialize_to_bytes(message)
```

### Kafka Producer에서 사용

```python
from aiokafka import AIOKafkaProducer
from common.avro_schemas import ConnectCommandSchema

producer = AIOKafkaProducer(
    value_serializer=lambda v: ConnectCommandSchema.serialize_to_bytes(v)
)

message = ConnectCommandSchema.create_message(
    exchange="upbit",
    region="korea", 
    request_type="ticker",
    subscribe_type="ticker",
    symbols=["BTC"]
)

await producer.send("ws.command", value=message)
```

## 📊 Schema 진화 가이드

### 호환성 규칙

1. **BACKWARD 호환성** (권장)
   - 필드 삭제 가능
   - 선택적 필드 추가 가능
   - 기본값이 있는 필드 추가 가능

2. **FORWARD 호환성**
   - 필드 추가 가능
   - 선택적 필드 삭제 가능

3. **FULL 호환성**
   - BACKWARD + FORWARD 호환성

### 변경 예시

#### ✅ 안전한 변경
```json
// 선택적 필드 추가
{
  "name": "priority",
  "type": ["null", "int"],
  "default": null
}

// 기본값이 있는 필드 추가  
{
  "name": "retry_count",
  "type": "int",
  "default": 0
}
```

#### ❌ 위험한 변경
```json
// 필수 필드 추가 (기본값 없음)
{
  "name": "required_field",
  "type": "string"
}

// 필드 타입 변경
{
  "name": "timestamp",
  "type": "string"  // 원래는 long
}
```

## 🔧 Schema Registry 설정

### Confluent Schema Registry 사용 시

```bash
# 스키마 등록
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @connect_command.avsc \
  http://localhost:8081/subjects/ws.command-value/versions

# 호환성 설정
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "BACKWARD"}' \
  http://localhost:8081/config/ws.command-value
```

### Python에서 Schema Registry 사용

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

schema_registry_client = SchemaRegistryClient({
    'url': 'http://localhost:8081'
})

with open('connect_command.avsc', 'r') as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str
)
```

## 📈 성능 고려사항

### Avro vs JSON 비교

| 항목 | Avro | JSON |
|------|------|------|
| 크기 | 작음 (30-50% 절약) | 큼 |
| 속도 | 빠름 | 보통 |
| 스키마 진화 | 우수 | 제한적 |
| 가독성 | 낮음 | 높음 |
| 타입 안정성 | 높음 | 낮음 |

### 최적화 팁

1. **필드 순서**: 자주 사용되는 필드를 앞에 배치
2. **Union 타입**: 가능한 한 적게 사용
3. **문자열 길이**: 긴 문자열은 압축 고려
4. **배열 크기**: 큰 배열은 별도 메시지로 분리 고려

## 🧪 테스트

```bash
# 스키마 유틸리티 테스트
python common/avro_schemas.py

# 전체 테스트 실행
pytest tests/test_avro_schemas.py -v
```
