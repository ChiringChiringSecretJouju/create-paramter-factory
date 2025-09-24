"""Avro Schema 관리 및 직렬화/역직렬화 유틸리티."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict
from dataclasses import dataclass
from datetime import datetime

import orjson


@dataclass(slots=True, frozen=True)
class ConnectCommandSchema:
    """ConnectCommand Avro Schema 관리 클래스."""
    
    SCHEMA_PATH = Path(__file__).parent.parent / "setting" / "schemas" / "connect_command.avsc"
    
    @classmethod
    def load_schema(cls) -> dict[str, Any]:
        """Avro Schema 파일을 로드합니다."""
        with open(cls.SCHEMA_PATH, 'r', encoding='utf-8') as f:
            return orjson.loads(f.read())
    
    @classmethod
    def validate_message(cls, message: dict[str, Any]) -> bool:
        """메시지가 스키마에 맞는지 기본 검증합니다.
        
        Note:
            완전한 Avro 검증을 위해서는 avro-python3 라이브러리 사용을 권장합니다.
        """
        try:
            # 필수 필드 검증
            required_fields = ['type', 'action', 'target', 'connection']
            for field in required_fields:
                if field not in message:
                    return False
            
            # target 필드 검증
            target = message.get('target', {})
            target_required = ['exchange', 'region', 'request_type']
            for field in target_required:
                if field not in target:
                    return False
            
            # connection 필드 검증
            connection = message.get('connection', {})
            if 'socket_params' not in connection:
                return False
            
            socket_params = connection['socket_params']
            socket_required = ['subscribe_type', 'symbols']
            for field in socket_required:
                if field not in socket_params:
                    return False
            
            # symbols가 배열인지 확인
            if not isinstance(socket_params['symbols'], list):
                return False
            
            return True
            
        except Exception:
            return False
    
    @classmethod
    def create_message(
        cls,
        *,
        exchange: str,
        region: str,
        request_type: str,
        subscribe_type: str,
        symbols: list[str],
        correlation_id: str | None = None,
        timestamp: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """ConnectCommand 메시지를 생성합니다."""
        
        if timestamp is None:
            timestamp = int(datetime.now().timestamp() * 1000)  # milliseconds
        
        message = {
            "type": "command",
            "action": "connect_and_subscribe",
            "target": {
                "exchange": exchange,
                "region": region,
                "request_type": request_type
            },
            "connection": {
                "socket_params": {
                    "subscribe_type": subscribe_type,
                    "symbols": symbols
                }
            },
            "correlation_id": correlation_id,
            "timestamp": timestamp,
            "metadata": metadata
        }
        
        return message
    
    @classmethod
    def serialize_to_bytes(cls, message: dict[str, Any]) -> bytes:
        """메시지를 JSON bytes로 직렬화합니다."""
        return orjson.dumps(message)
    
    @classmethod
    def deserialize_from_bytes(cls, data: bytes) -> dict[str, Any]:
        """JSON bytes를 메시지로 역직렬화합니다."""
        return orjson.loads(data)


# 사용 예제 및 테스트 함수
def example_usage() -> None:
    """ConnectCommandSchema 사용 예제."""
    
    # 1. 스키마 로드
    schema = ConnectCommandSchema.load_schema()
    print("📋 Loaded Avro Schema:")
    print(orjson.dumps(schema, option=orjson.OPT_INDENT_2).decode())
    
    # 2. 메시지 생성
    message = ConnectCommandSchema.create_message(
        exchange="upbit",
        region="korea", 
        request_type="ticker",
        subscribe_type="ticker",
        symbols=["BTC", "ETH", "XRP"],
        correlation_id="test-001"
    )
    
    print("\n📨 Created Message:")
    print(orjson.dumps(message, option=orjson.OPT_INDENT_2).decode())
    
    # 3. 검증
    is_valid = ConnectCommandSchema.validate_message(message)
    print(f"\n✅ Message validation: {'PASSED' if is_valid else 'FAILED'}")
    
    # 4. 직렬화/역직렬화
    serialized = ConnectCommandSchema.serialize_to_bytes(message)
    deserialized = ConnectCommandSchema.deserialize_from_bytes(serialized)
    
    print(f"\n🔄 Serialization test: {'PASSED' if message == deserialized else 'FAILED'}")
    print(f"Serialized size: {len(serialized)} bytes")


if __name__ == "__main__":
    example_usage()
