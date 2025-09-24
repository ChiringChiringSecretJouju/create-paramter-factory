#!/usr/bin/env python3
"""CLI 프로듀서 - ws.command 토픽에 테스트 메시지 전송"""

import argparse
import asyncio
import json
from aiokafka import AIOKafkaProducer


async def send_test_message(
    exchange: str,
    symbols: list[str],
    region: str,
    request_type: str,
    correlation_id: str = "cli-test-001",
):
    """테스트 메시지를 ws.command 토픽에 전송"""

    # 메시지 구조 생성
    test_message = {
        "target": {
            "region": region,
            "exchange": exchange,
            "request_type": request_type,
        },
        "connection": {"socket_params": {"symbols": symbols}},
        "socket_mode": request_type,  # RequestEnvelope가 기대하는 필드
        "symbols": symbols,  # 상위 레벨에도 필요
        "correlation_id": correlation_id,
    }

    producer = AIOKafkaProducer(
        bootstrap_servers="kafka1:19092,kafka2:29092,kafka3:39092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type=None,
        acks="all",
    )

    try:
        await producer.start()
        print("✅ Producer started")

        # 메시지 전송 (send_and_wait으로 확인)
        record_metadata = await producer.send_and_wait(
            topic="ws.command.test", value=test_message  # 새 토픽으로 테스트
        )

        print(f"📨 Message sent successfully!")
        print(f"   Topic: {record_metadata.topic}")
        print(f"   Partition: {record_metadata.partition}")
        print(f"   Offset: {record_metadata.offset}")
        print(f"   Message: {test_message}")

        return True

    except Exception as e:
        print(f"❌ Failed to send message: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        await producer.stop()
        print("✅ Producer stopped")


def main():
    parser = argparse.ArgumentParser(
        description="Send test message to ws.command topic"
    )
    parser.add_argument("--exchange", required=True, help="Exchange name (e.g., upbit)")
    parser.add_argument("--symbols", required=True, help="Symbol (e.g., BTC)")
    parser.add_argument("--region", default="korea", help="Region (default: korea)")
    parser.add_argument(
        "--request-type", required=True, help="Request type (e.g., ticker)"
    )
    parser.add_argument(
        "--correlation-id", default="cli-test-001", help="Correlation ID"
    )

    args = parser.parse_args()

    # symbols를 리스트로 변환 (쉼표로 구분)
    symbols = [s.strip() for s in args.symbols.split(",")]

    print(f"🚀 Sending message:")
    print(f"   Exchange: {args.exchange}")
    print(f"   Symbols: {symbols}")
    print(f"   Region: {args.region}")
    print(f"   Request Type: {args.request_type}")
    print(f"   Correlation ID: {args.correlation_id}")
    print()

    success = asyncio.run(
        send_test_message(
            exchange=args.exchange,
            symbols=symbols,
            region=args.region,
            request_type=args.request_type,
            correlation_id=args.correlation_id,
        )
    )

    if success:
        print("\n🎉 Test completed successfully!")
        print("Now check if the consumer receives this message...")
    else:
        print("\n💥 Test failed!")
        exit(1)


if __name__ == "__main__":
    main()
