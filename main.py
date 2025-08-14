from __future__ import annotations

import argparse
import asyncio
import json
from typing import Sequence

from transport.types.specs import SocketConnectMetaData as SCMeta
from core.properties import SocketRequestType
from transport.producer import (
    AioKafkaConnectProducer,
    ConnectMessageBuilder,
)
from transport.utils.projection import make_exchange_metadata


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build and (optionally) produce a connect+projection message"
    )
    p.add_argument("--exchange", "-e", default="bithumb", help="exchange name")
    p.add_argument(
        "--type",
        "-t",
        dest="req_type",
        choices=["ticker", "orderbook", "trade"],
        default="ticker",
        help="socket request type",
    )
    p.add_argument(
        "--symbols",
        "-s",
        default="BTC",
        help="comma-separated symbols (e.g., BTC,ETH)",
    )
    p.add_argument("--region", "-r", default="korea", help="region (default: korea)")
    p.add_argument(
        "--expiry-ms",
        type=int,
        default=None,
        help="optional expiry in milliseconds",
    )
    p.add_argument(
        "--produce",
        action="store_true",
        help="if set, send the message to Kafka using aiokafka",
    )
    return p.parse_args()


async def _run(
    *,
    exchange: str,
    req_type: SocketRequestType,
    symbols: Sequence[str],
    region: str,
    expiry_ms: int | None,
    produce: bool,
) -> int:
    # 메시지 빌드
    builder = ConnectMessageBuilder()
    source = make_exchange_metadata(
        region=region,
        exchange=exchange,
        req_type=req_type,
    )
    msg = await builder.build(
        source=source,
        symbols=symbols,
        expiry_ms=expiry_ms,
    )
    # 화면 출력 (검증용)
    print(json.dumps(msg, ensure_ascii=False, indent=4))

    # 카프카 발행 옵션
    if produce:
        producer = AioKafkaConnectProducer()
        await producer.start()
        try:
            await producer.produce_connect(
                spec=SCMeta(
                    region=region,
                    exchange=exchange,
                    req_type=req_type,
                    symbols=symbols,
                    expiry_ms=expiry_ms,
                )
            )
        finally:
            await producer.stop()

    return 0


def main() -> None:
    args = _parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    code = asyncio.run(
        _run(
            exchange=args.exchange,
            req_type=args.req_type,
            symbols=symbols,
            region=args.region,
            expiry_ms=args.expiry_ms,
            produce=bool(args.produce),
        )
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()
