"""
=============================================================================== test session starts ================================================================================
platform darwin -- Python 3.12.10, pytest-8.4.1, pluggy-1.6.0 -- /Users/imhaneul/Documents/project/ChiringChiringSecretJouju/CreateParamterFactory/.venv/bin/python3
cachedir: .pytest_cache
rootdir: /Users/imhaneul/Documents/project/ChiringChiringSecretJouju/CreateParamterFactory
configfile: pyproject.toml
collected 13 items

tests/test_exchange_service.py::test_get_region_urls_korea_socket PASSED                                                                                                     [  7%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=upbit-type=ticker] PASSED                                                               [ 15%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=upbit-type=orderbook] PASSED                                                            [ 23%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=upbit-type=trade] PASSED                                                                [ 30%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=bithumb-type=ticker] PASSED                                                             [ 38%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=bithumb-type=orderbook] PASSED                                                          [ 46%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=bithumb-type=trade] PASSED                                                              [ 53%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=korbit-type=ticker] PASSED                                                              [ 61%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=korbit-type=orderbook] PASSED                                                           [ 69%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=korbit-type=trade] PASSED                                                               [ 76%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=coinone-type=ticker] PASSED                                                             [ 84%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=coinone-type=orderbook] PASSED                                                          [ 92%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=coinone-type=trade] PASSED                                                              [100%]

====================================================================================== PASSES ======================================================================================
================================================================================ 13 passed in 0.05s ================================================================================

"""

import pytest
from core.socket_uri.uri_builder import ExchangeURLManager
from core.properties import ExchangeService
from core.types import Ok
from core.properties import SocketRequestType


def test_get_region_urls_korea_socket():
    manager = ExchangeURLManager()
    result = manager.get_region_urls("korea", "socket")

    assert isinstance(result, Ok), f"Expected Ok, got {type(result)}"
    urls = result.value()

    # Basic structure checks
    assert isinstance(urls, dict)
    for ex in ("upbit", "bithumb", "korbit", "coinone"):
        assert ex in urls, f"Missing exchange in urls: {ex}"
        assert isinstance(urls[ex], str) and urls[ex].startswith("wss://")


@pytest.mark.parametrize(
    "req_type", ["ticker", "orderbook", "trade"], ids=lambda v: f"type={v}"
)
@pytest.mark.parametrize(
    "exchange", ["upbit", "bithumb", "korbit", "coinone"], ids=lambda v: f"ex={v}"
)
def test_exchange_service_config_by_exchange_and_type(
    exchange: str, req_type: SocketRequestType
):
    svc = ExchangeService()

    res = svc.get_exchange_config(
        exchange=exchange,
        symbols=["BTC"],
        req_type=req_type,
        region="korea",
    )

    assert isinstance(res, Ok), f"Expected Ok, got {type(res)}"
    cfg = res.value()

    # URL checks
    assert (
        "url" in cfg and isinstance(cfg["url"], str) and cfg["url"].startswith("wss://")
    )

    # Socket params checks (shape varies by exchange)
    assert "socket_params" in cfg
    params = cfg["socket_params"]

    if exchange in ("upbit", "bithumb"):
        # Array payload with a ticket + subscription body
        assert isinstance(params, list) and len(params) >= 1
        # First element is often a ticket for these exchanges
        # Some templates may include ticket; check len and key existence defensively
        if len(params) >= 1 and isinstance(params[0], dict) and "ticket" in params[0]:
            pass  # ok
        # Subscription body should exist and include type and codes
        body = params[-1]
        assert isinstance(body, dict)
        assert body.get("type") == req_type
        assert (
            "codes" in body
            and isinstance(body["codes"], list)
            and len(body["codes"]) >= 1
        )

    elif exchange == "korbit":
        # Korbit uses array of objects with symbols
        assert isinstance(params, list) and len(params) >= 1
        body = params[0]
        assert isinstance(body, dict)
        assert body.get("type") == req_type
        assert (
            "symbols" in body
            and isinstance(body["symbols"], list)
            and len(body["symbols"]) >= 1
        )

    elif exchange == "coinone":
        # Coinone uses single object with channel/topic
        assert isinstance(params, dict)
        assert "channel" in params and isinstance(params["channel"], str)
        assert "topic" in params and isinstance(params["topic"], dict)
        topic = params["topic"]
        # topic typically includes target_currency for symbol
        assert "target_currency" in topic and isinstance(topic["target_currency"], str)
    else:
        pytest.fail(f"Unknown exchange tested: {exchange}")
