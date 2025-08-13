# CreateParamterFactory

웹소켓 수집을 위한 URL/파라미터 생성을 표준화한 모듈입니다. 거래소/요청타입별 템플릿을 코드와 분리해 유지보수성과 확장성을 높였습니다.

## 1) 목적(What)
- 거래소별 WebSocket 연결 설정을 일관된 인터페이스로 제공
- URI(웹소켓 URL) + 구독 파라미터를 하나의 구성으로 반환
- 템플릿 기반으로 요청타입별(ticker/orderbook/trade) 메시지 구조를 안전하게 관리

## 2) 현재 지원 현황(Where) 및 로드맵
- 현재 국가/거래소
  - korea: `upbit`, `bithumb`, `korbit`, `coinone`
- 로드맵(추가 예정)
  - 기타 지역/거래소 확장 시 `setting/templates/socket_templates/{region}/{exchange}.yml` 파일만 추가하면 코드 수정 없이 동작

## 3) 동작 방식(How)
- __핵심 구성요소__
  - `core/socket_uri/uri_builder.py` 의 `ExchangeURLManager`
    - 설정에서 거래소별 WebSocket URL 조회 (`get_region_urls()`, `get_symbol_collect_url()`)
  - `core/socket_params/` 의 `SocketParameterFactory` + 각 거래소 Creator
    - 템플릿(`setting/templates/socket_templates/{region}/{exchange}.yml`)을 로드해 심볼/요청타입에 맞는 구독 메시지 생성
  - `core/properties.py` 의 `ExchangeService`
    - 서비스 레이어. URL + 소켓 파라미터를 묶어 최종 구성 반환

- __템플릿 구조(단일 파일, 타입별 섹션)__
  - 경로: `setting/templates/socket_templates/korea/{exchange}.yml`
  - 내부 키: `ticker`, `orderbook`, `trade` 섹션을 한 파일에 정의

- __사용 예시__
  - 단일 거래소 구성 조회:
    - `ExchangeService().get_exchange_config(exchange, symbols, req_type, region="korea")`
  - 전체 거래소 구성 조회:
    - `ExchangeService().get_all_exchange_configs(symbols, req_type, region="korea")`

## 4) 테스트 결과(Test)
다음은 최신 테스트 세션 요약입니다. 자세한 항목은 `tests/test_exchange_service.py` 참고.

```
=============================================================================== test session starts ================================================================================
platform darwin -- Python 3.12.10, pytest-8.4.1, pluggy-1.6.0
rootdir: /Users/imhaneul/Documents/project/ChiringChiringSecretJouju/CreateParamterFactory
collected 13 items

tests/test_exchange_service.py::test_get_region_urls_korea_socket ✓                                [ 7%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=upbit-type=ticker] ✓     [15%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=upbit-type=orderbook] ✓   [23%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=upbit-type=trade] ✓      [30%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=bithumb-type=ticker] ✓   [38%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=bithumb-type=orderbook] ✓ [46%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=bithumb-type=trade] ✓    [53%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=korbit-type=ticker] ✓    [61%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=korbit-type=orderbook] ✓  [69%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=korbit-type=trade] ✓     [76%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=coinone-type=ticker] ✓   [84%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=coinone-type=orderbook] ✓ [92%]
tests/test_exchange_service.py::test_exchange_service_config_by_exchange_and_type[ex=coinone-type=trade] ✓    [100%]

================================================================================ 13 passed in 0.05s ================================================================================
```

## 개발 노트
- 타입 힌팅(Python 3.12 스타일)과 SRP 준수로 유지보수성 향상
- 템플릿 누락/오류 시 명확한 예외 메시지로 진단 용이
- pytest 파라미터라이즈로 거래소×요청타입 케이스 전수 검증
