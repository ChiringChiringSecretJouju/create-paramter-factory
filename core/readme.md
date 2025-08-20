# core/

이 디렉터리는 도메인 및 애플리케이션 핵심 로직을 담습니다. 거래소 연결을 위한 구성 생성, 소켓 파라미터/URL 조합, 타입 명세를 담당합니다.

## 1) 이 폴더의 역할
- Exchange/Region 별 웹소켓 연결 구성의 단일 진입점
- URL 빌더와 소켓 파라미터 팩토리 통합(`ExchangeConfigManager`, `ExchangeService`)
- 도메인 타입 및 스펙 정의(`core/types/`)

## 2) 포함 기능
- `core/properties.py`
  - `ExchangeService`: URL 조회/구성 조회 헬퍼
  - `ExchangeConfigManager`: URL + 소켓 파라미터 결합하여 `ExchangeSocketConfig` 생성
- `core/socket_params/`
  - 거래소별 파라미터 생성기(Upbit/Bithumb/Korbit/Coinone)
  - YAML 템플릿 로딩 및 placeholder 치환
- `core/socket_uri/`
  - `ExchangeURLManager`: 거래소/지역/유형별 URL 관리
- `core/types/`
  - 소켓 요청 타입, 구성 타입, 스펙(`SocketConnectMetaData`) 등 정의

## 3) 날짜
- 2025-08-20
