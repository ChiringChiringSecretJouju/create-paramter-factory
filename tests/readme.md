# tests/

프로젝트 테스트 코드를 포함합니다. pytest를 사용하며, 핵심 서비스/유틸의 동작을 검증합니다.

## 1) 이 폴더의 역할
- 단위/통합 테스트를 통한 품질 보증
- 리팩터링/기능 추가 시 회귀 방지

## 2) 포함 기능
- `tests/conftest.py`: 공용 픽스처/설정
- `tests/test_exchange_service.py`: `ExchangeService`/구성 생성 플로우 테스트

## 3) 날짜
- 2025-08-20
