# setting/

런타임/배포 관련 설정과 템플릿 파일을 보관합니다.

## 1) 이 폴더의 역할
- 환경/구성값과 템플릿(YAML) 관리
- 외부 시스템(Kafka 등) 연결 설정 제공

## 2) 포함 기능
- `setting/templates/`
  - `socket_templates/`: 거래소/지역별 소켓 파라미터 YAML 템플릿
  - `_market_all_ticker.yml`: 마켓 전체 티커 관련 템플릿 예시
  - `yml_config.py`: YAML 설정 로딩 유틸
- `setting/kafka.py`: Kafka 관련 기본 구성/모델
- `setting/config/`: 추가 환경설정 파일 위치(필요 시 확장)

## 3) 날짜
- 2025-08-20
