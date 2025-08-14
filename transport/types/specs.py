"""전송 계층 코어 스펙 타입의 재내보내기.

이 모듈은 core.types에서 데이터클래스를 재내보내어 전송 계층을 사용하는
호출자를 위한 임포트 경로를 안정적으로 유지하면서, 코어 를
유지합니다.
"""

from core.types import SocketConnectMetaData  # noqa: F401
