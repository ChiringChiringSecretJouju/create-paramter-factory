# 시간 관련 유틸리티
from datetime import datetime
from zoneinfo import ZoneInfo


def now_ms_kst() -> int:
    """KST(Asia/Seoul) 기준 epoch millis 반환"""
    return int(datetime.now(ZoneInfo("Asia/Seoul")).timestamp() * 1000)
