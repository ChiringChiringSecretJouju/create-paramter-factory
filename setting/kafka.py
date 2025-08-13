import configparser
from pathlib import Path

# Load KAFKA settings from the same urls.conf used across settings
_path = Path(__file__).parent
_parser = configparser.ConfigParser()
_parser.read(f"{_path}/_kafka_config.conf")

BOOTSTRAP_SERVER: str = _parser.get(
    "KAFKA", "bootstrap_servers", fallback="localhost:9092"
)
SECURITY_PROTOCOL: str = _parser.get("KAFKA", "security_protocol", fallback="PLAINTEXT")
MAX_BATCH_SIZE: int = int(_parser.get("KAFKA", "max_batch_size", fallback="1048576"))
MAX_REQUEST_SIZE: int = int(
    _parser.get("KAFKA", "max_request_size", fallback="1048576")
)
LINGER_MS: int = int(_parser.get("KAFKA", "linger_ms", fallback="0"))
ACKS_RAW: str = _parser.get("KAFKA", "acks", fallback="1")

# acks는 정수/문자 둘 다 허용하므로 파싱 시도
try:
    ACKS: int | str = int(ACKS_RAW)
except (TypeError, ValueError):
    ACKS = ACKS_RAW
