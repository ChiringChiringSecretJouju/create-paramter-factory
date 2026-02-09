import yaml
from pathlib import Path
from functools import lru_cache


# 기본 경로 설정
base_path: Path = Path(__file__).parent
ticker_path: Path = base_path / "socket_templates" / "_market_all_ticker.yml"
template_path: Path = base_path / "socket_templates" / "_socket_all_parameter.yml"

# 지역별 소켓 템플릿 디렉토리 경로
socket_templates_dir: Path = base_path / "socket_templates"


@lru_cache(maxsize=30)
def load_yml_file(path: str) -> dict:
    """YAML 파일을 로드하고 캐싱"""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class _YmlConfig:
    def __init__(self, yml_path: str) -> None:
        self.yml_path = yml_path

    def load(self) -> dict:
        # 실제로는 이미 캐싱된 load_yml_file을 호출하므로
        # I/O는 한 번만 일어나게 됨
        return load_yml_file(self.yml_path)

    def get_ticker_format(self, exchange_name: str) -> list[str] | None:
        ticker_schema = self.load()
        name = exchange_name.lower()
        if name not in ticker_schema:
            raise ValueError(f"지원하지 않는 거래소입니다: {exchange_name}")
        return ticker_schema[name].get("parameter", [])

    def load_templates_from_yaml(self) -> dict:
        return self.load()


def get_region_for_exchange(exchange_name: str) -> str:
    """거래소 이름에 따라 지역 반환"""
    # 한국 거래소
    if exchange_name.lower() in ["upbit", "bithumb", "coinone", "korbit"]:
        return "korea"
    # 아시아 거래소
    elif exchange_name.lower() in ["binance", "bybit", "okx"]:
        return "asia"
    # 기본값
    return ""


def load_socket_template(exchange_name: str) -> dict:
    """지역별 디렉토리에서 거래소 소켓 템플릿 로드"""
    region = get_region_for_exchange(exchange_name)
    if not region:
        # 지역이 정의되지 않은 경우 기존 경로에서 로드 시도
        try:
            path = socket_templates_dir / f"{exchange_name.lower()}.yml"
            return load_yml_file(str(path))
        except FileNotFoundError:
            return None

    # 지역별 디렉토리에서 파일 로드
    try:
        path = socket_templates_dir / region / f"{exchange_name.lower()}.yml"
        return load_yml_file(str(path))
    except FileNotFoundError:
        return None


def get_available_exchanges() -> dict[str, list[str]]:
    """지역별로 사용 가능한 거래소 목록 반환"""
    result = {"korea": [], "asia": [], "global": []}

    # 각 지역 디렉토리 검사
    for region in result.keys():
        region_dir = socket_templates_dir / region
        if region_dir.exists():
            for file_path in region_dir.glob("*.yml"):
                exchange_name = file_path.stem  # 파일 확장자 제외한 이름
                result[region].append(exchange_name)

    return result


# 기존 함수들을 그대로 유지
_ticker_config = _YmlConfig(ticker_path).get_ticker_format
_template_config = _YmlConfig(template_path).load_templates_from_yaml
