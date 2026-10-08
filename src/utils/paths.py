from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_DIR = PROJECT_ROOT / "config"
LOGS_DIR = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data"
ADDITIONAL_REPORTS = PROJECT_ROOT / "data" / "additional_reports"
RAW_DATA_DIR = DATA_DIR / "raw"
TESTS_DIR = PROJECT_ROOT / "tests"

INPUT_SOURCES_PATH = CONFIG_DIR / "input_sources.json"
OUTPUT_SOURCES_PATH = CONFIG_DIR / "output_sources.json"

TEST_RAW_DATA = TESTS_DIR / "sample_data"
TEST_EXPECTED_DATA = TESTS_DIR / "expected_data"