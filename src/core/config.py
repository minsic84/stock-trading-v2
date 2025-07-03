"""
Configuration management module
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv


class Config:
    """애플리케이션 설정 관리 클래스"""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.config_dir = self.project_root / "config"
        self.load_environment()
        self.load_configs()

    def load_environment(self):
        """환경변수 로드"""
        # .env 파일 로드
        env_path = self.project_root / ".env"
        if env_path.exists():
            load_dotenv(env_path)

        # 기본 환경변수 설정
        self.env = os.getenv("ENVIRONMENT", "development")
        self.debug = os.getenv("DEBUG", "True").lower() == "true"
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

        # 데이터베이스 설정
        self.db_type = os.getenv("DB_TYPE", "sqlite")
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = int(os.getenv("DB_PORT", "5432"))
        self.db_name = os.getenv("DB_NAME", "stock_trading_db")
        self.db_user = os.getenv("DB_USER", "")
        self.db_password = os.getenv("DB_PASSWORD", "")
        self.sqlite_db_path = os.getenv("SQLITE_DB_PATH", "./data/stock_data.db")

        # 키움 API 설정
        self.kiwoom_user_id = os.getenv("KIWOOM_USER_ID", "")
        self.kiwoom_password = os.getenv("KIWOOM_PASSWORD", "")
        self.kiwoom_cert_password = os.getenv("KIWOOM_CERT_PASSWORD", "")

        # 텔레그램 설정
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

        # 로깅 설정
        self.log_dir = os.getenv("LOG_DIR", "./logs")
        self.log_file_max_size = os.getenv("LOG_FILE_MAX_SIZE", "10MB")
        self.log_file_backup_count = int(os.getenv("LOG_FILE_BACKUP_COUNT", "5"))

        # API 요청 설정
        self.api_request_delay_ms = int(os.getenv("API_REQUEST_DELAY_MS", "3600"))
        self.max_retry_attempts = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
        self.retry_delay_seconds = float(os.getenv("RETRY_DELAY_SECONDS", "3.6"))

    def load_configs(self):
        """YAML 설정 파일 로드"""
        config_files = ["database.yaml", "api_config.yaml", "logging.yaml"]

        for config_file in config_files:
            config_path = self.config_dir / config_file
            if config_path.exists():
                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_data = yaml.safe_load(f)
                        # 파일명에서 확장자 제거하고 속성으로 설정
                        attr_name = config_file.replace('.yaml', '').replace('.yml', '')
                        setattr(self, attr_name, config_data)
                except Exception as e:
                    print(f"Warning: Failed to load {config_file}: {e}")

    def get_database_url(self) -> str:
        """데이터베이스 연결 URL 반환"""
        if self.db_type == 'sqlite':
            # SQLite 경로 처리
            db_path = Path(self.sqlite_db_path)
            if not db_path.is_absolute():
                db_path = self.project_root / db_path
            return f"sqlite:///{db_path}"

        elif self.db_type == 'postgresql':
            return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

        elif self.db_type == 'mysql':
            return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def get_active_database_config(self) -> Dict[str, Any]:
        """활성 데이터베이스 설정 반환"""
        if hasattr(self, 'database'):
            active_profile = self.database.get('active_profile', 'development')
            return self.database.get(active_profile, {})
        return {}

    def is_development(self) -> bool:
        """개발 환경 여부 확인"""
        return self.env.lower() == 'development'

    def is_production(self) -> bool:
        """운영 환경 여부 확인"""
        return self.env.lower() == 'production'

    def is_test(self) -> bool:
        """테스트 환경 여부 확인"""
        return self.env.lower() == 'test'

    def __repr__(self):
        return f"<Config(env='{self.env}', db_type='{self.db_type}', debug={self.debug})>"