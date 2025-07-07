"""
키움 API 공통 세션 관리 모듈
모든 데이터 수집기에서 공통으로 사용하는 연결/로그인 로직
"""
import sys
import os
from pathlib import Path
import logging
from typing import Optional, Dict, Any
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가 (다른 스크립트들과 동일한 방식)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.api.connector import KiwoomAPIConnector, get_kiwoom_connector
from src.core.config import Config

# 로거 설정
logger = logging.getLogger(__name__)

# 로깅 기본 설정 (로거가 설정되지 않은 경우를 위해)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class KiwoomSession:
    """키움 API 공통 세션 관리 클래스"""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.connector = None
        self.is_initialized = False
        self.connection_info = {}

        logger.info("키움 세션 매니저 초기화")

    def connect_and_login(self, auto_login: bool = True, show_progress: bool = True) -> bool:
        """
        키움 API 연결 및 로그인 (공통 로직)
        test_api_connector.py의 로직을 통합
        """
        try:
            if show_progress:
                print("🔌 키움 API 연결 준비")
                print("=" * 40)

            # 1. 설정 로드 확인
            if show_progress:
                print(f"✅ 설정 로드 완료")

            # 2. 커넥터 생성
            self.connector = get_kiwoom_connector(self.config)
            if show_progress:
                print(f"✅ 커넥터 생성 완료")

            # 3. 연결 상태 초기 확인
            status = self.connector.get_connection_status()
            if show_progress:
                print(f"📊 초기 연결 상태: {status['is_connected']}")

            # 4. 로그인 필요 여부 확인
            if not status['is_connected'] and auto_login:
                if show_progress:
                    print("🔄 키움 API 로그인 시도 중... (로그인 창이 나타날 수 있습니다)")

                login_success = self.connector.login()

                if login_success:
                    if show_progress:
                        print("✅ 키움 API 로그인 성공!")

                    # 5. 계좌 정보 확인
                    final_status = self.connector.get_connection_status()
                    account_info = final_status.get('account_info', {})

                    if show_progress:
                        print(f"👤 사용자: {account_info.get('user_name', 'N/A')}")
                        print(f"🏦 계좌번호: {account_info.get('account_num', 'N/A')}")
                        print(f"🕐 로그인 시간: {account_info.get('login_time', 'N/A')}")

                    # 연결 정보 저장
                    self.connection_info = {
                        'connected_at': datetime.now(),
                        'account_info': account_info,
                        'auto_login': auto_login
                    }

                    self.is_initialized = True
                    logger.info("키움 세션 준비 완료")
                    return True

                else:
                    if show_progress:
                        print("❌ 키움 API 로그인 실패")
                    logger.error("키움 API 로그인 실패")
                    return False

            elif status['is_connected']:
                # 이미 연결되어 있는 경우
                if show_progress:
                    print("✅ 키움 API 이미 연결됨")

                self.connection_info = {
                    'connected_at': datetime.now(),
                    'account_info': status.get('account_info', {}),
                    'auto_login': False
                }

                self.is_initialized = True
                return True

            else:
                # auto_login=False이고 연결되지 않은 경우
                if show_progress:
                    print("ℹ️ 자동 로그인이 비활성화되어 있습니다")

                self.is_initialized = True  # 커넥터는 준비됨
                return True

        except Exception as e:
            if show_progress:
                print(f"❌ 키움 세션 초기화 실패: {e}")
            logger.error(f"키움 세션 초기화 실패: {e}")
            return False

    def is_ready(self) -> bool:
        """세션이 사용 준비가 되었는지 확인"""
        if not self.is_initialized or not self.connector:
            return False

        status = self.connector.get_connection_status()
        return status['is_connected']

    def get_connector(self) -> Optional[KiwoomAPIConnector]:
        """커넥터 인스턴스 반환"""
        return self.connector

    def get_status(self) -> Dict[str, Any]:
        """현재 세션 상태 반환"""
        if not self.connector:
            return {
                'initialized': False,
                'connected': False,
                'error': '커넥터가 초기화되지 않음'
            }

        connector_status = self.connector.get_connection_status()

        return {
            'initialized': self.is_initialized,
            'connected': connector_status['is_connected'],
            'account_num': connector_status.get('account_num'),
            'request_count': connector_status.get('request_count', 0),
            'connection_info': self.connection_info,
            'last_request_time': connector_status.get('last_request_time')
        }

    def disconnect(self):
        """연결 해제"""
        if self.connector:
            try:
                self.connector.logout()
                logger.info("키움 세션 종료")
            except Exception as e:
                logger.error(f"세션 종료 중 오류: {e}")

        self.is_initialized = False
        self.connection_info.clear()


# 편의 함수들
def create_kiwoom_session(auto_login: bool = True, show_progress: bool = True,
                          config: Optional[Config] = None) -> Optional[KiwoomSession]:
    """키움 세션 생성 및 초기화 (편의 함수)"""
    try:
        session = KiwoomSession(config)

        if session.connect_and_login(auto_login, show_progress):
            return session
        else:
            logger.error("키움 세션 생성 실패")
            return None

    except Exception as e:
        logger.error(f"키움 세션 생성 중 오류: {e}")
        return None


def get_ready_session(config: Optional[Config] = None) -> Optional[KiwoomSession]:
    """즉시 사용 가능한 키움 세션 반환 (자동 로그인)"""
    return create_kiwoom_session(auto_login=True, show_progress=False, config=config)