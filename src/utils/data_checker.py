"""
일봉 데이터 누락 체크 및 수집 전략 결정 모듈
- 종목별 일봉 테이블 존재 여부 확인
- 마지막 데이터 날짜 조회
- 누락 기간 분석 및 수집 방법 결정
"""
import logging
from typing import Dict, Any, Optional
from datetime import date
import math
import sys
from pathlib import Path
from sqlalchemy import text  # 이 줄 추가

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.trading_date import get_trading_calculator
from src.core.database import get_database_manager

logger = logging.getLogger(__name__)

# 나머지 코드는 동일...


class DataGapChecker:
    """일봉 데이터 누락 체크 및 수집 전략 결정 클래스"""

    def __init__(self):
        self.db_manager = get_database_manager()
        self.trading_calc = get_trading_calculator()

        # API 제한 상수
        self.MAX_RECORDS_PER_REQUEST = 600  # 일봉 API 최대 레코드 수

    def check_daily_data_status(self, stock_code: str) -> Dict[str, Any]:
        """
        종목의 일봉 데이터 상태 체크

        Args:
            stock_code: 종목코드 (6자리)

        Returns:
            {
                'stock_code': str,           # 종목코드
                'has_table': bool,           # 테이블 존재 여부
                'last_date': str,            # 마지막 데이터 날짜 (YYYYMMDD)
                'missing_count': int,        # 누락된 거래일 수
                'collection_method': str,    # 'api' or 'convert' or 'skip'
                'api_requests_needed': int,  # 필요한 API 요청 횟수
                'missing_dates': list        # 누락된 날짜 리스트 (최대 10개까지)
            }
        """
        try:
            logger.info(f"{stock_code}: 일봉 데이터 상태 체크 시작")

            # 1. 테이블 존재 여부 확인
            has_table = self._check_table_exists(stock_code)

            if not has_table:
                # 테이블 없음 → 신규 생성 필요
                return self._create_new_table_status(stock_code)

            # 2. 마지막 데이터 날짜 조회
            last_date = self._get_last_data_date(stock_code)

            if not last_date:
                # 테이블 있지만 데이터 없음 → 전체 수집 필요
                return self._create_empty_table_status(stock_code)

            # 3. 누락 기간 계산
            missing_count, missing_dates = self.trading_calc.count_missing_trading_days(last_date)

            # 4. 수집 방법 결정
            collection_method, api_requests = self._determine_collection_method(missing_count)

            status = {
                'stock_code': stock_code,
                'has_table': True,
                'last_date': last_date,
                'missing_count': missing_count,
                'collection_method': collection_method,
                'api_requests_needed': api_requests,
                'missing_dates': [d.strftime('%Y%m%d') for d in missing_dates[:10]]  # 최대 10개
            }

            logger.info(f"{stock_code}: 체크 완료 - {missing_count}개 누락, 방법: {collection_method}")
            return status

        except Exception as e:
            logger.error(f"{stock_code}: 데이터 상태 체크 실패 - {e}")
            return self._create_error_status(stock_code, str(e))

    def _check_table_exists(self, stock_code: str) -> bool:
        try:
            from sqlalchemy import text
            table_name = f"daily_prices_{stock_code}"

            with self.db_manager.get_session() as session:
                # SQLite에서 테이블 존재 확인
                result = session.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"),
                    {"table_name": table_name}
                ).fetchone()

                exists = result is not None
                logger.debug(f"{stock_code}: 테이블 {table_name} 존재 여부 - {exists}")
                return exists

        except Exception as e:
            logger.error(f"{stock_code}: 테이블 존재 확인 실패 - {e}")
            return False

    def _get_last_data_date(self, stock_code: str) -> Optional[str]:
        try:
            from sqlalchemy import text
            table_name = f"daily_prices_{stock_code}"

            with self.db_manager.get_session() as session:
                # 가장 최근 날짜 조회
                result = session.execute(
                    text(f"SELECT MAX(date) FROM {table_name}")
                ).fetchone()

                last_date = result[0] if result and result[0] else None
                logger.debug(f"{stock_code}: 마지막 데이터 날짜 - {last_date}")
                return last_date

        except Exception as e:
            logger.error(f"{stock_code}: 마지막 날짜 조회 실패 - {e}")
            return None

    def _determine_collection_method(self, missing_count: int) -> tuple:
        """누락 개수에 따른 수집 방법 결정"""
        if missing_count == 0:
            return 'skip', 0
        elif missing_count == 1:
            return 'convert', 0  # 기본정보에서 변환
        else:
            # API 요청 필요
            api_requests = math.ceil(missing_count / self.MAX_RECORDS_PER_REQUEST)
            return 'api', api_requests

    def _create_new_table_status(self, stock_code: str) -> Dict[str, Any]:
        """신규 테이블 생성 상태"""
        # 신규 → 5년치 데이터 수집 (약 1,250개)
        estimated_missing = 1250  # 5년치 거래일
        api_requests = math.ceil(estimated_missing / self.MAX_RECORDS_PER_REQUEST)

        return {
            'stock_code': stock_code,
            'has_table': False,
            'last_date': None,
            'missing_count': estimated_missing,
            'collection_method': 'api',
            'api_requests_needed': api_requests,
            'missing_dates': []
        }

    def _create_empty_table_status(self, stock_code: str) -> Dict[str, Any]:
        """빈 테이블 상태"""
        # 테이블 있지만 데이터 없음 → 전체 수집
        return self._create_new_table_status(stock_code)

    def _create_error_status(self, stock_code: str, error_msg: str) -> Dict[str, Any]:
        """오류 상태"""
        return {
            'stock_code': stock_code,
            'has_table': False,
            'last_date': None,
            'missing_count': 0,
            'collection_method': 'error',
            'api_requests_needed': 0,
            'missing_dates': [],
            'error': error_msg
        }

    def check_multiple_stocks_status(self, stock_codes: list) -> Dict[str, Dict[str, Any]]:
        """여러 종목의 데이터 상태 일괄 체크"""
        results = {}

        logger.info(f"다중 종목 데이터 상태 체크 시작: {len(stock_codes)}개")

        for stock_code in stock_codes:
            results[stock_code] = self.check_daily_data_status(stock_code)

        # 요약 통계
        total_api_requests = sum(status['api_requests_needed'] for status in results.values())
        methods = [status['collection_method'] for status in results.values()]

        logger.info(f"다중 체크 완료 - 총 API 요청: {total_api_requests}회")
        logger.info(f"수집 방법 분포: skip({methods.count('skip')}), "
                    f"convert({methods.count('convert')}), api({methods.count('api')})")

        return results

    def print_status_summary(self, status: Dict[str, Any]):
        """데이터 상태 요약 출력 (디버깅용)"""
        code = status['stock_code']
        method = status['collection_method']
        missing = status['missing_count']
        requests = status['api_requests_needed']

        if method == 'skip':
            print(f"✅ {code}: 데이터 완전함")
        elif method == 'convert':
            print(f"🔄 {code}: 당일 데이터 변환 필요")
        elif method == 'api':
            print(f"📥 {code}: API 수집 필요 ({missing}개, {requests}회 요청)")
        elif method == 'error':
            print(f"❌ {code}: 오류 - {status.get('error', '알 수 없음')}")

    def test_checker(self, test_codes: list = None):
        """데이터 체커 테스트"""
        if test_codes is None:
            test_codes = ["005930", "000660", "035420"]  # 기본 테스트 종목

        print("🔍 데이터 누락 체커 테스트")
        print("=" * 50)

        for code in test_codes:
            print(f"\n📊 {code} 체크 중...")
            status = self.check_daily_data_status(code)
            self.print_status_summary(status)

            # 상세 정보
            if status['missing_dates']:
                sample_dates = status['missing_dates'][:5]
                print(f"   누락 날짜 샘플: {sample_dates}")

        print(f"\n✅ 데이터 체커 테스트 완료!")


def get_data_checker() -> DataGapChecker:
    """데이터 체커 인스턴스 반환 (편의 함수)"""
    return DataGapChecker()


# 편의 함수
def check_stock_data_status(stock_code: str) -> Dict[str, Any]:
    """단일 종목 데이터 상태 체크 (편의 함수)"""
    return get_data_checker().check_daily_data_status(stock_code)


def check_multiple_stocks_data_status(stock_codes: list) -> Dict[str, Dict[str, Any]]:
    """다중 종목 데이터 상태 체크 (편의 함수)"""
    return get_data_checker().check_multiple_stocks_status(stock_codes)


# 직접 실행 시 테스트
if __name__ == "__main__":
    # 기존 수집된 종목들로 테스트
    test_codes = ["005930", "000660", "035420", "005380", "068270"]
    checker = DataGapChecker()
    checker.test_checker(test_codes)