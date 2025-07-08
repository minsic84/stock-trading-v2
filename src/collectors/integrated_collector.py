"""
주식 기본정보 + 일봉 데이터 통합 수집기
- 기본정보 수집 → 일봉 데이터 상태 체크 → 적절한 방법으로 일봉 수집
- 5년치 데이터 수집 (약 1,250개 거래일)
- API 요청 최적화 및 오류 처리
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import time
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.api.base_session import KiwoomSession
from src.collectors.stock_info import StockInfoCollector
from src.collectors.daily_price import DailyPriceCollector
from src.utils.data_checker import get_data_checker
from src.utils.trading_date import get_market_today
from src.core.config import Config

logger = logging.getLogger(__name__)


class IntegratedStockCollector:
    """주식 기본정보 + 일봉 데이터 통합 수집기"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()

        # 개별 수집기들
        self.stock_info_collector = StockInfoCollector(session, config)
        self.daily_price_collector = DailyPriceCollector(config)
        self.data_checker = get_data_checker()

        # 통계
        self.total_collected = 0
        self.total_updated = 0
        self.total_daily_collected = 0
        self.total_errors = 0

        logger.info("통합 수집기 초기화 완료")

    def collect_stock_with_daily_data(self, stock_code: str) -> Dict[str, Any]:
        """
        단일 종목 기본정보 + 일봉 데이터 통합 수집

        Args:
            stock_code: 종목코드

        Returns:
            {
                'stock_code': str,
                'stock_info_success': bool,
                'daily_data_success': bool,
                'collection_method': str,
                'daily_records_collected': int,
                'api_requests_made': int,
                'elapsed_time': float
            }
        """
        start_time = datetime.now()

        print(f"\n{'=' * 50}")
        print(f"📊 {stock_code} 통합 수집 시작")
        print(f"{'=' * 50}")

        result = {
            'stock_code': stock_code,
            'stock_info_success': False,
            'daily_data_success': False,
            'collection_method': 'none',
            'daily_records_collected': 0,
            'api_requests_made': 0,
            'elapsed_time': 0.0,
            'error': None
        }

        try:
            # 1단계: 기본정보 수집
            print(f"📈 1단계: {stock_code} 기본정보 수집 중...")
            stock_success, is_new = self.stock_info_collector.collect_single_stock_info(stock_code)

            if stock_success:
                result['stock_info_success'] = True
                status = "신규 추가" if is_new else "업데이트"
                print(f"✅ 기본정보 수집 성공 ({status})")

                if is_new:
                    self.total_collected += 1
                else:
                    self.total_updated += 1
            else:
                print(f"❌ 기본정보 수집 실패")
                result['error'] = '기본정보 수집 실패'
                self.total_errors += 1
                return result

            # 2단계: 일봉 데이터 상태 체크
            print(f"🔍 2단계: {stock_code} 일봉 데이터 상태 체크 중...")
            data_status = self.data_checker.check_daily_data_status(stock_code)

            collection_method = data_status['collection_method']
            missing_count = data_status['missing_count']
            api_requests_needed = data_status['api_requests_needed']

            result['collection_method'] = collection_method

            print(f"📋 상태: {collection_method}, 누락: {missing_count}개, API 요청: {api_requests_needed}회")

            # 3단계: 일봉 데이터 수집
            print(f"📊 3단계: {stock_code} 일봉 데이터 수집 중...")

            if collection_method == 'skip':
                print(f"✅ 일봉 데이터 완전함 - 수집 건너뛰기")
                result['daily_data_success'] = True

            elif collection_method == 'convert':
                # 기본정보에서 당일 데이터 변환
                print(f"🔄 당일 데이터 변환 중...")
                convert_success = self._convert_today_data(stock_code)
                result['daily_data_success'] = convert_success
                if convert_success:
                    result['daily_records_collected'] = 1
                    print(f"✅ 당일 데이터 변환 완료")
                else:
                    print(f"❌ 당일 데이터 변환 실패")

            elif collection_method == 'api':
                # API를 통한 일봉 데이터 수집
                print(f"📥 API 일봉 데이터 수집 중... ({api_requests_needed}회 요청 예정)")

                # 키움 API 연결 확인
                if not self._ensure_kiwoom_connection():
                    print(f"❌ 키움 API 연결 실패")
                    result['error'] = '키움 API 연결 실패'
                    self.total_errors += 1
                    return result

                # 일봉 데이터 수집 실행
                daily_success = self.daily_price_collector.collect_single_stock(
                    stock_code, update_existing=True
                )

                result['daily_data_success'] = daily_success
                result['api_requests_made'] = api_requests_needed

                if daily_success:
                    result['daily_records_collected'] = missing_count
                    self.total_daily_collected += missing_count
                    print(f"✅ 일봉 데이터 수집 성공 ({missing_count}개)")
                else:
                    print(f"❌ 일봉 데이터 수집 실패")
                    self.total_errors += 1

            else:
                print(f"❌ 알 수 없는 수집 방법: {collection_method}")
                result['error'] = f'알 수 없는 수집 방법: {collection_method}'
                self.total_errors += 1

            # 최종 결과
            end_time = datetime.now()
            result['elapsed_time'] = (end_time - start_time).total_seconds()

            success_status = "✅ 성공" if (result['stock_info_success'] and result['daily_data_success']) else "⚠️ 부분 성공"
            print(f"\n📋 {stock_code} 수집 완료: {success_status}")
            print(f"⏱️ 소요시간: {result['elapsed_time']:.1f}초")

            return result

        except Exception as e:
            print(f"❌ {stock_code} 통합 수집 중 예외 발생: {e}")
            logger.error(f"{stock_code} 통합 수집 실패: {e}")
            result['error'] = str(e)
            result['elapsed_time'] = (datetime.now() - start_time).total_seconds()
            self.total_errors += 1
            return result

    def collect_multiple_stocks_integrated(self, stock_codes: List[str],
                                           test_mode: bool = True) -> Dict[str, Any]:
        """
        다중 종목 통합 수집 (기본정보 + 일봉)

        Args:
            stock_codes: 수집할 종목코드 리스트
            test_mode: 테스트 모드 (처음 5개만)

        Returns:
            수집 결과 딕셔너리
        """
        start_time = datetime.now()

        if test_mode:
            stock_codes = stock_codes[:5]

        print(f"🚀 통합 수집 시작")
        print(f"📊 대상 종목: {len(stock_codes)}개")
        print(f"🎯 목표: 기본정보 + 5년치 일봉 데이터")

        # 통계 초기화
        self.total_collected = 0
        self.total_updated = 0
        self.total_daily_collected = 0
        self.total_errors = 0

        results = {
            'success': [],
            'partial_success': [],
            'failed': [],
            'stock_details': {},
            'summary': {}
        }

        # 사전 체크 (예상 API 요청 수 계산)
        print(f"\n🔍 사전 체크: 예상 작업량 계산 중...")
        total_api_requests = 0
        for code in stock_codes:
            status = self.data_checker.check_daily_data_status(code)
            total_api_requests += status['api_requests_needed']

        estimated_time = total_api_requests * (self.config.api_request_delay_ms / 1000) / 60
        print(f"📊 예상 API 요청: {total_api_requests}회")
        print(f"⏱️ 예상 소요시간: {estimated_time:.1f}분")

        # 실제 수집 시작
        for idx, stock_code in enumerate(stock_codes):
            print(f"\n{'=' * 60}")
            print(f"진행률: {idx + 1}/{len(stock_codes)} - {stock_code}")
            print(f"{'=' * 60}")

            # 개별 종목 수집
            stock_result = self.collect_stock_with_daily_data(stock_code)
            results['stock_details'][stock_code] = stock_result

            # 결과 분류
            if stock_result['stock_info_success'] and stock_result['daily_data_success']:
                results['success'].append(stock_code)
            elif stock_result['stock_info_success'] or stock_result['daily_data_success']:
                results['partial_success'].append(stock_code)
            else:
                results['failed'].append(stock_code)

            # API 요청 간 딜레이 (마지막 종목 제외)
            if idx < len(stock_codes) - 1:
                delay_seconds = self.config.api_request_delay_ms / 1000
                print(f"⏱️ API 제한 준수를 위한 대기: {delay_seconds}초")
                time.sleep(delay_seconds)

        # 최종 통계
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()

        results['summary'] = {
            'total_stocks': len(stock_codes),
            'success_count': len(results['success']),
            'partial_success_count': len(results['partial_success']),
            'failed_count': len(results['failed']),
            'total_stock_info_collected': self.total_collected,
            'total_stock_info_updated': self.total_updated,
            'total_daily_records_collected': self.total_daily_collected,
            'total_errors': self.total_errors,
            'elapsed_time': elapsed_time,
            'start_time': start_time,
            'end_time': end_time
        }

        # 결과 출력
        self._print_final_summary(results)

        return results

    def _ensure_kiwoom_connection(self) -> bool:
        """키움 API 연결 확인 및 재연결"""
        try:
            if not self.session or not self.session.is_ready():
                print("⚠️ 키움 세션이 준비되지 않음")
                return False

            # daily_price_collector에 연결 설정
            if not self.daily_price_collector.kiwoom:
                connector = self.session.get_connector()
                self.daily_price_collector.kiwoom = connector

            return True

        except Exception as e:
            logger.error(f"키움 연결 확인 실패: {e}")
            return False

    def _convert_today_data(self, stock_code: str) -> bool:
        """기본정보에서 당일 일봉 데이터 변환"""
        try:
            from src.utils.data_converter import get_data_converter

            converter = get_data_converter()
            return converter.convert_stock_info_to_daily(stock_code)

        except Exception as e:
            logger.error(f"{stock_code} 당일 데이터 변환 실패: {e}")
            return False

    def _print_final_summary(self, results: Dict[str, Any]):
        """최종 결과 요약 출력"""
        summary = results['summary']

        print(f"\n{'=' * 60}")
        print(f"📋 통합 수집 최종 결과")
        print(f"{'=' * 60}")

        print(f"📊 전체 종목: {summary['total_stocks']}개")
        print(f"   ✅ 완전 성공: {summary['success_count']}개")
        print(f"   ⚠️ 부분 성공: {summary['partial_success_count']}개")
        print(f"   ❌ 실패: {summary['failed_count']}개")

        print(f"\n📈 기본정보 수집:")
        print(f"   📥 신규 수집: {summary['total_stock_info_collected']}개")
        print(f"   🔄 업데이트: {summary['total_stock_info_updated']}개")

        print(f"\n📊 일봉 데이터:")
        print(f"   📥 수집 레코드: {summary['total_daily_records_collected']:,}개")

        print(f"\n⏱️ 소요시간: {summary['elapsed_time']:.1f}초 ({summary['elapsed_time'] / 60:.1f}분)")

        if results['failed']:
            print(f"\n❌ 실패 종목: {results['failed']}")


def create_integrated_collector(session: KiwoomSession,
                                config: Optional[Config] = None) -> IntegratedStockCollector:
    """통합 수집기 생성 (편의 함수)"""
    return IntegratedStockCollector(session, config)


# 편의 함수
def collect_stocks_integrated(session: KiwoomSession, stock_codes: List[str],
                              test_mode: bool = True) -> Dict[str, Any]:
    """통합 수집 실행 (편의 함수)"""
    collector = create_integrated_collector(session)
    return collector.collect_multiple_stocks_integrated(stock_codes, test_mode)
