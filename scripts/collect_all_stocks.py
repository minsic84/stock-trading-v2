#!/usr/bin/env python3
"""
전체 시장 주식 데이터 수집 스크립트
코스피 + 코스닥 전체 종목 (2000+ 종목) 데이터 수집
"""
import sys
import os
import signal
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# utils 폴더 생성 및 경로 설정
utils_dir = Path(__file__).parent / "utils"
utils_dir.mkdir(exist_ok=True)
sys.path.insert(0, str(utils_dir.parent))

from src.core.config import Config
from src.core.database import get_database_manager, get_database_service, CollectionProgress
from src.api.base_session import create_kiwoom_session
from src.market.code_collector import StockCodeCollector
from src.collectors.integrated_collector import create_integrated_collector

# utils 모듈 import (상대 경로)
sys.path.insert(0, str(Path(__file__).parent))
from utils.console_dashboard import CollectionDashboard

# 로그 디렉토리 생성
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# 로거 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'collect_all_stocks.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AllStocksCollector:
    """전체 종목 데이터 수집기"""

    def __init__(self):
        self.config = Config()
        self.session = None
        self.collector = None
        self.db_service = None
        self.dashboard = None

        # 종료 신호 처리
        self.is_interrupted = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # 통계
        self.total_stocks = 0
        self.completed_stocks = 0
        self.failed_stocks = 0

    def _signal_handler(self, signum, frame):
        """안전한 종료 처리"""
        print(f"\n⚠️ 종료 신호 감지 (Signal: {signum})")
        print("🔄 현재 종목 처리 완료 후 안전하게 종료합니다...")
        self.is_interrupted = True

    def setup(self) -> bool:
        """초기 설정"""
        try:
            print("🚀 전체 종목 데이터 수집 시스템 시작")
            print("=" * 60)

            # 로그 디렉토리 생성
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)

            # 키움 세션 생성
            print("🔌 키움 API 세션 준비 중...")
            self.session = create_kiwoom_session(auto_login=True, show_progress=True)

            if not self.session or not self.session.is_ready():
                print("❌ 키움 세션 준비 실패")
                return False

            print("✅ 키움 세션 준비 완료")

            # 데이터베이스 서비스 초기화
            print("🗄️ 데이터베이스 서비스 초기화 중...")
            db_manager = get_database_manager()
            db_manager.create_tables()  # CollectionProgress 테이블 포함

            self.db_service = get_database_service()
            print("✅ 데이터베이스 서비스 준비 완료")

            # 통합 수집기 생성
            print("🔧 통합 수집기 준비 중...")
            self.collector = create_integrated_collector(self.session)
            print("✅ 통합 수집기 준비 완료")

            return True

        except Exception as e:
            print(f"❌ 초기 설정 실패: {e}")
            logger.error(f"초기 설정 실패: {e}")
            return False

    def get_all_stock_codes(self) -> List[Tuple[str, str]]:
        """전체 종목코드 및 종목명 수집"""
        try:
            print("\n📊 전체 종목코드 수집 중...")

            connector = self.session.get_connector()
            code_collector = StockCodeCollector(connector)

            # 전체 종목코드 수집
            codes_result = code_collector.get_all_stock_codes()

            if codes_result.get('error'):
                print(f"❌ 종목코드 수집 실패: {codes_result['error']}")
                return []

            all_codes = codes_result['all']
            print(f"✅ 전체 종목코드 수집 완료: {len(all_codes):,}개")
            print(f"   📈 코스피: {codes_result['kospi_count']:,}개")
            print(f"   📈 코스닥: {codes_result['kosdaq_count']:,}개")

            # 종목명은 수집 과정에서 얻어지므로 일단 빈 문자열로 초기화
            codes_with_names = [(code, '') for code in all_codes]

            return codes_with_names

        except Exception as e:
            print(f"❌ 종목코드 수집 실패: {e}")
            logger.error(f"종목코드 수집 실패: {e}")
            return []

    def initialize_progress_tracking(self, stock_codes_with_names: List[Tuple[str, str]]) -> bool:
        """진행상황 추적 초기화"""
        try:
            print("\n🗂️ 진행상황 추적 초기화 중...")

            # 기존 재시작인지 확인
            existing_progress = self.db_service.get_collection_status_summary()

            if existing_progress.get('total_stocks', 0) > 0:
                print(f"📋 기존 진행상황 발견!")
                print(f"   📊 총 종목: {existing_progress['total_stocks']:,}개")
                print(f"   ✅ 완료: {existing_progress.get('completed', 0):,}개")
                print(f"   📈 성공률: {existing_progress.get('success_rate', 0):.1f}%")

                response = input("\n기존 진행상황을 이어서 할까요? (y/N): ")
                if response.lower() == 'y':
                    print("🔄 기존 진행상황에서 이어서 시작합니다.")
                    return True
                else:
                    print("🗑️ 기존 진행상황을 초기화하고 새로 시작합니다.")

            # 새로 초기화
            if self.db_service.initialize_collection_progress(stock_codes_with_names):
                print(f"✅ 진행상황 추적 초기화 완료: {len(stock_codes_with_names):,}개 종목")
                return True
            else:
                print("❌ 진행상황 추적 초기화 실패")
                return False

        except Exception as e:
            print(f"❌ 진행상황 추적 초기화 실패: {e}")
            logger.error(f"진행상황 추적 초기화 실패: {e}")
            return False

    def collect_all_stocks(self) -> bool:
        """전체 종목 데이터 수집 실행"""
        try:
            # 진행상황 확인
            summary = self.db_service.get_collection_status_summary()
            self.total_stocks = summary.get('total_stocks', 0)
            self.completed_stocks = summary.get('completed', 0)

            if self.total_stocks == 0:
                print("❌ 수집할 종목이 없습니다.")
                return False

            # 대시보드 시작
            self.dashboard = CollectionDashboard(self.total_stocks)
            self.dashboard.update_completed(self.completed_stocks)
            self.dashboard.start()

            print(f"\n🚀 전체 종목 데이터 수집 시작!")
            print(f"📊 총 {self.total_stocks:,}개 종목 (이미 완료: {self.completed_stocks:,}개)")

            # 미완료 종목 가져오기
            pending_stocks = self.db_service.get_pending_stocks()
            print(f"🔄 수집 대상: {len(pending_stocks):,}개 종목")

            # 종목별 수집 실행
            for idx, stock_code in enumerate(pending_stocks):
                if self.is_interrupted:
                    print(f"\n⚠️ 사용자 요청으로 수집을 중단합니다.")
                    break

                self._collect_single_stock(stock_code, idx + 1, len(pending_stocks))

                # API 요청 제한 준수
                if idx < len(pending_stocks) - 1:
                    time.sleep(self.config.api_request_delay_ms / 1000)

            print(f"\n✅ 1차 수집 완료!")

            # 실패한 종목 재시도
            if not self.is_interrupted:
                self._retry_failed_stocks()

            # 최종 리포트
            self._show_final_report()

            return True

        except Exception as e:
            print(f"❌ 전체 수집 실패: {e}")
            logger.error(f"전체 수집 실패: {e}")
            return False
        finally:
            if self.dashboard:
                self.dashboard.stop()

    def _collect_single_stock(self, stock_code: str, current_idx: int, total_count: int):
        """단일 종목 수집"""
        try:
            # 진행상황 업데이트 (처리 시작)
            self.db_service.update_collection_progress(stock_code, 'processing')

            # 대시보드 업데이트 (종목명은 수집 후 업데이트)
            self.dashboard.update_current_stock(stock_code, "수집 중...")

            # 실제 수집 실행
            result = self.collector.collect_stock_with_daily_data(stock_code)

            # IntegratedStockCollector 결과 구조에 맞춤
            if result.get('stock_info_success', False) and result.get('daily_data_success', False):
                # 완전 성공 처리
                data_count = result.get('daily_records_collected', 0)

                # 종목명은 DB에서 조회
                from src.core.database import get_database_service
                db_service_temp = get_database_service()
                stock_info = db_service_temp.get_stock_info(stock_code)
                stock_name = stock_info.get('name', '') if stock_info else ''

                self.db_service.update_collection_progress(
                    stock_code, 'completed',
                    stock_name=stock_name,
                    data_count=data_count
                )

                self.dashboard.increment_completed()

                logger.info(f"✅ {stock_code} ({stock_name}) 수집 완료: {data_count}개 데이터")

            elif result.get('stock_info_success', False) or result.get('daily_data_success', False):
                # 부분 성공 처리 (완료로 간주)
                data_count = result.get('daily_records_collected', 0)

                self.db_service.update_collection_progress(
                    stock_code, 'completed',
                    stock_name='',
                    data_count=data_count
                )

                self.dashboard.increment_completed()

                logger.info(f"⚠️ {stock_code} 부분 수집 완료: {data_count}개 데이터")

            else:
                # 실패 처리
                error_msg = result.get('error', '알 수 없는 오류')
                self.db_service.update_collection_progress(
                    stock_code, 'failed',
                    error_message=error_msg
                )

                self.dashboard.increment_failed()

                logger.warning(f"❌ {stock_code} 수집 실패: {error_msg}")

        except Exception as e:
            # 예외 처리
            self.db_service.update_collection_progress(
                stock_code, 'failed',
                error_message=str(e)
            )

            self.dashboard.increment_failed()

            logger.error(f"❌ {stock_code} 수집 중 예외: {e}")

    def _retry_failed_stocks(self):
        """실패한 종목 재시도"""
        max_attempts = 3

        for retry_round in range(1, max_attempts + 1):
            failed_stocks = self.db_service.get_failed_stocks(max_attempts)

            if not failed_stocks:
                print(f"🎉 재시도할 종목이 없습니다!")
                break

            print(f"\n🔄 {retry_round}차 재시도 시작 ({len(failed_stocks)}개 종목)")

            if self.dashboard:
                self.dashboard.show_retry_info(failed_stocks, retry_round)

            for idx, stock_info in enumerate(failed_stocks):
                if self.is_interrupted:
                    break

                stock_code = stock_info['stock_code']
                self._collect_single_stock(stock_code, idx + 1, len(failed_stocks))

                # API 요청 제한 준수
                time.sleep(self.config.api_request_delay_ms / 1000)

            if self.is_interrupted:
                break

        # 최대 시도 횟수 초과한 종목들 스킵 처리
        final_failed = self.db_service.get_failed_stocks(max_attempts)
        if final_failed:
            print(f"\n⚠️ {len(final_failed)}개 종목이 {max_attempts}회 시도 후에도 실패하여 건너뜁니다.")

    def _show_final_report(self):
        """최종 리포트 표시"""
        try:
            summary = self.db_service.get_collection_status_summary()

            if self.dashboard:
                self.dashboard.show_final_report(summary)
            else:
                print("\n" + "=" * 60)
                print("🎉 전체 수집 완료 리포트")
                print("=" * 60)
                print(f"📊 총 종목: {summary.get('total_stocks', 0):,}개")
                print(f"✅ 성공: {summary.get('completed', 0):,}개")
                print(f"❌ 실패: {summary.get('status_breakdown', {}).get('failed', 0):,}개")
                print(f"📈 성공률: {summary.get('success_rate', 0):.1f}%")

        except Exception as e:
            print(f"❌ 최종 리포트 생성 실패: {e}")
            logger.error(f"최종 리포트 생성 실패: {e}")


def main():
    """메인 함수"""
    try:
        collector = AllStocksCollector()

        # 1단계: 초기 설정
        if not collector.setup():
            print("❌ 초기 설정 실패")
            return False

        # 2단계: 종목코드 수집
        stock_codes = collector.get_all_stock_codes()
        if not stock_codes:
            print("❌ 종목코드 수집 실패")
            return False

        # 3단계: 진행상황 추적 초기화
        if not collector.initialize_progress_tracking(stock_codes):
            print("❌ 진행상황 추적 초기화 실패")
            return False

        # 4단계: 전체 수집 실행
        success = collector.collect_all_stocks()

        if success:
            print("\n🎉 전체 종목 데이터 수집이 완료되었습니다!")
            return True
        else:
            print("\n⚠️ 수집이 완료되지 않았습니다.")
            return False

    except KeyboardInterrupt:
        print("\n\n👋 사용자가 수집을 중단했습니다.")
        return False
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        logger.error(f"메인 함수 실행 중 오류: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)