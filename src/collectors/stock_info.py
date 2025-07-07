"""
주식 기본정보 수집기 모듈
키움 API OPT10001(주식기본정보요청)을 사용하여 종목 정보를 수집하고 데이터베이스에 저장
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import time

from ..core.config import Config
from ..core.database import DatabaseService, get_database_manager
from ..api.base_session import KiwoomSession
from ..api.tr_codes import get_tr_info, create_opt10001_input, validate_input_data

# 로거 설정
logger = logging.getLogger(__name__)


class StockInfoCollector:
    """주식 기본정보 수집기 클래스 (OPT10001 사용)"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()
        self.db_service = None

        # 수집 상태
        self.collected_count = 0
        self.updated_count = 0
        self.skipped_count = 0
        self.error_count = 0

        # TR 정보 (tr_codes.py에서 가져오기)
        self.tr_info = get_tr_info('opt10001')
        self.TR_CODE = self.tr_info['code']
        self.RQ_NAME = self.tr_info['name']

        self._setup()

    def _setup(self):
        """초기화 설정"""
        try:
            # 데이터베이스 서비스 초기화
            db_manager = get_database_manager()
            self.db_service = DatabaseService(db_manager)

            logger.info("주식정보 수집기 초기화 완료")
        except Exception as e:
            logger.error(f"주식정보 수집기 초기화 실패: {e}")
            raise

    def collect_and_update_stocks(self, stock_codes: List[str],
                                  test_mode: bool = True,
                                  always_update: bool = True) -> Dict[str, Any]:
        """
        주식 코드 리스트를 순회하며 데이터 수집 (실시간 업데이트 모드)

        Args:
            stock_codes: 수집할 종목코드 리스트
            test_mode: 테스트 모드 (처음 5개만 수집)
            always_update: True면 항상 최신 데이터로 업데이트
        """
        try:
            print(f"🚀 주식 기본정보 수집 시작 (실시간 업데이트 모드)")
            print(f"📊 대상 종목: {len(stock_codes)}개")
            print(f"🔄 모든 종목을 최신 데이터로 업데이트합니다")

            if test_mode:
                stock_codes = stock_codes[:5]  # 테스트용 5개만
                print(f"🧪 테스트 모드: {len(stock_codes)}개 종목만 수집")

            # 통계 초기화
            self.collected_count = 0
            self.updated_count = 0
            self.skipped_count = 0
            self.error_count = 0

            results = {
                'success': [],
                'updated': [],
                'skipped': [],
                'failed': [],
                'total_collected': 0,
                'total_updated': 0,
                'total_skipped': 0,
                'total_errors': 0
            }

            start_time = datetime.now()

            for idx, stock_code in enumerate(stock_codes):
                try:
                    print(f"\n📈 진행률: {idx + 1}/{len(stock_codes)} - {stock_code}")

                    # 항상 최신 데이터로 업데이트
                    print(f"🔄 {stock_code}: 최신 데이터 수집 중...")

                    # 기존 데이터 확인 (신규/업데이트 구분용)
                    existing_data = self.db_service.get_stock_info(stock_code)
                    is_existing = len(existing_data) > 0

                    # 개별 종목 정보 수집
                    success, is_new = self.collect_single_stock_info(stock_code)

                    if success:
                        if is_new:
                            results['success'].append(stock_code)
                            self.collected_count += 1
                            print(f"✅ {stock_code}: 신규 데이터 수집 완료")
                        else:
                            results['updated'].append(stock_code)
                            self.updated_count += 1
                            print(f"🔄 {stock_code}: 최신 데이터로 업데이트 완료")
                    else:
                        results['failed'].append(stock_code)
                        self.error_count += 1
                        print(f"❌ {stock_code}: 수집 실패")

                    # API 요청 제한 준수
                    if idx < len(stock_codes) - 1:  # 마지막이 아닌 경우
                        delay_ms = self.tr_info.get('delay_ms', 3600)
                        time.sleep(delay_ms / 1000)

                except Exception as e:
                    print(f"❌ {stock_code} 수집 중 예외 발생: {e}")
                    results['failed'].append(stock_code)
                    self.error_count += 1

            # 최종 통계
            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()

            results.update({
                'total_collected': self.collected_count,
                'total_updated': self.updated_count,
                'total_skipped': self.skipped_count,  # 항상 0이 됨
                'total_errors': self.error_count,
                'elapsed_time': elapsed_time,
                'start_time': start_time,
                'end_time': end_time
            })

            # 결과 출력
            print(f"\n📋 실시간 수집 완료 결과:")
            print(f"   ✅ 신규 수집: {results['total_collected']}개")
            print(f"   🔄 업데이트: {results['total_updated']}개")
            print(f"   ❌ 실패: {results['total_errors']}개")
            print(f"   ⏱️ 소요시간: {elapsed_time:.1f}초")

            logger.info(f"실시간 주식정보 수집 완료: 신규 {results['total_collected']}개, "
                        f"업데이트 {results['total_updated']}개, "
                        f"실패 {results['total_errors']}개")

            return results

        except Exception as e:
            logger.error(f"실시간 주식정보 수집 중 치명적 오류: {e}")
            return {'error': str(e)}

    def _mark_stock_inactive(self, stock_code: str):
        """데이터 수집 실패 종목을 비활성화 처리"""
        try:
            with self.db_service.db_manager.get_session() as session:
                from ..core.database import Stock

                stock = session.query(Stock).filter(Stock.code == stock_code).first()
                if stock:
                    stock.is_active = 0  # 비활성화
                    stock.updated_at = datetime.now()
                    session.commit()

                    print(f"📝 {stock_code}: 비활성 종목으로 표시")
                    logger.info(f"{stock_code} 비활성 종목으로 처리")

        except Exception as e:
            logger.error(f"{stock_code} 비활성화 처리 실패: {e}")

    def is_update_needed(self, stock_code: str, force_daily: bool = True) -> bool:
        """주식 정보 업데이트 필요 여부 확인 (실시간 업데이트 모드)"""
        try:
            # 실시간 모드: 항상 업데이트 필요
            return True
        except Exception as e:
            logger.error(f"업데이트 필요 여부 확인 실패 {stock_code}: {e}")
            return True  # 오류 시 수집 수행

    def is_today_collected(self, stock_code: str) -> bool:
        """오늘 데이터가 이미 수집되었는지 확인"""
        try:
            return self.db_service.is_today_data_collected(stock_code)
        except Exception as e:
            logger.error(f"오늘 데이터 확인 실패 {stock_code}: {e}")
            return False

    def collect_single_stock_info(self, stock_code: str) -> Tuple[bool, bool]:
        """단일 종목 기본정보 수집 (OPT10001)"""
        try:
            print(f"🔍 {stock_code} 수집 시작...")

            if not self.session or not self.session.is_ready():
                print(f"❌ {stock_code}: 키움 세션이 준비되지 않음")
                logger.error("키움 세션이 준비되지 않음")
                return False, False

            # 입력 데이터 생성
            input_data = create_opt10001_input(stock_code)
            print(f"🔧 {stock_code} 입력 데이터: {input_data}")

            # 입력 데이터 유효성 검증
            if not validate_input_data('opt10001', input_data):
                print(f"❌ {stock_code}: 입력 데이터 유효성 검증 실패")
                logger.error(f"{stock_code} 입력 데이터 유효성 검증 실패")
                return False, False

            print(f"✅ {stock_code} 입력 데이터 검증 통과")

            # TR 요청
            connector = self.session.get_connector()
            print(f"🔄 {stock_code} TR 요청 중... (TR: {self.TR_CODE}, 요청명: {self.RQ_NAME})")

            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                screen_no="9001"
            )

            print(f"📥 {stock_code} TR 응답 받음: {response is not None}")

            if not response:
                print(f"❌ {stock_code}: TR 요청 실패 - 응답이 None")
                logger.error(f"{stock_code} TR 요청 실패")
                return False, False

            print(f"🔍 {stock_code} 응답 내용: {response}")

            # 데이터 파싱
            print(f"🔧 {stock_code} 데이터 파싱 시작...")
            stock_data = self._parse_stock_info(response, stock_code)

            if not stock_data:
                print(f"❌ {stock_code}: 데이터 파싱 실패")
                logger.error(f"{stock_code} 데이터 파싱 실패")
                return False, False

            print(f"✅ {stock_code} 파싱 완료: {stock_data}")

            # 기존 데이터 확인
            existing_data = self.db_service.get_stock_info(stock_code)
            is_new = len(existing_data) == 0
            print(f"🔍 {stock_code} 기존 데이터: {'없음 (신규)' if is_new else '있음 (업데이트)'}")

            # 데이터베이스 저장
            print(f"💾 {stock_code} 데이터베이스 저장 중...")
            success = self.db_service.add_or_update_stock_info(stock_code, stock_data)

            if success:
                print(f"✅ {stock_code} 저장 성공!")
                logger.info(f"{stock_code} 주식정보 {'추가' if is_new else '업데이트'} 완료")
                return True, is_new
            else:
                print(f"❌ {stock_code}: 데이터베이스 저장 실패")
                logger.error(f"{stock_code} 데이터베이스 저장 실패")
                return False, False

        except Exception as e:
            print(f"❌ {stock_code} 수집 중 예외 발생: {e}")
            import traceback
            print(f"스택 트레이스: {traceback.format_exc()}")
            logger.error(f"{stock_code} 주식정보 수집 중 오류: {e}")
            return False, False

    def _parse_stock_info(self, response: Dict[str, Any], stock_code: str) -> Optional[Dict[str, Any]]:
        """OPT10001 응답 데이터 파싱"""
        try:
            print(f"🔧 {stock_code} 파싱 시작 - 응답 TR 코드: {response.get('tr_code')}")

            if response.get('tr_code') != self.TR_CODE:
                print(f"❌ {stock_code}: TR 코드 불일치 - 예상: {self.TR_CODE}, 실제: {response.get('tr_code')}")
                logger.error(f"잘못된 TR 코드: {response.get('tr_code')}")
                return None

            # connector에서 이미 파싱된 데이터 사용
            data_info = response.get('data', {})
            print(f"🔍 {stock_code} 데이터 정보: parsed={data_info.get('parsed')}, count={data_info.get('repeat_count')}")

            if not data_info.get('parsed', False):
                print(f"❌ {stock_code}: 데이터가 파싱되지 않음 - {data_info}")
                logger.error(f"{stock_code} 데이터가 파싱되지 않음")
                return None

            raw_data = data_info.get('raw_data', [])
            print(f"🔍 {stock_code} 원시 데이터 개수: {len(raw_data)}")

            if not raw_data:
                print(f"❌ {stock_code}: 원시 데이터가 없음")
                logger.error(f"{stock_code} 원시 데이터가 없음")
                return None

            # 첫 번째 레코드 사용 (OPT10001은 단일 레코드)
            row_data = raw_data[0]
            print(f"🔍 {stock_code} 첫 번째 레코드: {row_data}")

            # connector에서 이미 파싱된 데이터 사용
            data_info = response.get('data', {})
            if not data_info.get('parsed', False):
                logger.error(f"{stock_code} 데이터가 파싱되지 않음")
                return None

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                logger.error(f"{stock_code} 원시 데이터가 없음")
                return None

            # 첫 번째 레코드 사용 (OPT10001은 단일 레코드)
            row_data = raw_data[0]

            # 필드 매핑 및 변환
            stock_data = {
                'name': self._clean_string(row_data.get('종목명', '')),
                'current_price': self._parse_int(row_data.get('현재가', 0)),
                'prev_day_diff': self._parse_int(row_data.get('전일대비', 0)),
                'change_rate': self._parse_rate(row_data.get('등락률', 0)),
                'volume': self._parse_int(row_data.get('거래량', 0)),
                'open_price': self._parse_int(row_data.get('시가', 0)),
                'high_price': self._parse_int(row_data.get('고가', 0)),
                'low_price': self._parse_int(row_data.get('저가', 0)),
                'upper_limit': self._parse_int(row_data.get('상한가', 0)),
                'lower_limit': self._parse_int(row_data.get('하한가', 0)),
                'market_cap': self._parse_int(row_data.get('시가총액', 0)),
                'market_cap_size': self._clean_string(row_data.get('시가총액규모', '')),
                'listed_shares': self._parse_int(row_data.get('상장주수', 0)),
                'per_ratio': self._parse_rate(row_data.get('PER', 0)),
                'pbr_ratio': self._parse_rate(row_data.get('PBR', 0)),
            }

            # 시장 구분 추가 (코스피/코스닥 구분)
            if stock_code.startswith(('00', '01', '02', '03', '04', '05')):
                stock_data['market'] = 'KOSPI'
            else:
                stock_data['market'] = 'KOSDAQ'

            return stock_data

        except Exception as e:
            logger.error(f"{stock_code} 데이터 파싱 중 오류: {e}")
            return None

    def _clean_string(self, value: str) -> str:
        """문자열 정리 (공백, 특수문자 제거)"""
        if not value:
            return ""
        return str(value).strip()

    def _parse_int(self, value) -> int:
        """정수 변환 (부호, 콤마 제거)"""
        try:
            if not value:
                return 0

            # 문자열 정리
            clean_value = str(value).replace('+', '').replace('-', '').replace(',', '').strip()

            if not clean_value:
                return 0

            return int(clean_value)
        except (ValueError, TypeError):
            return 0

    def _parse_rate(self, value) -> int:
        """비율 변환 (소수점 2자리 * 100으로 정수화)"""
        try:
            if not value:
                return 0

            # 문자열 정리
            clean_value = str(value).replace('+', '').replace('-', '').replace('%', '').strip()

            if not clean_value:
                return 0

            float_value = float(clean_value)
            return int(float_value * 100)  # 소수점 2자리를 정수로
        except (ValueError, TypeError):
            return 0

    def get_collection_status(self) -> Dict[str, Any]:
        """수집 상태 정보 반환"""
        return {
            'collected_count': self.collected_count,
            'updated_count': self.updated_count,
            'skipped_count': self.skipped_count,
            'error_count': self.error_count,
            'session_ready': self.session.is_ready() if self.session else False,
            'db_connected': self.db_service is not None,
            'tr_code': self.TR_CODE,
            'tr_name': self.RQ_NAME
        }


# 편의 함수들
def collect_stock_info_batch(session: KiwoomSession, stock_codes: List[str],
                            test_mode: bool = True, config: Optional[Config] = None) -> Dict[str, Any]:
    """배치 주식정보 수집 (편의 함수) - 실시간 업데이트 모드"""
    collector = StockInfoCollector(session, config)
    return collector.collect_and_update_stocks(stock_codes, test_mode, always_update=True)


def collect_single_stock_info_simple(session: KiwoomSession, stock_code: str,
                                     config: Optional[Config] = None) -> bool:
    """단일 종목 주식정보 수집 (편의 함수)"""
    collector = StockInfoCollector(session, config)
    success, _ = collector.collect_single_stock_info(stock_code)
    return success