"""
키움 API를 통한 전체 종목코드 수집 모듈
GetCodeListByMarket() 함수를 사용하여 코스피/코스닥 전체 종목 조회
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime

# 로거 설정
logger = logging.getLogger(__name__)


class StockCodeCollector:
    """키움 API를 통한 종목코드 수집 클래스"""

    def __init__(self, kiwoom_connector):
        """
        Args:
            kiwoom_connector: 키움 API 커넥터 인스턴스
        """
        self.kiwoom = kiwoom_connector

        # 시장 코드 정의
        self.market_codes = {
            'kospi': '0',  # 코스피
            'kosdaq': '10',  # 코스닥
            'etf': '8',  # ETF
            'konex': '50',  # KONEX
            'mutual': '4',  # 뮤추얼펀드
            'new_stock': '5',  # 신주인수권
            'reit': '6',  # 리츠
            'high_yield': '9',  # 하이일드펀드
            'kotc': '30'  # K-OTC
        }

    def get_market_codes(self, market: str) -> List[str]:
        """
        특정 시장의 전체 종목코드 수집

        Args:
            market: 시장 구분 ('kospi', 'kosdaq', 'etf' 등)

        Returns:
            List[str]: 종목코드 리스트
        """
        try:
            if market not in self.market_codes:
                logger.error(f"지원하지 않는 시장: {market}")
                return []

            market_code = self.market_codes[market]
            logger.info(f"{market.upper()} 종목코드 수집 시작 (시장코드: {market_code})")

            # GetCodeListByMarket 함수 호출
            codes_str = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", market_code)

            if not codes_str:
                logger.warning(f"{market.upper()} 종목코드 수집 결과 없음")
                return []

            # 세미콜론으로 구분된 문자열을 리스트로 변환
            codes = [code.strip() for code in codes_str.split(';') if code.strip()]

            logger.info(f"{market.upper()} 종목코드 수집 완료: {len(codes)}개")
            return codes

        except Exception as e:
            logger.error(f"{market.upper()} 종목코드 수집 실패: {e}")
            return []

    def get_kospi_codes(self) -> List[str]:
        """코스피 전체 종목코드 수집"""
        return self.get_market_codes('kospi')

    def get_kosdaq_codes(self) -> List[str]:
        """코스닥 전체 종목코드 수집"""
        return self.get_market_codes('kosdaq')

    def get_all_stock_codes(self) -> Dict[str, List[str]]:
        """
        코스피 + 코스닥 전체 종목코드 수집

        Returns:
            Dict: {'kospi': [...], 'kosdaq': [...], 'all': [...]}
        """
        try:
            print("🔍 전체 종목코드 수집 시작...")

            # 코스피 종목 수집
            print("📊 코스피 종목코드 수집 중...")
            kospi_codes = self.get_kospi_codes()

            # 코스닥 종목 수집
            print("📊 코스닥 종목코드 수집 중...")
            kosdaq_codes = self.get_kosdaq_codes()

            # 전체 통합
            all_codes = kospi_codes + kosdaq_codes

            # 결과 출력
            print(f"\n📋 수집 결과:")
            print(f"   📈 코스피: {len(kospi_codes):,}개")
            print(f"   📈 코스닥: {len(kosdaq_codes):,}개")
            print(f"   📊 전체: {len(all_codes):,}개")

            result = {
                'kospi': kospi_codes,
                'kosdaq': kosdaq_codes,
                'all': all_codes,
                'kospi_count': len(kospi_codes),
                'kosdaq_count': len(kosdaq_codes),
                'total_count': len(all_codes),
                'collected_at': datetime.now()
            }

            logger.info(f"전체 종목코드 수집 완료: 코스피 {len(kospi_codes)}개, 코스닥 {len(kosdaq_codes)}개")
            return result

        except Exception as e:
            logger.error(f"전체 종목코드 수집 실패: {e}")
            print(f"❌ 종목코드 수집 실패: {e}")
            return {
                'kospi': [],
                'kosdaq': [],
                'all': [],
                'kospi_count': 0,
                'kosdaq_count': 0,
                'total_count': 0,
                'error': str(e)
            }

    def validate_stock_codes(self, codes: List[str], sample_size: int = 20) -> Dict[str, any]:
        """
        종목코드 유효성 검증

        Args:
            codes: 검증할 종목코드 리스트
            sample_size: 검증할 샘플 개수

        Returns:
            Dict: 검증 결과
        """
        try:
            if not codes:
                return {'valid': False, 'reason': '종목코드 리스트가 비어있음'}

            # 샘플 추출
            sample_codes = codes[:sample_size] if len(codes) > sample_size else codes

            # 검증 통계
            valid_count = 0
            invalid_codes = []

            print(f"\n🔍 종목코드 유효성 검증 ({len(sample_codes)}개 샘플)...")

            for code in sample_codes:
                # 기본 형식 검증 (6자리 숫자)
                if len(code) == 6 and code.isdigit():
                    valid_count += 1
                else:
                    invalid_codes.append(code)

            # 결과 계산
            validity_rate = (valid_count / len(sample_codes)) * 100

            # 결과 출력
            print(f"✅ 유효한 종목코드: {valid_count}/{len(sample_codes)}개 ({validity_rate:.1f}%)")

            if invalid_codes:
                print(f"❌ 잘못된 형식: {invalid_codes[:5]}")  # 처음 5개만 표시

            result = {
                'valid': validity_rate >= 95,  # 95% 이상이면 유효로 판단
                'total_sample': len(sample_codes),
                'valid_count': valid_count,
                'invalid_count': len(invalid_codes),
                'validity_rate': validity_rate,
                'invalid_codes': invalid_codes[:10]  # 최대 10개까지만 기록
            }

            return result

        except Exception as e:
            logger.error(f"종목코드 검증 실패: {e}")
            return {'valid': False, 'error': str(e)}

    def show_sample_codes(self, codes_dict: Dict[str, List[str]], sample_size: int = 10):
        """
        수집된 종목코드 샘플 출력

        Args:
            codes_dict: get_all_stock_codes() 결과
            sample_size: 표시할 샘플 개수
        """
        try:
            kospi_codes = codes_dict.get('kospi', [])
            kosdaq_codes = codes_dict.get('kosdaq', [])

            if kospi_codes:
                print(f"\n📊 코스피 샘플 (처음 {min(sample_size, len(kospi_codes))}개):")
                for i, code in enumerate(kospi_codes[:sample_size]):
                    print(f"   {i + 1:2d}. {code}")

            if kosdaq_codes:
                print(f"\n📊 코스닥 샘플 (처음 {min(sample_size, len(kosdaq_codes))}개):")
                for i, code in enumerate(kosdaq_codes[:sample_size]):
                    print(f"   {i + 1:2d}. {code}")

        except Exception as e:
            logger.error(f"샘플 코드 출력 실패: {e}")
            print(f"❌ 샘플 출력 실패: {e}")

    def get_connection_status(self) -> Dict[str, any]:
        """키움 API 연결 상태 확인"""
        try:
            if not self.kiwoom:
                return {'connected': False, 'reason': '커넥터 없음'}

            if not hasattr(self.kiwoom, 'is_connected'):
                return {'connected': False, 'reason': '연결 상태 확인 불가'}

            return {
                'connected': self.kiwoom.is_connected,
                'account_num': getattr(self.kiwoom, 'account_num', None)
            }

        except Exception as e:
            return {'connected': False, 'error': str(e)}

    def test_api_function(self) -> bool:
        """GetCodeListByMarket 함수 테스트"""
        try:
            print("🧪 GetCodeListByMarket 함수 테스트 중...")

            # 코스피로 테스트 (가장 확실한 시장)
            test_result = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", "0")

            if test_result and len(test_result) > 0:
                print("✅ GetCodeListByMarket 함수 정상 작동")
                print(f"   반환 데이터 길이: {len(test_result)}자")
                print(f"   샘플 데이터: {test_result[:100]}...")
                return True
            else:
                print("❌ GetCodeListByMarket 함수 반환값 없음")
                return False

        except Exception as e:
            print(f"❌ GetCodeListByMarket 함수 테스트 실패: {e}")
            return False