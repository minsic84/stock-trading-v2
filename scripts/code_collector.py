#!/usr/bin/env python3
"""
종목코드 수집 테스트 스크립트
GetCodeListByMarket() 함수를 사용한 전체 종목코드 수집 테스트
공통 세션 모듈 사용으로 개선
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.api.base_session import create_kiwoom_session
from src.market.code_collector import StockCodeCollector


def setup_kiwoom_session():
    """키움 세션 준비 (공통 모듈 사용)"""
    print("🔌 키움 세션 준비")
    print("=" * 40)

    try:
        session = create_kiwoom_session(auto_login=True, show_progress=True)

        if session and session.is_ready():
            print("✅ 키움 세션 준비 완료")

            # 세션 상태 출력
            status = session.get_status()
            print(f"📊 계좌번호: {status.get('account_num', 'N/A')}")

            return session
        else:
            print("❌ 키움 세션 준비 실패")
            return None

    except Exception as e:
        print(f"❌ 키움 세션 준비 실패: {e}")
        return None


def test_api_function(session):
    """GetCodeListByMarket 함수 기본 테스트"""
    print("\n🧪 GetCodeListByMarket 함수 테스트")
    print("=" * 40)

    try:
        connector = session.get_connector()
        collector = StockCodeCollector(connector)

        # API 함수 테스트
        if collector.test_api_function():
            print("✅ API 함수 테스트 성공")
            return True
        else:
            print("❌ API 함수 테스트 실패")
            return False

    except Exception as e:
        print(f"❌ API 함수 테스트 중 오류: {e}")
        return False


def test_individual_markets(session):
    """개별 시장별 종목코드 수집 테스트"""
    print("\n📊 개별 시장 종목코드 수집 테스트")
    print("=" * 40)

    try:
        connector = session.get_connector()
        collector = StockCodeCollector(connector)

        # 코스피 테스트
        print("📈 코스피 종목코드 수집 중...")
        kospi_codes = collector.get_kospi_codes()
        if kospi_codes:
            print(f"✅ 코스피 종목 수집 성공: {len(kospi_codes)}개")
            print(f"   샘플: {kospi_codes[:5]}")
        else:
            print("❌ 코스피 종목 수집 실패")

        # 코스닥 테스트
        print("\n📈 코스닥 종목코드 수집 중...")
        kosdaq_codes = collector.get_kosdaq_codes()
        if kosdaq_codes:
            print(f"✅ 코스닥 종목 수집 성공: {len(kosdaq_codes)}개")
            print(f"   샘플: {kosdaq_codes[:5]}")
        else:
            print("❌ 코스닥 종목 수집 실패")

        return len(kospi_codes) > 0 and len(kosdaq_codes) > 0

    except Exception as e:
        print(f"❌ 개별 시장 테스트 실패: {e}")
        return False


def test_full_collection(session):
    """전체 종목코드 수집 테스트"""
    print("\n🚀 전체 종목코드 수집 테스트")
    print("=" * 40)

    try:
        connector = session.get_connector()
        collector = StockCodeCollector(connector)

        # 전체 수집 실행
        codes_result = collector.get_all_stock_codes()

        if codes_result.get('error'):
            print(f"❌ 전체 수집 실패: {codes_result['error']}")
            return False

        # 결과 상세 출력
        print(f"\n📋 상세 수집 결과:")
        print(f"   📊 코스피: {codes_result['kospi_count']:,}개")
        print(f"   📊 코스닥: {codes_result['kosdaq_count']:,}개")
        print(f"   📊 총합: {codes_result['total_count']:,}개")
        print(f"   🕐 수집시간: {codes_result['collected_at']}")

        # 샘플 코드 출력
        collector.show_sample_codes(codes_result, sample_size=10)

        # 유효성 검증
        print(f"\n🔍 종목코드 유효성 검증...")
        validation_result = collector.validate_stock_codes(codes_result['all'])

        if validation_result['valid']:
            print("✅ 종목코드 유효성 검증 통과")
        else:
            print("⚠️ 종목코드 유효성 검증 실패")
            if 'error' in validation_result:
                print(f"   오류: {validation_result['error']}")
            elif 'invalid_codes' in validation_result:
                print(f"   잘못된 코드들: {validation_result['invalid_codes'][:5]}")

        return codes_result['total_count'] > 0

    except Exception as e:
        print(f"❌ 전체 수집 테스트 실패: {e}")
        return False


def test_market_analysis(session):
    """시장 분석 및 통계"""
    print("\n📈 시장 분석 및 통계")
    print("=" * 40)

    try:
        connector = session.get_connector()
        collector = StockCodeCollector(connector)
        codes_result = collector.get_all_stock_codes()

        if codes_result.get('error'):
            print("❌ 분석을 위한 데이터 수집 실패")
            return False

        kospi_count = codes_result['kospi_count']
        kosdaq_count = codes_result['kosdaq_count']
        total_count = codes_result['total_count']

        # 시장 비율 계산
        kospi_ratio = (kospi_count / total_count) * 100 if total_count > 0 else 0
        kosdaq_ratio = (kosdaq_count / total_count) * 100 if total_count > 0 else 0

        print(f"📊 시장 구성 비율:")
        print(f"   📈 코스피: {kospi_count:,}개 ({kospi_ratio:.1f}%)")
        print(f"   📈 코스닥: {kosdaq_count:,}개 ({kosdaq_ratio:.1f}%)")

        # 예상 데이터 수집 시간 계산
        api_calls_needed = total_count  # 각 종목당 OPT10001 호출 1회
        estimated_minutes = api_calls_needed / 100  # 분당 100회 제한

        print(f"\n⏱️ 예상 데이터 수집 시간:")
        print(f"   🔄 필요 API 호출: {api_calls_needed:,}회")
        print(f"   ⏰ 예상 소요시간: {estimated_minutes:.0f}분 ({estimated_minutes / 60:.1f}시간)")

        # 권장 배치 크기
        batch_size = 100
        num_batches = (total_count + batch_size - 1) // batch_size
        print(f"   📦 권장 배치 처리: {batch_size}개씩 {num_batches}배치")

        return True

    except Exception as e:
        print(f"❌ 시장 분석 실패: {e}")
        return False


def main():
    """메인 테스트 함수"""
    print("🚀 종목코드 수집 테스트 시작")
    print("=" * 50)

    # 테스트 목록
    tests = [
        ("API 함수 테스트", test_api_function),
        ("개별 시장 테스트", test_individual_markets),
        ("전체 수집 테스트", test_full_collection),
        ("시장 분석", test_market_analysis)
    ]

    results = []

    # 1단계: 키움 세션 준비
    session = setup_kiwoom_session()
    if not session:
        print("\n❌ 키움 세션 준비 실패로 테스트 중단")
        return False

    results.append(("키움 세션 준비", True))

    # 2-5단계: 나머지 테스트들
    for test_name, test_func in tests:
        print(f"\n{'=' * 20} {test_name} {'=' * 20}")
        try:
            result = test_func(session)
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} 중 예외 발생: {e}")
            results.append((test_name, False))

    # 최종 결과 요약
    print("\n" + "=" * 50)
    print("📋 테스트 결과 요약")
    print("=" * 50)

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "✅ 성공" if result else "❌ 실패"
        print(f"{test_name:.<25} {status}")
        if result:
            passed += 1

    print(f"\n🎯 전체 결과: {passed}/{total} 테스트 통과")

    if passed == total:
        print("🎉 모든 테스트 통과! 종목코드 수집 시스템 준비 완료.")
        print("💡 다음 단계: OPT10001 필드 조사 및 전체 데이터 수집 시스템 구축")
    elif passed >= 3:
        print("✨ 핵심 기능 테스트 통과! 기본 종목코드 수집 가능.")
        print("💡 일부 실패한 기능들을 점검 후 다음 단계 진행 가능")
    else:
        print("⚠️ 주요 테스트 실패. 키움 API 연결 및 설정을 확인해주세요.")

    return passed >= 3


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n👋 사용자가 테스트를 중단했습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 테스트 실행 중 예상치 못한 오류: {e}")
        sys.exit(1)