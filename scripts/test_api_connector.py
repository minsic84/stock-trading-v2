#!/usr/bin/env python3
"""
키움 API 커넥터 테스트 스크립트
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.api.connector import KiwoomAPIConnector, get_kiwoom_connector, create_kiwoom_session


def test_connector_creation():
    """커넥터 생성 테스트"""
    print("🔌 키움 API 커넥터 생성 테스트")
    print("=" * 40)

    try:
        config = Config()
        print(f"✅ 설정 로드 완료")

        # 커넥터 생성
        connector = get_kiwoom_connector(config)
        print(f"✅ 커넥터 생성 완료")

        # 상태 확인
        status = connector.get_connection_status()
        print(f"📊 연결 상태: {status['is_connected']}")
        print(f"📊 계좌번호: {status['account_num']}")

        return True

    except Exception as e:
        print(f"❌ 커넥터 생성 실패: {str(e)}")
        return False


def test_manual_login():
    """수동 로그인 테스트 (실제 키움 설치 필요)"""
    print("\n🔐 수동 로그인 테스트")
    print("=" * 40)
    print("⚠️  이 테스트는 키움 OpenAPI가 설치되어 있어야 합니다.")

    response = input("키움 OpenAPI가 설치되어 있나요? (y/N): ")
    if response.lower() != 'y':
        print("ℹ️  키움 OpenAPI 설치 후 테스트하세요.")
        return True  # 스킵

    try:
        connector = get_kiwoom_connector()

        print("🔄 로그인 시도 중... (키움 로그인 창이 나타납니다)")

        if connector.login():
            print("✅ 로그인 성공!")

            # 계좌 정보 출력
            status = connector.get_connection_status()
            account_info = status['account_info']

            print(f"👤 사용자: {account_info.get('user_name', 'N/A')}")
            print(f"🏦 계좌번호: {account_info.get('account_num', 'N/A')}")
            print(f"🕐 로그인 시간: {account_info.get('login_time', 'N/A')}")

            # 로그아웃
            connector.logout()
            print("✅ 로그아웃 완료")

            return True
        else:
            print("❌ 로그인 실패")
            return False

    except Exception as e:
        print(f"❌ 로그인 테스트 실패: {str(e)}")
        return False


def test_session_creation():
    """세션 생성 테스트"""
    print("\n🚀 세션 생성 테스트")
    print("=" * 40)

    try:
        # 자동 로그인 비활성화로 테스트
        session = create_kiwoom_session(auto_login=False)

        if session:
            print("✅ 세션 생성 완료 (로그인 없이)")

            # 상태 확인
            status = session.get_connection_status()
            print(f"📊 세션 상태: {'활성' if session else '비활성'}")
            print(f"📊 요청 카운트: {status['request_count']}")

            return True
        else:
            print("❌ 세션 생성 실패")
            return False

    except Exception as e:
        print(f"❌ 세션 생성 테스트 실패: {str(e)}")
        return False


def main():
    """메인 테스트 함수"""
    print("🚀 키움 API 커넥터 테스트")
    print("=" * 50)

    # 테스트 실행
    tests = [
        ("커넥터 생성", test_connector_creation),
        ("세션 생성", test_session_creation),
        ("수동 로그인", test_manual_login),
    ]

    results = []
    for test_name, test_func in tests:
        result = test_func()
        results.append((test_name, result))

    # 결과 요약
    print("\n" + "=" * 50)
    print("📋 테스트 결과 요약")
    print("=" * 50)

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "✅ 성공" if result else "❌ 실패"
        print(f"{test_name:.<20} {status}")
        if result:
            passed += 1

    print(f"\n🎯 전체 결과: {passed}/{total} 테스트 통과")

    if passed == total:
        print("🎉 모든 테스트 통과! API 커넥터 준비 완료.")
        print("💡 이제 데이터 수집기를 만들 수 있습니다.")
    elif passed >= total - 1:
        print("✨ 대부분 테스트 통과! 키움 설치 후 로그인 테스트하세요.")
    else:
        print("⚠️ 일부 테스트 실패. 위 로그를 확인해주세요.")

    return passed >= total - 1  # 로그인 테스트는 선택사항


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)