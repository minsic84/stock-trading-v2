#!/usr/bin/env python3
"""
간단한 일봉 데이터 수집기 테스트
"""
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_import():
    """모듈 import 테스트"""
    print("📦 모듈 import 테스트")
    try:
        from src.core.config import Config
        from src.collectors.daily_price import DailyPriceCollector
        print("✅ 모든 모듈 import 성공")
        return True
    except Exception as e:
        print(f"❌ import 실패: {e}")
        return False


def test_collector_init():
    """수집기 초기화 테스트"""
    print("\n🔧 수집기 초기화 테스트")
    try:
        from src.collectors.daily_price import DailyPriceCollector
        collector = DailyPriceCollector()
        print("✅ 수집기 초기화 성공")
        return True
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        return False


def main():
    print("🚀 간단한 일봉 수집기 테스트")
    print("=" * 40)

    tests = [
        test_import,
        test_collector_init
    ]

    passed = 0
    for test_func in tests:
        if test_func():
            passed += 1

    print(f"\n🎯 결과: {passed}/{len(tests)} 테스트 통과")

    if passed == len(tests):
        print("🎉 기본 테스트 통과!")
    else:
        print("❌ 일부 테스트 실패")


if __name__ == "__main__":
    main()