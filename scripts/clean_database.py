#!/usr/bin/env python3
"""
데이터베이스 완전 삭제 및 재생성 스크립트
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def clean_database():
    """데이터베이스 완전 삭제"""
    print("🗑️ 데이터베이스 완전 삭제 중...")

    # SQLite DB 파일 삭제
    db_path = project_root / "data" / "stock_data.db"
    if db_path.exists():
        db_path.unlink()
        print(f"✅ 삭제 완료: {db_path}")
    else:
        print("ℹ️ 삭제할 DB 파일이 없습니다.")

    # data 폴더 재생성
    data_dir = project_root / "data"
    data_dir.mkdir(exist_ok=True)
    print("✅ data 폴더 준비 완료")


if __name__ == "__main__":
    clean_database()
    print("🎉 데이터베이스 완전 삭제 완료!")
    print("💡 이제 test_database.py를 실행하세요.")