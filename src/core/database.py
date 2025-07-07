"""
Database connection and ORM models for Stock Trading System
"""
import os
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

from sqlalchemy import (
    create_engine, Column, Integer, String, BigInteger,
    DateTime, VARCHAR, Index, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from .config import Config

# 로거 설정
logger = logging.getLogger(__name__)

# SQLAlchemy Base
Base = declarative_base()


class Stock(Base):
    """주식 기본 정보 모델 (OPT10001 데이터 포함)"""
    __tablename__ = 'stocks'

    # 기본 식별 정보
    code = Column(VARCHAR(10), primary_key=True, comment='종목코드')
    name = Column(VARCHAR(100), nullable=False, comment='종목명')
    market = Column(VARCHAR(10), comment='시장구분(KOSPI/KOSDAQ)')

    # OPT10001 주식기본정보 데이터
    current_price = Column(Integer, comment='현재가')
    prev_day_diff = Column(Integer, comment='전일대비')
    change_rate = Column(Integer, comment='등락률(소수점2자리*100)')
    volume = Column(BigInteger, comment='거래량')
    open_price = Column(Integer, comment='시가')
    high_price = Column(Integer, comment='고가')
    low_price = Column(Integer, comment='저가')
    upper_limit = Column(Integer, comment='상한가')
    lower_limit = Column(Integer, comment='하한가')
    market_cap = Column(BigInteger, comment='시가총액')
    market_cap_size = Column(VARCHAR(20), comment='시가총액규모')
    listed_shares = Column(BigInteger, comment='상장주수')
    per_ratio = Column(Integer, comment='PER(소수점2자리*100)')
    pbr_ratio = Column(Integer, comment='PBR(소수점2자리*100)')

    # 메타 정보
    data_source = Column(VARCHAR(20), default='OPT10001', comment='데이터 출처')
    last_updated = Column(DateTime, comment='마지막 업데이트')
    is_active = Column(Integer, default=1, comment='활성 여부(1:활성, 0:비활성)')
    created_at = Column(DateTime, default=datetime.now, comment='생성일시')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='수정일시')

    # 인덱스 설정
    __table_args__ = (
        Index('idx_market', 'market'),
        Index('idx_last_updated', 'last_updated'),
        Index('idx_is_active', 'is_active'),
        Index('idx_market_cap', 'market_cap'),
    )

    def __repr__(self):
        return f"<Stock(code='{self.code}', name='{self.name}', market='{self.market}', current_price={self.current_price})>"


class DailyPrice(Base):
    """일봉 데이터 모델 (키움 API 호환)"""
    __tablename__ = 'daily_prices'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(VARCHAR(10), nullable=False, comment='종목코드')
    date = Column(VARCHAR(8), nullable=False, comment='일자(YYYYMMDD)')

    # 키움 API 필드명과 일치하도록 수정
    start_price = Column(Integer, comment='시가')
    high_price = Column(Integer, comment='고가')
    low_price = Column(Integer, comment='저가')
    current_price = Column(Integer, comment='현재가')
    volume = Column(BigInteger, comment='거래량')
    trading_value = Column(BigInteger, comment='거래대금')

    # 추가 필드들
    prev_day_diff = Column(Integer, comment='전일대비', default=0)
    change_rate = Column(Integer, comment='등락율(소수점2자리*100)', default=0)

    created_at = Column(DateTime, default=datetime.now, comment='생성일시')

    # 인덱스 설정
    __table_args__ = (
        Index('idx_stock_date', 'stock_code', 'date'),
        Index('idx_date', 'date'),
        UniqueConstraint('stock_code', 'date', name='uq_stock_date'),
    )

    def __repr__(self):
        return f"<DailyPrice(stock_code='{self.stock_code}', date='{self.date}', current_price={self.current_price})>"


class DatabaseManager:
    """데이터베이스 연결 및 관리 클래스"""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.engine = None
        self.SessionLocal = None
        self._setup_database()

    def _setup_database(self):
        """데이터베이스 설정 및 연결"""
        try:
            database_url = self._get_database_url()
            logger.info(f"Database URL: {database_url}")

            # SQLAlchemy 엔진 생성
            self.engine = create_engine(
                database_url,
                echo=False,  # SQL 쿼리 로그 출력
                pool_timeout=30,
                pool_recycle=3600
            )

            # 세션 팩토리 생성
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )

            # 데이터 디렉토리 생성 (SQLite용)
            if self.config.env == 'development':
                data_dir = Path('./data')
                data_dir.mkdir(exist_ok=True)

            logger.info("Database setup completed successfully")

        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

    def _get_database_url(self) -> str:
        """환경에 따른 데이터베이스 URL 반환"""
        db_type = os.getenv('DB_TYPE', 'sqlite')

        if db_type == 'sqlite':
            db_path = os.getenv('SQLITE_DB_PATH', './data/stock_data.db')
            return f"sqlite:///{db_path}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def create_tables(self):
        """모든 테이블 생성"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("All tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def drop_tables(self):
        """모든 테이블 삭제 (주의!)"""
        try:
            # SQLite인 경우 DB 파일 자체를 삭제
            if hasattr(self, 'engine') and self.engine:
                self.engine.dispose()  # 연결 해제

            db_type = os.getenv('DB_TYPE', 'sqlite')
            if db_type == 'sqlite':
                db_path = os.getenv('SQLITE_DB_PATH', './data/stock_data.db')
                if os.path.exists(db_path):
                    os.remove(db_path)
                    logger.info(f"Removed existing SQLite database: {db_path}")

                # 새로운 엔진 생성
                self._setup_database()

            logger.warning("All tables dropped!")
        except Exception as e:
            logger.error(f"Failed to drop tables: {e}")

    def get_session(self) -> Session:
        """새 데이터베이스 세션 반환"""
        if not self.SessionLocal:
            raise RuntimeError("Database not initialized")
        return self.SessionLocal()

    def test_connection(self) -> bool:
        """데이터베이스 연결 테스트"""
        try:
            from sqlalchemy import text
            with self.get_session() as session:
                result = session.execute(text("SELECT 1"))
                result.fetchone()
                logger.info("Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def get_table_info(self) -> Dict[str, Any]:
        """테이블 정보 조회"""
        try:
            with self.get_session() as session:
                tables_info = {}
                tables_info['stocks'] = session.query(Stock).count()
                tables_info['daily_prices'] = session.query(DailyPrice).count()
                return tables_info
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return {}


class DatabaseService:
    """데이터베이스 서비스 클래스"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def add_or_update_stock_info(self, stock_code: str, stock_data: dict) -> bool:
        """주식 기본정보 추가 또는 업데이트 (OPT10001 데이터)"""
        try:
            with self.db_manager.get_session() as session:
                # 기존 데이터 확인
                existing = session.query(Stock).filter(Stock.code == stock_code).first()

                if existing:
                    # 기존 데이터 업데이트
                    for key, value in stock_data.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    existing.last_updated = datetime.now()
                    existing.updated_at = datetime.now()

                    logger.info(f"주식정보 업데이트: {stock_code} - {stock_data.get('name', '')}")
                else:
                    # 새 데이터 추가
                    new_stock = Stock(
                        code=stock_code,
                        last_updated=datetime.now(),
                        **stock_data
                    )
                    session.add(new_stock)

                    logger.info(f"새 주식정보 추가: {stock_code} - {stock_data.get('name', '')}")

                session.commit()
                return True

        except Exception as e:
            logger.error(f"주식정보 저장 실패 {stock_code}: {e}")
            return False

    def is_stock_update_needed(self, stock_code: str, force_daily: bool = True) -> bool:
        """주식 정보 업데이트 필요 여부 확인 (일일 업데이트 기준)"""
        try:
            with self.db_manager.get_session() as session:
                stock = session.query(Stock).filter(Stock.code == stock_code).first()

                if not stock:
                    return True  # 데이터가 없으면 수집 필요

                if not stock.last_updated:
                    return True  # 업데이트 시간이 없으면 수집 필요

                if force_daily:
                    # 일일 업데이트: 오늘 날짜와 비교
                    today = datetime.now().date()
                    last_update_date = stock.last_updated.date()

                    return last_update_date < today  # 오늘 업데이트되지 않았으면 수집 필요
                else:
                    # 기존 방식: 5일 기준 (필요시 사용)
                    days_passed = (datetime.now() - stock.last_updated).days
                    return days_passed >= 5

        except Exception as e:
            logger.error(f"업데이트 필요 여부 확인 실패 {stock_code}: {e}")
            return True  # 오류 시 수집 수행

    def is_today_data_collected(self, stock_code: str) -> bool:
        """오늘 날짜의 데이터가 이미 수집되었는지 확인"""
        try:
            with self.db_manager.get_session() as session:
                stock = session.query(Stock).filter(Stock.code == stock_code).first()

                if not stock or not stock.last_updated:
                    return False

                # 오늘 날짜와 비교
                today = datetime.now().date()
                last_update_date = stock.last_updated.date()

                return last_update_date >= today  # 오늘 이후 업데이트되었으면 True

        except Exception as e:
            logger.error(f"오늘 데이터 확인 실패 {stock_code}: {e}")
            return False

    def get_stock_info(self, stock_code: str) -> dict:
        """주식 기본정보 조회"""
        try:
            with self.db_manager.get_session() as session:
                stock = session.query(Stock).filter(Stock.code == stock_code).first()

                if stock:
                    return {
                        'code': stock.code,
                        'name': stock.name,
                        'market': stock.market,
                        'current_price': stock.current_price,
                        'change_rate': stock.change_rate,
                        'volume': stock.volume,
                        'market_cap': stock.market_cap,
                        'last_updated': stock.last_updated
                    }
                else:
                    return {}

        except Exception as e:
            logger.error(f"주식정보 조회 실패 {stock_code}: {e}")
            return {}

    def add_daily_price(self, stock_code: str, date: str,
                       current_price: int, volume: int, trading_value: int,
                       start_price: int, high_price: int, low_price: int,
                       prev_day_diff: int = None, change_rate: float = None) -> bool:
        """일봉 데이터 추가 (키움 API 호환)"""
        try:
            with self.db_manager.get_session() as session:
                # 기존 데이터 확인 (중복 방지)
                existing = session.query(DailyPrice).filter(
                    DailyPrice.stock_code == stock_code,
                    DailyPrice.date == date
                ).first()

                # 등락율을 정수로 변환 (소수점 2자리 * 100)
                change_rate_int = int(change_rate * 100) if change_rate is not None else None

                if existing:
                    # 기존 데이터 업데이트
                    existing.current_price = current_price
                    existing.volume = volume
                    existing.trading_value = trading_value
                    existing.start_price = start_price
                    existing.high_price = high_price
                    existing.low_price = low_price
                    existing.prev_day_diff = prev_day_diff
                    existing.change_rate = change_rate_int
                    existing.updated_at = datetime.now()
                else:
                    # 새 데이터 추가
                    new_daily = DailyPrice(
                        stock_code=stock_code,
                        date=date,
                        current_price=current_price,
                        volume=volume,
                        trading_value=trading_value,
                        start_price=start_price,
                        high_price=high_price,
                        low_price=low_price,
                        prev_day_diff=prev_day_diff,
                        change_rate=change_rate_int
                    )
                    session.add(new_daily)

                session.commit()
                return True

        except Exception as e:
            logger.error(f"Failed to add daily price for {stock_code} on {date}: {e}")
            return False

    def get_latest_date(self, stock_code: str, data_type: str = 'daily') -> Optional[str]:
        """종목의 최신 데이터 날짜 조회"""
        try:
            with self.db_manager.get_session() as session:
                if data_type == 'daily':
                    result = session.query(DailyPrice.date).filter(
                        DailyPrice.stock_code == stock_code
                    ).order_by(DailyPrice.date.desc()).first()
                else:
                    return None

                return result[0] if result else None

        except Exception as e:
            logger.error(f"Failed to get latest date for {stock_code}: {e}")
            return None


# 싱글톤 패턴으로 데이터베이스 매니저 인스턴스 생성
_db_manager: Optional[DatabaseManager] = None

def get_database_manager() -> DatabaseManager:
    """데이터베이스 매니저 싱글톤 인스턴스 반환"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager

def get_database_service() -> DatabaseService:
    """데이터베이스 서비스 인스턴스 반환"""
    return DatabaseService(get_database_manager())