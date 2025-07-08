"""
Database connection and ORM models for Stock Trading System
"""
from sqlalchemy import (
    create_engine, Column, Integer, String, BigInteger,
    DateTime, VARCHAR, Index, UniqueConstraint, func, String
)

import os
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import logging

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from .config import Config

from sqlalchemy import (
    create_engine, Column, Integer, String, BigInteger,
    DateTime, VARCHAR, Index, UniqueConstraint, func  # func 추가
)

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

class CollectionProgress(Base):
    """전체 수집 진행상황 추적 모델"""
    __tablename__ = 'collection_progress'

    stock_code = Column(VARCHAR(10), primary_key=True, comment='종목코드')
    stock_name = Column(VARCHAR(100), comment='종목명')
    status = Column(VARCHAR(20), default='pending', comment='상태')  # pending, processing, completed, failed, skipped
    attempt_count = Column(Integer, default=0, comment='시도 횟수')
    last_attempt_time = Column(DateTime, comment='마지막 시도 시간')
    success_time = Column(DateTime, comment='성공 시간')
    error_message = Column(String(500), comment='오류 메시지')
    data_count = Column(Integer, default=0, comment='수집된 데이터 개수')
    created_at = Column(DateTime, default=datetime.now, comment='생성일시')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='수정일시')

    # 인덱스 설정
    __table_args__ = (
        Index('idx_status', 'status'),
        Index('idx_attempt_count', 'attempt_count'),
        Index('idx_last_attempt_time', 'last_attempt_time'),
    )

    def __repr__(self):
        return f"<CollectionProgress(stock_code='{self.stock_code}', status='{self.status}', attempt_count={self.attempt_count})>"

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
        """주식 정보 업데이트 필요 여부 확인 (실시간 업데이트 모드 - 항상 업데이트)"""
        try:
            # 실시간 업데이트 모드: 항상 최신 데이터로 업데이트
            logger.info(f"{stock_code}: 실시간 업데이트 모드 - 항상 수집 필요")
            return True

        except Exception as e:
            logger.error(f"업데이트 필요 여부 확인 실패 {stock_code}: {e}")
            return True  # 오류 시에도 수집 수행

    def is_today_data_collected(self, stock_code: str) -> bool:
        """오늘 날짜의 데이터가 이미 수집되었는지 확인 (실시간 모드에서는 항상 False)"""
        try:
            # 실시간 업데이트 모드: 항상 수집 필요하다고 반환
            logger.info(f"{stock_code}: 실시간 모드 - 재수집 허용")
            return False

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

    def update_collection_progress(self, stock_code: str, status: str, **kwargs) -> bool:
        """수집 진행상황 업데이트"""
        try:
            with self.db_manager.get_session() as session:
                # 기존 레코드 확인
                progress = session.query(CollectionProgress).filter(
                    CollectionProgress.stock_code == stock_code
                ).first()

                if progress:
                    # 기존 레코드 업데이트
                    progress.status = status
                    progress.last_attempt_time = datetime.now()
                    progress.updated_at = datetime.now()

                    # 추가 필드 업데이트
                    if status == 'processing':
                        progress.attempt_count += 1
                    elif status == 'completed':
                        progress.success_time = datetime.now()
                        progress.data_count = kwargs.get('data_count', 0)
                    elif status == 'failed':
                        progress.error_message = kwargs.get('error_message', '')

                    if 'stock_name' in kwargs:
                        progress.stock_name = kwargs['stock_name']

                else:
                    # 새 레코드 생성
                    progress = CollectionProgress(
                        stock_code=stock_code,
                        stock_name=kwargs.get('stock_name', ''),
                        status=status,
                        attempt_count=1 if status == 'processing' else 0,
                        last_attempt_time=datetime.now(),
                        error_message=kwargs.get('error_message', '') if status == 'failed' else None,
                        data_count=kwargs.get('data_count', 0) if status == 'completed' else 0
                    )
                    session.add(progress)

                session.commit()
                return True

        except Exception as e:
            logger.error(f"진행상황 업데이트 실패 {stock_code}: {e}")
            return False

    def get_collection_status_summary(self) -> Dict[str, Any]:
        """전체 수집 현황 요약"""
        try:
            with self.db_manager.get_session() as session:
                # 상태별 통계
                status_counts = session.query(
                    CollectionProgress.status,
                    func.count(CollectionProgress.stock_code).label('count')
                ).group_by(CollectionProgress.status).all()

                # 전체 통계
                total_count = session.query(CollectionProgress).count()
                completed_count = session.query(CollectionProgress).filter(
                    CollectionProgress.status == 'completed'
                ).count()

                # 성공률 계산
                success_rate = (completed_count / total_count * 100) if total_count > 0 else 0

                # 최근 활동
                latest_activity = session.query(CollectionProgress).order_by(
                    CollectionProgress.last_attempt_time.desc()
                ).first()

                return {
                    'total_stocks': total_count,
                    'completed': completed_count,
                    'success_rate': round(success_rate, 2),
                    'status_breakdown': {status: count for status, count in status_counts},
                    'latest_activity': {
                        'stock_code': latest_activity.stock_code if latest_activity else None,
                        'stock_name': latest_activity.stock_name if latest_activity else None,
                        'status': latest_activity.status if latest_activity else None,
                        'time': latest_activity.last_attempt_time if latest_activity else None
                    }
                }

        except Exception as e:
            logger.error(f"수집 현황 조회 실패: {e}")
            return {}

    def get_failed_stocks(self, max_attempts: int = 3) -> List[Dict[str, Any]]:
        """실패한 종목 목록 반환 (재시도 대상)"""
        try:
            with self.db_manager.get_session() as session:
                failed_stocks = session.query(CollectionProgress).filter(
                    CollectionProgress.status == 'failed',
                    CollectionProgress.attempt_count < max_attempts
                ).all()

                return [
                    {
                        'stock_code': stock.stock_code,
                        'stock_name': stock.stock_name,
                        'attempt_count': stock.attempt_count,
                        'error_message': stock.error_message
                    }
                    for stock in failed_stocks
                ]

        except Exception as e:
            logger.error(f"실패 종목 조회 실패: {e}")
            return []

    def get_pending_stocks(self) -> List[str]:
        """아직 처리되지 않은 종목 목록 반환"""
        try:
            with self.db_manager.get_session() as session:
                pending_stocks = session.query(CollectionProgress.stock_code).filter(
                    CollectionProgress.status.in_(['pending', 'failed'])
                ).all()

                return [stock[0] for stock in pending_stocks]

        except Exception as e:
            logger.error(f"대기 종목 조회 실패: {e}")
            return []

    def initialize_collection_progress(self, stock_codes_with_names: List[Tuple[str, str]]) -> bool:
        """수집 진행상황 테이블 초기화"""
        try:
            with self.db_manager.get_session() as session:
                # 기존 데이터 삭제
                session.query(CollectionProgress).delete()

                # 새 데이터 추가
                progress_records = [
                    CollectionProgress(
                        stock_code=code,
                        stock_name=name,
                        status='pending'
                    )
                    for code, name in stock_codes_with_names
                ]

                session.add_all(progress_records)
                session.commit()

                logger.info(f"수집 진행상황 초기화 완료: {len(progress_records)}개 종목")
                return True

        except Exception as e:
            logger.error(f"수집 진행상황 초기화 실패: {e}")
            return False

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