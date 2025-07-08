#!/usr/bin/env python3
"""
콘솔 대시보드 모듈
실시간 수집 진행상황을 터미널에 표시
"""
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.layout import Layout
from rich.panel import Panel
from rich.live import Live
from rich.text import Text


class CollectionDashboard:
    """전체 수집 진행상황 콘솔 대시보드"""

    def __init__(self, total_stocks: int):
        self.console = Console()
        self.total_stocks = total_stocks
        self.start_time = datetime.now()

        # 통계 데이터
        self.current_stock = ""
        self.current_stock_name = ""
        self.completed_count = 0
        self.failed_count = 0
        self.processed_count = 0

        # 실시간 업데이트용
        self.is_running = False
        self.live_display = None
        self.update_thread = None

    def start(self):
        """대시보드 시작"""
        self.is_running = True
        self.live_display = Live(
            self._create_layout(),
            console=self.console,
            refresh_per_second=2,
            screen=False
        )
        self.live_display.start()

    def stop(self):
        """대시보드 종료"""
        self.is_running = False
        if self.live_display:
            self.live_display.stop()

    def update_current_stock(self, stock_code: str, stock_name: str):
        """현재 처리 중인 종목 업데이트"""
        self.current_stock = stock_code
        self.current_stock_name = stock_name
        self._refresh_display()

    def update_completed(self, count: int):
        """완료된 종목 수 업데이트"""
        self.completed_count = count
        self.processed_count = self.completed_count + self.failed_count
        self._refresh_display()

    def update_failed(self, count: int):
        """실패한 종목 수 업데이트"""
        self.failed_count = count
        self.processed_count = self.completed_count + self.failed_count
        self._refresh_display()

    def increment_completed(self):
        """완료 카운트 증가"""
        self.completed_count += 1
        self.processed_count = self.completed_count + self.failed_count
        self._refresh_display()

    def increment_failed(self):
        """실패 카운트 증가"""
        self.failed_count += 1
        self.processed_count = self.completed_count + self.failed_count
        self._refresh_display()

    def _refresh_display(self):
        """화면 새로고침"""
        if self.live_display and self.is_running:
            self.live_display.update(self._create_layout())

    def _create_layout(self) -> Layout:
        """레이아웃 생성"""
        layout = Layout()

        # 상단: 전체 진행률
        layout.split_column(
            Layout(self._create_progress_panel(), name="progress", size=8),
            Layout(self._create_stats_panel(), name="stats", size=10),
            Layout(self._create_current_panel(), name="current", size=5)
        )

        return layout

    def _create_progress_panel(self) -> Panel:
        """진행률 패널 생성"""
        # 진행률 계산
        progress_percentage = (self.processed_count / self.total_stocks * 100) if self.total_stocks > 0 else 0

        # 진행률 바 생성
        bar_width = 50
        filled_width = int(bar_width * progress_percentage / 100)
        progress_bar = "█" * filled_width + "░" * (bar_width - filled_width)

        # 시간 계산
        elapsed_time = datetime.now() - self.start_time
        elapsed_str = str(elapsed_time).split('.')[0]  # 마이크로초 제거

        # 예상 남은 시간 계산
        if self.processed_count > 0:
            avg_time_per_stock = elapsed_time.total_seconds() / self.processed_count
            remaining_stocks = self.total_stocks - self.processed_count
            remaining_seconds = remaining_stocks * avg_time_per_stock
            remaining_time = timedelta(seconds=int(remaining_seconds))
            remaining_str = str(remaining_time)
        else:
            remaining_str = "계산 중..."

        progress_text = f"""
📊 전체 진행률: {self.processed_count:,}/{self.total_stocks:,} ({progress_percentage:.1f}%)

{progress_bar}

⏱️  소요시간: {elapsed_str}
⏳ 예상 남은시간: {remaining_str}
"""

        return Panel(
            progress_text.strip(),
            title="🚀 전체 수집 진행률",
            border_style="blue"
        )

    def _create_stats_panel(self) -> Panel:
        """통계 패널 생성"""
        success_rate = (self.completed_count / self.processed_count * 100) if self.processed_count > 0 else 0

        # 처리 속도 계산
        elapsed_seconds = (datetime.now() - self.start_time).total_seconds()
        processing_speed = self.processed_count / (elapsed_seconds / 60) if elapsed_seconds > 0 else 0

        stats_text = f"""
✅ 성공: {self.completed_count:,}개
❌ 실패: {self.failed_count:,}개
📈 성공률: {success_rate:.1f}%

🚀 처리 속도: {processing_speed:.1f}개/분
📊 남은 종목: {self.total_stocks - self.processed_count:,}개
"""

        return Panel(
            stats_text.strip(),
            title="📈 수집 통계",
            border_style="green"
        )

    def _create_current_panel(self) -> Panel:
        """현재 처리 상황 패널 생성"""
        if self.current_stock:
            current_text = f"🔄 현재 처리 중: {self.current_stock} ({self.current_stock_name})"
        else:
            current_text = "⏸️ 대기 중..."

        return Panel(
            current_text,
            title="🔄 현재 상태",
            border_style="yellow"
        )

    def show_final_report(self, summary: Dict[str, Any]):
        """최종 리포트 표시"""
        self.stop()

        # 최종 통계 테이블 생성
        table = Table(title="🎉 전체 수집 완료 리포트")
        table.add_column("항목", style="cyan", no_wrap=True)
        table.add_column("수량", style="magenta")
        table.add_column("비율", style="green")

        total_time = datetime.now() - self.start_time

        table.add_row("총 종목 수", f"{self.total_stocks:,}개", "100.0%")
        table.add_row("성공", f"{self.completed_count:,}개", f"{self.completed_count / self.total_stocks * 100:.1f}%")
        table.add_row("실패", f"{self.failed_count:,}개", f"{self.failed_count / self.total_stocks * 100:.1f}%")
        table.add_row("총 소요시간", str(total_time).split('.')[0], "-")

        self.console.print("\n")
        self.console.print(table)

        # HeidiSQL 확인 쿼리 출력
        self.console.print("\n" + "=" * 60)
        self.console.print("📊 HeidiSQL 확인 쿼리", style="bold blue")
        self.console.print("=" * 60)

        queries = [
            "-- 전체 수집 현황",
            "SELECT status, COUNT(*) as count FROM collection_progress GROUP BY status;",
            "",
            "-- 성공률 통계",
            "SELECT ",
            "    COUNT(*) as total,",
            "    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as success,",
            "    ROUND(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate",
            "FROM collection_progress;",
            "",
            "-- 실패한 종목들",
            "SELECT stock_code, stock_name, attempt_count, error_message",
            "FROM collection_progress",
            "WHERE status = 'failed'",
            "ORDER BY attempt_count DESC;",
            "",
            "-- 수집 데이터 확인",
            "SELECT s.name, cp.data_count, cp.success_time",
            "FROM collection_progress cp",
            "JOIN stocks s ON cp.stock_code = s.code",
            "WHERE cp.status = 'completed'",
            "ORDER BY cp.data_count DESC",
            "LIMIT 10;"
        ]

        for query in queries:
            if query.startswith("--"):
                self.console.print(query, style="bold yellow")
            else:
                self.console.print(query, style="white")

        self.console.print("\n✅ 대시보드가 종료되었습니다.")

    def show_retry_info(self, retry_stocks: list, retry_round: int):
        """재시도 정보 표시"""
        self.console.print(f"\n🔄 {retry_round}차 재시도 시작")
        self.console.print(f"📊 재시도 대상: {len(retry_stocks)}개 종목")

        if len(retry_stocks) <= 10:
            for stock in retry_stocks:
                self.console.print(
                    f"   - {stock['stock_code']}: {stock['stock_name']} (시도: {stock['attempt_count']}/3)")
        else:
            for i, stock in enumerate(retry_stocks[:5]):
                self.console.print(
                    f"   - {stock['stock_code']}: {stock['stock_name']} (시도: {stock['attempt_count']}/3)")
            self.console.print(f"   ... 외 {len(retry_stocks) - 5}개")

        self.console.print("")