"""
매주 월요일 아침 Windows 작업 스케줄러로 실행되는 독립 스크립트.
멘션 없이 '전주 월요일~일요일(7일)' brainology 성과를 키워드/제품별로 집계해
노션에 한 페이지로 기록하고, 결과를 슬랙 채널로 알린다.
(Socket Mode 아님 — 1회 실행 후 종료)

기간 계산: 실행일이 속한 주의 월요일 직전 한 주(전주 월~일).
  예) 2026-06-15(월) 실행 → 2026-06-08(월) ~ 2026-06-14(일)

사용:
  python weekly_performance.py                 # 전주 월~일 자동
  python weekly_performance.py --since 2026-06-01 --until 2026-06-07  # 수동 지정
"""
import argparse
from datetime import timedelta

from slack_sdk import WebClient

import performance_report as perf
import notion_writer
from common import now_kst, SLACK_BOT_TOKEN, PERF_WEEKLY_CHANNEL_ID


def prev_week_range():
    """실행일 기준 전주 월요일~일요일을 'YYYY-MM-DD' 로 반환."""
    today = now_kst().date()
    this_monday = today - timedelta(days=today.weekday())  # 이번 주 월요일
    prev_monday = this_monday - timedelta(days=7)
    prev_sunday = this_monday - timedelta(days=1)
    return prev_monday.strftime("%Y-%m-%d"), prev_sunday.strftime("%Y-%m-%d")


def notify_slack(text: str) -> None:
    if not PERF_WEEKLY_CHANNEL_ID:
        print("[WARN] PERF_WEEKLY_CHANNEL_ID 가 비어 있어 슬랙 전송을 건너뜁니다.")
        return
    try:
        WebClient(token=SLACK_BOT_TOKEN).chat_postMessage(
            channel=PERF_WEEKLY_CHANNEL_ID, text=text
        )
        print(f"[INFO] 슬랙 전송 완료 -> {PERF_WEEKLY_CHANNEL_ID}")
    except Exception as e:
        print(f"[WARN] 슬랙 전송 실패: {e}")


def main():
    parser = argparse.ArgumentParser(description="주간 성과 집계 → 노션 기록 → 슬랙 알림")
    parser.add_argument("--since", default="", help="YYYY-MM-DD (수동 지정 시)")
    parser.add_argument("--until", default="", help="YYYY-MM-DD (수동 지정 시)")
    args = parser.parse_args()

    if args.since and args.until:
        since, until = args.since.strip(), args.until.strip()
    else:
        since, until = prev_week_range()

    print(f"[INFO] 주간 성과 집계: brainology | {since} ~ {until} (KST)")

    try:
        report = perf.build_report(since, until)
        url = notion_writer.create_performance_page(report)
        kw_count = len(report["groups"]) - 1  # 미분류 제외
        t = report["total"]
        print(f"[DONE] 노션 기록 완료 — {url}")
        notify_slack(
            f"✅ 주간 성과 집계 완료 ({since} ~ {until})\n"
            f"키워드 {kw_count}개 집계 · 총 지출 {t['spend']:,.0f}원 · ROAS {t['roas']:.2f}\n"
            f"🔗 {url}"
        )
    except Exception as e:
        notify_slack(
            f"⚠️ 주간 성과 집계 실패 ({since} ~ {until})\n{type(e).__name__}: {e}"
        )
        raise


if __name__ == "__main__":
    main()
