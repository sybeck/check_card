"""
매일 아침 Windows 작업 스케줄러로 실행되는 독립 스크립트.
brainology 메타 광고 계정의 '광고소재(ad) 단위' raw 데이터를
구글 스프레드시트 단일 탭에 누적(upsert)한다. (Socket Mode 아님 — 1회 실행 후 종료)

수집 끝점은 항상 '어제(KST)' — 오늘은 데이터가 불완전하므로 제외한다.

사용 예:
  # 매일 아침: 어제를 끝점으로 최근 7일 재수집(소급 보정 포함)
  python update_meta_creative_sheet.py

  # 어제 하루만 갱신
  python update_meta_creative_sheet.py --days 1

  # 4월 백필: 2026-04-01 ~ 어제 전체
  python update_meta_creative_sheet.py --since 2026-04-01
"""
import os
import sys
import argparse
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

# connectors/meta 디렉토리를 path에 추가해서 meta_ads_creative 를 모듈로 재사용
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "connectors", "meta")
)
import meta_ads_creative as creative  # noqa: E402

from gsheets import get_worksheet, upsert_rows  # noqa: E402

load_dotenv()

KST = timezone(timedelta(hours=9))


def must_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"[ENV ERROR] {key} 가 필요합니다. .env를 확인하세요.")
    return v


def resolve_range(args) -> tuple:
    """수집 기간 (since, until) 을 'YYYY-MM-DD' 로 결정. until 은 항상 어제(KST)."""
    yesterday = (datetime.now(KST).date() - timedelta(days=1))
    until = yesterday

    if args.since:
        since = datetime.strptime(args.since.strip(), "%Y-%m-%d").date()
    else:
        days = max(1, int(args.days))
        since = yesterday - timedelta(days=days - 1)

    if since > until:
        raise RuntimeError(
            f"[RANGE ERROR] since({since}) 가 until({until}, 어제) 보다 뒤입니다."
        )
    return since.strftime("%Y-%m-%d"), until.strftime("%Y-%m-%d")


def main():
    parser = argparse.ArgumentParser(
        description="brainology 메타 광고소재 raw 데이터를 구글 시트에 누적"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="어제를 끝점으로 최근 N일 재수집 (기본 7). 어제 하루만이면 1",
    )
    parser.add_argument(
        "--since",
        type=str,
        default="",
        help="YYYY-MM-DD. 지정 시 해당일부터 어제까지 전체 수집(백필). --days 무시",
    )
    args = parser.parse_args()

    since, until = resolve_range(args)
    print(f"[INFO] Meta 광고소재 수집: brainology | {since} ~ {until} (KST)")

    token = must_env("META_BRAINOLOGY_ACCESS_TOKEN")
    ad_account = must_env("META_BRAINOLOGY_AD_ACCOUNT_ID")

    raw_rows = creative.fetch_creative_insights(token, ad_account, since, until)
    print(f"[INFO] API 수집 행수: {len(raw_rows)}")

    if not raw_rows:
        print("[INFO] 수집된 데이터가 없습니다. 전송 없이 종료합니다.")
        return

    rows = [creative.normalize_row(r) for r in raw_rows]

    ws = get_worksheet(creative.COLUMNS)
    result = upsert_rows(ws, rows, creative.COLUMNS, key_cols=("일자", "ad_id"))

    print(
        f"[DONE] 시트 반영 완료 — 갱신: {result['updated']}행 / 신규: {result['appended']}행"
    )


if __name__ == "__main__":
    main()
