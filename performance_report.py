"""
기간 성과 집계 로직 (순수 로직 — Slack/Notion 비의존).

멘션 "성과 0601-0612" 의 기간을 파싱해 brainology 메타 광고 성과를
keywords.txt 의 키워드별로 분류·합산하고, 미분류/합계까지 계산한다.

광고 단위 raw 는 connectors/meta/meta_ads_creative.py 를 재사용한다.
"""
import os
import re
import sys

# connectors/meta 를 path 에 추가해서 meta_ads_creative 재사용
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "connectors", "meta")
)
import meta_ads_creative as creative  # noqa: E402

from common import now_kst, script_dir  # noqa: E402

KEYWORDS_FILE = os.path.join(script_dir(), "keywords.txt")

# 집계 대상 원자 지표 키 (meta_ads_creative.normalize_row 의 키와 일치)
SPEND_K = "지출 금액(KRW)"
PURCH_K = "구매"
REV_K = "구매 전환값"
CLICK_K = "링크클릭"
NAME_K = "광고 이름"

UNCLASSIFIED_LABEL = "미분류"
TOTAL_LABEL = "합계"


def must_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"[ENV ERROR] {key} 가 필요합니다. .env를 확인하세요.")
    return v


def parse_period(text: str):
    """
    멘션 텍스트에서 기간을 파싱해 (since, until) 'YYYY-MM-DD' 로 반환.
    지원: "성과 0601-0612" (MMDD-MMDD, 연도는 현재 KST 연도)
          "성과 20260601-20260612" (YYYYMMDD-YYYYMMDD)
    """
    t = text or ""

    # 8자리(YYYYMMDD) 우선
    m = re.search(r"(\d{8})\s*[-~]\s*(\d{8})", t)
    if m:
        since = _fmt8(m.group(1))
        until = _fmt8(m.group(2))
    else:
        m = re.search(r"(\d{4})\s*[-~]\s*(\d{4})", t)
        if not m:
            raise ValueError(
                "기간을 인식하지 못했습니다. 예) `성과 0601-0612` 또는 `성과 20260601-20260612`"
            )
        year = now_kst().year
        since = _fmt4(m.group(1), year)
        until = _fmt4(m.group(2), year)

    if until < since:
        raise ValueError(f"종료일({until})이 시작일({since})보다 빠릅니다. 기간을 확인하세요.")
    return since, until


def _fmt4(mmdd: str, year: int) -> str:
    mm, dd = mmdd[:2], mmdd[2:]
    _validate_md(mm, dd)
    return f"{year:04d}-{mm}-{dd}"


def _fmt8(ymd: str) -> str:
    yyyy, mm, dd = ymd[:4], ymd[4:6], ymd[6:]
    _validate_md(mm, dd)
    return f"{yyyy}-{mm}-{dd}"


def _validate_md(mm: str, dd: str):
    if not (1 <= int(mm) <= 12) or not (1 <= int(dd) <= 31):
        raise ValueError(f"잘못된 월/일입니다: {mm}-{dd}")


def load_keywords():
    """keywords.txt -> list[str] (빈 줄 / '#' 주석 제외)"""
    if not os.path.exists(KEYWORDS_FILE):
        return []
    items = []
    with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            items.append(line)
    return items


def _blank_acc(label: str) -> dict:
    return {"label": label, "spend": 0.0, "purchases": 0, "revenue": 0.0, "link_clicks": 0}


def _add(acc: dict, row: dict):
    acc["spend"] += float(row.get(SPEND_K) or 0)
    acc["purchases"] += int(row.get(PURCH_K) or 0)
    acc["revenue"] += float(row.get(REV_K) or 0)
    acc["link_clicks"] += int(row.get(CLICK_K) or 0)


def _finalize(acc: dict) -> dict:
    """원자 합계 -> 파생지표 재계산 포함 최종 dict."""
    spend = acc["spend"]
    purchases = acc["purchases"]
    revenue = acc["revenue"]
    clicks = acc["link_clicks"]

    def div(n, d):
        return (n / d) if d else 0.0

    return {
        "label": acc["label"],
        "spend": spend,
        "cpc": div(spend, clicks),
        "conv_rate": div(purchases, clicks) * 100,  # 유입 대비 전환율 (%)
        "purchases": purchases,
        "revenue": revenue,
        "cpa": div(spend, purchases),               # 구매당 비용
        "roas": div(revenue, spend),
        "aov": div(revenue, purchases),             # 객단가
        "link_clicks": clicks,
    }


def build_report(since: str, until: str) -> dict:
    """기간 성과를 키워드별/미분류/합계로 집계한 구조를 반환."""
    token = must_env("META_BRAINOLOGY_ACCESS_TOKEN")
    ad_account = must_env("META_BRAINOLOGY_AD_ACCOUNT_ID")

    raw_rows = creative.fetch_creative_insights(token, ad_account, since, until)
    rows = [creative.normalize_row(r) for r in raw_rows]

    keywords = load_keywords()
    kw_acc = {kw: _blank_acc(kw) for kw in keywords}
    unclassified = _blank_acc(UNCLASSIFIED_LABEL)
    total = _blank_acc(TOTAL_LABEL)

    for row in rows:
        name = row.get(NAME_K) or ""
        matched = False
        for kw in keywords:
            if kw and kw in name:
                _add(kw_acc[kw], row)
                matched = True  # 독립 매칭: 여러 키워드에 중복 합산 가능
        if not matched:
            _add(unclassified, row)
        _add(total, row)  # 합계는 전체 광고 1회씩

    groups = [_finalize(kw_acc[kw]) for kw in keywords]
    groups.append(_finalize(unclassified))

    return {
        "period": (since, until),
        "generated_at": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "account": "brainology",
        "groups": groups,
        "total": _finalize(total),
        "row_count": len(rows),
    }
