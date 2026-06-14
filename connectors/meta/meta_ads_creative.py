"""
메타 광고소재(ad/광고) 단위 인사이트 수집 모듈.

기존 meta_ads_current.py 의 헬퍼(토큰/HTTP/구매 파싱 등)를 재사용하며,
level="ad" + time_increment=1 로 1광고 × 1일 단위 raw 데이터를 가져온다.

페이지네이션(paging.next)을 따라가며 기간 내 전체 행을 수집한다.
"""
import json
from typing import Dict, List

import requests

from meta_ads_current import (
    GRAPH_BASE,
    TIMEOUT,
    normalize_act_id,
    safe_json,
)

# 구매/구매전환값 집계용 단일 action_type 우선순위.
# 메타는 한 건의 구매를 여러 버킷(purchase / omni_purchase /
# offsite_conversion.fb_pixel_purchase / onsite_web_purchase ...)에 중복 보고하므로
# 절대 합산하지 않고 아래 우선순위에서 '존재하는 첫 번째' 하나만 사용한다.
# 'purchase' 표준 이벤트 기준으로 집계(구매 건수 / 구매전환값 동일 기준).
PURCHASE_TYPE_PRIORITY = [
    "purchase",
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
]

# raw fetch 시 요청하는 필드
INSIGHT_FIELDS = (
    "date_start,campaign_name,ad_name,ad_id,"
    "spend,impressions,inline_link_clicks,actions,action_values"
)

# 시트 컬럼 순서 (gsheets 헤더와 일치해야 함)
COLUMNS = [
    "일자",
    "캠페인 이름",
    "광고 이름",
    "ad_id",
    "지출 금액(KRW)",
    "구매 전환값",
    "ROAS",
    "구매",
    "구매당 비용",
    "CPM",
    "CPC",
    "CTR",
    "유입 대비 전환율",
    "객단가",
    "노출",
    "링크클릭",
]


def pick_purchase_value(items) -> float:
    """
    actions / action_values 리스트에서 '구매' 값을 단일 action_type 기준으로 뽑는다.
    PURCHASE_TYPE_PRIORITY 순서대로 존재하는 첫 type 하나만 사용(합산 금지 → 중복 집계 방지).
    """
    if not items:
        return 0.0
    by_type = {}
    for a in items:
        at = (a.get("action_type") or "").strip()
        val = a.get("value")
        if val is None:
            continue
        try:
            by_type[at] = float(val)
        except Exception:
            pass
    for t in PURCHASE_TYPE_PRIORITY:
        if t in by_type:
            return by_type[t]
    return 0.0


def fetch_creative_insights(
    access_token: str, ad_account_id: str, since: str, until: str
) -> List[dict]:
    """
    광고(ad) 단위 일별 인사이트 raw 행을 기간 전체에 대해 수집한다.

    since/until: "YYYY-MM-DD" (포함). time_increment=1 이므로
    하루당 광고당 1행이 반환된다.
    """
    act_id = normalize_act_id(ad_account_id)
    url = f"{GRAPH_BASE}/{act_id}/insights"
    params = {
        "access_token": access_token,
        "fields": INSIGHT_FIELDS,
        "level": "ad",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "limit": 500,
    }

    rows: List[dict] = []
    next_url = url
    next_params = params

    # 첫 요청은 url+params, 이후 paging.next 는 전체 URL(쿼리 포함)이므로 params 없이 GET
    while True:
        r = requests.get(next_url, params=next_params, timeout=TIMEOUT)
        data = safe_json(r)
        if r.status_code != 200:
            err = (data or {}).get("error") if isinstance(data, dict) else None
            raise RuntimeError(
                f"[CREATIVE INSIGHTS FAIL] {act_id} ({since}~{until})\n"
                f"  HTTP {r.status_code}\n"
                f"  error_message: {(err or {}).get('message') if isinstance(err, dict) else None}\n"
                f"  body: {data or r.text[:300]}"
            )
        page = (data or {}).get("data") or []
        rows.extend(page)

        next_url = (((data or {}).get("paging") or {}).get("next")) or ""
        if not next_url:
            break
        # paging.next 는 access_token 포함 완성 URL
        next_params = None

    return rows


def _safe_div(numer: float, denom: float):
    """0 나눗셈 방지. 분모가 0이면 빈 문자열 반환(시트에서 공란)."""
    try:
        if not denom:
            return ""
        return numer / denom
    except Exception:
        return ""


def normalize_row(raw: dict) -> Dict[str, object]:
    """
    API raw 행 -> 시트 컬럼 dict.
    base 필드 정리 + 파생 지표(ROAS/CPA/CPM/CPC/CTR/전환율/객단가) 계산.
    """
    date = (raw.get("date_start") or "").strip()
    campaign = (raw.get("campaign_name") or "").strip()
    ad_name = (raw.get("ad_name") or "").strip()
    ad_id = (raw.get("ad_id") or "").strip()

    try:
        spend = float(raw.get("spend") or 0.0)
    except Exception:
        spend = 0.0
    try:
        impressions = int(float(raw.get("impressions") or 0))
    except Exception:
        impressions = 0
    try:
        link_clicks = int(float(raw.get("inline_link_clicks") or 0))
    except Exception:
        link_clicks = 0

    purchases = int(pick_purchase_value(raw.get("actions")))
    revenue = pick_purchase_value(raw.get("action_values"))

    roas = _safe_div(revenue, spend)
    cpa = _safe_div(spend, purchases)
    cpm = _safe_div(spend, impressions)
    cpm = (cpm * 1000) if isinstance(cpm, float) else cpm
    cpc = _safe_div(spend, link_clicks)
    ctr = _safe_div(link_clicks, impressions)
    ctr = (ctr * 100) if isinstance(ctr, float) else ctr
    conv_rate = _safe_div(purchases, link_clicks)
    conv_rate = (conv_rate * 100) if isinstance(conv_rate, float) else conv_rate
    aov = _safe_div(revenue, purchases)

    return {
        "일자": date,
        "캠페인 이름": campaign,
        "광고 이름": ad_name,
        "ad_id": ad_id,
        "지출 금액(KRW)": round(spend, 2),
        "구매 전환값": round(revenue, 2),
        "ROAS": round(roas, 4) if isinstance(roas, float) else roas,
        "구매": purchases,
        "구매당 비용": round(cpa, 2) if isinstance(cpa, float) else cpa,
        "CPM": round(cpm, 2) if isinstance(cpm, float) else cpm,
        "CPC": round(cpc, 2) if isinstance(cpc, float) else cpc,
        "CTR": round(ctr, 4) if isinstance(ctr, float) else ctr,
        "유입 대비 전환율": round(conv_rate, 4) if isinstance(conv_rate, float) else conv_rate,
        "객단가": round(aov, 2) if isinstance(aov, float) else aov,
        "노출": impressions,
        "링크클릭": link_clicks,
    }
