import os
import sys
import json
from typing import Dict, List, Optional

import requests

# connectors/meta 디렉토리를 path에 추가해서 meta_ads_current 를 모듈로 재사용
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "connectors", "meta"))
import meta_ads_current as meta  # noqa: E402

# 메타는 같은 전환 1건을 여러 action_type(purchase / omni_purchase /
# offsite_conversion.fb_pixel_purchase ...)으로 중복해서 돌려준다.
# 전부 더하면 과대 계상되므로, "purchase" / "initiate_checkout" action_type만 집계한다.
def _exact_action_value(actions, action_type: str) -> int:
    if not actions:
        return 0
    for a in actions:
        if (a.get("action_type") or "").strip() != action_type:
            continue
        v = a.get("value")
        if v is None:
            continue
        try:
            return int(float(v))
        except Exception:
            return 0
    return 0


def parse_purchases_from_actions(actions) -> int:
    return _exact_action_value(actions, "purchase")


def parse_checkouts_from_actions(actions) -> int:
    return _exact_action_value(actions, "initiate_checkout")


def parse_purchase_value(action_values) -> float:
    """action_values 에서 action_type == purchase 의 전환값(매출)만 사용."""
    if not action_values:
        return 0.0
    for a in action_values:
        if (a.get("action_type") or "").strip() != "purchase":
            continue
        v = a.get("value")
        if v is None:
            return 0.0
        try:
            return float(v)
        except Exception:
            return 0.0
    return 0.0


def fetch_insights_extended(access_token: str, ad_account_id: str, ymd: str) -> List[dict]:
    """meta_ads_current.fetch_insights_current_by_campaign 와 동일하나 fields에 clicks 추가."""
    act_id = meta.normalize_act_id(ad_account_id)
    url = f"{meta.GRAPH_BASE}/{act_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "campaign_name,spend,actions,action_values,clicks,date_start,date_stop",
        "level": "campaign",
        "time_range": json.dumps({"since": ymd, "until": ymd}),
        "time_increment": 1,
        "limit": 500,
    }
    r = requests.get(url, params=params, timeout=meta.TIMEOUT)
    data = meta.safe_json(r)
    if r.status_code != 200:
        err = (data or {}).get("error") if isinstance(data, dict) else None
        raise RuntimeError(
            f"[INSIGHTS FAIL] {act_id}\n"
            f"  HTTP {r.status_code}\n"
            f"  error_message: {(err or {}).get('message') if isinstance(err, dict) else None}\n"
            f"  body: {data or r.text[:300]}"
        )
    return (data or {}).get("data") or []


def _empty_metrics() -> dict:
    return {"spend": 0.0, "purchases": 0, "checkouts": 0, "clicks": 0, "revenue": 0.0}


def _add_row(acc: dict, row: dict) -> None:
    try:
        acc["spend"] += float(row.get("spend") or 0.0)
    except Exception:
        pass
    acc["purchases"] += parse_purchases_from_actions(row.get("actions"))
    acc["checkouts"] += parse_checkouts_from_actions(row.get("actions"))
    acc["revenue"] += parse_purchase_value(row.get("action_values"))
    try:
        acc["clicks"] += int(float(row.get("clicks") or 0))
    except Exception:
        pass


def _finalize(acc: dict) -> dict:
    spend = float(acc["spend"])
    purchases = int(acc["purchases"])
    checkouts = int(acc["checkouts"])
    clicks = int(acc["clicks"])
    revenue = float(acc["revenue"])
    return {
        "spend": spend,
        "purchases": purchases,
        "checkouts": checkouts,
        "clicks": clicks,
        "revenue": revenue,
        "cpa": (spend / purchases) if purchases > 0 else 0.0,
        "cost_per_checkout": (spend / checkouts) if checkouts > 0 else 0.0,
        "cpc": (spend / clicks) if clicks > 0 else 0.0,
        "roas": (revenue / spend) if spend > 0 else 0.0,
    }


def get_burdenzero(ymd: Optional[str] = None) -> dict:
    ymd = (ymd or "").strip() or meta.ymd_today_kst()
    token = meta.must_env("META_BURDENZERO_ACCESS_TOKEN")
    ad_account = meta.must_env("META_BURDENZERO_AD_ACCOUNT_ID")
    rows = fetch_insights_extended(token, ad_account, ymd)

    acc = _empty_metrics()
    for row in rows:
        ds = (row.get("date_start") or "").strip()
        if ds and ds != ymd:
            continue
        _add_row(acc, row)

    return {"date": ymd, "metrics": _finalize(acc)}


def get_brainology(ymd: Optional[str] = None) -> dict:
    ymd = (ymd or "").strip() or meta.ymd_today_kst()
    token = meta.must_env("META_BRAINOLOGY_ACCESS_TOKEN")
    ad_account = meta.must_env("META_BRAINOLOGY_AD_ACCOUNT_ID")
    rows = fetch_insights_extended(token, ad_account, ymd)

    products = meta.load_brainology_products()
    kw_map = meta.product_keywords_map()
    per_product = {name: _empty_metrics() for name in products}
    total = _empty_metrics()

    for row in rows:
        ds = (row.get("date_start") or "").strip()
        if ds and ds != ymd:
            continue
        campaign_name = (row.get("campaign_name") or "").strip()
        if not campaign_name:
            continue
        for product, keywords in kw_map.items():
            if meta.match_campaign_to_product(campaign_name, product, keywords):
                _add_row(per_product[product], row)
                _add_row(total, row)
                break

    return {
        "date": ymd,
        "products": {name: _finalize(acc) for name, acc in per_product.items()},
        "total": _finalize(total),
    }


# =========================
# 예산 조회 (일일예산 합계)
# =========================
# 메타는 예산을 통화의 최소단위로 반환한다. 소수점 없는 통화(KRW 등)는 그대로,
# 2자리 통화(USD 등)는 cents라 100으로 나눠야 한다.
ZERO_DECIMAL_CURRENCIES = {
    "KRW", "JPY", "VND", "CLP", "ISK", "HUF", "TWD",
    "KMF", "XAF", "XOF", "XPF", "PYG", "RWF", "UGX", "VUV", "BIF", "DJF", "GNF",
}


def _budget_to_currency(minor_units: int, currency: str) -> float:
    if (currency or "").upper() in ZERO_DECIMAL_CURRENCIES:
        return float(minor_units)
    return minor_units / 100.0


def _get_all(url: str, params: dict, label: str) -> List[dict]:
    """paging.next 를 따라가며 edge 데이터를 모두 모은다."""
    out: List[dict] = []
    next_url = url
    next_params = params
    while next_url:
        r = requests.get(next_url, params=next_params, timeout=meta.TIMEOUT)
        data = meta.safe_json(r)
        if r.status_code != 200:
            err = (data or {}).get("error") if isinstance(data, dict) else None
            raise RuntimeError(
                f"[{label} FAIL] HTTP {r.status_code} "
                f"error_message: {(err or {}).get('message') if isinstance(err, dict) else None} "
                f"body: {data or r.text[:300]}"
            )
        out.extend((data or {}).get("data") or [])
        next_url = ((data or {}).get("paging") or {}).get("next")
        next_params = None  # next URL already carries the full querystring
    return out


def _fetch_account_currency(token: str, act_id: str) -> str:
    url = f"{meta.GRAPH_BASE}/{act_id}"
    r = requests.get(url, params={"access_token": token, "fields": "currency"}, timeout=meta.TIMEOUT)
    data = meta.safe_json(r) or {}
    return (data.get("currency") or "KRW").upper()


def _sum_active_daily_budget(edge_rows: List[dict]) -> tuple:
    """effective_status == ACTIVE 이고 daily_budget 이 있는 항목만 합산. (합계minor, 개수)"""
    total = 0
    count = 0
    for row in edge_rows:
        if (row.get("effective_status") or "").upper() != "ACTIVE":
            continue
        db = row.get("daily_budget")
        if db in (None, "", "0"):
            continue
        try:
            total += int(float(db))
            count += 1
        except Exception:
            pass
    return total, count


def get_daily_budget(token_env: str, account_env: str) -> dict:
    token = meta.must_env(token_env)
    act_id = meta.normalize_act_id(meta.must_env(account_env))
    currency = _fetch_account_currency(token, act_id)

    campaigns = _get_all(
        f"{meta.GRAPH_BASE}/{act_id}/campaigns",
        {"access_token": token, "fields": "daily_budget,effective_status", "limit": 500},
        "campaigns",
    )
    adsets = _get_all(
        f"{meta.GRAPH_BASE}/{act_id}/adsets",
        {"access_token": token, "fields": "daily_budget,effective_status", "limit": 500},
        "adsets",
    )

    camp_minor, camp_count = _sum_active_daily_budget(campaigns)
    adset_minor, adset_count = _sum_active_daily_budget(adsets)

    camp_budget = _budget_to_currency(camp_minor, currency)
    adset_budget = _budget_to_currency(adset_minor, currency)
    return {
        "currency": currency,
        "campaign_budget": camp_budget,
        "campaign_count": camp_count,
        "adset_budget": adset_budget,
        "adset_count": adset_count,
        "total": camp_budget + adset_budget,
    }


def get_burdenzero_budget() -> dict:
    return get_daily_budget("META_BURDENZERO_ACCESS_TOKEN", "META_BURDENZERO_AD_ACCOUNT_ID")


def _classify_product(campaign_name: str, kw_map: Dict[str, List[str]]) -> Optional[str]:
    for product, keywords in kw_map.items():
        if meta.match_campaign_to_product(campaign_name, product, keywords):
            return product
    return None


def _budget_block_dict(camp_minor: int, camp_count: int, adset_minor: int, adset_count: int, currency: str) -> dict:
    cb = _budget_to_currency(camp_minor, currency)
    ab = _budget_to_currency(adset_minor, currency)
    return {
        "campaign_budget": cb,
        "campaign_count": camp_count,
        "adset_budget": ab,
        "adset_count": adset_count,
        "total": cb + ab,
    }


def get_brainology_budget() -> dict:
    """브레인올로지 일일예산을 제품별로 분리 집계. 캠페인은 캠페인명으로,
    광고세트는 상위 캠페인명으로 제품을 분류한다 (대시보드 분류 기준과 동일)."""
    token = meta.must_env("META_BRAINOLOGY_ACCESS_TOKEN")
    act_id = meta.normalize_act_id(meta.must_env("META_BRAINOLOGY_AD_ACCOUNT_ID"))
    currency = _fetch_account_currency(token, act_id)

    products = meta.load_brainology_products()
    kw_map = meta.product_keywords_map()
    # 누적용 minor 합계
    acc = {name: {"cm": 0, "cc": 0, "am": 0, "ac": 0} for name in products}

    campaigns = _get_all(
        f"{meta.GRAPH_BASE}/{act_id}/campaigns",
        {"access_token": token, "fields": "name,daily_budget,effective_status", "limit": 500},
        "campaigns",
    )
    for c in campaigns:
        if (c.get("effective_status") or "").upper() != "ACTIVE":
            continue
        db = c.get("daily_budget")
        if db in (None, "", "0"):
            continue
        product = _classify_product((c.get("name") or "").strip(), kw_map)
        if not product:
            continue
        try:
            acc[product]["cm"] += int(float(db))
            acc[product]["cc"] += 1
        except Exception:
            pass

    adsets = _get_all(
        f"{meta.GRAPH_BASE}/{act_id}/adsets",
        {"access_token": token, "fields": "daily_budget,effective_status,campaign{name}", "limit": 500},
        "adsets",
    )
    for a in adsets:
        if (a.get("effective_status") or "").upper() != "ACTIVE":
            continue
        db = a.get("daily_budget")
        if db in (None, "", "0"):
            continue
        cname = ((a.get("campaign") or {}).get("name") or "").strip()
        product = _classify_product(cname, kw_map)
        if not product:
            continue
        try:
            acc[product]["am"] += int(float(db))
            acc[product]["ac"] += 1
        except Exception:
            pass

    per_product = {
        name: _budget_block_dict(v["cm"], v["cc"], v["am"], v["ac"], currency)
        for name, v in acc.items()
    }
    tcm = sum(v["cm"] for v in acc.values())
    tcc = sum(v["cc"] for v in acc.values())
    tam = sum(v["am"] for v in acc.values())
    tac = sum(v["ac"] for v in acc.values())
    return {
        "currency": currency,
        "products": per_product,
        "total": _budget_block_dict(tcm, tcc, tam, tac, currency),
    }
