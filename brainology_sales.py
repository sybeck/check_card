"""
브레인올로지 제품별 '지금 매출' 집계 모듈 (Slack '브올매출' 명령용).

../time_check2 의 매출/메타 커넥터를 그대로 subprocess 로 호출해 결과 JSON 을 모으고,
제품별로 카페24 + 쿠팡 + 네이버 매출을 합산한 뒤 메타 광고비로 ROAS/CPA 를 계산한다.
집계 정의는 time_check2/run_current_to_gsheet.py 와 동일하게 맞춘다.

카페24/쿠팡은 Playwright 로 브라우저를 띄우므로 수십 초~수 분이 걸릴 수 있다.
"""
import os
import sys
import json
import subprocess
from typing import Any, Dict, List

# time_check2 는 check_card 의 형제 디렉토리(배포 머신마다 절대경로가 다르므로 상대로 해석).
#   - 개발: C:\Users\bsysy\Desktop\bsy-auto\time_check2
#   - 서버: C:\Users\Administrator\Desktop\bsy\time_check2
# 필요시 TIME_CHECK2_DIR 환경변수로 덮어쓸 수 있다.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TIME_CHECK2_DIR = os.getenv("TIME_CHECK2_DIR", "").strip() or os.path.abspath(
    os.path.join(_THIS_DIR, "..", "time_check2")
)

# 커넥터 스크립트 (time_check2 기준 상대경로)와 인자
CAFE24 = ("connectors/sales/cafe24_current.py", ["--all", "--json"])
COUPANG = ("connectors/sales/coupang_current.py", ["--json"])
NAVER = ("connectors/sales/naver_current.py", ["--json"])
META = ("connectors/meta/meta_ads_current.py", ["--json"])

SAFE_TEMP_DIR = r"C:\Temp"


def _run_script_json(rel_path: str, args: List[str]) -> Dict[str, Any]:
    """time_check2 커넥터를 subprocess 로 실행하고 마지막 stdout 줄을 JSON 으로 파싱.

    run_current_to_gsheet.py 의 run_script_json 과 동일한 규약(stderr 는 디버그,
    stdout 마지막 줄이 결과 JSON)을 따른다.
    """
    os.makedirs(SAFE_TEMP_DIR, exist_ok=True)
    env = os.environ.copy()
    env["TEMP"] = SAFE_TEMP_DIR
    env["TMP"] = SAFE_TEMP_DIR
    env["TMPDIR"] = SAFE_TEMP_DIR
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    p = subprocess.run(
        [sys.executable, rel_path] + args,
        cwd=TIME_CHECK2_DIR,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )
    if (p.stderr or "").strip():
        sys.stderr.write(f"[{rel_path} stderr]\n{p.stderr}\n")
        sys.stderr.flush()
    if p.returncode != 0:
        raise RuntimeError(f"[SCRIPT FAIL] {rel_path}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}\n")
    lines = [ln.strip() for ln in (p.stdout or "").splitlines() if ln.strip()]
    if not lines:
        raise RuntimeError(f"[SCRIPT NO OUTPUT] {rel_path}")
    return json.loads(lines[-1])


def _mapped(res: Dict[str, Any], key: str) -> Dict[str, Any]:
    return (res.get("mapped") or {}).get(key) or {}


def _metrics_for(target: str, cafe24, coupang, naver, meta) -> dict:
    """run_current_to_gsheet.compute_roas_cpa_for_brand 와 동일한 정의."""
    m = _mapped(meta, target)
    c = _mapped(cafe24, target)
    cp = _mapped(coupang, target)
    nv = _mapped(naver, target)
    spend = float(m.get("spend") or 0.0)
    revenue = int(c.get("sales") or 0) + int(cp.get("sales") or 0) + int(nv.get("sales") or 0)
    purchases = int(c.get("orders") or 0) + int(cp.get("orders") or 0) + int(nv.get("orders") or 0)
    return {
        "spend": spend,
        "revenue": revenue,
        "purchases": purchases,
        "roas": (revenue / spend) if spend > 0 else 0.0,
        "cpa": (spend / purchases) if purchases > 0 else 0.0,
    }


def _product_names(meta: Dict[str, Any]) -> List[str]:
    """메타 mapped 키에서 burdenzero(부담제로)를 뺀 브레인올로지 제품 목록.

    split_brainology_by_product 가 BRAINOLOGY_PRODUCT_NAMES 순서대로 채우므로 순서가 보존된다.
    """
    return [k for k in (meta.get("mapped") or {}).keys() if k != "burdenzero"]


def get_brainology_sales() -> dict:
    """브레인올로지 제품별 + 합계 매출/광고비/ROAS/CPA 를 집계해 반환.

    반환:
      {
        "date": "YYYY-MM-DD",
        "coupang_failed": bool,
        "products": {제품명: {spend, revenue, purchases, roas, cpa}, ...},
        "total": {spend, revenue, purchases, roas, cpa},
      }
    """
    cafe24_res = _run_script_json(*CAFE24)

    coupang_failed = False
    try:
        coupang_res = _run_script_json(*COUPANG)
        if coupang_res.get("error"):
            coupang_failed = True
    except Exception as e:
        sys.stderr.write(f"[COUPANG FAIL] {e}\n")
        coupang_res = {"mapped": {}}
        coupang_failed = True

    naver_res = _run_script_json(*NAVER)
    meta_res = _run_script_json(*META)

    products = _product_names(meta_res)
    per_product = {
        name: _metrics_for(name, cafe24_res, coupang_res, naver_res, meta_res)
        for name in products
    }

    t_spend = sum(p["spend"] for p in per_product.values())
    t_revenue = sum(p["revenue"] for p in per_product.values())
    t_purchases = sum(p["purchases"] for p in per_product.values())
    total = {
        "spend": t_spend,
        "revenue": t_revenue,
        "purchases": t_purchases,
        "roas": (t_revenue / t_spend) if t_spend > 0 else 0.0,
        "cpa": (t_spend / t_purchases) if t_purchases > 0 else 0.0,
    }

    return {
        "date": meta_res.get("date") or cafe24_res.get("date"),
        "coupang_failed": coupang_failed,
        "products": per_product,
        "total": total,
    }
