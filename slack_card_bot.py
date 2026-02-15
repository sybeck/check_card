import os
import re
import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import pandas as pd
import requests
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# =========================
# Load .env
# =========================
load_dotenv()

# =========================
# Config
# =========================
KST = timezone(timedelta(hours=9))

CARDS_KEEP = {"0858", "2866", "2493", "3799", "0811", "0620", "7945", "3054", "4298", "9733"}
CARD_LIMIT = 250_000  # 카드별 월 한도

COL_DT = "이용일시"
COL_APPR = "승인번호"
COL_CARD = "이용카드"
COL_AMT = "이용금액"

DEFAULT_INPUT_NAME = "thisone.xls"

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN", "").strip()

if not SLACK_BOT_TOKEN or not SLACK_APP_TOKEN:
    raise RuntimeError("환경변수 SLACK_BOT_TOKEN(xoxb-...), SLACK_APP_TOKEN(xapp-...)가 필요합니다. (.env 확인)")


# =========================
# Helpers (기존 로직 유지)
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)


def month_key(dt: datetime) -> str:
    return dt.strftime("%Y%m")


def normalize_last4(v) -> str:
    if pd.isna(v):
        return ""
    digits = re.sub(r"\D", "", str(v))
    return digits[-4:] if len(digits) >= 4 else ""


def normalize_approval(v) -> str:
    if pd.isna(v):
        return ""
    return re.sub(r"\s+", "", str(v).strip())


def normalize_amount(v) -> int:
    if pd.isna(v):
        return 0
    if isinstance(v, (int, float)):
        return int(round(v))
    s = str(v).replace(",", "").replace("원", "").strip()
    try:
        return int(round(float(s)))
    except Exception:
        digits = re.sub(r"[^\d\-]", "", s)
        return int(digits) if digits else 0


def parse_dt_kst(v):
    if pd.isna(v):
        return None
    if isinstance(v, datetime):
        return v.replace(tzinfo=KST) if v.tzinfo is None else v.astimezone(KST)

    dt = pd.to_datetime(v, errors="coerce")
    if pd.isna(dt):
        return None
    dt = dt.to_pydatetime()
    return dt.replace(tzinfo=KST) if dt.tzinfo is None else dt.astimezone(KST)


def load_existing_approvals(txt_path: str) -> set[str]:
    approvals = set()
    if not os.path.exists(txt_path):
        return approvals

    with open(txt_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                approvals.add(parts[1])
    return approvals


def append_rows(txt_path: str, yyyymm: str, new_rows):
    is_new = not os.path.exists(txt_path)
    mode = "w" if is_new else "a"

    with open(txt_path, mode, encoding="utf-8") as f:
        if is_new:
            f.write(f"# Corporate card approvals - {yyyymm}\n")
            f.write("# Columns: used_at_kst\tapproval_no\tcard_last4\tamount_krw\n")

        for dt, approval, last4, amt in sorted(new_rows, key=lambda x: (x[0], x[1])):
            f.write(f"{dt.strftime('%Y-%m-%d %H:%M:%S')}\t{approval}\t{last4}\t{amt}\n")

    return len(new_rows)


def read_excel_auto(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xls":
        return pd.read_excel(path, engine="xlrd")
    elif ext == ".xlsx":
        return pd.read_excel(path, engine="openpyxl")
    else:
        raise RuntimeError(f"지원하지 않는 확장자: {ext}")


def script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def safe_get_column(df: pd.DataFrame, preferred: str, fallback_index: int):
    if preferred in df.columns:
        return preferred
    if len(df.columns) > fallback_index:
        return df.columns[fallback_index]
    raise RuntimeError(f"필수 컬럼 없음: {preferred}")


def totals_from_txt(txt_path: str):
    by_approval = {}

    if not os.path.exists(txt_path):
        return [], 0

    with open(txt_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                continue

            approval = parts[1].strip()
            last4 = parts[2].strip()
            try:
                amt = int(parts[3].strip())
            except Exception:
                amt = normalize_amount(parts[3])

            by_approval.setdefault(approval, (last4, amt))

    totals = defaultdict(int)
    for last4, amt in by_approval.values():
        totals[last4] += amt

    used_cards = sorted((k, v) for k, v in totals.items() if v != 0)
    total_sum = sum(v for _, v in used_cards)
    return used_cards, total_sum


def run_pipeline(excel_path: str):
    """
    엑셀 읽기 -> 이번달 txt 업데이트 -> (업데이트된 txt 기준) 카드별 합계 리턴
    """
    outdir = script_dir()
    kst_now = now_kst()
    yyyymm = month_key(kst_now)
    out_txt = os.path.join(outdir, f"corp_cards_{yyyymm}.txt")

    df = read_excel_auto(excel_path)

    col_dt = safe_get_column(df, COL_DT, 0)
    col_appr = safe_get_column(df, COL_APPR, 1)
    col_card = safe_get_column(df, COL_CARD, 2)
    col_amt = safe_get_column(df, COL_AMT, 6)

    existing = load_existing_approvals(out_txt)

    rows = []
    for _, r in df.iterrows():
        dt = parse_dt_kst(r.get(col_dt))
        if not dt:
            continue
        if dt.year != kst_now.year or dt.month != kst_now.month:
            continue

        approval = normalize_approval(r.get(col_appr))
        last4 = normalize_last4(r.get(col_card))
        amt = normalize_amount(r.get(col_amt))

        if not approval:
            continue
        if last4 not in CARDS_KEEP:
            continue

        rows.append((dt, approval, last4, amt))

    new_rows = [x for x in rows if x[1] not in existing]
    added = append_rows(out_txt, yyyymm, new_rows)

    used_cards, total_sum = totals_from_txt(out_txt)
    return out_txt, added, used_cards, total_sum


def build_result_text(used_cards) -> str:
    lines = []
    lines.append("현재까지 카드별로 사용된 금액입니다. 한도는 25만원입니다.\n")
    for last4, amt in used_cards:
        remain = CARD_LIMIT - amt
        if remain < 0:
            remain = 0
        lines.append(f"- {last4} 카드: {amt:,}원 사용 / {remain:,}원 남음")
    return "\n".join(lines)


# =========================
# Slack: dedupe (중복 이벤트 방지)
# =========================
DEDUP_FILE = os.path.join(script_dir(), ".slack_dedup.json")


def load_dedup():
    if not os.path.exists(DEDUP_FILE):
        return {}
    try:
        with open(DEDUP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_dedup(d):
    try:
        with open(DEDUP_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def dedup_seen(key: str, ttl_sec: int = 3600) -> bool:
    d = load_dedup()
    now = int(time.time())

    for k, ts in list(d.items()):
        if now - int(ts) > ttl_sec:
            d.pop(k, None)

    if key in d:
        return True

    d[key] = now
    save_dedup(d)
    return False


# =========================
# Slack App (Socket Mode)
# =========================
app = App(token=SLACK_BOT_TOKEN)


def download_slack_file(file_info: dict, save_path: str):
    url = file_info.get("url_private_download") or file_info.get("url_private")
    if not url:
        raise RuntimeError("파일 다운로드 URL(url_private_download/url_private)을 찾을 수 없습니다.")

    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    r = requests.get(url, headers=headers, stream=True, timeout=60)
    r.raise_for_status()

    with open(save_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)


@app.event("message")
def on_message_events(body, logger):
    event = body.get("event", {})
    channel = event.get("channel")
    ts = event.get("ts")  # thread_ts
    files = event.get("files", [])

    if not channel or not ts or not files:
        return

    if dedup_seen(f"{channel}:{ts}"):
        return

    # 엑셀 파일을 모두 순차 처리
    excel_file_ids = []
    for f in files:
        name = (f.get("name") or "").lower()
        if name.endswith(".xls") or name.endswith(".xlsx"):
            if f.get("id"):
                excel_file_ids.append(f["id"])

    if not excel_file_ids:
        return

    try:
        # ✅ 변경: 중간 메시지 없이 "최종(마지막 처리 후) 결과"만 전송
        out_txt_latest = None
        added_total = 0
        final_used_cards = None

        for idx, file_id in enumerate(excel_file_ids, start=1):
            finfo = app.client.files_info(file=file_id).get("file", {})

            fname = finfo.get("name") or DEFAULT_INPUT_NAME
            ext = os.path.splitext(fname)[1].lower()

            local_name = f"thisone_{idx}{ext if ext in ('.xls', '.xlsx') else '.xls'}"
            save_path = os.path.join(script_dir(), local_name)

            download_slack_file(finfo, save_path)

            out_txt, added, used_cards, _total_sum = run_pipeline(save_path)
            out_txt_latest = out_txt
            added_total += added
            final_used_cards = used_cards  # 마지막 파일 처리 결과로 갱신

        # 마지막 결과만 출력
        final_text = "엑셀 처리 완료 ✅\n"
        if out_txt_latest:
            final_text += f"TXT: {out_txt_latest}\n\n"
        if final_used_cards is not None:
            final_text += build_result_text(final_used_cards)
        else:
            final_text += "처리할 엑셀 데이터가 없습니다."

        # (선택) 총 추가 건수 표시하고 싶으면 아래 한 줄만 주석 해제
        # final_text = final_text.replace("엑셀 처리 완료 ✅", f"엑셀 처리 완료 ✅ (추가 {added_total}건)")

        app.client.chat_postMessage(
            channel=channel,
            thread_ts=ts,
            text=final_text,
        )

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        try:
            app.client.chat_postMessage(
                channel=channel,
                thread_ts=ts,
                text=f"처리 실패 ❌\n{type(e).__name__}: {e}",
            )
        except Exception as ee:
            print(f"[ERROR] failed to report to slack: {type(ee).__name__}: {ee}")
            logger.exception("Failed to report error to Slack")
        logger.exception("Failed to process message event")


if __name__ == "__main__":
    print("[INFO] slack_card_bot starting (socket mode)...")
    SocketModeHandler(app, SLACK_APP_TOKEN).start()
